package match

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

// ErrStopped is returned when talking to an actor whose goroutine has exited.
var ErrStopped = errors.New("match: actor stopped")

// inputQueueSize bounds the inputs buffered per seat. Each seat has its own queue, so a
// player who floods the server only ever drops their own inputs, never the opponent's.
const inputQueueSize = 128

// forceSnapshotEvery: a match whose state did not change is still re-snapshotted every
// this many periodic intervals, so its TTL and SavedAt stay fresh.
const forceSnapshotEvery = 10

type inputCmd struct {
	seq uint64
	act game.Action
}

// Actor runs one Match in its own goroutine. The goroutine is the only code that ever
// touches the Match; everything else sends it closures over cmds (or inputs over the
// per-seat queues). That is the whole concurrency story: no mutexes around game state,
// and simultaneous inputs from both players are simply serialised in arrival order.
type Actor struct {
	id     string
	m      *Match
	mgr    *Manager
	log    *slog.Logger
	cmds   chan func(now time.Time)
	inputs [2]chan inputCmd

	stop        chan struct{} // closed by Stop: snapshot and exit
	stopOnce    sync.Once
	evicted     chan struct{} // closed when another node owns the match: exit, don't persist
	evictOnce   sync.Once
	done        chan struct{} // closed when the goroutine exits
	lastPhase   Phase
	snapshotRev uint64 // Match.Rev at the last snapshot
	finished    bool   // matchFinished ran (only touched by the actor goroutine)
	deleted     <-chan struct{}
}

func newActor(m *Match, mgr *Manager) *Actor {
	return &Actor{
		id: m.ID, m: m, mgr: mgr,
		log:     mgr.log.With("match", m.ID),
		cmds:    make(chan func(time.Time), 64),
		inputs:  [2]chan inputCmd{make(chan inputCmd, inputQueueSize), make(chan inputCmd, inputQueueSize)},
		stop:    make(chan struct{}),
		evicted: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// ID returns the match id.
func (a *Actor) ID() string { return a.id }

// Done is closed when the actor has exited.
func (a *Actor) Done() <-chan struct{} { return a.done }

func (a *Actor) run() {
	defer close(a.done)
	// A bug in one match must not take down the process and every other match on it.
	defer func() {
		if r := recover(); r != nil {
			a.crashed(r)
		}
	}()
	clk, cfg := a.mgr.clock, a.mgr.opts
	ticker := clk.NewTicker(cfg.Match.TickInterval)
	defer ticker.Stop()
	snapEvery := max(1, int(cfg.SnapshotEvery/cfg.Match.TickInterval))
	ticks := 0
	a.lastPhase = a.m.Phase
	a.persist(clk.Now(), false) // a brand-new match is recoverable right away

	for {
		select {
		case <-a.stop:
			if a.m.Phase != MatchEnd { // a finished match has nothing to resume
				a.persist(clk.Now(), true)
			}
			a.m.CloseSinks("server shutting down")
			return
		case <-a.evicted:
			a.log.Warn("match is owned by another node; stopping here without persisting")
			a.m.CloseSinks("match moved to another server")
			return
		case f := <-a.cmds:
			now := clk.Now()
			a.drainInputs(now) // inputs sent before a command (e.g. Detach) apply before it
			f(now)
			a.observe(now)
		case in := <-a.inputs[0]:
			a.input(0, in, clk.Now())
		case in := <-a.inputs[1]:
			a.input(1, in, clk.Now())
		case <-ticker.C():
			start := time.Now() // real time: this measures CPU cost, not game time
			now := clk.Now()
			a.m.Advance(now)
			a.m.Broadcast(now)
			a.mgr.metrics.ObserveTick(time.Since(start))
			ticks++
			a.observe(now)

			if a.m.Phase == MatchEnd {
				if now.Sub(a.m.EndedAt) >= cfg.Linger {
					a.m.CloseSinks("match over")
					a.awaitDelete() // the match is over: waiting here costs no gameplay
					return
				}
				continue
			}
			if ticks%snapEvery == 0 && (a.m.Rev() != a.snapshotRev || ticks%(snapEvery*forceSnapshotEvery) == 0) {
				a.persist(now, false)
			}
		}
	}
}

func (a *Actor) awaitDelete() {
	if a.deleted == nil {
		return
	}
	t := time.NewTimer(2 * a.mgr.opts.StoreTimeout)
	defer t.Stop()
	select {
	case <-a.deleted:
	case <-t.C:
	}
}

// observe reacts to phase changes after every step (tick, command or input): a top-out
// inside Input ends the round without waiting for the next tick to notice.
func (a *Actor) observe(now time.Time) {
	p := a.m.Phase
	if p == a.lastPhase {
		return
	}
	a.log.Info("phase", "from", a.lastPhase.String(), "to", p.String(), "round", a.m.Round)
	a.lastPhase = p
	if p == MatchEnd {
		a.finished = true
		a.deleted = a.mgr.matchFinished(a.m)
		return
	}
	a.persist(now, false)
}

func (a *Actor) drainInputs(now time.Time) {
	for {
		select {
		case in := <-a.inputs[0]:
			a.input(0, in, now)
		case in := <-a.inputs[1]:
			a.input(1, in, now)
		default:
			return
		}
	}
}

func (a *Actor) input(seat int, in inputCmd, now time.Time) {
	r := a.m.Input(seat, in.seq, in.act, now)
	a.mgr.inputCounters[r].Inc()
	a.observe(now)
}

// crashed runs on the actor goroutine after a panic. The match state may be corrupt, so it
// is not snapshotted again; the stored snapshot is deleted so a restart does not restore
// (and re-crash on) the same state.
func (a *Actor) crashed(r any) {
	a.mgr.metrics.ActorPanics.Inc()
	a.log.Error("match actor panicked; match aborted", "panic", r, "stack", string(debug.Stack()))
	func() {
		defer func() { _ = recover() }()
		over := protocol.EventMsg{Type: protocol.TypeEvent, Event: protocol.Event{
			Seq: a.m.EventSeq + 1, AtMs: a.mgr.clock.Now().UnixMilli(), Kind: protocol.EvMatchOver,
			Winner: intp(NoSeat), Reason: "internal_error",
		}}
		for _, s := range a.m.Seats {
			if s != nil && s.sink != nil {
				s.sink.Send(over)
			}
		}
		a.m.CloseSinks("internal error")
	}()
	if !a.finished {
		a.finished = true
		a.mgr.matchAborted(a.id)
	}
}

func (a *Actor) persist(now time.Time, sync bool) {
	data, err := a.m.Snapshot(now)
	if err != nil {
		a.log.Error("snapshot encode failed", "err", err)
		return
	}
	a.snapshotRev = a.m.Rev()
	a.mgr.metrics.SnapshotBytes.Observe(float64(len(data)))
	a.mgr.save(a.id, data, sync)
}

// send queues f for the actor goroutine, blocking until it is accepted.
func (a *Actor) send(ctx context.Context, f func(time.Time)) error {
	select {
	case a.cmds <- f:
		return nil
	case <-a.done:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Attach connects a client to its seat. See Match.Attach.
func (a *Actor) Attach(ctx context.Context, token string, sink Sink, lastEventSeq uint64) (int, error) {
	type result struct {
		seat int
		err  error
	}
	reply := make(chan result, 1)
	if err := a.send(ctx, func(now time.Time) {
		seat, err := a.m.Attach(token, sink, lastEventSeq, now)
		reply <- result{seat, err}
	}); err != nil {
		return NoSeat, err
	}
	select {
	case r := <-reply:
		return r.seat, r.err
	case <-a.done:
		return NoSeat, ErrStopped
	case <-ctx.Done():
		return NoSeat, ctx.Err()
	}
}

// Detach reports that a client connection closed.
func (a *Actor) Detach(seat int, sink Sink) {
	_ = a.send(context.Background(), func(now time.Time) { a.m.Detach(seat, sink, now) })
}

// Input queues a player action without blocking the caller (the socket reader). If the
// seat's queue is full the input is dropped; the client sees it un-acked and may resend
// with the same seq, which is safe because inputs are idempotent.
func (a *Actor) Input(seat int, seq uint64, act game.Action) {
	if seat < 0 || seat > 1 {
		return
	}
	select {
	case a.inputs[seat] <- inputCmd{seq, act}:
	default:
		a.mgr.overflowCounter.Inc()
	}
}

// Inspect runs f on the actor goroutine and waits for it. Intended for tests and
// diagnostics; f must not retain m.
func (a *Actor) Inspect(ctx context.Context, f func(m *Match)) error {
	done := make(chan struct{})
	if err := a.send(ctx, func(time.Time) { f(a.m); close(done) }); err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-a.done:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// requestStop asks the actor to snapshot and exit, without waiting.
func (a *Actor) requestStop() { a.stopOnce.Do(func() { close(a.stop) }) }

// evict makes the actor exit without persisting (another node owns the match).
func (a *Actor) evict() { a.evictOnce.Do(func() { close(a.evicted) }) }

// Stop asks the actor to snapshot and exit, and waits for it.
func (a *Actor) Stop(ctx context.Context) error {
	a.requestStop()
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
