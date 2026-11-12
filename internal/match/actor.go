package match

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
)

// ErrStopped is returned when talking to an actor whose goroutine has exited.
var ErrStopped = errors.New("match: actor stopped")

// Actor runs one Match in its own goroutine. The goroutine is the only code that ever
// touches the Match; everything else sends it closures over cmds. That is the whole
// concurrency story: no mutexes around game state, and simultaneous inputs from both
// players are simply serialised in arrival order.
type Actor struct {
	id       string
	m        *Match
	mgr      *Manager
	log      *slog.Logger
	cmds     chan func(now time.Time)
	stop     chan struct{} // closed by Stop
	stopOnce sync.Once
	done     chan struct{} // closed when the goroutine exits
}

func newActor(m *Match, mgr *Manager) *Actor {
	return &Actor{
		id: m.ID, m: m, mgr: mgr,
		log:  mgr.log.With("match", m.ID),
		cmds: make(chan func(time.Time), 256),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// ID returns the match id.
func (a *Actor) ID() string { return a.id }

// Done is closed when the actor has exited.
func (a *Actor) Done() <-chan struct{} { return a.done }

func (a *Actor) run() {
	defer close(a.done)
	clk, cfg := a.mgr.clock, a.mgr.opts
	ticker := clk.NewTicker(cfg.Match.TickInterval)
	defer ticker.Stop()
	snapEvery := max(1, int(cfg.SnapshotEvery/cfg.Match.TickInterval))
	ticks := 0
	a.persist(clk.Now(), false) // a brand-new match is recoverable right away

	for {
		select {
		case <-a.stop:
			if a.m.Phase != MatchEnd { // a finished match has nothing to resume
				a.persist(clk.Now(), true)
			}
			a.m.CloseSinks("server shutting down")
			return
		case f := <-a.cmds:
			f(clk.Now())
		case <-ticker.C():
			start := time.Now() // real time: this measures CPU cost, not game time
			now := clk.Now()
			before := a.m.Phase
			a.m.Advance(now)
			a.m.Broadcast(now)
			a.mgr.metrics.TickDuration.Observe(time.Since(start).Seconds())
			ticks++

			if a.m.Phase != before {
				a.log.Info("phase", "from", before.String(), "to", a.m.Phase.String(), "round", a.m.Round)
			}
			if a.m.Phase == MatchEnd {
				if before != MatchEnd {
					a.mgr.matchFinished(a.m)
				}
				if now.Sub(a.m.EndedAt) >= cfg.Linger {
					a.m.CloseSinks("match over")
					return
				}
				continue
			}
			if a.m.Phase != before || ticks%snapEvery == 0 {
				a.persist(now, false)
			}
		}
	}
}

func (a *Actor) persist(now time.Time, sync bool) {
	data, err := a.m.Snapshot(now)
	if err != nil {
		a.log.Error("snapshot encode failed", "err", err)
		return
	}
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
// actor is backed up the input is dropped; the client sees it un-acked and may resend
// with the same seq, which is safe because inputs are idempotent.
func (a *Actor) Input(seat int, seq uint64, act game.Action) {
	f := func(now time.Time) {
		r := a.m.Input(seat, seq, act, now)
		a.mgr.metrics.Inputs.WithLabelValues(r.String()).Inc()
	}
	select {
	case a.cmds <- f:
	default:
		a.mgr.metrics.Inputs.WithLabelValues("overflow").Inc()
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

// Stop asks the actor to snapshot and exit, and waits for it.
func (a *Actor) Stop(ctx context.Context) error {
	a.stopOnce.Do(func() { close(a.stop) })
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
