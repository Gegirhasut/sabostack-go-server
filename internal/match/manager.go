package match

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// Options configures a Manager.
type Options struct {
	Match          Config
	SnapshotEvery  time.Duration // periodic snapshot interval while a match is live
	Linger         time.Duration // how long a finished match keeps answering resumes
	StoreTimeout   time.Duration // per store round trip (one batch)
	PersistWorkers int           // parallel store writers; a match always maps to the same one
	PersistBatch   int           // max writes sent to the store in one round trip
}

// DefaultOptions returns production-ish defaults.
func DefaultOptions() Options {
	return Options{
		Match: DefaultConfig(), SnapshotEvery: time.Second, Linger: 10 * time.Second,
		StoreTimeout: 2 * time.Second, PersistWorkers: 4, PersistBatch: 64,
	}
}

// Circuit breaker for the store: after breakerFailures failed batches in a row, writes are
// skipped for breakerCooldown instead of each one waiting out StoreTimeout. Match loops
// never wait for the store anyway; this keeps the queues short and shutdown fast while
// the store is down.
const (
	breakerFailures = 5
	breakerCooldown = 5 * time.Second
)

// Manager owns all match actors in the process and the goroutines that write snapshots
// to the store.
type Manager struct {
	opts    Options
	clock   clock.Clock
	store   store.Store
	metrics *metrics.Metrics
	log     *slog.Logger

	mu     sync.Mutex
	actors map[string]*Actor
	closed bool
	wg     sync.WaitGroup // actor goroutines
	live   atomic.Int64   // matches not yet finished

	shards    []*persistShard
	persistWG sync.WaitGroup

	breakerMu    sync.Mutex
	failStreak   int
	breakerUntil time.Time

	inputCounters   [4]prometheus.Counter // by InputResult
	overflowCounter prometheus.Counter
}

// persistShard is one store writer. All writes for a match go to the same shard, in
// order, so a late periodic snapshot can never overwrite a final one or resurrect a
// deleted match. Writes are coalesced per match: while a write is still pending, a newer
// one replaces it (the latest snapshot supersedes older ones), so the queue never holds
// more than one entry per match and nothing is dropped blindly when the store is slow.
type persistShard struct {
	mu      sync.Mutex
	pending map[string]*persistJob
	order   []string
	closed  bool
	wake    chan struct{}
}

type persistJob struct {
	id      string
	data    []byte // nil = delete
	waiters []chan struct{}
}

// NewManager starts the persistence goroutines. Call Shutdown to stop everything.
func NewManager(opts Options, clk clock.Clock, st store.Store, met *metrics.Metrics, log *slog.Logger) *Manager {
	opts.PersistWorkers = max(1, opts.PersistWorkers)
	opts.PersistBatch = max(1, opts.PersistBatch)
	if opts.StoreTimeout <= 0 {
		opts.StoreTimeout = 2 * time.Second
	}
	m := &Manager{
		opts: opts, clock: clk, store: st, metrics: met, log: log,
		actors:          map[string]*Actor{},
		overflowCounter: met.Input("overflow"),
	}
	for r := range m.inputCounters {
		m.inputCounters[r] = met.Input(InputResult(r).String())
	}
	for i := 0; i < opts.PersistWorkers; i++ {
		sh := &persistShard{pending: map[string]*persistJob{}, wake: make(chan struct{}, 1)}
		m.shards = append(m.shards, sh)
		m.persistWG.Add(1)
		go m.persistLoop(sh)
	}
	return m
}

// ErrShuttingDown is returned by Create once Shutdown has begun.
var ErrShuttingDown = errors.New("match: server shutting down")

// Create starts a new match for two queued players and returns its id and the per-seat
// session tokens.
func (m *Manager) Create(names [2]string) (string, [2]string, error) {
	id := randomID()
	mt, tokens := New(id, m.opts.Match, names, randomSeed(), m.clock.Now())
	if !m.start(mt) {
		return "", [2]string{}, ErrShuttingDown
	}
	m.log.Info("match created", "match", id, "p0", names[0], "p1", names[1])
	return id, tokens, nil
}

// Get returns the actor for a match id.
func (m *Manager) Get(id string) (*Actor, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.actors[id]
	return a, ok
}

// Len returns the number of running matches.
func (m *Manager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.actors)
}

// LiveCount returns the number of matches that have not finished yet (lingering finished
// matches excluded). A draining server waits for this to reach zero.
func (m *Manager) LiveCount() int { return int(m.live.Load()) }

// Store returns the snapshot store.
func (m *Manager) Store() store.Store { return m.store }

// Restore loads every snapshot from the store and resumes those matches that are not
// running. It is called at startup, before the server accepts connections, and again by
// a cluster node that got its lease back after stopping its matches.
func (m *Manager) Restore(ctx context.Context) (int, error) {
	snaps, err := m.store.LoadAll(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for id, data := range snaps {
		if _, running := m.Get(id); running {
			continue // Restore again after a lost cluster lease: only what is missing
		}
		mt, err := Restore(data, m.opts.Match, m.clock.Now())
		if err != nil || mt.Phase == MatchEnd {
			m.log.Warn("discarding snapshot", "match", id, "err", err)
			_ = m.store.Delete(ctx, id)
			continue
		}
		if !m.start(mt) {
			return n, ErrShuttingDown
		}
		n++
		m.log.Info("match restored", "match", id, "phase", mt.Phase.String(), "round", mt.Round)
	}
	return n, nil
}

// Adopt starts matches taken over from a dead node (see store.Cluster). It is Restore for
// snapshots another process wrote: deadlines shift by the time since that node's last
// snapshot, every seat starts disconnected and Countdown/Playing wait paused until the
// players resume here.
func (m *Manager) Adopt(snaps map[string][]byte) int {
	n := 0
	for id, data := range snaps {
		if _, running := m.Get(id); running {
			continue
		}
		mt, err := Restore(data, m.opts.Match, m.clock.Now())
		if err != nil || mt.Phase == MatchEnd {
			m.log.Warn("discarding adopted snapshot", "match", id, "err", err)
			m.enqueue(id, nil, false)
			continue
		}
		if !m.start(mt) {
			return n
		}
		n++
		m.metrics.MatchesAdopted.Inc()
		m.log.Info("match adopted", "match", id, "phase", mt.Phase.String(), "round", mt.Round)
	}
	return n
}

// EvictAll stops every local match without persisting it: this node lost its cluster
// lease and another node is about to adopt (or has adopted) its matches.
func (m *Manager) EvictAll() int {
	m.mu.Lock()
	actors := make([]*Actor, 0, len(m.actors))
	for _, a := range m.actors {
		actors = append(actors, a)
	}
	m.mu.Unlock()
	for _, a := range actors {
		a.evict()
	}
	return len(actors)
}

// Shutdown snapshots and stops every match, then drains the persistence queues. All
// actors are asked to stop at once (their final snapshots go out in parallel batches),
// and nothing here waits past ctx.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	actors := make([]*Actor, 0, len(m.actors))
	for _, a := range m.actors {
		actors = append(actors, a)
	}
	m.mu.Unlock()

	for _, a := range actors {
		a.requestStop()
	}
	var errs []error
	if err := waitCtx(ctx, m.wg.Wait); err != nil {
		errs = append(errs, err)
	}
	for _, sh := range m.shards {
		sh.mu.Lock()
		sh.closed = true
		sh.mu.Unlock()
		sh.signal()
	}
	if err := waitCtx(ctx, m.persistWG.Wait); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func waitCtx(ctx context.Context, wait func()) error {
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) start(mt *Match) bool {
	a := newActor(mt, m)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	m.actors[mt.ID] = a
	m.wg.Add(1)
	m.mu.Unlock()
	m.metrics.ActiveMatches.Inc()
	m.metrics.LiveMatches.Inc()
	m.live.Add(1)
	go func() {
		defer m.wg.Done()
		a.run()
		if !a.finished { // stopped by shutdown or eviction; otherwise already counted
			m.metrics.LiveMatches.Dec()
			m.live.Add(-1)
		}
		m.mu.Lock()
		delete(m.actors, mt.ID)
		m.mu.Unlock()
		m.metrics.ActiveMatches.Dec()
	}()
	return true
}

// matchFinished is called from the actor goroutine when its match reaches MatchEnd. The
// delete is queued, not awaited: the match loop never blocks on the store (the TTL is
// the safety net if the delete is lost). The returned channel closes once it is written.
func (m *Manager) matchFinished(mt *Match) <-chan struct{} {
	m.metrics.MatchesFinished.WithLabelValues(mt.EndReason).Inc()
	m.metrics.LiveMatches.Dec()
	m.live.Add(-1)
	m.log.Info("match finished", "match", mt.ID, "winner", mt.Winner, "reason", mt.EndReason,
		"wins", []int{mt.Seats[0].Wins, mt.Seats[1].Wins})
	return m.enqueue(mt.ID, nil, true)
}

// matchAborted is called from a crashed actor goroutine.
func (m *Manager) matchAborted(id string) {
	m.metrics.MatchesFinished.WithLabelValues("internal_error").Inc()
	m.metrics.LiveMatches.Dec()
	m.live.Add(-1)
	m.enqueue(id, nil, false)
}

// save queues a snapshot. Periodic saves are fire-and-forget; sync saves (shutdown)
// wait until written, but never longer than two store timeouts.
func (m *Manager) save(id string, data []byte, sync bool) {
	done := m.enqueue(id, data, sync)
	if done == nil {
		return
	}
	t := time.NewTimer(2 * m.opts.StoreTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		m.log.Warn("snapshot write did not finish in time", "match", id)
	}
}

func (m *Manager) shard(id string) *persistShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return m.shards[h.Sum32()%uint32(len(m.shards))]
}

func opName(data []byte) string {
	if data == nil {
		return "delete"
	}
	return "save"
}

// enqueue adds a write for id, replacing a still-pending one. With wait it returns a
// channel closed once the write is done (or skipped).
func (m *Manager) enqueue(id string, data []byte, wait bool) <-chan struct{} {
	sh := m.shard(id)
	var done chan struct{}
	if wait {
		done = make(chan struct{})
	}
	sh.mu.Lock()
	if sh.closed {
		sh.mu.Unlock()
		m.metrics.SnapshotWrites.WithLabelValues(opName(data), "skipped").Inc()
		if done != nil {
			close(done)
		}
		return done
	}
	if j, ok := sh.pending[id]; ok {
		m.metrics.SnapshotWrites.WithLabelValues(opName(j.data), "superseded").Inc()
		j.data = data
		if done != nil {
			j.waiters = append(j.waiters, done)
		}
	} else {
		j := &persistJob{id: id, data: data}
		if done != nil {
			j.waiters = append(j.waiters, done)
		}
		sh.pending[id] = j
		sh.order = append(sh.order, id)
		m.metrics.PersistPending.Inc()
	}
	sh.mu.Unlock()
	sh.signal()
	return done
}

func (sh *persistShard) signal() {
	select {
	case sh.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) persistLoop(sh *persistShard) {
	defer m.persistWG.Done()
	for {
		sh.mu.Lock()
		for len(sh.order) == 0 && !sh.closed {
			sh.mu.Unlock()
			<-sh.wake
			sh.mu.Lock()
		}
		if len(sh.order) == 0 { // closed and drained
			sh.mu.Unlock()
			return
		}
		n := min(len(sh.order), m.opts.PersistBatch)
		jobs := make([]*persistJob, n)
		for i, id := range sh.order[:n] {
			jobs[i] = sh.pending[id]
			delete(sh.pending, id)
		}
		if n == len(sh.order) {
			sh.order = sh.order[:0]
		} else {
			sh.order = append(sh.order[:0], sh.order[n:]...)
		}
		sh.mu.Unlock()
		m.metrics.PersistPending.Sub(float64(n))

		m.write(jobs)
		for _, j := range jobs {
			for _, w := range j.waiters {
				close(w)
			}
		}
	}
}

func (m *Manager) write(jobs []*persistJob) {
	if m.breakerOpen() {
		for _, j := range jobs {
			m.metrics.SnapshotWrites.WithLabelValues(opName(j.data), "skipped").Inc()
		}
		return
	}
	ops := make([]store.Op, len(jobs))
	for i, j := range jobs {
		ops[i] = store.Op{ID: j.id, Data: j.data}
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.opts.StoreTimeout)
	var errs []error
	if b, ok := m.store.(store.Batcher); ok {
		errs = b.Apply(ctx, ops)
	} else {
		errs = make([]error, len(ops))
		for i, op := range ops {
			if op.Data == nil {
				errs[i] = m.store.Delete(ctx, op.ID)
			} else {
				errs[i] = m.store.Save(ctx, op.ID, op.Data)
			}
		}
	}
	cancel()

	failed := 0
	var firstErr error
	for i, err := range errs {
		op, result := opName(ops[i].Data), "ok"
		switch {
		case errors.Is(err, store.ErrNotOwner):
			result = "not_owner"
			m.evict(ops[i].ID)
		case err != nil:
			result = "error"
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
		m.metrics.SnapshotWrites.WithLabelValues(op, result).Inc()
	}
	if failed > 0 { // one log line per batch, not per match
		m.log.Error("snapshot store failed", "failed", failed, "batch", len(ops), "err", firstErr)
	}
	m.breakerRecord(failed > 0 && failed == len(ops))
}

func (m *Manager) breakerOpen() bool {
	m.breakerMu.Lock()
	defer m.breakerMu.Unlock()
	return time.Now().Before(m.breakerUntil)
}

func (m *Manager) breakerRecord(allFailed bool) {
	m.breakerMu.Lock()
	defer m.breakerMu.Unlock()
	if !allFailed {
		m.failStreak = 0
		return
	}
	m.failStreak++
	if m.failStreak >= breakerFailures {
		m.failStreak = 0
		m.breakerUntil = time.Now().Add(breakerCooldown)
		m.log.Warn("snapshot store circuit open: skipping writes", "for", breakerCooldown)
	}
}

// evict stops a local actor whose match another node now owns (fencing).
func (m *Manager) evict(id string) {
	if a, ok := m.Get(id); ok {
		a.evict()
	}
}

func randomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func randomSeed() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}
