package match

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// Options configures a Manager.
type Options struct {
	Match         Config
	SnapshotEvery time.Duration // periodic snapshot interval while a match is live
	Linger        time.Duration // how long a finished match keeps answering resumes
	StoreTimeout  time.Duration // per store operation
}

// DefaultOptions returns production-ish defaults.
func DefaultOptions() Options {
	return Options{Match: DefaultConfig(), SnapshotEvery: time.Second, Linger: 10 * time.Second, StoreTimeout: 2 * time.Second}
}

// Manager owns all match actors in the process and the single goroutine that writes
// snapshots to the store.
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

	jobs      chan persistJob
	persistWG sync.WaitGroup
}

// persistJob is one store write. All writes go through one FIFO queue so a late periodic
// snapshot can never overwrite a final one or resurrect a deleted match.
type persistJob struct {
	id   string
	data []byte // nil = delete
	done chan struct{}
}

// NewManager starts the persistence goroutine. Call Shutdown to stop everything.
func NewManager(opts Options, clk clock.Clock, st store.Store, met *metrics.Metrics, log *slog.Logger) *Manager {
	m := &Manager{
		opts: opts, clock: clk, store: st, metrics: met, log: log,
		actors: map[string]*Actor{},
		jobs:   make(chan persistJob, 1024),
	}
	m.persistWG.Add(1)
	go m.persistLoop()
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

// Restore loads every snapshot from the store and resumes those matches. It is called
// once at startup, before the server accepts connections.
func (m *Manager) Restore(ctx context.Context) (int, error) {
	snaps, err := m.store.LoadAll(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for id, data := range snaps {
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

// Shutdown snapshots and stops every match, then drains the persistence queue.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	actors := make([]*Actor, 0, len(m.actors))
	for _, a := range m.actors {
		actors = append(actors, a)
	}
	m.mu.Unlock()

	var errs []error
	for _, a := range actors {
		if err := a.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	m.wg.Wait()
	close(m.jobs)
	m.persistWG.Wait()
	return errors.Join(errs...)
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
	go func() {
		defer m.wg.Done()
		a.run()
		m.mu.Lock()
		delete(m.actors, mt.ID)
		m.mu.Unlock()
		m.metrics.ActiveMatches.Dec()
	}()
	return true
}

// matchFinished is called from the actor goroutine when its match reaches MatchEnd.
func (m *Manager) matchFinished(mt *Match) {
	m.metrics.MatchesFinished.WithLabelValues(mt.EndReason).Inc()
	m.log.Info("match finished", "match", mt.ID, "winner", mt.Winner, "reason", mt.EndReason,
		"wins", []int{mt.Seats[0].Wins, mt.Seats[1].Wins})
	m.enqueue(persistJob{id: mt.ID}, true)
}

// save queues a snapshot. Periodic saves are best effort (dropped if the queue is full,
// the next one supersedes them); sync saves wait until written.
func (m *Manager) save(id string, data []byte, sync bool) {
	m.enqueue(persistJob{id: id, data: data}, sync)
}

func (m *Manager) enqueue(j persistJob, sync bool) {
	if !sync {
		select {
		case m.jobs <- j:
		default:
			m.metrics.SnapshotWrites.WithLabelValues("save", "dropped").Inc()
		}
		return
	}
	j.done = make(chan struct{})
	m.jobs <- j
	<-j.done
}

func (m *Manager) persistLoop() {
	defer m.persistWG.Done()
	for j := range m.jobs {
		ctx, cancel := context.WithTimeout(context.Background(), m.opts.StoreTimeout)
		op, err := "save", error(nil)
		if j.data == nil {
			op, err = "delete", m.store.Delete(ctx, j.id)
		} else {
			err = m.store.Save(ctx, j.id, j.data)
		}
		cancel()
		result := "ok"
		if err != nil {
			result = "error"
			m.log.Error("snapshot store failed", "op", op, "match", j.id, "err", err)
		}
		m.metrics.SnapshotWrites.WithLabelValues(op, result).Inc()
		if j.done != nil {
			close(j.done)
		}
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
