package match

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// A resuming client must get its snapshot first; player_back is inside it, not ahead of
// it as a separate event (and not twice).
func TestResumeSnapshotComesFirst(t *testing.T) {
	m, tokens, sinks, now := newPlaying(t, testConfig())
	seen := m.EventSeq
	m.Detach(1, sinks[1], now)
	m.Advance(now)

	fresh := &fakeSink{}
	if _, err := m.Attach(tokens[1], fresh, seen, now); err != nil {
		t.Fatal(err)
	}
	if len(fresh.msgs) != 1 {
		t.Fatalf("resuming client got %d messages, want just the snapshot: %#v", len(fresh.msgs), fresh.msgs)
	}
	snap, ok := fresh.msgs[0].(protocol.Snapshot)
	if !ok {
		t.Fatalf("first message is %T, want Snapshot", fresh.msgs[0])
	}
	if last := snap.Events[len(snap.Events)-1]; last.Kind != protocol.EvPlayerBack {
		t.Fatalf("snapshot events end with %s, want player_back", last.Kind)
	}
	// The opponent still hears about it as a live event.
	evs := sinks[0].events()
	if evs[len(evs)-1].Kind != protocol.EvPlayerBack {
		t.Fatalf("opponent's last event = %s, want player_back", evs[len(evs)-1].Kind)
	}
	// Events after the attach do reach the new sink.
	m.Advance(now)
	if evs := fresh.events(); len(evs) != 1 || evs[0].Kind != protocol.EvResumed {
		t.Fatalf("events after attach = %+v, want [resumed]", evs)
	}
}

// Reconnecting just before the grace runs out must not freeze the match forever: the
// time away adds up against the pause budget.
func TestPauseBudgetStopsEndlessPausing(t *testing.T) {
	cfg := testConfig()
	cfg.ReconnectGrace = 20 * time.Second
	cfg.PauseBudget = 10 * time.Second
	m, tokens, sinks, now := newPlaying(t, cfg)
	var sink Sink = sinks[1]
	for cycle := 0; cycle < 2; cycle++ { // 2 x 4 s away = 8 s used
		m.Detach(1, sink, now)
		m.Advance(now)
		now = now.Add(4 * time.Second)
		m.Advance(now)
		fresh := &fakeSink{}
		if _, err := m.Attach(tokens[1], fresh, 0, now); err != nil {
			t.Fatal(err)
		}
		sink = fresh
		m.Advance(now)
		if m.Phase != Playing || !m.PausedAt.IsZero() {
			t.Fatalf("cycle %d: phase=%s paused=%v", cycle, m.Phase, !m.PausedAt.IsZero())
		}
	}
	if m.Seats[1].AwayUsed != 8*time.Second {
		t.Fatalf("AwayUsed = %v, want 8s", m.Seats[1].AwayUsed)
	}
	m.Detach(1, sink, now)
	m.Advance(now)
	m.Advance(now.Add(2*time.Second - time.Millisecond))
	if m.Phase != Playing {
		t.Fatalf("forfeited before the budget was used up: %s", m.Phase)
	}
	m.Advance(now.Add(2 * time.Second)) // 8 s + 2 s = budget, long before the 20 s grace
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_disconnected" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
	// The budget is persisted, so a restart cannot reset it.
	data, _ := m.Snapshot(now)
	if !strings.Contains(string(data), `"awayUsed"`) {
		t.Fatal("awayUsed missing from the snapshot")
	}
}

func TestBothIdleHasNoWinner(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour
	m, _, _, now := newPlaying(t, cfg)
	m.Advance(now.Add(cfg.IdleTimeout))
	if m.Phase != MatchEnd || m.Winner != NoSeat || m.EndReason != "both_idle" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
}

func TestSnapshotKeepsOnlyNewestEvents(t *testing.T) {
	cfg := testConfig()
	cfg.EventLogSize, cfg.SnapshotEvents = 64, 5
	m, _ := New("m", cfg, [2]string{"a", "b"}, 1, t0)
	for i := 0; i < 40; i++ {
		m.emit(t0, protocol.Event{Kind: protocol.EvPaused})
	}
	data, err := m.Snapshot(t0)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Restore(data, cfg, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Events) != 5 || r.Events[0].Seq != 36 || r.EventSeq != 40 {
		t.Fatalf("restored log: %d events from %d, EventSeq %d", len(r.Events), r.Events[0].Seq, r.EventSeq)
	}
	if len(m.Events) != 40 {
		t.Fatal("Snapshot trimmed the live log")
	}
	if _, complete := r.EventsSince(10); complete {
		t.Fatal("a client far behind must be told the log is incomplete")
	}
}

// A panic in one match aborts that match only: its clients are told and closed, its
// snapshot is removed, and other matches keep running.
func TestActorPanicIsContained(t *testing.T) {
	clk := clock.NewFake(t0)
	st := store.NewMemory()
	mgr := newTestManager(t, clk, st)
	defer mgr.Shutdown(context.Background())
	bad, _, sinks := startPlaying(t, mgr, clk)
	good, _, _ := startPlaying(t, mgr, clk)

	err := bad.Inspect(context.Background(), func(*Match) { panic("boom") })
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Inspect err = %v, want ErrStopped", err)
	}
	<-bad.Done()
	if got := counterValue(mgr.metrics.ActorPanics); got != 1 {
		t.Fatalf("actor panics = %v", got)
	}
	for i, s := range sinks {
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed != "internal error" {
			t.Fatalf("sink %d closed = %q", i, closed)
		}
		evs := s.events()
		if last := evs[len(evs)-1]; last.Kind != protocol.EvMatchOver || last.Reason != "internal_error" {
			t.Fatalf("sink %d last event = %+v", i, last)
		}
	}
	eventually(t, "crashed match removed from the store", func() bool {
		all, _ := st.LoadAll(context.Background())
		_, ok := all[bad.ID()]
		return !ok
	})
	if p := phaseOf(t, good); p != Playing {
		t.Fatalf("the other match is %s", p)
	}
	if mgr.LiveCount() != 1 {
		t.Fatalf("live = %d", mgr.LiveCount())
	}
}

// A flooding player fills only their own input queue; the opponent's inputs still apply.
func TestInputFloodDoesNotStarveOpponent(t *testing.T) {
	clk := clock.NewFake(t0)
	mgr := newTestManager(t, clk, store.NewMemory())
	defer mgr.Shutdown(context.Background())
	a, _, _ := startPlaying(t, mgr, clk)

	release := make(chan struct{})
	blocked := make(chan struct{})
	go func() { // hold the actor so the queues fill up
		_ = a.Inspect(context.Background(), func(*Match) { close(blocked); <-release })
	}()
	<-blocked
	for seq := uint64(1); seq <= 1000; seq++ {
		a.Input(0, seq, game.Left)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		a.Input(1, seq, game.Rotate)
	}
	close(release)
	inspect(t, a, func(m *Match) {
		if m.Seats[1].LastSeq != 10 {
			t.Errorf("victim LastSeq = %d, want 10", m.Seats[1].LastSeq)
		}
		if m.Seats[0].LastSeq != inputQueueSize {
			t.Errorf("flooder LastSeq = %d, want %d (the rest overflowed)", m.Seats[0].LastSeq, inputQueueSize)
		}
	})
	if got := counterValue(mgr.overflowCounter); got != 1000-inputQueueSize {
		t.Fatalf("overflow = %v", got)
	}
}

// A round decided inside Input (top-out) is snapshotted at once, not on the next tick.
func TestPhaseChangeFromInputIsPersisted(t *testing.T) {
	clk := clock.NewFake(t0)
	st := store.NewMemory()
	mgr := newTestManager(t, clk, st)
	defer mgr.Shutdown(context.Background())
	a, _, _ := startPlaying(t, mgr, clk)
	var seq uint64
	for phaseOf(t, a) == Playing {
		seq++
		a.Input(0, seq, game.HardDrop)
	}
	eventually(t, "round_end snapshot", func() bool {
		all, _ := st.LoadAll(context.Background())
		return strings.Contains(string(all[a.ID()]), `"phase":"round_end"`)
	})
}

// plainStore is a store.Store without the Batcher fast path, so the fakes below can
// override Save and have every write go through it.
type plainStore struct{ mem *store.Memory }

func newPlain() plainStore { return plainStore{store.NewMemory()} }

func (p plainStore) Delete(ctx context.Context, id string) error { return p.mem.Delete(ctx, id) }
func (p plainStore) LoadAll(ctx context.Context) (map[string][]byte, error) {
	return p.mem.LoadAll(ctx)
}

// slowStore blocks every write until released, and records what was written.
type slowStore struct {
	plainStore
	gate   chan struct{}
	mu     sync.Mutex
	writes []string
}

func (s *slowStore) Save(ctx context.Context, id string, data []byte) error {
	<-s.gate
	s.mu.Lock()
	s.writes = append(s.writes, string(data))
	s.mu.Unlock()
	return nil
}

// While the store is slow, newer snapshots of a match replace pending ones instead of
// piling up (or being dropped blindly); the newest one is what gets written.
func TestPersistCoalescesPendingWrites(t *testing.T) {
	st := &slowStore{plainStore: newPlain(), gate: make(chan struct{})}
	mgr := newTestManager(t, clock.NewFake(t0), st)
	mgr.save("x", []byte("v0"), false) // taken by the worker, blocked in Save
	eventually(t, "worker busy", func() bool {
		sh := mgr.shard("x")
		sh.mu.Lock()
		defer sh.mu.Unlock()
		return len(sh.order) == 0
	})
	for i := 1; i <= 50; i++ {
		mgr.save("x", []byte("mid"), false)
	}
	mgr.save("x", []byte("final"), false)
	close(st.gate)
	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.writes) != 2 || st.writes[0] != "v0" || st.writes[1] != "final" {
		t.Fatalf("writes = %q, want [v0 final]", st.writes)
	}
	if got := counterValue(mgr.metrics.SnapshotWrites.WithLabelValues("save", "superseded")); got != 50 {
		t.Fatalf("superseded = %v", got)
	}
}

type failingStore struct {
	plainStore
	calls atomic.Int64
}

func (s *failingStore) Save(context.Context, string, []byte) error {
	s.calls.Add(1)
	return errors.New("redis down")
}

// When the store is down, the circuit opens: writes are skipped instead of every one of
// them waiting out the store timeout.
func TestPersistCircuitBreaker(t *testing.T) {
	st := &failingStore{plainStore: newPlain()}
	mgr := newTestManager(t, clock.NewFake(t0), st)
	defer mgr.Shutdown(context.Background())
	for i := 0; i < 20; i++ {
		mgr.save("x", []byte("v"), true)
	}
	if n := st.calls.Load(); n != breakerFailures {
		t.Fatalf("store called %d times, want %d before the circuit opens", n, breakerFailures)
	}
	if got := counterValue(mgr.metrics.SnapshotWrites.WithLabelValues("save", "skipped")); got != 20-breakerFailures {
		t.Fatalf("skipped = %v", got)
	}
}

type hungStore struct{ plainStore }

func (*hungStore) Save(context.Context, string, []byte) error { select {} }

// Shutdown never waits past its context, even if the store hangs.
func TestShutdownRespectsContextWhenStoreHangs(t *testing.T) {
	clk := clock.NewFake(t0)
	opts := DefaultOptions()
	opts.Match = testConfig()
	opts.StoreTimeout = 50 * time.Millisecond
	mgr := NewManager(opts, clk, &hungStore{newPlain()}, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i := 0; i < 3; i++ {
		if _, _, err := mgr.Create([2]string{"a", "b"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := mgr.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
}

type notOwnerStore struct{ plainStore }

func (*notOwnerStore) Save(context.Context, string, []byte) error { return store.ErrNotOwner }

// If another node owns the match, this node stops driving it (fencing).
func TestNotOwnerEvictsActor(t *testing.T) {
	clk := clock.NewFake(t0)
	mgr := newTestManager(t, clk, &notOwnerStore{newPlain()})
	defer mgr.Shutdown(context.Background())
	id, tokens, _ := mgr.Create([2]string{"a", "b"})
	a, _ := mgr.Get(id)
	sink := &fakeSink{}
	_, _ = a.Attach(context.Background(), tokens[0], sink, 0)
	select {
	case <-a.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("actor kept running a match owned elsewhere")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.closed != "match moved to another server" {
		t.Fatalf("closed = %q", sink.closed)
	}
}
