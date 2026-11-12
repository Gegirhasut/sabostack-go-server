package match

// Tests for the new knobs and behaviours added while hardening the server. The bug
// reproductions themselves are in regression_test.go.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// The pause budget adds up time away across disconnects and forfeits exactly when it
// runs out, independently of the per-disconnect grace.
func TestPauseBudgetAccounting(t *testing.T) {
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
	data, _ := m.Snapshot(now) // persisted, so a restart cannot reset it
	if !strings.Contains(string(data), `"awayUsed"`) {
		t.Fatal("awayUsed missing from the snapshot")
	}
}

// PauseBudget 0 disables the cap (only the grace applies).
func TestPauseBudgetZeroDisables(t *testing.T) {
	cfg := testConfig()
	cfg.PauseBudget = 0
	m, _, sinks, now := newPlaying(t, cfg)
	m.Seats[1].AwayUsed = time.Hour
	m.Detach(1, sinks[1], now)
	m.Advance(now)
	m.Advance(now.Add(cfg.ReconnectGrace - time.Millisecond))
	if m.Phase != Playing {
		t.Fatalf("phase = %s with the budget disabled", m.Phase)
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

// After a panic: clients get match_over(internal_error) and are closed, the snapshot is
// deleted (so a restart does not re-crash), and the metrics account for it.
func TestActorPanicCleanup(t *testing.T) {
	clk := clock.NewFake(t0)
	st := store.NewMemory()
	mgr := newTestManager(t, clk, st)
	defer mgr.Shutdown(context.Background())
	bad, _, sinks := startPlaying(t, mgr, clk)
	_, _, _ = startPlaying(t, mgr, clk)

	_ = bad.Inspect(context.Background(), func(*Match) { panic("boom") })
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
	if mgr.LiveCount() != 1 {
		t.Fatalf("live = %d", mgr.LiveCount())
	}
}

// Superseded writes and writes skipped by the open circuit are visible in metrics.
func TestPersistMetrics(t *testing.T) {
	st := &slowStore{plainStore: newPlain(), started: make(chan struct{}), gate: make(chan struct{})}
	mgr := newTestManager(t, clock.NewFake(t0), st)
	mgr.save("x", []byte("v0"), false)
	<-st.started
	for i := 0; i < 50; i++ {
		mgr.save("x", []byte("old"), false)
	}
	close(st.gate)
	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := counterValue(mgr.metrics.SnapshotWrites.WithLabelValues("save", "superseded")); got != 49 {
		t.Fatalf("superseded = %v, want 49", got)
	}

	fs := &failingStore{plainStore: newPlain()}
	mgr = newTestManager(t, clock.NewFake(t0), fs)
	defer mgr.Shutdown(context.Background())
	for i := 0; i < 20; i++ {
		mgr.save("x", []byte("v"), true)
	}
	if n := fs.calls.Load(); n != breakerFailures {
		t.Fatalf("store called %d times, want %d before the circuit opens", n, breakerFailures)
	}
	if got := counterValue(mgr.metrics.SnapshotWrites.WithLabelValues("save", "skipped")); got != 20-breakerFailures {
		t.Fatalf("skipped = %v", got)
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
