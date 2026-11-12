package match

// Regression tests for the bugs found in the code review. Each one reproduces the bug
// through APIs that existed before the fix, so it compiles against the old code and
// fails there. docs/HARDENING.md lists the outcome of every test before and after.

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

// plainStore is a store.Store with no batching fast path, so the fakes below can override
// Save and see every write.
type plainStore struct{ mem *store.Memory }

func newPlain() plainStore { return plainStore{store.NewMemory()} }

func (p plainStore) Delete(ctx context.Context, id string) error { return p.mem.Delete(ctx, id) }
func (p plainStore) LoadAll(ctx context.Context) (map[string][]byte, error) {
	return p.mem.LoadAll(ctx)
}

// Bug: on resume, player_back reached the client as an event before its snapshot, and then
// again inside the snapshot.
func TestBugResumeEventArrivesBeforeSnapshot(t *testing.T) {
	m, tokens, sinks, now := newPlaying(t, testConfig())
	seen := m.EventSeq
	m.Detach(1, sinks[1], now)
	m.Advance(now)

	fresh := &fakeSink{}
	if _, err := m.Attach(tokens[1], fresh, seen, now); err != nil {
		t.Fatal(err)
	}
	if len(fresh.msgs) != 1 {
		t.Fatalf("resuming client got %d messages, want only the snapshot: %#v", len(fresh.msgs), fresh.msgs)
	}
	snap, ok := fresh.msgs[0].(protocol.Snapshot)
	if !ok {
		t.Fatalf("first message is %T, want Snapshot", fresh.msgs[0])
	}
	if last := snap.Events[len(snap.Events)-1]; last.Kind != protocol.EvPlayerBack {
		t.Fatalf("snapshot events end with %s, want player_back", last.Kind)
	}
	evs := sinks[0].events() // the opponent still hears it as a live event
	if evs[len(evs)-1].Kind != protocol.EvPlayerBack {
		t.Fatalf("opponent's last event = %s, want player_back", evs[len(evs)-1].Kind)
	}
	m.Advance(now) // events after the attach reach the new sink
	if evs := fresh.events(); len(evs) != 1 || evs[0].Kind != protocol.EvResumed {
		t.Fatalf("events after attach = %+v, want [resumed]", evs)
	}
}

// Bug: reconnecting just before the reconnect grace ran out reset it, so a losing player
// could keep the match paused forever (measured live: 15 s of real time cost 0.25 s of
// round time).
func TestBugEndlessPauseByReconnecting(t *testing.T) {
	cfg := testConfig() // default timings: 20 s grace
	m, tokens, sinks, now := newPlaying(t, cfg)
	var sink Sink = sinks[1]
	away := cfg.ReconnectGrace - time.Second // always back just in time
	for cycle := 0; cycle < 10 && m.Phase != MatchEnd; cycle++ {
		m.Detach(1, sink, now)
		for end := now.Add(away); now.Before(end) && m.Phase != MatchEnd; {
			now = now.Add(time.Second)
			m.Advance(now)
		}
		if m.Phase == MatchEnd {
			break
		}
		fresh := &fakeSink{}
		if _, err := m.Attach(tokens[1], fresh, 0, now); err != nil {
			t.Fatal(err)
		}
		sink = fresh
		m.Advance(now)
	}
	// 10 cycles x 19 s = 190 s of frozen game. It must have been cut short.
	if m.Phase != MatchEnd || m.Winner != 0 {
		t.Fatalf("after %s of pausing: phase=%s winner=%d (match frozen indefinitely)",
			now.Sub(t0), m.Phase, m.Winner)
	}
}

// Bug: when both players were idle, seat 0 always lost.
func TestBugBothIdleSeatZeroLoses(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour
	m, _, _, now := newPlaying(t, cfg)
	m.Advance(now.Add(cfg.IdleTimeout))
	if m.Phase != MatchEnd || m.Winner != NoSeat {
		t.Fatalf("phase=%s winner=%d reason=%s, want no winner", m.Phase, m.Winner, m.EndReason)
	}
}

// Bug: every periodic snapshot carried the whole reconnect log, which was most of its
// size (up to 256 events, written once a second per match).
func TestBugSnapshotCarriesWholeEventLog(t *testing.T) {
	cfg := testConfig() // log of 64 events
	m, _ := New("m", cfg, [2]string{"a", "b"}, 1, t0)
	for i := 0; i < cfg.EventLogSize; i++ {
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
	if len(r.Events) > 32 {
		t.Fatalf("snapshot holds %d events (%d bytes)", len(r.Events), len(data))
	}
}

// Bug: a panic in one match goroutine crashed the whole process, i.e. every match on
// the server. (Against the old code this test kills the test binary.)
func TestBugActorPanicKillsProcess(t *testing.T) {
	clk := clock.NewFake(t0)
	mgr := newTestManager(t, clk, store.NewMemory())
	defer mgr.Shutdown(context.Background())
	bad, _, _ := startPlaying(t, mgr, clk)
	good, _, _ := startPlaying(t, mgr, clk)

	err := bad.Inspect(context.Background(), func(*Match) { panic("boom") })
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Inspect err = %v, want ErrStopped", err)
	}
	if p := phaseOf(t, good); p != Playing {
		t.Fatalf("the other match is %s", p)
	}
}

// Bug: both players' inputs shared one actor queue, so a flooding player made the
// opponent's inputs overflow and get dropped.
func TestBugFloodStarvesOpponentInputs(t *testing.T) {
	clk := clock.NewFake(t0)
	mgr := newTestManager(t, clk, store.NewMemory())
	defer mgr.Shutdown(context.Background())
	a, _, _ := startPlaying(t, mgr, clk)

	release := make(chan struct{})
	blocked := make(chan struct{})
	go func() { // hold the actor busy so the queues fill up
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
			t.Errorf("victim's inputs applied up to seq %d of 10", m.Seats[1].LastSeq)
		}
	})
}

// Bug: a round decided inside Input (top-out) was only noticed on the next tick's phase
// comparison, so the phase change was neither logged nor snapshotted at once.
func TestBugTopOutPhaseChangeNotPersisted(t *testing.T) {
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

// slowStore blocks every write until released, and records what was written.
type slowStore struct {
	plainStore
	started chan struct{}
	once    sync.Once
	gate    chan struct{}
	mu      sync.Mutex
	writes  []string
}

func (s *slowStore) Save(ctx context.Context, id string, data []byte) error {
	s.once.Do(func() { close(s.started) })
	<-s.gate
	s.mu.Lock()
	s.writes = append(s.writes, string(data))
	s.mu.Unlock()
	return nil
}

// Bug: while the store was slow, snapshots queued up in one FIFO and, once it was full,
// new ones were dropped. The newest snapshot of a match is exactly the one that must not
// be lost.
func TestBugSlowStoreDropsNewestSnapshot(t *testing.T) {
	st := &slowStore{plainStore: newPlain(), started: make(chan struct{}), gate: make(chan struct{})}
	mgr := newTestManager(t, clock.NewFake(t0), st)
	mgr.save("x", []byte("v0"), false)
	<-st.started // the writer is now stuck in Save
	for i := 0; i < 2000; i++ {
		mgr.save("x", []byte("old"), false)
	}
	mgr.save("x", []byte("newest"), false)
	close(st.gate)
	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(st.writes); n == 0 || st.writes[n-1] != "newest" {
		t.Fatalf("%d writes, last = %q: the newest snapshot was lost", n, st.writes[n-1])
	}
	if len(st.writes) > 2 {
		t.Fatalf("%d writes of one match: superseded snapshots were written too", len(st.writes))
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

// Bug: with the store down, every write still went to it and waited for its error or
// timeout; nothing stopped hammering a dead store.
func TestBugDeadStoreIsHammered(t *testing.T) {
	st := &failingStore{plainStore: newPlain()}
	mgr := newTestManager(t, clock.NewFake(t0), st)
	defer mgr.Shutdown(context.Background())
	for i := 0; i < 20; i++ {
		mgr.save("x", []byte("v"), true)
	}
	if n := st.calls.Load(); n > 5 {
		t.Fatalf("dead store called %d times for 20 writes", n)
	}
}

type hungStore struct{ plainStore }

func (*hungStore) Save(context.Context, string, []byte) error { select {} }

// Bug: Shutdown waited for every final snapshot without a limit, so a hung store hung
// the shutdown until the orchestrator's SIGKILL (losing every snapshot).
func TestBugShutdownHangsOnStuckStore(t *testing.T) {
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
	done := make(chan struct{})
	go func() { _ = mgr.Shutdown(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown ignored its 300 ms context and was still waiting after 3 s")
	}
}
