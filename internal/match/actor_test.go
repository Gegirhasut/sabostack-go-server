package match

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

func newTestManager(t *testing.T, clk clock.Clock, st store.Store) *Manager {
	t.Helper()
	opts := DefaultOptions()
	opts.Match = testConfig()
	opts.Linger = time.Second
	return NewManager(opts, clk, st, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func inspect(t *testing.T, a *Actor, f func(m *Match)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Inspect(ctx, f); err != nil {
		t.Fatal(err)
	}
}

func phaseOf(t *testing.T, a *Actor) Phase {
	var p Phase
	inspect(t, a, func(m *Match) { p = m.Phase })
	return p
}

// startPlaying creates a match on mgr, attaches two fake clients and drives the fake clock
// into round 1.
func startPlaying(t *testing.T, mgr *Manager, clk *clock.Fake) (*Actor, [2]string, [2]*fakeSink) {
	t.Helper()
	id, tokens, err := mgr.Create([2]string{"alice", "bob"})
	if err != nil {
		t.Fatal(err)
	}
	a, ok := mgr.Get(id)
	if !ok {
		t.Fatal("match not registered")
	}
	sinks := [2]*fakeSink{{}, {}}
	for i := range sinks {
		if _, err := a.Attach(context.Background(), tokens[i], sinks[i], 0); err != nil {
			t.Fatal(err)
		}
	}
	tick := mgr.opts.Match.TickInterval
	clk.Advance(tick)
	if p := phaseOf(t, a); p != Countdown {
		t.Fatalf("phase = %s, want countdown", p)
	}
	clk.Advance(mgr.opts.Match.Countdown)
	if p := phaseOf(t, a); p != Playing {
		t.Fatalf("phase = %s, want playing", p)
	}
	return a, tokens, sinks
}

// Both players hammer the actor concurrently, including duplicates. Run with -race.
func TestActorSerialisesConcurrentInputs(t *testing.T) {
	clk := clock.NewFake(t0)
	mgr := newTestManager(t, clk, store.NewMemory())
	defer mgr.Shutdown(context.Background())
	a, _, _ := startPlaying(t, mgr, clk)

	const n = 50
	var wg sync.WaitGroup
	for seat := 0; seat < 2; seat++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := uint64(1); seq <= n; seq++ {
				a.Input(seat, seq, game.Rotate)
				a.Input(seat, seq, game.Rotate) // duplicate
			}
		}()
	}
	wg.Wait()
	inspect(t, a, func(m *Match) {
		for i, s := range m.Seats {
			if s.LastSeq != n {
				t.Errorf("seat %d LastSeq = %d, want %d", i, s.LastSeq, n)
			}
			// 50 rotations = 12.5 full turns; 50 % 4 = 2.
			if s.Board.Piece.Rot != n%4 {
				t.Errorf("seat %d rotation = %d, want %d (duplicates applied?)", i, s.Board.Piece.Rot, n%4)
			}
		}
	})
}

func TestRestartResumesMatch(t *testing.T) {
	clk := clock.NewFake(t0)
	st := store.NewMemory()
	mgr1 := newTestManager(t, clk, st)
	a, tokens, sinks := startPlaying(t, mgr1, clk)
	a.Input(0, 1, game.HardDrop)
	var before []string
	inspect(t, a, func(m *Match) { before = m.Seats[0].Board.Grid.Rows() })

	// Graceful shutdown: final snapshot, clients told to go away.
	if err := mgr1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sinks[0].closed == "" || sinks[1].closed == "" {
		t.Fatal("clients not closed on shutdown")
	}

	clk.Advance(30 * time.Second) // the process is down for a while
	mgr2 := newTestManager(t, clk, st)
	defer mgr2.Shutdown(context.Background())
	n, err := mgr2.Restore(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("restored %d matches, err=%v", n, err)
	}
	a2, ok := mgr2.Get(a.ID())
	if !ok {
		t.Fatal("restored match not registered")
	}
	fresh := [2]*fakeSink{{}, {}}
	for i := range fresh {
		if _, err := a2.Attach(context.Background(), tokens[i], fresh[i], 0); err != nil {
			t.Fatalf("resume seat %d: %v", i, err)
		}
	}
	snap, _ := fresh[0].lastSnapshot()
	if snap.State.Phase != "playing" || snap.State.Ack != 1 {
		t.Fatalf("snapshot after restart: phase=%s ack=%d", snap.State.Phase, snap.State.Ack)
	}
	for i, row := range snap.State.Players[0].Rows {
		if row != before[i] {
			t.Fatalf("board row %d differs after restart: %q vs %q", i, row, before[i])
		}
	}
	clk.Advance(mgr2.opts.Match.TickInterval)
	inspect(t, a2, func(m *Match) {
		if m.Phase != Playing || m.ConnectedCount() != 2 {
			t.Fatalf("match did not resume: phase=%s connected=%d", m.Phase, m.ConnectedCount())
		}
	})
}

func TestFinishedMatchIsDeletedAndExits(t *testing.T) {
	clk := clock.NewFake(t0)
	st := store.NewMemory()
	mgr := newTestManager(t, clk, st)
	defer mgr.Shutdown(context.Background())
	id, _, _ := mgr.Create([2]string{"a", "b"})
	a, _ := mgr.Get(id)
	_ = phaseOf(t, a) // the initial snapshot has been queued by now
	clk.Advance(mgr.opts.Match.JoinTimeout)
	if p := phaseOf(t, a); p != MatchEnd {
		t.Fatalf("phase = %s", p)
	}
	clk.Advance(mgr.opts.Linger)
	select {
	case <-a.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("actor did not exit after linger")
	}
	all, _ := st.LoadAll(context.Background())
	if len(all) != 0 {
		t.Fatalf("finished match still in store: %d", len(all))
	}
	// The registry entry is removed right after the goroutine exits.
	for deadline := time.Now().Add(5 * time.Second); mgr.Len() != 0; {
		if time.Now().After(deadline) {
			t.Fatal("finished match still registered")
		}
		runtime.Gosched()
	}
}
