package match

// Regression tests for the bugs found in the external code review of 2026-11-12. Each one
// fails on b37f934 (the reviewed commit) and passes after the fix.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
)

// Bug: Attach reset LastInput to "now" and the end of the pause then shifted it forward by
// the whole pause again, so after 15 s away a player's LastInput was 15 s in the future:
// 45 s instead of 30 s until the idle forfeit. Repeated disconnects stretched it further.
func TestReviewReconnectAddsIdleCredit(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour // nobody tops out while we wait
	m, tokens, sinks, start := newPlaying(t, cfg)
	now := start
	away := 15 * time.Second

	m.Detach(1, sinks[1], now)
	for end := now.Add(away); now.Before(end); {
		now = now.Add(time.Second)
		m.Advance(now)
	}
	if _, err := m.Attach(tokens[1], &fakeSink{}, 0, now); err != nil {
		t.Fatal(err)
	}

	// Seat 0 keeps playing, seat 1 never sends an input. Seat 1 has been idle for all of
	// the playing time, which excludes the pause: it forfeits at start+away+IdleTimeout.
	seq := uint64(0)
	deadline := start.Add(away + cfg.IdleTimeout + time.Second)
	for m.Phase == Playing && now.Before(deadline) {
		now = now.Add(time.Second)
		seq++
		m.Input(0, seq, game.Left, now)
		m.Advance(now)
	}
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_idle" {
		t.Fatalf("%s of idle play after a %s pause: phase=%s winner=%d reason=%q (idle timer got extra credit)",
			now.Sub(start)-away, away, m.Phase, m.Winner, m.EndReason)
	}
}

// Bug (same line): replacing a live connection with a new one also reset LastInput, so a
// player who never moved could dodge the idle forfeit forever by reconnecting every 20 s.
func TestReviewReplacingConnectionResetsIdle(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour
	m, tokens, _, start := newPlaying(t, cfg)
	now, seq := start, uint64(0)
	for m.Phase == Playing && now.Sub(start) < 3*cfg.IdleTimeout {
		now = now.Add(time.Second)
		seq++
		m.Input(0, seq, game.Left, now)
		if now.Sub(start)%(20*time.Second) == 0 { // a fresh socket, no input
			if _, err := m.Attach(tokens[1], &fakeSink{}, 0, now); err != nil {
				t.Fatal(err)
			}
		}
		m.Advance(now)
	}
	if m.Phase != MatchEnd || m.Winner != 0 {
		t.Fatalf("idle player survived %s by reconnecting: phase=%s winner=%d", now.Sub(start), m.Phase, m.Winner)
	}
}

// flakyStore fails the first `fail` saves and records the successful ones.
type flakyStore struct {
	plainStore
	fail  atomic.Int32
	saved atomic.Int32
}

func (f *flakyStore) Save(ctx context.Context, id string, data []byte) error {
	if f.fail.Add(-1) >= 0 {
		return errors.New("redis: connection reset")
	}
	f.saved.Add(1)
	return f.plainStore.mem.Save(ctx, id, data)
}

// Bug: a snapshot write that failed (or was skipped by the open circuit breaker) was
// counted as written. A match whose state did not change after that was only written
// again by the forced re-snapshot, 10 s later, so a crash in between lost it.
func TestReviewFailedSnapshotIsRetried(t *testing.T) {
	st := &flakyStore{plainStore: newPlain()}
	st.fail.Store(1)
	opts := DefaultOptions()
	opts.SnapshotEvery = 20 * time.Millisecond // forced re-snapshot every 200 ms
	opts.Match.TickInterval = 10 * time.Millisecond
	mgr := NewManager(opts, clock.Real{}, st, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer mgr.Shutdown(context.Background())
	if _, _, err := mgr.Create([2]string{"a", "b"}); err != nil { // Waiting: state is static
		t.Fatal(err)
	}
	deadline := time.Now().Add(150 * time.Millisecond) // well before the forced snapshot
	for st.saved.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("snapshot not retried after a failed write; the match had no stored copy")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
