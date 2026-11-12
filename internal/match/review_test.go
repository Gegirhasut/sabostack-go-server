package match

// Regression tests for the bugs found in the external code review of 2026-11-12. Each one
// fails on b37f934 (the reviewed commit) and passes after the fix.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
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

// laggyStore takes its time with every write, like a remote Redis does.
type laggyStore struct{ plainStore }

func (s laggyStore) Save(ctx context.Context, id string, data []byte) error {
	time.Sleep(50 * time.Millisecond)
	return s.plainStore.mem.Save(ctx, id, data)
}

// Found by running the cluster tests against a real Redis (fourth review round): Create
// returned, and players were told about the match, while its first snapshot (which also
// records its owner) was still queued. A resume reaching another node in that window got
// match_not_found, final for the client; a crash in it lost a match players had been
// sent to. Create now returns only once the match is in the store.
func TestReviewMatchIsStoredBeforeItIsAnnounced(t *testing.T) {
	st := laggyStore{newPlain()}
	mgr := NewManager(DefaultOptions(), clock.Real{}, st, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer mgr.Shutdown(context.Background())
	id, _, err := mgr.Create([2]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	all, err := st.LoadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := all[id]; !ok {
		t.Fatal("Create returned before the match's first snapshot was stored")
	}
}

// gatedStore blocks its first write until the gate opens and records the order of all.
type gatedStore struct {
	plainStore
	gate  chan struct{}
	first sync.Once
	mu    sync.Mutex
	order []string
}

func (g *gatedStore) Save(ctx context.Context, id string, data []byte) error {
	g.first.Do(func() { <-g.gate })
	g.mu.Lock()
	g.order = append(g.order, id)
	g.mu.Unlock()
	return g.plainStore.mem.Save(ctx, id, data)
}

// Fourth review, note 3: the first write that Create waits for went to the back of the
// shard's queue, behind every periodic snapshot already waiting there, so a burst of
// load slowed matchmaking down with the write queue. Writes someone waits for now go
// first.
func TestReviewAwaitedWriteSkipsTheQueue(t *testing.T) {
	st := &gatedStore{plainStore: newPlain(), gate: make(chan struct{})}
	opts := DefaultOptions()
	opts.PersistWorkers, opts.PersistBatch = 1, 8 // one shard, small batches
	mgr := NewManager(opts, clock.Real{}, st, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer mgr.Shutdown(context.Background())

	mgr.save("in-flight", []byte("x"), false) // the writer is now stuck on this one
	eventually(t, "writer busy", func() bool {
		mgr.shards[0].mu.Lock()
		defer mgr.shards[0].mu.Unlock()
		return len(mgr.shards[0].pending) == 0
	})
	for i := 0; i < 200; i++ { // a backlog of routine snapshots
		mgr.save(fmt.Sprintf("routine-%03d", i), []byte("x"), false)
	}
	done := make(chan struct{})
	go func() { mgr.save("awaited", []byte("x"), true); close(done) }()
	eventually(t, "awaited write queued", func() bool {
		mgr.shards[0].mu.Lock()
		defer mgr.shards[0].mu.Unlock()
		_, ok := mgr.shards[0].pending["awaited"]
		return ok
	})
	close(st.gate)
	<-done

	st.mu.Lock()
	defer st.mu.Unlock()
	pos := -1
	for i, id := range st.order {
		if id == "awaited" {
			pos = i
		}
	}
	// Written in the first batch after the one in flight, not after 200 routine writes.
	if pos < 0 || pos > opts.PersistBatch {
		t.Fatalf("awaited write was write #%d of %d", pos, len(st.order))
	}
}

// Fifth review, note 2: with more awaited writes than a batch holds arriving all the
// time, routine snapshots on that shard would have waited indefinitely. Awaited writes
// now take at most half a batch while routine ones are queued.
func TestReviewAwaitedWritesDoNotStarveRoutineOnes(t *testing.T) {
	st := &gatedStore{plainStore: newPlain(), gate: make(chan struct{})}
	opts := DefaultOptions()
	opts.PersistWorkers, opts.PersistBatch = 1, 8
	mgr := NewManager(opts, clock.Real{}, st, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer mgr.Shutdown(context.Background())
	sh := mgr.shards[0]
	queued := func() int { sh.mu.Lock(); defer sh.mu.Unlock(); return len(sh.pending) }

	mgr.save("in-flight", []byte("x"), false)
	eventually(t, "writer busy", func() bool { return queued() == 0 })
	for i := 0; i < 20; i++ {
		mgr.save(fmt.Sprintf("routine-%02d", i), []byte("x"), false)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ { // a burst of new matches, each waiting for its write
		wg.Add(1)
		go func(i int) { defer wg.Done(); mgr.save(fmt.Sprintf("awaited-%02d", i), []byte("x"), true) }(i)
	}
	eventually(t, "burst queued", func() bool { return queued() == 70 })
	close(st.gate)
	wg.Wait()

	st.mu.Lock()
	defer st.mu.Unlock()
	routine := 0
	for _, id := range st.order[1 : 1+opts.PersistBatch] { // the first batch after the one in flight
		if strings.HasPrefix(id, "routine-") {
			routine++
		}
	}
	if routine < opts.PersistBatch/2 {
		t.Fatalf("first batch after the burst had %d routine writes of %d: %v", routine, opts.PersistBatch, st.order[1:1+opts.PersistBatch])
	}
}

// withVersion rewrites a snapshot's version field.
func withVersion(t *testing.T, data []byte, v int) []byte {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	m["version"] = json.RawMessage(strconv.Itoa(v))
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Seventh review: any snapshot Restore could not read was deleted, and a version
// mismatch counted as unreadable. The first deploy that changed the format would have
// had restarting nodes delete their own final snapshots, and new nodes delete the matches
// they took over from old ones: every live match lost exactly when recovery should save
// it. Snapshots of an unknown version are now left alone.
func TestReviewUnknownSnapshotVersionIsKept(t *testing.T) {
	cfg := testConfig()
	m, _ := New("future", cfg, [2]string{"a", "b"}, 1, t0)
	data, err := m.FinalSnapshot(t0)
	if err != nil {
		t.Fatal(err)
	}
	newer := withVersion(t, data, snapshotVersion+1)
	if SnapshotSupported(newer) || !SnapshotSupported(data) {
		t.Fatal("SnapshotSupported does not tell the versions apart")
	}

	st := laggyStore{newPlain()}
	_ = st.mem.Save(context.Background(), "future", newer)
	mgr := NewManager(DefaultOptions(), clock.Real{}, st, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer mgr.Shutdown(context.Background())
	if n, err := mgr.Restore(context.Background()); err != nil || n != 0 {
		t.Fatalf("restored %d (err %v) from a snapshot of an unknown version", n, err)
	}
	if n := mgr.Adopt(map[string][]byte{"future": newer}); n != 0 {
		t.Fatalf("adopted %d from a snapshot of an unknown version", n)
	}
	time.Sleep(50 * time.Millisecond) // a delete would have been queued by now
	all, _ := st.LoadAll(context.Background())
	if string(all["future"]) != string(newer) {
		t.Fatal("a snapshot of an unknown version was deleted; a node of that version needed it")
	}
}

// The other half of the fix: a build that bumps the version registers a migration from
// the previous one, and the previous build's snapshots still restore.
func TestReviewSnapshotMigrationFromPreviousVersion(t *testing.T) {
	m, _ := New("old", testConfig(), [2]string{"a", "b"}, 1, t0)
	data, _ := m.FinalSnapshot(t0)
	old := withVersion(t, data, snapshotVersion-1)
	if _, err := Restore(old, testConfig(), t0); !errors.Is(err, ErrSnapshotVersion) {
		t.Fatalf("no migration registered: err = %v, want ErrSnapshotVersion", err)
	}
	called := false
	snapshotMigrations[snapshotVersion-1] = func(raw json.RawMessage) (json.RawMessage, error) {
		called = true
		return raw, nil
	}
	defer delete(snapshotMigrations, snapshotVersion-1)
	r, err := Restore(old, testConfig(), t0)
	if err != nil || !called || r.ID != "old" {
		t.Fatalf("restore through migration: err=%v called=%v", err, called)
	}
}
