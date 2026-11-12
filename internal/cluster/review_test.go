package cluster

// Regression tests for the external code review of 2026-11-12.

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// hangingCluster passes heartbeats through until hang is set, then lets them hang until
// their context ends: Redis stopped answering (the worst case for fencing; a refused
// connection fails fast).
type hangingCluster struct {
	store.Cluster
	hang     atomic.Bool
	lastSent atomic.Int64 // unix nanos when the last successful heartbeat was sent
}

func (h *hangingCluster) Heartbeat(ctx context.Context, ttl time.Duration) error {
	if h.hang.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	sent := time.Now()
	err := h.Cluster.Heartbeat(ctx, ttl)
	if err == nil {
		h.lastSent.Store(sent.UnixNano())
	}
	return err
}

// Bug: a node fenced itself only after a full lease without a successful heartbeat,
// counted from when Redis answered, and only noticed at the end of a heartbeat that could
// hang for another third of the lease. Other nodes may adopt as soon as the alive key
// expires (a lease after the last heartbeat was sent), so a match ran on two nodes for
// several seconds and the moves made on the stale copy were lost.
func TestReviewNodeFencesBeforeItsLeaseExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rs := store.NewRedis(rdb, "n:", time.Hour).ForNode("a", "ws://a")
	met := metrics.New(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := match.NewManager(match.DefaultOptions(), clock.Real{}, rs, met, log)
	defer mgr.Shutdown(context.Background())

	const lease = 600 * time.Millisecond
	hc := &hangingCluster{Cluster: rs}
	node := NewNode(hc, mgr, met, log, Options{Lease: lease, AdoptEvery: time.Hour, AdoptBatch: 10}, nil)
	if err := node.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go node.Run(ctx)
	if _, _, err := mgr.Create([2]string{"x", "y"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a few heartbeats", func() bool { return hc.lastSent.Load() != 0 })

	hc.hang.Store(true)
	eventually(t, "matches stopped", func() bool { return mgr.Len() == 0 })
	stoppedAfter := time.Since(time.Unix(0, hc.lastSent.Load()))
	// The alive key expires at lastSent + lease at the earliest; the matches must be gone
	// before that, with room to spare.
	if stoppedAfter >= lease*9/10 {
		t.Fatalf("matches stopped %s after the last heartbeat was sent; the lease (%s) runs out at that point and other nodes may already run them",
			stoppedAfter.Round(time.Millisecond), lease)
	}
}

// slowAdopter makes every adoption scan take far longer than the lease.
type slowAdopter struct {
	*hangingCluster
	scanning chan struct{}
}

func (s *slowAdopter) AdoptOrphans(ctx context.Context, max int) (map[string][]byte, error) {
	select {
	case s.scanning <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
	}
	return nil, nil
}

// Bug: adoption scans ran on the heartbeat's goroutine, so a slow scan (its timeout was
// 5 s, half the default lease) delayed heartbeats; the node's alive key could expire
// while it was healthy, and other nodes would take over its matches.
func TestReviewSlowAdoptionDoesNotDelayHeartbeats(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rs := store.NewRedis(rdb, "n:", time.Hour).ForNode("a", "ws://a")
	met := metrics.New(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := match.NewManager(match.DefaultOptions(), clock.Real{}, rs, met, log)
	defer mgr.Shutdown(context.Background())

	const lease = 300 * time.Millisecond
	sa := &slowAdopter{hangingCluster: &hangingCluster{Cluster: rs}, scanning: make(chan struct{}, 1)}
	node := NewNode(sa, mgr, met, log, Options{Lease: lease, AdoptEvery: 20 * time.Millisecond, AdoptBatch: 10}, nil)
	if err := node.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go node.Run(ctx)
	<-sa.scanning // a scan is now stuck for 5 s

	for end := time.Now().Add(3 * lease); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if since := time.Since(time.Unix(0, sa.lastSent.Load())); sa.lastSent.Load() != 0 && since >= lease {
			t.Fatalf("no heartbeat for %s during a slow adoption scan (lease %s)", since.Round(time.Millisecond), lease)
		}
	}
	if sa.lastSent.Load() == 0 {
		t.Fatal("no heartbeat at all during a slow adoption scan")
	}
}

func shortRequeue(t *testing.T) {
	oldAfter, oldEvery := requeueAfter, checkLostEvery
	requeueAfter, checkLostEvery = 100*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { requeueAfter, checkLostEvery = oldAfter, oldEvery })
}

// popAsRemote takes the oldest ticket out of the queue the way another node's pair script
// would, claiming it for that node. It returns the ticket id.
func popAsRemote(t *testing.T, e *mmEnv, node string) string {
	t.Helper()
	raw, err := e.rdb.LPop(context.Background(), "t:mm:queue").Result()
	if err != nil {
		t.Fatal(err)
	}
	tk, ok := parseTicket(raw)
	if !ok {
		t.Fatalf("bad ticket %q", raw)
	}
	e.mr.Set("t:mm:claim:"+tk.id, node)
	return tk.id
}

func queueLen(e *mmEnv) int64 {
	n, _ := e.rdb.LLen(context.Background(), "t:mm:queue").Result()
	return n
}

// Bug: when the pub/sub message with a player's assignment was lost (or came late), the
// player's ticket was requeued after 3 s and could be put into a second match, while the
// first one waited for them until opponent_never_joined. The popping node now records the
// assignment before publishing, and the waiting node delivers the record.
func TestReviewLostAssignmentMessageIsRecovered(t *testing.T) {
	shortRequeue(t)
	e := newEnv(t)
	n1 := e.node(t, "n1")
	e.mr.Set("t:alive:n2", "ws://n2")
	tk := n1.JoinContext(context.Background(), "alice")
	id := popAsRemote(t, e, "n2")
	// n2 created the match and recorded the assignment; its publish never arrived.
	e.mr.Set("t:mm:assign:"+id, `{"ticket":"`+id+`","matchId":"n2-m1","seat":1,"token":"tok","opponent":"bob","url":"ws://n2"}`)

	select {
	case a := <-tk.C():
		if a.MatchID != "n2-m1" || a.URL != "ws://n2" || a.Token != "tok" {
			t.Fatalf("recovered assignment %+v", a)
		}
	case <-time.After(10 * requeueAfter):
		t.Fatalf("assignment never delivered (queue length %d: the ticket was requeued instead)", queueLen(e))
	}
	if n := queueLen(e); n != 0 {
		t.Fatalf("ticket requeued although its assignment existed (queue length %d)", n)
	}
}

// Bug (same fix): a popping node that was alive but slow (create took longer than 3 s)
// had its tickets requeued under it, so its players could end up in two matches. A
// ticket claimed by a live node is never requeued; one claimed by a dead node still is.
func TestReviewLiveClaimerIsNotRequeuedDeadOneIs(t *testing.T) {
	shortRequeue(t)
	e := newEnv(t)
	n1 := e.node(t, "n1")
	e.mr.Set("t:alive:slow", "ws://slow")
	n1.JoinContext(context.Background(), "alice")
	popAsRemote(t, e, "slow")
	time.Sleep(10 * requeueAfter)
	if n := queueLen(e); n != 0 {
		t.Fatalf("ticket claimed by a live node was requeued (queue length %d)", n)
	}

	e.mr.Del("t:alive:slow") // the claimer died before announcing the match
	deadline := time.Now().Add(5 * time.Second)
	for queueLen(e) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("ticket of a dead claimer was not requeued")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Second review: a match that started on a fenced node (an adoption racing the fence, or
// a Restore already under way when the lease was lost again) was never stopped: its writes
// failed with plain Redis errors, not ErrNotOwner, so it ran as a zombie that players
// reconnecting to this node could join. Fencing and adoption now exclude each other, and
// every failed heartbeat while fenced stops whatever is running.
func TestReviewMatchStartedWhileFencedIsStopped(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rs := store.NewRedis(rdb, "n:", time.Hour).ForNode("a", "ws://a")
	met := metrics.New(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := match.NewManager(match.DefaultOptions(), clock.Real{}, rs, met, log)
	defer mgr.Shutdown(context.Background())

	const lease = 300 * time.Millisecond
	hc := &hangingCluster{Cluster: rs}
	node := NewNode(hc, mgr, met, log, Options{Lease: lease, AdoptEvery: time.Hour, AdoptBatch: 10}, nil)
	if err := node.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go node.Run(ctx)
	if _, _, err := mgr.Create([2]string{"x", "y"}); err != nil {
		t.Fatal(err)
	}
	hc.hang.Store(true)
	eventually(t, "fenced", func() bool { return mgr.Len() == 0 })

	// The race, forced: a match starts on the node after it fenced itself.
	if _, _, err := mgr.Create([2]string{"zombie", "copy"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * lease) // a couple of heartbeat intervals
	for mgr.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("a match started on a fenced node is still running after %s", 2*lease)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waiting(m *Matchmaker) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.waiting)
}

// Second review: a player whose ticket had been popped, and who then disconnected while
// the assignment was lost, stayed in m.waiting forever (LRem found nothing, so the waiter
// was never forgotten) and cost a Redis call every second. Leaked for good.
func TestReviewGoneTicketAfterPopIsForgotten(t *testing.T) {
	shortRequeue(t)
	e := newEnv(t)
	n1 := e.node(t, "n1")

	// Assignment lost for good: the popping node died before recording it.
	ctxLost, leaveLost := context.WithCancel(context.Background())
	n1.JoinContext(ctxLost, "lost")
	popAsRemote(t, e, "dead-node")
	leaveLost()

	// Assignment recorded but its message lost: delivered (and forgotten) on the way out.
	e.mr.Set("t:alive:n2", "ws://n2")
	ctxRec, leaveRec := context.WithCancel(context.Background())
	n1.JoinContext(ctxRec, "recorded")
	id := popAsRemote(t, e, "n2")
	e.mr.Set("t:mm:assign:"+id, `{"ticket":"`+id+`","matchId":"n2-m1","seat":0,"token":"tok","opponent":"x","url":"ws://n2"}`)
	leaveRec()

	deadline := time.Now().Add(20 * checkLostEvery)
	for waiting(n1) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d waiter(s) of disconnected players still tracked", waiting(n1))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := queueLen(e); n != 0 {
		t.Fatalf("a disconnected player's ticket was requeued (queue length %d)", n)
	}
}
