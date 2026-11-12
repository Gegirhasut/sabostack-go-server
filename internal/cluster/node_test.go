package cluster

import (
	"context"
	"io"
	"log/slog"
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

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// When Redis is down for longer than the lease, the node stops its matches (it can no
// longer prove it owns them). When Redis comes back and nobody adopted them, the node
// brings them back from their snapshots instead of leaving them orphaned.
func TestLeaseLossStopsAndRecoveryRestores(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	st := store.NewRedis(rdb, "n:", time.Hour).ForNode("a", "ws://a")
	met := metrics.New(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := match.DefaultOptions()
	opts.StoreTimeout = 100 * time.Millisecond
	mgr := match.NewManager(opts, clock.Real{}, st, met, log)
	defer mgr.Shutdown(context.Background())

	node := NewNode(st, mgr, met, log, Options{Lease: 300 * time.Millisecond, AdoptEvery: time.Hour, AdoptBatch: 10}, nil)
	if err := node.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go node.Run(ctx)

	id, _, err := mgr.Create([2]string{"x", "y"})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "first snapshot", func() bool { return mr.Exists("n:match:" + id) })

	mr.SetError("connection refused") // Redis down for everyone
	eventually(t, "matches stopped after the lease", func() bool { return mgr.Len() == 0 })

	mr.SetError("")
	eventually(t, "match restored after the lease came back", func() bool {
		_, ok := mgr.Get(id)
		return ok
	})
}
