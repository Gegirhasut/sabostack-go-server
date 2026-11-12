package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const lease = 10 * time.Second

func clusterNodes(t *testing.T, names ...string) (*miniredis.Miniredis, map[string]*Redis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	base := NewRedis(rdb, "c:", time.Hour)
	nodes := map[string]*Redis{}
	for _, n := range names {
		nodes[n] = base.ForNode(n, "wss://"+n+"/ws")
		if err := nodes[n].Heartbeat(context.Background(), lease); err != nil {
			t.Fatal(err)
		}
	}
	return mr, nodes
}

// A dead node's matches move to a live node; the dead node, if it comes back, can no
// longer write them.
func TestAdoptFromDeadNode(t *testing.T) {
	ctx := context.Background()
	mr, n := clusterNodes(t, "a", "b")
	for _, id := range []string{"m1", "m2"} {
		if err := n["a"].Save(ctx, id, []byte("snap-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := n["b"].AdoptOrphans(ctx, 10); len(got) != 0 {
		t.Fatalf("adopted from a live node: %q", got)
	}

	mr.FastForward(lease + time.Second) // a stops heartbeating
	if err := n["b"].Heartbeat(ctx, lease); err != nil {
		t.Fatal(err)
	}
	got, err := n["b"].AdoptOrphans(ctx, 10)
	if err != nil || len(got) != 2 || string(got["m1"]) != "snap-m1" {
		t.Fatalf("adopted %q, err %v", got, err)
	}
	if node, url, ok, _ := n["b"].Owner(ctx, "m1"); !ok || node != "b" || url != "wss://b/ws" {
		t.Fatalf("owner after adoption: %s %s %v", node, url, ok)
	}
	if all, _ := n["b"].LoadAll(ctx); len(all) != 2 {
		t.Fatalf("b restores %d matches after adopting, want 2", len(all))
	}

	// a comes back (a zombie, or restarted with the same id).
	if err := n["a"].Save(ctx, "m1", []byte("stale")); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("old owner overwrote an adopted match: %v", err)
	}
	if all, _ := n["a"].LoadAll(ctx); len(all) != 0 {
		t.Fatalf("old owner would restore %q", all)
	}
	if again, _ := n["b"].AdoptOrphans(ctx, 10); len(again) != 0 {
		t.Fatalf("adopted twice: %q", again)
	}
}

// Adoption is capped per scan, and concurrent adopters never take the same match.
func TestAdoptionSpreadsAndNeverDuplicates(t *testing.T) {
	ctx := context.Background()
	mr, n := clusterNodes(t, "dead", "b", "c")
	for i := 0; i < 7; i++ {
		if err := n["dead"].Save(ctx, fmt.Sprintf("m%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	mr.FastForward(lease + time.Second)
	_ = n["b"].Heartbeat(ctx, lease)
	_ = n["c"].Heartbeat(ctx, lease)

	owner := map[string]string{}
	for round := 0; round < 10; round++ {
		for _, name := range []string{"b", "c"} {
			got, err := n[name].AdoptOrphans(ctx, 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) > 2 {
				t.Fatalf("%s adopted %d in one scan, cap is 2", name, len(got))
			}
			for id := range got {
				if prev, dup := owner[id]; dup {
					t.Fatalf("%s adopted %s already adopted by %s", name, id, prev)
				}
				owner[id] = name
			}
		}
	}
	if len(owner) != 7 {
		t.Fatalf("adopted %d of 7 matches", len(owner))
	}
	if members, _ := mr.Members("c:nodes"); len(members) != 2 {
		t.Fatalf("dead node still registered once drained: %v", members)
	}
}

// Release hands matches over at once (graceful shutdown), without waiting for the lease.
func TestReleaseHandsOverImmediately(t *testing.T) {
	ctx := context.Background()
	_, n := clusterNodes(t, "a", "b")
	if err := n["a"].Save(ctx, "m", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := n["a"].Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := n["b"].AdoptOrphans(ctx, 10); len(got) != 1 {
		t.Fatalf("adopted %d after release, want 1", len(got))
	}
}
