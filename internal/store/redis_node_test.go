package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/testredis"
)

func TestRedisNodeModeContract(t *testing.T) {
	mr := testredis.New(t)
	rdb := mr.Client
	defer rdb.Close()
	testContract(t, NewRedis(rdb, "test:", time.Minute).ForNode("a", "wss://a.example/ws"))
}

// Two servers sharing one Redis: each restores only its own matches, neither can write a
// match the other owns, and either can tell a client where a match lives.
func TestRedisNodeOwnership(t *testing.T) {
	mr := testredis.New(t)
	rdb := mr.Client
	defer rdb.Close()
	ctx := context.Background()
	base := NewRedis(rdb, "test:", time.Minute)
	a, b := base.ForNode("a", "wss://a.example/ws"), base.ForNode("b", "wss://b.example/ws")

	if err := a.Save(ctx, "m1", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(ctx, "m2", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(ctx, "m1", []byte("stolen")); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("b overwrote a's match: err=%v", err)
	}
	if err := b.Delete(ctx, "m1"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("b deleted a's match: err=%v", err)
	}

	all, err := a.LoadAll(ctx)
	if err != nil || len(all) != 1 || string(all["m1"]) != "one" {
		t.Fatalf("a restores %q (err %v), want only m1=one", all, err)
	}
	all, _ = b.LoadAll(ctx)
	if len(all) != 1 || string(all["m2"]) != "two" {
		t.Fatalf("b restores %q, want only m2", all)
	}

	node, url, ok, err := b.Owner(ctx, "m1")
	if err != nil || !ok || node != "a" || url != "wss://a.example/ws" {
		t.Fatalf("Owner(m1) = %q %q %v %v", node, url, ok, err)
	}
	if _, _, ok, _ := b.Owner(ctx, "nope"); ok {
		t.Fatal("unknown match has an owner")
	}

	// Once a deletes its finished match, the id is free again.
	if err := a.Delete(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := a.Owner(ctx, "m1"); ok {
		t.Fatal("owner key left after delete")
	}

	// Batches report a result per op.
	errs := a.Apply(ctx, []Op{{ID: "m3", Data: []byte("x")}, {ID: "m2", Data: []byte("y")}, {ID: "m3"}})
	if errs[0] != nil || !errors.Is(errs[1], ErrNotOwner) || errs[2] != nil {
		t.Fatalf("batch errs = %v", errs)
	}
}

// Legacy mode keeps the single "<prefix>matches" set and no owner keys.
func TestRedisLegacyHasNoDirectory(t *testing.T) {
	mr := testredis.New(t)
	rdb := mr.Client
	defer rdb.Close()
	s := NewRedis(rdb, "test:", time.Minute)
	ctx := context.Background()
	if err := s.Save(ctx, "m", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := s.Owner(ctx, "m"); ok || err != nil {
		t.Fatalf("legacy Owner = %v %v", ok, err)
	}
	if !mr.Exists("test:match:m") || mr.Exists("test:owner:m") {
		t.Fatal("unexpected legacy key layout")
	}
}
