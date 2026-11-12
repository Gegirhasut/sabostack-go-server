package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Both implementations must satisfy the same contract.
func testContract(t *testing.T, s Store) {
	ctx := context.Background()
	if err := s.Save(ctx, "a", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "a", []byte(`{"n":2}`)); err != nil { // upsert
		t.Fatal(err)
	}
	if err := s.Save(ctx, "b", []byte(`{"n":3}`)); err != nil {
		t.Fatal(err)
	}
	all, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || string(all["a"]) != `{"n":2}` || string(all["b"]) != `{"n":3}` {
		t.Fatalf("LoadAll = %q", all)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	all, _ = s.LoadAll(ctx)
	if _, ok := all["a"]; ok || len(all) != 1 {
		t.Fatalf("after delete LoadAll = %q", all)
	}
}

func TestMemory(t *testing.T) { testContract(t, NewMemory()) }

func TestRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	s := NewRedis(rdb, "test:", time.Minute)
	testContract(t, s)

	// An expired snapshot disappears from LoadAll and its id is cleaned from the set.
	ctx := context.Background()
	if err := s.Save(ctx, "old", []byte("x")); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Minute)
	all, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("expired snapshots returned: %q", all)
	}
	if members, _ := rdb.SMembers(ctx, "test:matches").Result(); len(members) != 0 {
		t.Fatalf("dangling ids left: %v", members)
	}
}
