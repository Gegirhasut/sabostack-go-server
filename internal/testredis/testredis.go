// Package testredis gives tests a Redis: an in-process miniredis by default, or a real
// server when SABO_TEST_REDIS=host:port is set. CI runs the Redis-backed tests both ways,
// because miniredis does not reproduce every behaviour of real Redis (Lua semantics,
// EVALSHA/NOSCRIPT, TTL precision) and the cluster's ownership rules live in Lua.
//
// The helpers mirror the miniredis methods the tests use, so a test is written once.
// A real server is flushed at the start of each test: run such packages with -p 1.
package testredis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Env names the variable that selects a real Redis server.
const Env = "SABO_TEST_REDIS"

// R is a Redis for one test.
type R struct {
	t      testing.TB
	Client *redis.Client
	mini   *miniredis.Miniredis // nil on a real server
	addr   string
}

// New returns a fresh, empty Redis for t.
func New(t testing.TB) *R {
	t.Helper()
	r := &R{t: t}
	if addr := os.Getenv(Env); addr != "" {
		r.addr = addr
		r.Client = redis.NewClient(&redis.Options{Addr: addr})
		if err := r.Client.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("real redis at %s: %v", addr, err)
		}
	} else {
		r.mini = miniredis.RunT(t)
		r.addr = r.mini.Addr()
		r.Client = redis.NewClient(&redis.Options{Addr: r.addr})
	}
	t.Cleanup(func() { r.Client.Close() })
	return r
}

// Real reports whether this is a real server.
func (r *R) Real() bool { return r.mini == nil }

// Addr is the server address, for clients a test creates itself.
func (r *R) Addr() string { return r.addr }

var bg = context.Background()

// Set sets key to value with no TTL.
func (r *R) Set(key, value string) error { return r.Client.Set(bg, key, value, 0).Err() }

// Del deletes key and reports whether it existed.
func (r *R) Del(key string) bool { return r.Client.Del(bg, key).Val() == 1 }

// Exists reports whether key exists.
func (r *R) Exists(key string) bool { return r.Client.Exists(bg, key).Val() == 1 }

// Members returns the members of the set at key.
func (r *R) Members(key string) ([]string, error) { return r.Client.SMembers(bg, key).Result() }

// FastForward moves time forward by d for every key's TTL. On miniredis that is its own
// clock; on a real server each key's remaining TTL is shortened by d, and keys whose TTL
// runs out are deleted, which is what waiting d would do to them.
func (r *R) FastForward(d time.Duration) {
	r.t.Helper()
	if r.mini != nil {
		r.mini.FastForward(d)
		return
	}
	iter := r.Client.Scan(bg, 0, "*", 1000).Iterator()
	for iter.Next(bg) {
		key := iter.Val()
		ttl, err := r.Client.PTTL(bg, key).Result()
		if err != nil {
			r.t.Fatalf("PTTL %s: %v", key, err)
		}
		switch {
		case ttl < 0: // no TTL (-1) or gone meanwhile (-2)
		case ttl <= d:
			r.Client.Del(bg, key)
		default:
			r.Client.PExpire(bg, key, ttl-d)
		}
	}
	if err := iter.Err(); err != nil {
		r.t.Fatalf("SCAN: %v", err)
	}
}

// SetError makes every command fail with msg ("" restores service). Only miniredis can
// do that: on a real server the test is skipped.
func (r *R) SetError(msg string) {
	r.t.Helper()
	if r.mini == nil {
		r.t.Skip("needs fault injection, which only miniredis provides")
	}
	r.mini.SetError(msg)
}
