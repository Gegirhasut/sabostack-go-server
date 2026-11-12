// Package testredis gives tests a Redis: an in-process miniredis by default, or a real
// server when SABO_TEST_REDIS=host:port/db is set. CI runs the Redis-backed tests both ways,
// because miniredis does not reproduce every behaviour of real Redis (Lua semantics,
// EVALSHA/NOSCRIPT, TTL precision) and the cluster's ownership rules live in Lua.
//
// The helpers mirror the miniredis methods the tests use, so a test is written once.
// The chosen database of a real server is flushed at the start of each test, so it must
// be named explicitly and must not be database 0 (where real data usually lives): a
// production address set by mistake fails the test instead of being wiped. Run such
// packages with -p 1.
package testredis

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Env names the variable that selects a real Redis server: host:port/db, db 1-15.
const Env = "SABO_TEST_REDIS"

// parseTarget splits host:port/db and refuses a missing or zero database.
func parseTarget(v string) (addr string, db int, err error) {
	i := strings.LastIndexByte(v, '/')
	if i < 0 {
		return "", 0, fmt.Errorf("%s=%q names no database; use host:port/15 (the database is flushed by every test)", Env, v)
	}
	db, err = strconv.Atoi(v[i+1:])
	if err != nil || db <= 0 {
		return "", 0, fmt.Errorf("%s=%q: database must be a number from 1 (0 is where real data usually lives, and it is flushed by every test)", Env, v)
	}
	return v[:i], db, nil
}

// R is a Redis for one test.
type R struct {
	t      testing.TB
	Client *redis.Client
	mini   *miniredis.Miniredis // nil on a real server
	addr   string
	db     int
}

// New returns a fresh, empty Redis for t.
func New(t testing.TB) *R {
	t.Helper()
	r := &R{t: t}
	if v := os.Getenv(Env); v != "" {
		addr, db, err := parseTarget(v)
		if err != nil {
			t.Fatal(err)
		}
		r.addr, r.db = addr, db
		r.Client = redis.NewClient(&redis.Options{Addr: addr, DB: db})
		if err := r.Client.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("real redis at %s db %d: %v", addr, db, err)
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

// Addr is the server address, for clients a test creates itself (with Options).
func (r *R) Addr() string { return r.addr }

// Options returns client options for this Redis (address and database).
func (r *R) Options() *redis.Options { return &redis.Options{Addr: r.addr, DB: r.db} }

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
