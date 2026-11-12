package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/redis/go-redis/v9"
)

// Redis stores snapshots in Redis.
//
// Legacy (single-node) mode, NewRedis: each snapshot lives under "<prefix>match:<id>" with
// a TTL, and the set "<prefix>matches" lists the ids so LoadAll needs no KEYS/SCAN.
//
// Node mode, ForNode: several game servers can share one Redis. Every match has an owner
// key "<prefix>owner:<id>" = "<node> <publicURL>". Saves and deletes run as Lua scripts
// that refuse to touch a match another node owns (ErrNotOwner), so two processes can
// never both drive one match. Each node restores only its own set
// "<prefix>node:<node>:matches", and Owner lets any node redirect a client to the right
// server. Node ids must be stable across restarts (e.g. a StatefulSet pod name).
//
// The scripts touch keys in different hash slots, so this targets a single Redis (or a
// Sentinel-managed primary), not Redis Cluster.
type Redis struct {
	rdb    redis.UniversalClient
	prefix string
	ttl    time.Duration
	node   string // empty = legacy single-node mode
	url    string // this node's public WebSocket URL, for redirects
	// leaving is set by Leaving: this node is shutting down and must not be advertised
	// as a redirect target any more (shared by the copies ForNode hands out).
	leaving *atomic.Bool
	// adoptable, if set, tells AdoptOrphans which snapshots this node can run; the others
	// are left with their dead owner for a node that can (see SetAdoptFilter).
	adoptable func(data []byte) bool
}

// NewRedis wraps a client in legacy single-node mode. ttl bounds how long an orphaned
// snapshot (a server that died and never came back) lingers.
func NewRedis(rdb redis.UniversalClient, prefix string, ttl time.Duration) *Redis {
	return &Redis{rdb: rdb, prefix: prefix, ttl: ttl}
}

// ForNode returns a copy of s in node mode: matches are owned by node, and clients that
// reach another server are redirected to publicURL.
// ValidateNode checks a node id and public URL before they are written into Redis. The
// owner value is "<node> <url>" and matchmaking tickets are tab-separated, so whitespace
// in either would make other nodes misread them; ids are also used inside key names.
func ValidateNode(node, publicURL string) error {
	if node == "" || len(node) > 64 {
		return fmt.Errorf("node id %q: must be 1-64 characters", node)
	}
	for _, r := range node {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return fmt.Errorf("node id %q: only letters, digits, '-', '_' and '.' are allowed", node)
		}
	}
	if strings.ContainsFunc(publicURL, unicode.IsSpace) {
		return fmt.Errorf("public url %q: must not contain whitespace", publicURL)
	}
	return nil
}

func (s *Redis) ForNode(node, publicURL string) *Redis {
	c := *s
	c.node, c.url = node, publicURL
	c.leaving = new(atomic.Bool)
	return &c
}

// chunk bounds MGET sizes so a restore never sends one giant command.
const chunk = 500

func (s *Redis) key(id string) string { return s.prefix + "match:" + id }

func (s *Redis) keys(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = s.key(id)
	}
	return out
}
func (s *Redis) ownerKey(id string) string { return s.prefix + "owner:" + id }
func (s *Redis) setKey() string {
	if s.node == "" {
		return s.prefix + "matches"
	}
	return s.prefix + "node:" + s.node + ":matches"
}
func (s *Redis) ownerValue() string { return s.node + " " + s.url }

// KEYS: data, owner, node set. ARGV: data, ttl ms, owner value, node, id.
var saveScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[2])
if cur and string.sub(cur, 1, #ARGV[4] + 1) ~= ARGV[4] .. ' ' then
  redis.call('SREM', KEYS[3], ARGV[5])
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[2])
redis.call('SADD', KEYS[3], ARGV[5])
return 1
`)

// KEYS: data, owner, node set. ARGV: node, id.
var deleteScript = redis.NewScript(`
redis.call('SREM', KEYS[3], ARGV[2])
local cur = redis.call('GET', KEYS[2])
if cur and string.sub(cur, 1, #ARGV[1] + 1) ~= ARGV[1] .. ' ' then
  return 0
end
redis.call('DEL', KEYS[1], KEYS[2])
return 1
`)

func (s *Redis) Save(ctx context.Context, id string, data []byte) error {
	return s.Apply(ctx, []Op{{ID: id, Data: data}})[0]
}

func (s *Redis) Delete(ctx context.Context, id string) error {
	return s.Apply(ctx, []Op{{ID: id}})[0]
}

// Apply implements Batcher: all ops go out in one pipeline (one round trip).
func (s *Redis) Apply(ctx context.Context, ops []Op) []error {
	errs := make([]error, len(ops))
	if len(ops) == 0 {
		return errs
	}
	if s.node == "" {
		_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			for _, op := range ops {
				if op.Data == nil {
					p.Del(ctx, s.key(op.ID))
					p.SRem(ctx, s.setKey(), op.ID)
				} else {
					p.Set(ctx, s.key(op.ID), op.Data, s.ttl)
					p.SAdd(ctx, s.setKey(), op.ID)
				}
			}
			return nil
		})
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	cmds := s.runScripts(ctx, ops, true)
	var retry []int
	for i, c := range cmds {
		if err := c.Err(); err != nil && isNoScript(err) {
			retry = append(retry, i)
		}
	}
	if len(retry) > 0 { // Redis restarted and lost its script cache: send the bodies
		sub := make([]Op, len(retry))
		for j, i := range retry {
			sub[j] = ops[i]
		}
		for j, c := range s.runScripts(ctx, sub, false) {
			cmds[retry[j]] = c
		}
	}
	for i, c := range cmds {
		n, err := c.Int()
		switch {
		case err != nil:
			errs[i] = err
		case n == 0:
			errs[i] = ErrNotOwner
		}
	}
	return errs
}

func (s *Redis) runScripts(ctx context.Context, ops []Op, sha bool) []*redis.Cmd {
	cmds := make([]*redis.Cmd, len(ops))
	ttl := s.ttl.Milliseconds()
	_, _ = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, op := range ops {
			keys := []string{s.key(op.ID), s.ownerKey(op.ID), s.setKey()}
			script, args := deleteScript, []any{s.node, op.ID}
			if op.Data != nil {
				script, args = saveScript, []any{op.Data, ttl, s.ownerValue(), s.node, op.ID}
			}
			if sha {
				cmds[i] = script.EvalSha(ctx, p, keys, args...)
			} else {
				cmds[i] = script.Eval(ctx, p, keys, args...)
			}
		}
		return nil
	})
	return cmds
}

func isNoScript(err error) bool { return strings.HasPrefix(err.Error(), "NOSCRIPT") }

func (s *Redis) LoadAll(ctx context.Context) (map[string][]byte, error) {
	ids, err := s.rdb.SMembers(ctx, s.setKey()).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(ids))
	var stale []any
	for start := 0; start < len(ids); start += chunk {
		part := ids[start:min(start+chunk, len(ids))]
		keys := make([]string, len(part))
		for i, id := range part {
			keys[i] = s.key(id)
		}
		vals, err := s.rdb.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, err
		}
		var owners []any
		if s.node != "" {
			for i, id := range part {
				keys[i] = s.ownerKey(id)
			}
			if owners, err = s.rdb.MGet(ctx, keys...).Result(); err != nil {
				return nil, err
			}
		}
		for i, v := range vals {
			str, ok := v.(string)
			if !ok { // nil: the snapshot expired, drop the dangling id
				stale = append(stale, part[i])
				continue
			}
			if owners != nil {
				if o, ok := owners[i].(string); ok && !strings.HasPrefix(o, s.node+" ") {
					stale = append(stale, part[i]) // another node took it over
					continue
				}
			}
			out[part[i]] = []byte(str)
		}
	}
	if len(stale) > 0 {
		if err := s.rdb.SRem(ctx, s.setKey(), stale...).Err(); err != nil && !errors.Is(err, redis.Nil) {
			return out, err
		}
	}
	return out, nil
}

// Owner implements Directory. In legacy mode nothing is known about other nodes.
//
// If the owner is a cluster member whose heartbeat has expired (it died and its matches
// are about to be adopted), url is empty: sending the client to a dead address would only
// make it wait for a connect timeout.
func (s *Redis) Owner(ctx context.Context, id string) (node, url string, ok bool, err error) {
	if s.node == "" {
		return "", "", false, nil
	}
	v, err := s.rdb.Get(ctx, s.ownerKey(id)).Result()
	if errors.Is(err, redis.Nil) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	node, url, _ = strings.Cut(v, " ")
	var alive *redis.StringCmd
	var isMember *redis.BoolCmd
	_, err = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		alive = p.Get(ctx, s.aliveKey(node))
		isMember = p.SIsMember(ctx, s.nodesKey(), node)
		return nil
	})
	if (err == nil || errors.Is(err, redis.Nil)) && isMember.Val() {
		if v, aerr := alive.Result(); errors.Is(aerr, redis.Nil) || (aerr == nil && v == leavingMark) {
			url = "" // dead or shutting down: the match is moving
		}
	}
	return node, url, true, nil
}
