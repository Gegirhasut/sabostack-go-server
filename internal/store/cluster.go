package store

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cluster is implemented by stores that coordinate several live nodes: a node proves it is
// alive with a heartbeat, and the matches of a node whose heartbeat stopped can be taken
// over by the others.
type Cluster interface {
	// Heartbeat marks this node alive for ttl.
	Heartbeat(ctx context.Context, ttl time.Duration) error
	// AdoptOrphans moves up to max matches of dead nodes to this node and returns their
	// snapshots. Every match is adopted by exactly one node.
	AdoptOrphans(ctx context.Context, max int) (map[string][]byte, error)
	// Leaving announces that this node is shutting down: it stays alive (nobody adopts
	// its matches while it writes their final snapshots), but other nodes stop sending
	// players to it and answer "match_moving" instead.
	Leaving(ctx context.Context) error
	// Release ends this node's heartbeat at once, so after a graceful shutdown (final
	// snapshots written) the other nodes adopt its matches without waiting for the ttl.
	Release(ctx context.Context) error
}

// leavingMark is the alive key's value while its node shuts down (normally it holds the
// node's URL).
const leavingMark = "-leaving-"

// Leaving implements Cluster.
func (s *Redis) Leaving(ctx context.Context) error {
	if s.node == "" {
		return nil
	}
	s.leaving.Store(true) // later heartbeats keep the mark
	err := s.rdb.SetArgs(ctx, s.aliveKey(s.node), leavingMark, redis.SetArgs{KeepTTL: true, Mode: "XX"}).Err()
	if errors.Is(err, redis.Nil) {
		return nil // the key had already expired: the node already counts as dead
	}
	return err
}

// Release implements Cluster.
func (s *Redis) Release(ctx context.Context) error {
	if s.node == "" {
		return nil
	}
	return s.rdb.Del(ctx, s.aliveKey(s.node)).Err()
}

// adoptLockTTL bounds how long a crashed adopter can block a dead node's matches.
const adoptLockTTL = 30 * time.Second

func (s *Redis) aliveKey(node string) string { return s.prefix + "alive:" + node }
func (s *Redis) nodesKey() string            { return s.prefix + "nodes" }
func (s *Redis) adoptKey(node string) string { return s.prefix + "adopt:" + node }
func (s *Redis) nodeSetKey(node string) string {
	return s.prefix + "node:" + node + ":matches"
}

// Heartbeat implements Cluster. It is one key per node, not per match, so its cost does
// not grow with the number of matches.
func (s *Redis) Heartbeat(ctx context.Context, ttl time.Duration) error {
	if s.node == "" {
		return errors.New("store: heartbeat needs node mode")
	}
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		v := s.url
		if s.leaving.Load() {
			v = leavingMark
		}
		p.Set(ctx, s.aliveKey(s.node), v, ttl)
		p.SAdd(ctx, s.nodesKey(), s.node)
		return nil
	})
	return err
}

// KEYS: data, owner, dead node set, my node set. ARGV: dead node, my owner value, id, ttl ms.
// Returns the snapshot if this node now owns the match, nil otherwise.
var adoptScript = redis.NewScript(`
redis.call('SREM', KEYS[3], ARGV[3])
local cur = redis.call('GET', KEYS[2])
if cur and string.sub(cur, 1, #ARGV[1] + 1) ~= ARGV[1] .. ' ' then
  return false
end
local data = redis.call('GET', KEYS[1])
if not data then
  return false
end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
redis.call('SADD', KEYS[4], ARGV[3])
return data
`)

// AdoptOrphans implements Cluster.
//
// A node is dead when its alive key has expired. One adopter at a time works on a dead
// node (a SET NX lock), taking at most max matches per call, so the matches of a dead node
// spread over the live nodes that scan after it. Ownership moves inside a Lua script that
// checks the current owner, so a match is never adopted twice, and the old owner, if it
// comes back, gets ErrNotOwner on its next write and stops driving the match.
func (s *Redis) AdoptOrphans(ctx context.Context, max int) (map[string][]byte, error) {
	if s.node == "" {
		return nil, errors.New("store: adopt needs node mode")
	}
	nodes, err := s.rdb.SMembers(ctx, s.nodesKey()).Result()
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for _, dead := range nodes {
		if dead == s.node || len(out) >= max {
			continue
		}
		alive, err := s.rdb.Exists(ctx, s.aliveKey(dead)).Result()
		if err != nil {
			return out, err
		}
		if alive == 1 {
			continue
		}
		got, err := s.rdb.SetNX(ctx, s.adoptKey(dead), s.node, adoptLockTTL).Result()
		if err != nil || !got {
			continue // another node is adopting this one
		}
		err = s.adoptFrom(ctx, dead, max-len(out), out)
		s.rdb.Del(ctx, s.adoptKey(dead))
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func (s *Redis) adoptFrom(ctx context.Context, dead string, n int, out map[string][]byte) error {
	ids, err := s.rdb.SRandMemberN(ctx, s.nodeSetKey(dead), int64(n)).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		// Nothing left: forget the node (it re-registers itself if it comes back).
		return s.rdb.SRem(ctx, s.nodesKey(), dead).Err()
	}
	cmds := make([]*redis.Cmd, len(ids))
	ttl := s.ttl.Milliseconds()
	_, _ = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, id := range ids {
			keys := []string{s.key(id), s.ownerKey(id), s.nodeSetKey(dead), s.setKey()}
			cmds[i] = adoptScript.Eval(ctx, p, keys, dead, s.ownerValue(), id, ttl)
		}
		return nil
	})
	for i, c := range cmds {
		v, err := c.Text()
		switch {
		case errors.Is(err, redis.Nil):
			continue // gone or owned elsewhere
		case err != nil:
			return err
		}
		out[ids[i]] = []byte(v)
	}
	return nil
}
