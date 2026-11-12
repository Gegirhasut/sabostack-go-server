package store

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis stores each snapshot under "<prefix>match:<id>" with a TTL, and keeps the set of
// live ids in "<prefix>matches" so LoadAll does not need KEYS/SCAN.
type Redis struct {
	rdb    redis.UniversalClient
	prefix string
	ttl    time.Duration
}

// NewRedis wraps a client. ttl bounds how long an orphaned snapshot (a server that died
// and never came back) lingers.
func NewRedis(rdb redis.UniversalClient, prefix string, ttl time.Duration) *Redis {
	return &Redis{rdb: rdb, prefix: prefix, ttl: ttl}
}

func (s *Redis) key(id string) string { return s.prefix + "match:" + id }
func (s *Redis) setKey() string       { return s.prefix + "matches" }

func (s *Redis) Save(ctx context.Context, id string, data []byte) error {
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, s.key(id), data, s.ttl)
		p.SAdd(ctx, s.setKey(), id)
		return nil
	})
	return err
}

func (s *Redis) Delete(ctx context.Context, id string) error {
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, s.key(id))
		p.SRem(ctx, s.setKey(), id)
		return nil
	})
	return err
}

func (s *Redis) LoadAll(ctx context.Context) (map[string][]byte, error) {
	ids, err := s.rdb.SMembers(ctx, s.setKey()).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.key(id)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	var expired []any
	for i, v := range vals {
		str, ok := v.(string)
		if !ok { // nil: the snapshot expired, drop the dangling id
			expired = append(expired, ids[i])
			continue
		}
		out[ids[i]] = []byte(str)
	}
	if len(expired) > 0 {
		if err := s.rdb.SRem(ctx, s.setKey(), expired...).Err(); err != nil && !errors.Is(err, redis.Nil) {
			return out, err
		}
	}
	return out, nil
}
