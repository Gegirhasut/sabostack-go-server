// Package store persists match snapshots so a restarted server can resume live matches.
//
// Snapshots are opaque bytes to the store; the match package owns the format.
package store

import (
	"context"
	"sync"
)

// Store is the snapshot persistence interface.
type Store interface {
	// Save upserts the latest snapshot for a match.
	Save(ctx context.Context, id string, data []byte) error
	// Delete removes a match (it ended, nothing to resume).
	Delete(ctx context.Context, id string) error
	// LoadAll returns every stored snapshot, keyed by match id.
	LoadAll(ctx context.Context) (map[string][]byte, error)
}

// Memory is an in-process Store. It survives nothing but is handy for tests, for local
// runs without Redis, and for simulating a restart inside one process.
type Memory struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{data: map[string][]byte{}} }

func (s *Memory) Save(_ context.Context, id string, data []byte) error {
	cp := append([]byte(nil), data...)
	s.mu.Lock()
	s.data[id] = cp
	s.mu.Unlock()
	return nil
}

func (s *Memory) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	delete(s.data, id)
	s.mu.Unlock()
	return nil
}

func (s *Memory) LoadAll(_ context.Context) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}
