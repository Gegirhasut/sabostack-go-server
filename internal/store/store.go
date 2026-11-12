// Package store persists match snapshots so a restarted server can resume live matches.
//
// Snapshots are opaque bytes to the store; the match package owns the format.
package store

import (
	"context"
	"errors"
	"sync"
)

// ErrNotOwner is returned when a write targets a match that another node owns. It is the
// fencing signal that stops two processes from both driving (and persisting) one match.
var ErrNotOwner = errors.New("store: match is owned by another node")

// Store is the snapshot persistence interface.
type Store interface {
	// Save upserts the latest snapshot for a match.
	Save(ctx context.Context, id string, data []byte) error
	// Delete removes a match (it ended, nothing to resume).
	Delete(ctx context.Context, id string) error
	// LoadAll returns every stored snapshot this node should restore, keyed by match id.
	LoadAll(ctx context.Context) (map[string][]byte, error)
}

// Op is one write in a batch: a save, or a delete when Data is nil.
type Op struct {
	ID   string
	Data []byte
}

// Batcher is implemented by stores that can apply several writes in one round trip.
// Apply returns one error per op (nil on success), in the same order as ops.
type Batcher interface {
	Apply(ctx context.Context, ops []Op) []error
}

// Directory is implemented by stores that know which node owns a match, so a node that
// does not have the match can send the client to the one that does.
type Directory interface {
	// Owner returns the owning node id and its public URL; ok is false if nobody owns it.
	Owner(ctx context.Context, id string) (node, url string, ok bool, err error)
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

// Apply implements Batcher.
func (s *Memory) Apply(ctx context.Context, ops []Op) []error {
	errs := make([]error, len(ops))
	for i, op := range ops {
		if op.Data == nil {
			errs[i] = s.Delete(ctx, op.ID)
		} else {
			errs[i] = s.Save(ctx, op.ID, op.Data)
		}
	}
	return errs
}
