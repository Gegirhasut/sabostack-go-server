package match

import (
	"sync"
	"testing"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func testConfig() Config {
	c := DefaultConfig()
	c.EventLogSize = 64
	return c
}

// fakeSink records what a client would receive.
type fakeSink struct {
	mu     sync.Mutex
	msgs   []any
	closed string
}

func (s *fakeSink) Send(msg any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msg)
	return true
}

func (s *fakeSink) Close(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = reason
}

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

func (s *fakeSink) snapshots() (n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.msgs {
		if _, ok := m.(protocol.Snapshot); ok {
			n++
		}
	}
	return n
}

// heardSink is a fakeSink that says when its client was last heard from (Heard).
type heardSink struct {
	fakeSink
	last time.Time
}

func (s *heardSink) hear(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = at
}

func (s *heardSink) LastHeard() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func (s *fakeSink) events() []protocol.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []protocol.Event
	for _, m := range s.msgs {
		if e, ok := m.(protocol.EventMsg); ok {
			out = append(out, e.Event)
		}
	}
	return out
}

func (s *fakeSink) lastSnapshot() (protocol.Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.msgs) - 1; i >= 0; i-- {
		if snap, ok := s.msgs[i].(protocol.Snapshot); ok {
			return snap, true
		}
	}
	return protocol.Snapshot{}, false
}

// newConnected returns a match with both players attached, still in Waiting.
func newConnected(t *testing.T, cfg Config) (*Match, [2]string, [2]*fakeSink) {
	t.Helper()
	m, tokens := New("m1", cfg, [2]string{"alice", "bob"}, 1234, t0)
	sinks := [2]*fakeSink{{}, {}}
	for i := range sinks {
		seat, err := m.Attach(tokens[i], sinks[i], 0, t0)
		if err != nil || seat != i {
			t.Fatalf("attach %d: seat=%d err=%v", i, seat, err)
		}
	}
	return m, tokens, sinks
}

// newPlaying returns a match in round 1, Playing, and the current time.
func newPlaying(t *testing.T, cfg Config) (*Match, [2]string, [2]*fakeSink, time.Time) {
	t.Helper()
	m, tokens, sinks := newConnected(t, cfg)
	m.Advance(t0)
	if m.Phase != Countdown {
		t.Fatalf("phase = %s, want countdown", m.Phase)
	}
	now := t0.Add(cfg.Countdown)
	m.Advance(now)
	if m.Phase != Playing {
		t.Fatalf("phase = %s, want playing", m.Phase)
	}
	return m, tokens, sinks, now
}
