// Package lobby is a minimal FIFO matchmaking queue: the first two waiting players are
// paired into a new match.
package lobby

import (
	"errors"
	"sync"
)

// ErrClosed is delivered to waiting tickets when the queue shuts down.
var ErrClosed = errors.New("lobby: closed")

// Assignment tells one player where to go.
type Assignment struct {
	MatchID  string
	Seat     int
	Token    string
	Opponent string
	Err      error
}

// CreateFunc starts a match and returns its id and per-seat tokens.
type CreateFunc func(names [2]string) (string, [2]string, error)

// Ticket is a player's place in the queue. Exactly one Assignment is delivered on C.
type Ticket struct {
	Name string
	c    chan Assignment
}

// C delivers the assignment (or an error).
func (t *Ticket) C() <-chan Assignment { return t.c }

// Queue pairs players in arrival order.
type Queue struct {
	create CreateFunc

	mu      sync.Mutex
	waiting []*Ticket
	closed  bool
}

// New returns a queue that uses create to start matches.
func New(create CreateFunc) *Queue { return &Queue{create: create} }

// Join enqueues a player. If someone was already waiting, both are matched immediately.
func (q *Queue) Join(name string) *Ticket {
	t := &Ticket{Name: name, c: make(chan Assignment, 1)}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		t.c <- Assignment{Err: ErrClosed}
		return t
	}
	if len(q.waiting) == 0 {
		q.waiting = append(q.waiting, t)
		q.mu.Unlock()
		return t
	}
	first := q.waiting[0]
	q.waiting = q.waiting[1:]
	q.mu.Unlock()

	// Create outside the lock: starting a match must not stall other joins.
	id, tokens, err := q.create([2]string{first.Name, t.Name})
	first.c <- Assignment{MatchID: id, Seat: 0, Token: tokens[0], Opponent: t.Name, Err: err}
	t.c <- Assignment{MatchID: id, Seat: 1, Token: tokens[1], Opponent: first.Name, Err: err}
	return t
}

// Leave removes a ticket that is still waiting (the player disconnected). It reports
// whether the ticket was removed; false means it was already matched.
func (q *Queue) Leave(t *Ticket) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, w := range q.waiting {
		if w == t {
			q.waiting = append(q.waiting[:i], q.waiting[i+1:]...)
			return true
		}
	}
	return false
}

// Len is the number of waiting players.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiting)
}

// Close rejects new joins and fails every waiting ticket.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for _, t := range q.waiting {
		t.c <- Assignment{Err: ErrClosed}
	}
	q.waiting = nil
}
