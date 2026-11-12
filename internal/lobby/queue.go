// Package lobby is a minimal FIFO matchmaking queue: the first two waiting players are
// paired into a new match.
package lobby

import (
	"container/list"
	"context"
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
	ctx  context.Context // done when the player's connection is gone
	elem *list.Element   // position in the queue while waiting (guarded by Queue.mu)
}

// C delivers the assignment (or an error).
func (t *Ticket) C() <-chan Assignment { return t.c }

func (t *Ticket) gone() bool { return t.ctx.Err() != nil }

// Queue pairs players in arrival order. Join and Leave are O(1).
type Queue struct {
	create CreateFunc

	mu      sync.Mutex
	waiting list.List // of *Ticket
	closed  bool
}

// New returns a queue that uses create to start matches.
func New(create CreateFunc) *Queue { return &Queue{create: create} }

// Join enqueues a player. If someone was already waiting, both are matched immediately.
func (q *Queue) Join(name string) *Ticket { return q.JoinContext(context.Background(), name) }

// JoinContext is Join for a player whose connection is tied to ctx. A waiting player whose
// ctx is done is skipped instead of being paired, so nobody is matched with an opponent
// who already left and then has to sit out the join timeout.
func (q *Queue) JoinContext(ctx context.Context, name string) *Ticket {
	t := &Ticket{Name: name, c: make(chan Assignment, 1), ctx: ctx}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		t.c <- Assignment{Err: ErrClosed}
		return t
	}
	var first *Ticket
	for e := q.waiting.Front(); e != nil; e = q.waiting.Front() {
		w := q.waiting.Remove(e).(*Ticket)
		w.elem = nil
		if w.gone() {
			w.c <- Assignment{Err: context.Canceled}
			continue
		}
		first = w
		break
	}
	if first == nil {
		t.elem = q.waiting.PushBack(t)
		q.mu.Unlock()
		return t
	}
	q.mu.Unlock()

	// Create outside the lock: starting a match must not stall other joins.
	id, tokens, err := q.create([2]string{first.Name, t.Name})
	first.c <- Assignment{MatchID: id, Seat: 0, Token: tokens[0], Opponent: t.Name, Err: err}
	t.c <- Assignment{MatchID: id, Seat: 1, Token: tokens[1], Opponent: first.Name, Err: err}
	return t
}

// Leave removes a ticket that is still waiting (the player disconnected). It reports
// whether the ticket was removed; false means it was already matched (or skipped).
func (q *Queue) Leave(t *Ticket) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if t.elem == nil {
		return false
	}
	q.waiting.Remove(t.elem)
	t.elem = nil
	return true
}

// Len is the number of waiting players.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.waiting.Len()
}

// Close rejects new joins and fails every waiting ticket.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for e := q.waiting.Front(); e != nil; e = e.Next() {
		t := e.Value.(*Ticket)
		t.elem = nil
		t.c <- Assignment{Err: ErrClosed}
	}
	q.waiting.Init()
}
