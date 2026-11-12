package lobby

import (
	"context"
	"testing"
)

// A waiting player whose connection is already gone is skipped, not matched.
func TestDeadWaitingPlayerIsSkipped(t *testing.T) {
	create, created := fakeCreate()
	q := New(create)
	ctx, cancel := context.WithCancel(context.Background())
	ghost := q.JoinContext(ctx, "ghost")
	cancel() // the socket died before the player could Leave

	b := q.Join("b")
	if got := <-ghost.C(); got.Err == nil {
		t.Fatalf("ghost was matched: %+v", got)
	}
	if q.Len() != 1 || len(*created) != 0 {
		t.Fatalf("b should be waiting alone: len=%d created=%v", q.Len(), *created)
	}
	c := q.Join("c")
	gb, gc := <-b.C(), <-c.C()
	if gb.Err != nil || gb.MatchID != gc.MatchID || gb.Opponent != "c" {
		t.Fatalf("b/c not paired: %+v %+v", gb, gc)
	}
	if q.Leave(ghost) || q.Leave(b) {
		t.Fatal("Leave of a ticket that is no longer queued must report false")
	}
}

func TestLeaveIsIdempotent(t *testing.T) {
	create, _ := fakeCreate()
	q := New(create)
	a := q.Join("a")
	if !q.Leave(a) || q.Len() != 0 || q.Leave(a) {
		t.Fatal("leave/len mismatch")
	}
	b, c := q.Join("b"), q.Join("c") // the queue still pairs after a removal
	if gb, gc := <-b.C(), <-c.C(); gb.MatchID == "" || gb.MatchID != gc.MatchID {
		t.Fatalf("not paired: %+v %+v", gb, gc)
	}
}
