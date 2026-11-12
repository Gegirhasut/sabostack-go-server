package lobby

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func fakeCreate() (CreateFunc, *[][2]string) {
	var mu sync.Mutex
	var created [][2]string
	return func(names [2]string) (string, [2]string, error) {
		mu.Lock()
		defer mu.Unlock()
		created = append(created, names)
		id := fmt.Sprintf("m%d", len(created))
		return id, [2]string{id + "-t0", id + "-t1"}, nil
	}, &created
}

func TestPairsInArrivalOrder(t *testing.T) {
	create, created := fakeCreate()
	q := New(create)
	a := q.Join("a")
	if q.Len() != 1 {
		t.Fatal("first player should wait")
	}
	b := q.Join("b")
	ga, gb := <-a.C(), <-b.C()
	if ga.MatchID != "m1" || gb.MatchID != "m1" || ga.Seat != 0 || gb.Seat != 1 {
		t.Fatalf("assignments %+v %+v", ga, gb)
	}
	if ga.Opponent != "b" || gb.Opponent != "a" || ga.Token == gb.Token {
		t.Fatalf("assignments %+v %+v", ga, gb)
	}
	if len(*created) != 1 || q.Len() != 0 {
		t.Fatal("expected exactly one match and an empty queue")
	}
}

func TestLeaveRemovesWaitingPlayer(t *testing.T) {
	create, created := fakeCreate()
	q := New(create)
	a := q.Join("a")
	if !q.Leave(a) {
		t.Fatal("leave failed")
	}
	q.Join("b")
	if len(*created) != 0 || q.Len() != 1 {
		t.Fatal("a player who left was matched")
	}
}

func TestConcurrentJoinsPairEveryone(t *testing.T) {
	create, created := fakeCreate()
	q := New(create)
	const n = 200
	var wg sync.WaitGroup
	results := make(chan Assignment, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- <-q.Join(fmt.Sprint(i)).C()
		}()
	}
	wg.Wait()
	close(results)
	seats := map[string]int{}
	for r := range results {
		seats[r.MatchID]++
	}
	if len(*created) != n/2 || len(seats) != n/2 {
		t.Fatalf("created %d matches for %d players", len(*created), n)
	}
	for id, c := range seats {
		if c != 2 {
			t.Fatalf("match %s has %d players", id, c)
		}
	}
}

func TestCloseFailsWaiting(t *testing.T) {
	create, _ := fakeCreate()
	q := New(create)
	a := q.Join("a")
	q.Close()
	if got := <-a.C(); !errors.Is(got.Err, ErrClosed) {
		t.Fatalf("got %+v", got)
	}
	if got := <-q.Join("b").C(); !errors.Is(got.Err, ErrClosed) {
		t.Fatalf("join after close: %+v", got)
	}
}
