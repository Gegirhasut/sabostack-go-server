package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Gegirhasut/sabostack-go-server/internal/lobby"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/testredis"
)

type mmEnv struct {
	mr  *testredis.R
	rdb *redis.Client
	mu  sync.Mutex
	// matches created, by hosting node
	created map[string][][2]string
	hashes  []string // token hashes the hosts were given
}

func newEnv(t *testing.T) *mmEnv {
	t.Helper()
	mr := testredis.New(t)
	rdb := mr.Client
	t.Cleanup(func() { rdb.Close() })
	return &mmEnv{mr: mr, rdb: rdb, created: map[string][][2]string{}}
}

func (e *mmEnv) node(t *testing.T, name string) *Matchmaker {
	t.Helper()
	e.mr.Set("t:alive:"+name, "ws://"+name) // alive (no TTL in this test)
	create := func(names, tokenHashes [2]string) (string, error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.created[name] = append(e.created[name], names)
		e.hashes = append(e.hashes, tokenHashes[0], tokenHashes[1])
		return fmt.Sprintf("%s-m%d", name, len(e.created[name])), nil
	}
	m, err := NewMatchmaker(e.rdb, "t:", name, "ws://"+name, create, metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func wait(t *testing.T, tk *lobby.Ticket) lobby.Assignment {
	t.Helper()
	select {
	case a := <-tk.C():
		return a
	case <-time.After(5 * time.Second):
		t.Fatal("no assignment")
		return lobby.Assignment{}
	}
}

// Players connected to different nodes are paired. The hosting node's player plays
// locally; the other one is told the host's url.
func TestCrossNodePairing(t *testing.T) {
	e := newEnv(t)
	n1, n2 := e.node(t, "n1"), e.node(t, "n2")
	alice := n1.JoinContext(context.Background(), "alice")
	bob := n2.JoinContext(context.Background(), "bob")
	ga, gb := wait(t, alice), wait(t, bob)
	if ga.Err != nil || gb.Err != nil || ga.MatchID != gb.MatchID || ga.Seat == gb.Seat {
		t.Fatalf("assignments: %+v / %+v", ga, gb)
	}
	if ga.Opponent != "bob" || gb.Opponent != "alice" {
		t.Fatalf("opponents: %q / %q", ga.Opponent, gb.Opponent)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var host string
	for h, ms := range e.created {
		if len(ms) == 1 {
			host = h
		}
	}
	local, remote := ga, gb
	if host == "n2" {
		local, remote = gb, ga
	}
	if local.URL != "" || remote.URL != "ws://"+host {
		t.Fatalf("host %s: local url %q, remote url %q", host, local.URL, remote.URL)
	}
	if n, _ := e.rdb.LLen(context.Background(), "t:mm:queue:v2").Result(); n != 0 {
		t.Fatalf("queue length %d after pairing", n)
	}
}

// Tickets of nodes whose heartbeat expired are skipped, not paired.
func TestDeadNodeTicketsAreSkipped(t *testing.T) {
	e := newEnv(t)
	e.rdb.RPush(context.Background(), "t:mm:queue:v2", "ghost-ticket\tghost\thash\tcasper")
	n1 := e.node(t, "n1")
	a := n1.JoinContext(context.Background(), "a")
	b := n1.JoinContext(context.Background(), "b")
	ga, gb := wait(t, a), wait(t, b)
	if ga.MatchID == "" || ga.MatchID != gb.MatchID || ga.Opponent != "b" {
		t.Fatalf("a/b not paired with each other: %+v / %+v", ga, gb)
	}
}

func TestLeaveRemovesQueuedTicket(t *testing.T) {
	e := newEnv(t)
	n1 := e.node(t, "n1")
	tk := n1.JoinContext(context.Background(), "solo")
	if !n1.Leave(tk) {
		t.Fatal("leave of a queued ticket failed")
	}
	if n, _ := e.rdb.LLen(context.Background(), "t:mm:queue:v2").Result(); n != 0 {
		t.Fatalf("queue length %d after leave", n)
	}
	if n1.Leave(tk) {
		t.Fatal("second leave succeeded")
	}
}

// A ticket that was popped but whose assignment never arrived (the popping node died)
// goes back into the queue and still gets a match.
func TestLostAssignmentIsRequeued(t *testing.T) {
	oldAfter, oldEvery := requeueAfter, checkLostEvery
	requeueAfter, checkLostEvery = 200*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { requeueAfter, checkLostEvery = oldAfter, oldEvery })

	e := newEnv(t)
	n1 := e.node(t, "n1")
	tk := n1.JoinContext(context.Background(), "alice")
	if _, err := e.rdb.LPop(context.Background(), "t:mm:queue:v2").Result(); err != nil { // a popper that crashed
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, _ := e.rdb.LLen(context.Background(), "t:mm:queue:v2").Result(); n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lost ticket was not requeued")
		}
		time.Sleep(10 * time.Millisecond)
	}
	other := e.node(t, "n2").JoinContext(context.Background(), "bob")
	if a, b := wait(t, tk), wait(t, other); a.MatchID == "" || a.MatchID != b.MatchID {
		t.Fatalf("requeued ticket not matched: %+v / %+v", a, b)
	}
}

// Close takes this node's players out of the shared queue and tells them.
func TestCloseRemovesLocalTickets(t *testing.T) {
	e := newEnv(t)
	n1 := e.node(t, "n1")
	tk := n1.JoinContext(context.Background(), "x")
	n1.Close()
	if a := wait(t, tk); a.Err == nil {
		t.Fatalf("assignment after close: %+v", a)
	}
	if n, _ := e.rdb.LLen(context.Background(), "t:mm:queue:v2").Result(); n != 0 {
		t.Fatalf("queue length %d after close", n)
	}
	if a := wait(t, n1.JoinContext(context.Background(), "late")); a.Err == nil {
		t.Fatal("join after close succeeded")
	}
}
