package server

// Server-side detection of dead clients. A half-open connection (the phone changed
// networks; no close ever arrives) is only noticed by silence: after SilenceTimeout
// without a message or a pong, the match marks the seat away and the reconnect grace
// starts, while the socket stays open in case the client was only slow.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// syncPing sends a ping and waits for its pong: by then the server has handled
// everything the client sent before it. Events that arrive meanwhile are returned.
func syncPing(t *testing.T, c *wsClient, n int64) []protocol.Event {
	t.Helper()
	c.send(protocol.ClientMsg{Type: protocol.TypePing, T: n})
	var evs []protocol.Event
	timeout := time.After(10 * time.Second)
	for {
		select {
		case e, ok := <-c.msgs:
			if !ok {
				t.Fatal("connection closed while waiting for pong")
			}
			switch e.Type {
			case protocol.TypePong:
				if decode[protocol.Pong](t, e.Raw).T == n {
					return evs
				}
			case protocol.TypeEvent:
				evs = append(evs, decode[protocol.EventMsg](t, e.Raw).Event)
			}
		case <-timeout:
			t.Fatal("timed out waiting for pong")
		}
	}
}

func seatEvent(kind string, seat int) func([]byte) bool {
	return func(b []byte) bool {
		var em protocol.EventMsg
		return json.Unmarshal(b, &em) == nil && em.Event.Kind == kind && em.Event.Seat != nil && *em.Event.Seat == seat
	}
}

// Deterministic, on the match clock: a client that sends nothing for SilenceTimeout is
// marked away while its socket stays open, comes back once it has been heard from
// steadily for SilenceTimeout/2 (no snapshot needed: it never missed a message), and
// when it goes silent for good it forfeits once the grace that started with the silence
// runs out.
func TestSilentClientIsMarkedAwayAndBack(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := clock.NewFake(start)
	opts := match.DefaultOptions()
	opts.Match.Gravity, opts.Match.IdleTimeout = time.Hour, time.Hour // only presence decides here
	opts.Linger = time.Hour
	cfg := DefaultConfig()
	cfg.InputRate = 0
	cfg.PingInterval = time.Hour // no server pings: here only messages count as being heard
	silence := opts.Match.SilenceTimeout
	ts := startServerOn(t, store.NewMemory(), clk, opts, cfg)

	alice, bob, ma, _ := joinPair(t, ts.url)
	n := int64(0)
	ping := func(c *wsClient) []protocol.Event { n++; return syncPing(t, c, n) }
	ping(alice) // both sessions now run their silence watch, heard at start
	ping(bob)
	actor, ok := ts.srv.mgr.Get(ma.MatchID)
	if !ok {
		t.Fatal("match not running")
	}
	seats := func() (s [2]match.Seat) {
		t.Helper()
		if err := actor.Inspect(context.Background(), func(m *match.Match) { s = [2]match.Seat{*m.Seats[0], *m.Seats[1]} }); err != nil {
			t.Fatal(err)
		}
		return s
	}
	// advance moves the clock and waits until the match has handled the ticks: Advance
	// returns once a tick is received, and the next Advance must not start before it is
	// handled (the actor reads the clock while handling it).
	advance := func(d time.Duration) {
		t.Helper()
		clk.Advance(d)
		if err := actor.Inspect(context.Background(), func(*match.Match) {}); err != nil {
			t.Fatal(err)
		}
	}

	advance(opts.Match.TickInterval) // countdown
	advance(opts.Match.Countdown)    // playing
	alice.waitFor(protocol.TypeEvent, isPhase("playing"))
	ping(alice) // alice keeps talking; bob has said nothing since start

	advance(start.Add(silence).Sub(clk.Now()) + opts.Match.TickInterval) // just past start+silence
	alice.waitFor(protocol.TypeEvent, seatEvent(protocol.EvPlayerLeft, 1))
	s := seats()
	if !s[0].Connected || s[1].Connected {
		t.Fatalf("connected = %v/%v, want only bob marked away", s[0].Connected, s[1].Connected)
	}
	if away := s[1].LeftAt.Sub(start); away < silence || away > silence+time.Second {
		t.Fatalf("bob marked away %s after his last message, want %s", away, silence)
	}

	// Bob was only slow: he talks steadily again and is back, on the same socket.
	// (Bob's messages are only sent here, not waited on, so everything the server sends
	// him stays in bob.msgs for the check below.)
	heardAgain := clk.Now()
	sawBack := false
	for i := int64(0); clk.Now().Sub(heardAgain) <= silence/2+opts.Match.TickInterval; i++ {
		bob.send(protocol.ClientMsg{Type: protocol.TypePing, T: 1000 + i})
		for _, e := range ping(alice) { // by its pong the server has read bob's message too
			sawBack = sawBack || e.Kind == protocol.EvPlayerBack && e.Seat != nil && *e.Seat == 1
		}
		advance(opts.Match.TickInterval)
	}
	if !sawBack {
		alice.waitFor(protocol.TypeEvent, seatEvent(protocol.EvPlayerBack, 1))
	}
	for back := false; !back; {
		select {
		case e := <-bob.msgs:
			if e.Type == protocol.TypeSnapshot {
				t.Fatal("a client heard again was sent a snapshot")
			}
			back = e.Type == protocol.TypeEvent && seatEvent(protocol.EvPlayerBack, 1)(e.Raw)
		case <-time.After(10 * time.Second):
			t.Fatal("bob never heard he was back")
		}
	}
	if s := seats(); !s[1].Connected {
		t.Fatal("bob still away after being heard")
	}

	// Bob goes silent for good (his socket never closes): silence, then the grace.
	lastHeard := clk.Now()
	var evs []protocol.Event
	for i := 0; i < 30; i++ {
		evs = append(evs, ping(alice)...)
		var phase match.Phase
		_ = actor.Inspect(context.Background(), func(m *match.Match) { phase = m.Phase })
		if phase == match.MatchEnd {
			break
		}
		advance(2 * time.Second)
	}
	var over *protocol.Event
	for i := range evs {
		if evs[i].Kind == protocol.EvMatchOver {
			over = &evs[i]
		}
	}
	if over == nil {
		e := decode[protocol.EventMsg](t, alice.waitFor(protocol.TypeEvent, isEvent(protocol.EvMatchOver))).Event
		over = &e
	}
	if over.Winner == nil || *over.Winner != 0 || over.Reason != "opponent_disconnected" {
		t.Fatalf("match_over = %+v", *over)
	}
	var ended time.Time
	_ = actor.Inspect(context.Background(), func(m *match.Match) { ended = m.EndedAt })
	want := silence + opts.Match.ReconnectGrace
	if got := ended.Sub(lastHeard); got < want || got > want+3*time.Second {
		t.Fatalf("forfeit %s after bob's last message, want silence %s + grace %s", got, silence, opts.Match.ReconnectGrace)
	}
}

// Over real sockets: a client that sends nothing but answers the server's pings (as
// browsers and WebSocket libraries do by themselves) stays present; one that does not
// answer goes silent, and anything it sends brings it back.
func TestSilenceIsJudgedByPongsToo(t *testing.T) {
	opts := fastOptions()
	opts.Match.Gravity, opts.Match.IdleTimeout = time.Hour, time.Hour
	cfg := DefaultConfig()
	cfg.InputRate = 0
	cfg.PingInterval, opts.Match.SilenceTimeout = 20*time.Millisecond, time.Second
	ts := startServerOn(t, store.NewMemory(), clock.Real{}, opts, cfg)

	alice := dial(t, ts.url) // gorilla's default ping handler answers every ping
	deaf := dialWith(t, ts.url, func(ws *websocket.Conn) { ws.SetPingHandler(func(string) error { return nil }) })
	alice.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "alice"})
	alice.waitFor(protocol.TypeQueued, nil)
	deaf.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "bob"})
	ma := decode[protocol.Matched](t, alice.waitFor(protocol.TypeMatched, nil))
	deaf.waitFor(protocol.TypeMatched, nil)

	// Neither sends anything from here on.
	alice.waitFor(protocol.TypeEvent, seatEvent(protocol.EvPlayerLeft, 1))
	time.Sleep(2 * opts.Match.SilenceTimeout)
	actor, _ := ts.srv.mgr.Get(ma.MatchID)
	var connected [2]bool
	if err := actor.Inspect(context.Background(), func(m *match.Match) {
		connected = [2]bool{m.Seats[0].Connected, m.Seats[1].Connected}
	}); err != nil {
		t.Fatal(err)
	}
	if connected != [2]bool{true, false} {
		t.Fatalf("connected = %v after %s of quiet, want the ping-answering client present", connected, 2*opts.Match.SilenceTimeout)
	}
	// Anything it sends counts; heard steadily, it comes back.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := int64(1); ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				deaf.send(protocol.ClientMsg{Type: protocol.TypePing, T: i})
			}
		}
	}()
	alice.waitFor(protocol.TypeEvent, seatEvent(protocol.EvPlayerBack, 1))
}
