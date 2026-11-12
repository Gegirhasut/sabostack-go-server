package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// fastOptions shrinks every timer so a whole match runs in well under a second.
func fastOptions() match.Options {
	o := match.DefaultOptions()
	o.Match.TickInterval = 10 * time.Millisecond
	o.Match.Gravity = 200 * time.Millisecond
	o.Match.Countdown = 100 * time.Millisecond
	o.Match.RoundEndPause = 100 * time.Millisecond
	o.Match.RoundTime = 30 * time.Second
	o.Match.JoinTimeout = 5 * time.Second
	o.Match.ReconnectGrace = 5 * time.Second
	o.Match.IdleTimeout = 30 * time.Second
	o.SnapshotEvery = 50 * time.Millisecond
	o.Linger = 500 * time.Millisecond
	return o
}

type testServer struct {
	srv      *Server
	http     *httptest.Server
	url      string
	stopOnce sync.Once
}

func startServer(t *testing.T, st store.Store) *testServer {
	t.Helper()
	reg := prometheus.NewRegistry()
	met := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := match.NewManager(fastOptions(), clock.Real{}, st, met, log)
	if _, err := mgr.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.InputRate = 0 // hammer() sends 200 inputs/s to end matches fast
	s := NewWithConfig(mgr, met, reg, log, cfg)
	hs := httptest.NewServer(s.Handler())
	ts := &testServer{srv: s, http: hs, url: "ws" + strings.TrimPrefix(hs.URL, "http") + "/ws"}
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

// stop mimics the production shutdown order: stop accepting, then snapshot and close.
func (ts *testServer) stop(t *testing.T) {
	ts.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ts.http.Listener.Close()
		if err := ts.srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		ts.http.Close()
	})
}

// wsClient is a minimal protocol client for tests.
type wsClient struct {
	t        *testing.T
	ws       *websocket.Conn
	msgs     chan envelope
	mu       sync.Mutex
	eventSeq uint64
}

type envelope struct {
	Type string `json:"type"`
	Raw  json.RawMessage
}

func dial(t *testing.T, url string) *wsClient {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := &wsClient{t: t, ws: ws, msgs: make(chan envelope, 4096)}
	go func() {
		defer close(c.msgs)
		for {
			_, b, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var e envelope
			if json.Unmarshal(b, &e) != nil {
				continue
			}
			e.Raw = b
			if e.Type == protocol.TypeEvent {
				var em protocol.EventMsg
				_ = json.Unmarshal(b, &em)
				c.mu.Lock()
				c.eventSeq = em.Event.Seq
				c.mu.Unlock()
			}
			c.msgs <- e
		}
	}()
	t.Cleanup(func() { ws.Close() })
	return c
}

func (c *wsClient) send(v any) {
	c.t.Helper()
	if err := c.ws.WriteJSON(v); err != nil {
		c.t.Fatal(err)
	}
}

func (c *wsClient) lastEventSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.eventSeq
}

// waitFor returns the first message of type typ for which match (if non-nil) is true.
func (c *wsClient) waitFor(typ string, match func([]byte) bool) []byte {
	c.t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case e, ok := <-c.msgs:
			if !ok {
				c.t.Fatalf("connection closed while waiting for %s", typ)
			}
			if e.Type == typ && (match == nil || match(e.Raw)) {
				return e.Raw
			}
		case <-timeout:
			c.t.Fatalf("timed out waiting for %s", typ)
		}
	}
}

func isEvent(kind string) func([]byte) bool {
	return func(b []byte) bool {
		var em protocol.EventMsg
		return json.Unmarshal(b, &em) == nil && em.Event.Kind == kind
	}
}

func isPhase(phase string) func([]byte) bool {
	return func(b []byte) bool {
		var em protocol.EventMsg
		return json.Unmarshal(b, &em) == nil && em.Event.Kind == protocol.EvPhase && em.Event.Phase == phase
	}
}

func decode[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func joinPair(t *testing.T, url string) (a, b *wsClient, ma, mb protocol.Matched) {
	t.Helper()
	a, b = dial(t, url), dial(t, url)
	a.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "alice"})
	a.waitFor(protocol.TypeQueued, nil)
	b.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "bob"})
	ma = decode[protocol.Matched](t, a.waitFor(protocol.TypeMatched, nil))
	mb = decode[protocol.Matched](t, b.waitFor(protocol.TypeMatched, nil))
	if ma.MatchID != mb.MatchID || ma.Seat != 0 || mb.Seat != 1 || ma.Opponent != "bob" {
		t.Fatalf("matched: %+v / %+v", ma, mb)
	}
	a.waitFor(protocol.TypeSnapshot, nil)
	b.waitFor(protocol.TypeSnapshot, nil)
	return
}

// hammer sends hard drops from c until stop is closed.
func hammer(c *wsClient, stop <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		var seq uint64
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				seq++
				if c.ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeInput, Seq: seq, Action: "hard_drop"}) != nil {
					return
				}
			}
		}
	}()
	return done
}

// TestFullMatchWithReconnect plays a whole match over real WebSockets: matchmaking,
// countdown, a mid-round disconnect, a resume with the session token that returns the
// missed events, and two rounds decided by top-out.
func TestFullMatchWithReconnect(t *testing.T) {
	ts := startServer(t, store.NewMemory())
	alice, bob, ma, _ := joinPair(t, ts.url)
	alice.waitFor(protocol.TypeEvent, isPhase("playing"))
	bob.waitFor(protocol.TypeEvent, isPhase("playing"))

	// Alice's connection drops mid-round.
	seen := alice.lastEventSeq()
	alice.ws.Close()
	bob.waitFor(protocol.TypeEvent, isEvent(protocol.EvPlayerLeft))
	bob.waitFor(protocol.TypeEvent, isEvent(protocol.EvPaused))

	// She comes back on a new socket with her token and the last event she saw.
	alice = dial(t, ts.url)
	alice.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: ma.MatchID, Token: ma.Token, LastEventSeq: seen})
	snap := decode[protocol.Snapshot](t, alice.waitFor(protocol.TypeSnapshot, nil))
	if snap.Seat != 0 || snap.State.Phase != "playing" || !snap.EventsComplete {
		t.Fatalf("resume snapshot: seat=%d phase=%s complete=%v", snap.Seat, snap.State.Phase, snap.EventsComplete)
	}
	var kinds []string
	for _, e := range snap.Events {
		kinds = append(kinds, e.Kind)
	}
	if strings.Join(kinds, ",") != "player_left,paused,player_back" {
		t.Fatalf("missed events = %v", kinds)
	}
	alice.waitFor(protocol.TypeEvent, isEvent(protocol.EvResumed))

	// A wrong token is refused.
	intruder := dial(t, ts.url)
	intruder.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: ma.MatchID, Token: "deadbeef"})
	if e := decode[protocol.Error](t, intruder.waitFor(protocol.TypeError, nil)); e.Code != "bad_token" {
		t.Fatalf("intruder error = %+v", e)
	}

	// Bob stacks pieces as fast as he can and tops out in both rounds.
	stop := make(chan struct{})
	done := hammer(bob, stop)
	over := decode[protocol.EventMsg](t, alice.waitFor(protocol.TypeEvent, isEvent(protocol.EvMatchOver)))
	close(stop)
	<-done
	if over.Event.Winner == nil || *over.Event.Winner != 0 || over.Event.Reason != "rounds" {
		t.Fatalf("match_over = %+v", over.Event)
	}
	final := decode[protocol.EventMsg](t, bob.waitFor(protocol.TypeEvent, isEvent(protocol.EvMatchOver)))
	if final.Event.Seq != over.Event.Seq {
		t.Fatal("players saw different final events")
	}
}

// TestResumeAfterServerRestart stops a server mid-match and resumes the match on a new
// server instance that shares the snapshot store, as after a deploy or crash.
func TestResumeAfterServerRestart(t *testing.T) {
	st := store.NewMemory()
	ts1 := startServer(t, st)
	alice, bob, ma, mb := joinPair(t, ts1.url)
	alice.waitFor(protocol.TypeEvent, isPhase("playing"))
	alice.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: 1, Action: "hard_drop"})
	alice.waitFor(protocol.TypeState, func(b []byte) bool { return decode[protocol.State](t, b).Ack == 1 })

	ts1.stop(t) // graceful: final snapshot, sockets closed with "going away"
	for _, c := range []*wsClient{alice, bob} {
		for range c.msgs { // drain until the server closes the socket
		}
	}

	ts2 := startServer(t, st)
	alice, bob = dial(t, ts2.url), dial(t, ts2.url)
	alice.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: ma.MatchID, Token: ma.Token})
	bob.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: mb.MatchID, Token: mb.Token})
	snap := decode[protocol.Snapshot](t, alice.waitFor(protocol.TypeSnapshot, nil))
	if snap.State.Phase != "playing" || snap.State.Ack != 1 || snap.State.Players[0].Name != "alice" {
		t.Fatalf("restored snapshot: phase=%s ack=%d", snap.State.Phase, snap.State.Ack)
	}
	bob.waitFor(protocol.TypeSnapshot, nil)
	alice.waitFor(protocol.TypeEvent, isEvent(protocol.EvResumed))

	// Seq 1 was consumed before the restart, so a retransmit is ignored; seq 2 applies.
	alice.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: 1, Action: "hard_drop"})
	alice.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: 2, Action: "left"})
	alice.waitFor(protocol.TypeState, func(b []byte) bool { return decode[protocol.State](t, b).Ack == 2 })
}

func TestHealthAndMetrics(t *testing.T) {
	ts := startServer(t, store.NewMemory())
	base := strings.Replace(ts.url, "ws", "http", 1)
	base = strings.TrimSuffix(base, "/ws")
	for path, want := range map[string]string{"/healthz": `"ok"`, "/metrics": "sabostack_active_matches"} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(body), want) {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, body)
		}
	}
}
