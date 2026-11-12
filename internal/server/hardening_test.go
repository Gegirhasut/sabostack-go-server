package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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

func startServerWith(t *testing.T, st store.Store, cfg Config) *testServer {
	t.Helper()
	reg := prometheus.NewRegistry()
	met := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := match.NewManager(fastOptions(), clock.Real{}, st, met, log)
	s := NewWithConfig(mgr, met, reg, log, cfg)
	hs := httptest.NewServer(s.Handler())
	ts := &testServer{srv: s, http: hs, url: "ws" + strings.TrimPrefix(hs.URL, "http") + "/ws"}
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

func (ts *testServer) httpURL(path string) string {
	return strings.TrimSuffix(strings.Replace(ts.url, "ws", "http", 1), "/ws") + path
}

// bareClient is a client whose writer is not running, to test the outbox in isolation.
func bareClient() *client {
	return &client{stateIdx: -1, wake: make(chan struct{}, 1), quit: make(chan struct{}), cancel: func() {}}
}

func st(tick uint64) protocol.State { return protocol.State{Type: protocol.TypeState, Tick: tick} }
func ev(seq uint64) protocol.EventMsg {
	return protocol.EventMsg{Type: protocol.TypeEvent, Event: protocol.Event{Seq: seq}}
}

// States are conflated (only the newest unsent one survives, after the events queued
// before it); events keep their order and are never dropped.
func TestOutboxConflatesStates(t *testing.T) {
	c := bareClient()
	for _, m := range []any{st(1), ev(1), st(2), ev(2), st(3)} {
		if !c.Send(m) {
			t.Fatal("send refused")
		}
	}
	var kinds []string
	for _, m := range c.take() {
		switch v := m.(type) {
		case protocol.State:
			kinds = append(kinds, fmt.Sprintf("st%d", v.Tick))
		case protocol.EventMsg:
			kinds = append(kinds, fmt.Sprintf("ev%d", v.Event.Seq))
		}
	}
	if got := strings.Join(kinds, " "); got != "ev1 ev2 st3" {
		t.Fatalf("outbox = %s, want ev1 ev2 st3", got)
	}
	// A slow reader is never dropped for states alone.
	for i := uint64(0); i < 10*outboxSize; i++ {
		c.Send(st(i))
	}
	if got := c.take(); len(got) != 1 {
		t.Fatalf("%d states queued, want 1", len(got))
	}
	select {
	case <-c.quit:
		t.Fatal("client closed for states")
	default:
	}
	// Too many undelivered events is still a slow consumer.
	for i := uint64(0); i < outboxSize; i++ {
		c.Send(ev(i))
	}
	if c.Send(ev(999)) {
		t.Fatal("event accepted past the outbox limit")
	}
	if c.reason != "slow consumer" {
		t.Fatalf("closed with %q", c.reason)
	}
}

func TestValidName(t *testing.T) {
	for name, want := range map[string]bool{
		"alice": true, "Игрок_1": true, "名前": true, strings.Repeat("я", 24): true,
		"": false, strings.Repeat("я", 25): false, "a\x00b": false, "bad\nname": false,
		"zero\u200bwidth": false, string([]byte{0xff, 0xfe}): false,
	} {
		if validName(name) != want {
			t.Errorf("validName(%q) = %v, want %v", name, !want, want)
		}
	}
}

// Draining: /readyz turns 503, new joins are refused, but resumes keep working so running
// matches can finish.
func TestDrainRefusesJoinsButAllowsResume(t *testing.T) {
	ts := startServer(t, store.NewMemory())
	alice, _, ma, _ := joinPair(t, ts.url)
	alice.waitFor(protocol.TypeEvent, isPhase("playing"))

	ts.srv.Drain()
	resp, err := http.Get(ts.httpURL("/readyz"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d while draining", resp.StatusCode)
	}

	late := dial(t, ts.url)
	late.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "late"})
	if e := decode[protocol.Error](t, late.waitFor(protocol.TypeError, nil)); e.Code != "unavailable" {
		t.Fatalf("join while draining: %+v", e)
	}

	alice.ws.Close()
	back := dial(t, ts.url)
	back.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: ma.MatchID, Token: ma.Token})
	back.waitFor(protocol.TypeSnapshot, nil)
}

func TestOriginCheck(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowedOrigins = []string{"game.example.com"}
	ts := startServerWith(t, store.NewMemory(), cfg)
	for origin, ok := range map[string]bool{"https://game.example.com": true, "https://evil.example": false, "": true} {
		h := http.Header{}
		if origin != "" {
			h.Set("Origin", origin)
		}
		ws, _, err := websocket.DefaultDialer.Dial(ts.url, h)
		if (err == nil) != ok {
			t.Errorf("origin %q: err=%v, want ok=%v", origin, err, ok)
		}
		if ws != nil {
			ws.Close()
		}
	}
}

func TestMaxConnsPerIP(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConnsPerIP = 2
	ts := startServerWith(t, store.NewMemory(), cfg)
	var open []*websocket.Conn
	for i := 0; i < 2; i++ {
		ws, _, err := websocket.DefaultDialer.Dial(ts.url, nil)
		if err != nil {
			t.Fatal(err)
		}
		open = append(open, ws)
	}
	_, resp, err := websocket.DefaultDialer.Dial(ts.url, nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("third connection: err=%v resp=%v", err, resp)
	}
	open[0].Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ws, _, err := websocket.DefaultDialer.Dial(ts.url, nil)
		if err == nil {
			ws.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slot not released after a connection closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	open[1].Close()
}

// directoryStore pretends every unknown match is owned by another node.
type directoryStore struct{ *store.Memory }

func (directoryStore) Owner(context.Context, string) (string, string, bool, error) {
	return "other", "wss://other.example/ws", true, nil
}

func TestResumeRedirectsToOwningNode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "me"
	ts := startServerWith(t, directoryStore{store.NewMemory()}, cfg)
	c := dial(t, ts.url)
	c.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: "elsewhere", Token: "t"})
	e := decode[protocol.Error](t, c.waitFor(protocol.TypeError, nil))
	if e.Code != "wrong_server" || e.URL != "wss://other.example/ws" {
		t.Fatalf("error = %+v", e)
	}
}

// Metrics and pprof are available on the admin handler, and the public handler can be
// configured without /metrics.
func TestAdminHandler(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PublicMetrics = false
	ts := startServerWith(t, store.NewMemory(), cfg)
	resp, err := http.Get(ts.httpURL("/metrics"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("public /metrics = %d, want 404", resp.StatusCode)
	}
	admin := httptest.NewServer(ts.srv.AdminHandler())
	defer admin.Close()
	for path, want := range map[string]string{"/metrics": "sabostack_actor_panics_total", "/debug/pprof/": "goroutine", "/readyz": "ready"} {
		resp, err := http.Get(admin.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(body), want) {
			t.Fatalf("admin %s: %d", path, resp.StatusCode)
		}
	}
}
