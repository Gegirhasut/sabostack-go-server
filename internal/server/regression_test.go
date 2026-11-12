package server

// Regression tests for the bugs found while hardening the server, at the protocol level. Each
// one uses only APIs that existed before the fix (New, the test client from
// integration_test.go), so it compiles against the old code and fails there.

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// startDefaultServer runs a server exactly as New builds it (production defaults).
func startDefaultServer(t *testing.T) *testServer {
	t.Helper()
	reg := prometheus.NewRegistry()
	met := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := match.NewManager(fastOptions(), clock.Real{}, store.NewMemory(), met, log)
	s := New(mgr, met, reg, log)
	hs := httptest.NewServer(s.Handler())
	ts := &testServer{srv: s, http: hs, url: "ws" + strings.TrimPrefix(hs.URL, "http") + "/ws"}
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

// Bug: nothing limited client messages. One client sent 42k inputs in 5.5 s and every
// one was applied (a speed hack). Now a flooder is cut off, and the opponent plays on.
func TestBugInputFloodNotRateLimited(t *testing.T) {
	ts := startDefaultServer(t)
	alice, bob, _, _ := joinPair(t, ts.url)
	alice.waitFor(protocol.TypeEvent, isPhase("playing"))
	bob.waitFor(protocol.TypeEvent, isPhase("playing"))

	go func() {
		for seq := uint64(1); seq <= 20000; seq++ {
			if alice.ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeInput, Seq: seq, Action: "left"}) != nil {
				return
			}
		}
	}()
	e := decode[protocol.Error](t, alice.waitFor(protocol.TypeError, nil))
	if e.Code != "rate_limited" {
		t.Fatalf("error = %+v", e)
	}
	for range alice.msgs { // the server closes the flooder's socket
	}
	bob.waitFor(protocol.TypeEvent, isEvent(protocol.EvPlayerLeft))
	bob.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: 1, Action: "rotate"})
	bob.waitFor(protocol.TypeState, func(b []byte) bool { return decode[protocol.State](t, b).Ack == 1 })
}

// Bug: a message with a wrongly typed field ("seq":"x") closed the connection, while a
// syntax error only produced an error reply.
func TestBugBadFieldTypeClosesConnection(t *testing.T) {
	ts := startDefaultServer(t)
	alice, _, _, _ := joinPair(t, ts.url)
	if err := alice.ws.WriteMessage(websocket.TextMessage, []byte(`{"type":"input","seq":"x","action":"left"}`)); err != nil {
		t.Fatal(err)
	}
	if e := decode[protocol.Error](t, alice.waitFor(protocol.TypeError, nil)); e.Code != "bad_request" {
		t.Fatalf("error = %+v", e)
	}
	alice.send(protocol.ClientMsg{Type: protocol.TypePing, T: 42}) // the socket is still usable
	if p := decode[protocol.Pong](t, alice.waitFor(protocol.TypePong, nil)); p.T != 42 {
		t.Fatalf("pong = %+v", p)
	}
}

// Bug: name length was counted in bytes and any bytes were accepted, so a 13-letter
// Cyrillic name was refused while names with control characters got through.
func TestBugNameValidationByBytes(t *testing.T) {
	ts := startDefaultServer(t)
	for name, ok := range map[string]bool{
		"Александрович": true, // 13 letters, 26 bytes
		"bad\x07name":   false,
		"line\nbreak":   false,
	} {
		c := dial(t, ts.url)
		c.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: name})
		if ok {
			c.waitFor(protocol.TypeQueued, nil)
			continue
		}
		if e := decode[protocol.Error](t, c.waitFor(protocol.TypeError, nil)); e.Code != "bad_name" {
			t.Fatalf("name %q: %+v", name, e)
		}
	}
}
