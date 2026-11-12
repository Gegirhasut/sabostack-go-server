package server

// Regression tests for defects found after the first hardening pass.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// Bug: behind a proxy the client IP was the FIRST X-Forwarded-For entry, which the client
// writes itself, so a random fake address per connection bypassed -max-conns-per-ip.
func TestForwardedForIsReadFromTheRight(t *testing.T) {
	cases := []struct {
		proxies int
		xff     []string
		want    string
	}{
		{1, []string{"203.0.113.7"}, "203.0.113.7"},                    // honest client
		{1, []string{"1.2.3.4, 203.0.113.7"}, "203.0.113.7"},           // forged prefix
		{1, []string{"1.2.3.4", "203.0.113.7"}, "203.0.113.7"},         // repeated header
		{2, []string{"1.2.3.4, 203.0.113.7, 10.0.0.2"}, "203.0.113.7"}, // CDN + balancer
		{2, []string{"203.0.113.7"}, "203.0.113.7"},                    // shorter chain
		{1, []string{"1.2.3.4, not-an-ip"}, "192.0.2.1"},               // garbage: the peer
		{0, []string{"1.2.3.4"}, "192.0.2.1"},                          // not trusted
		{1, []string{" 2001:db8::1 "}, "2001:db8::1"},                  // IPv6, spaces
	}
	for _, c := range cases {
		s := &Server{cfg: Config{TrustedProxies: c.proxies}}
		r := httptest.NewRequest(http.MethodGet, "/ws", nil)
		r.RemoteAddr = "192.0.2.1:5555"
		for _, v := range c.xff {
			r.Header.Add("X-Forwarded-For", v)
		}
		if got := s.clientIP(r); got != c.want {
			t.Errorf("proxies=%d xff=%q: ip %q, want %q", c.proxies, c.xff, got, c.want)
		}
	}
}

// Bug: a queued player's messages were all ignored, app-level ping included, so a client
// measuring latency or checking the link got no pong until it was matched.
func TestPingIsAnsweredWhileQueued(t *testing.T) {
	ts := startServer(t, store.NewMemory())
	c := dial(t, ts.url)
	c.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "solo"})
	c.waitFor(protocol.TypeQueued, nil)
	c.send(protocol.ClientMsg{Type: protocol.TypePing, T: 42})
	pong := decode[protocol.Pong](t, c.waitFor(protocol.TypePong, nil))
	if pong.T != 42 {
		t.Fatalf("pong t=%d, want 42", pong.T)
	}
}

// Bug: handleWS called conns.Add after the upgrade, so a connection upgrading
// while Shutdown was already in conns.Wait added to a WaitGroup at zero concurrently with
// Wait, which sync.WaitGroup forbids (the race detector reported it in TestOriginCheck
// about once in six runs). Connections are now registered before the upgrade, under the
// same lock Shutdown uses to close the door, and refused after it.
func TestShutdownDuringUpgrades(t *testing.T) {
	for round := 0; round < 20; round++ {
		ts := startServer(t, store.NewMemory())
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if ws, _, err := websocket.DefaultDialer.Dial(ts.url, nil); err == nil {
						ws.Close()
					}
				}
			}()
		}
		time.Sleep(5 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := ts.srv.Shutdown(ctx); err != nil { // listener still open: upgrades keep coming
			t.Fatalf("shutdown: %v", err)
		}
		cancel()
		// After Shutdown returned, no new connection may be served.
		ws, resp, err := websocket.DefaultDialer.Dial(ts.url, nil)
		if err == nil {
			ws.Close()
			t.Fatal("a connection was upgraded after Shutdown returned")
		}
		if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("connection after shutdown: err=%v resp=%v, want 503", err, resp)
		}
		close(stop)
		wg.Wait()
		ts.stop(t)
	}
}
