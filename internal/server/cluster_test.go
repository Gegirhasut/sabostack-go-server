package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/cluster"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
	"github.com/Gegirhasut/sabostack-go-server/internal/testredis"
)

// clusterServer is one game server of a test cluster sharing a Redis.
type clusterServer struct {
	name   string
	url    string
	mgr    *match.Manager
	srv    *Server
	http   *httptest.Server
	cancel context.CancelFunc
}

func startClusterServer(t *testing.T, mr *testredis.R, name string) *clusterServer {
	t.Helper()
	rdb := redis.NewClient(mr.Options())
	t.Cleanup(func() { rdb.Close() })
	hs := httptest.NewUnstartedServer(nil)
	url := "ws://" + hs.Listener.Addr().String() + "/ws"
	st := store.NewRedis(rdb, "k:", time.Hour).ForNode(name, url)
	st.SetAdoptFilter(match.SnapshotSupported)

	reg := prometheus.NewRegistry()
	met := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := match.NewManager(fastOptions(), clock.Real{}, st, met, log)
	cfg := DefaultConfig()
	cfg.InputRate, cfg.NodeID = 0, name
	srv := NewWithConfig(mgr, met, reg, log, cfg)
	opts := cluster.Options{Lease: time.Second, AdoptEvery: 50 * time.Millisecond, AdoptBatch: 100}
	node := cluster.NewNode(st, mgr, met, log, opts, func() bool { return !srv.Draining() })
	if err := node.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	mm, err := cluster.NewMatchmaker(rdb, "k:", name, url, mgr.CreateWithTokenHashes, met, log)
	if err != nil {
		t.Fatal(err)
	}
	srv.UseMatchmaker(mm)
	ctx, cancel := context.WithCancel(context.Background())
	go node.Run(ctx)
	hs.Config.Handler = srv.Handler()
	hs.Start()
	cs := &clusterServer{name: name, url: url, mgr: mgr, srv: srv, http: hs, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		hs.Listener.Close()
		_ = srv.Shutdown(ctx)
		hs.Close()
	})
	return cs
}

// crash kills the server as a crash would: sockets drop, no final snapshot, no heartbeat.
func (cs *clusterServer) crash() {
	cs.cancel()
	cs.http.CloseClientConnections()
	cs.mgr.EvictAll()
	cs.http.Listener.Close()
}

// joinAnywhere joins the queue through url, like a client behind a load balancer.
func joinAnywhere(t *testing.T, url, name string) *wsClient {
	t.Helper()
	c := dial(t, url)
	c.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: name})
	c.waitFor(protocol.TypeQueued, nil)
	return c
}

// matchedAnywhere waits for the pairing and follows its redirect.
func matchedAnywhere(t *testing.T, c *wsClient) (*wsClient, protocol.Matched) {
	t.Helper()
	m := decode[protocol.Matched](t, c.waitFor(protocol.TypeMatched, nil))
	if m.URL != "" {
		c.ws.Close()
		c = dial(t, m.URL)
		c.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: m.MatchID, Token: m.Token})
	}
	c.waitFor(protocol.TypeSnapshot, nil)
	return c, m
}

// resumeAnywhere resumes through url, following a wrong_server redirect.
func resumeAnywhere(t *testing.T, url string, m protocol.Matched, seen uint64) (*wsClient, protocol.Snapshot) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		c := dial(t, url)
		c.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: m.MatchID, Token: m.Token, LastEventSeq: seen})
		b := waitAny(c, protocol.TypeSnapshot, protocol.TypeError)
		var e envelope
		_ = json.Unmarshal(b, &e)
		if e.Type == protocol.TypeSnapshot {
			return c, decode[protocol.Snapshot](t, b)
		}
		er := decode[protocol.Error](t, b)
		c.ws.Close()
		if er.Code == "wrong_server" {
			url = er.URL
		}
		// match_not_found: not adopted yet; try again
	}
	t.Fatal("could not resume the match on any server")
	return nil, protocol.Snapshot{}
}

func waitAny(c *wsClient, types ...string) []byte {
	c.t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case e, ok := <-c.msgs:
			if !ok {
				c.t.Fatal("connection closed")
			}
			for _, typ := range types {
				if e.Type == typ {
					return e.Raw
				}
			}
		case <-timeout:
			c.t.Fatalf("timed out waiting for %v", types)
		}
	}
}

// Two players on different servers get one match on one of them; when that server
// crashes mid-round the other server adopts the match from its snapshot, both players
// resume there and the match plays to the end.
func TestClusterCrossNodeMatchSurvivesNodeCrash(t *testing.T) {
	mr := testredis.New(t)
	a := startClusterServer(t, mr, "a")
	b := startClusterServer(t, mr, "b")

	alice0, bob0 := joinAnywhere(t, a.url, "alice"), joinAnywhere(t, b.url, "bob")
	alice, ma := matchedAnywhere(t, alice0)
	bob, mb := matchedAnywhere(t, bob0)
	if ma.MatchID != mb.MatchID || (ma.URL == "") == (mb.URL == "") {
		t.Fatalf("expected one match with exactly one redirect: %+v / %+v", ma, mb)
	}
	host, other := a, b
	if _, ok := a.mgr.Get(ma.MatchID); !ok {
		host, other = b, a
	}
	if _, ok := host.mgr.Get(ma.MatchID); !ok {
		t.Fatal("match runs on neither server")
	}
	alice.waitFor(protocol.TypeEvent, isPhase("playing"))
	bob.waitFor(protocol.TypeEvent, isPhase("playing"))
	alice.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: 1, Action: "hard_drop"})
	alice.waitFor(protocol.TypeState, func(b []byte) bool { return decode[protocol.State](t, b).Ack == 1 })
	time.Sleep(150 * time.Millisecond) // a periodic snapshot (50 ms) has captured the drop

	host.crash()
	for _, c := range []*wsClient{alice, bob} {
		for range c.msgs { // their sockets died with the host
		}
	}
	mr.FastForward(2 * time.Second) // the host's lease runs out
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := other.mgr.Get(ma.MatchID); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the surviving server did not adopt the match")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Both reconnect through the surviving server (the only one left behind the LB).
	alice, snap := resumeAnywhere(t, other.url, ma, 0)
	if snap.State.Phase != "playing" || snap.State.Ack != 1 {
		t.Fatalf("adopted match: phase=%s ack=%d", snap.State.Phase, snap.State.Ack)
	}
	bob, _ = resumeAnywhere(t, other.url, mb, 0)

	// The seq consumed before the crash is still deduplicated on the new server (once the
	// adopted match runs again: it waits for both players).
	alice.waitFor(protocol.TypeEvent, isEvent(protocol.EvResumed))
	alice.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: 1, Action: "left"})
	alice.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: 2, Action: "left"})
	alice.waitFor(protocol.TypeState, func(b []byte) bool { return decode[protocol.State](t, b).Ack == 2 })

	stop := make(chan struct{})
	done := hammer(bob, stop)
	over := decode[protocol.EventMsg](t, alice.waitFor(protocol.TypeEvent, isEvent(protocol.EvMatchOver)))
	close(stop)
	<-done
	if over.Event.Winner == nil || *over.Event.Winner != ma.Seat || over.Event.Reason != "rounds" {
		t.Fatalf("match_over = %+v (alice is seat %d)", over.Event, ma.Seat)
	}
}

// A resume that reaches a server not running the match is redirected to its owner.
func TestClusterResumeRedirect(t *testing.T) {
	mr := testredis.New(t)
	a := startClusterServer(t, mr, "a")
	b := startClusterServer(t, mr, "b")
	alice0, bob0 := joinAnywhere(t, a.url, "alice"), joinAnywhere(t, a.url, "bob")
	alice, m := matchedAnywhere(t, alice0)
	_, _ = matchedAnywhere(t, bob0)
	owner := a
	if _, ok := a.mgr.Get(m.MatchID); !ok {
		owner = b
	}
	wrong := b
	if owner == b {
		wrong = a
	}
	alice.ws.Close()
	c := dial(t, wrong.url)
	c.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: m.MatchID, Token: m.Token})
	e := decode[protocol.Error](t, c.waitFor(protocol.TypeError, nil))
	if e.Code != "wrong_server" || !strings.HasSuffix(e.URL, strings.TrimPrefix(owner.url, "ws://")) {
		t.Fatalf("error = %+v, owner %s", e, owner.url)
	}
}
