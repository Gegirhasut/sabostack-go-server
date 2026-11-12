// Package server exposes matches over WebSocket and the ops endpoints over HTTP.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Gegirhasut/sabostack-go-server/internal/lobby"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
	"github.com/Gegirhasut/sabostack-go-server/internal/wire"
)

// Config holds the transport and abuse-protection settings.
type Config struct {
	// AllowedOrigins lists the accepted browser Origin hosts (e.g. "game.example.com").
	// Empty accepts any origin. Requests without an Origin header (native apps) are
	// always accepted.
	AllowedOrigins []string
	MaxConns       int // open WebSockets per process; 0 = unlimited
	MaxConnsPerIP  int // open WebSockets per client IP; 0 = unlimited
	// TrustedProxies is how many reverse proxies of ours stand in front of the server
	// (0 = none: X-Forwarded-For is ignored). The client IP is then the address the
	// outermost of them saw: the TrustedProxies-th entry of X-Forwarded-For from the right.
	TrustedProxies int

	HelloTimeout time.Duration // first message (join/resume) must arrive within this
	PingInterval time.Duration // server ping frames
	PongWait     time.Duration // a socket silent for this long is considered dead

	InputRate  float64 // client messages per second per connection; 0 = unlimited
	InputBurst float64

	PublicMetrics bool   // also serve /metrics on the public handler
	NodeID        string // this node's id, to tell local from remote owners on resume

	// BinaryFullFrames sends binary clients a full state frame every time instead of a
	// delta. For measuring what the format alone saves; leave it off in production.
	BinaryFullFrames bool
}

// DefaultConfig returns safe defaults. The heartbeat keeps the original tolerant timings:
// a 3 s ping / 10 s pong-wait was tried and, on an overloaded host, disconnected clients
// that were merely slow to read. Tighten PingInterval/PongWait (flags) only after
// measuring on real mobile networks; PongWait is how long a silently dropped connection
// keeps counting as connected (and so is not paused).
func DefaultConfig() Config {
	return Config{
		HelloTimeout:  10 * time.Second,
		PingInterval:  20 * time.Second,
		PongWait:      30 * time.Second,
		InputRate:     40,
		InputBurst:    20,
		PublicMetrics: true,
	}
}

// Matchmaker pairs queued players. *lobby.Queue does it inside one process;
// cluster.Matchmaker does it across all nodes sharing a Redis.
type Matchmaker interface {
	JoinContext(ctx context.Context, name string) *lobby.Ticket
	Leave(t *lobby.Ticket) bool
	Close()
}

// errRedirected ends a session whose match runs on another server: the client was told
// where (matched.url) and reconnects there.
var errRedirected = errors.New("match runs on another server")

// assignWait bounds how long a player who left the queue waits for an assignment that
// was already on its way (a lost one must not leak the connection handler).
const assignWait = 5 * time.Second

// Server wires the matchmaking queue and match manager to WebSocket clients.
type Server struct {
	mgr      *match.Manager
	queue    Matchmaker
	metrics  *metrics.Metrics
	gatherer prometheus.Gatherer
	log      *slog.Logger
	cfg      Config
	upgrader websocket.Upgrader
	origins  map[string]bool
	limits   connLimits

	// conns counts connection handlers. A handler registers (track) before its upgrade,
	// under connMu, and Shutdown sets closing under the same lock before it waits: so no
	// Add can race with Wait (which sync.WaitGroup forbids), and no connection is served
	// after Shutdown. http.Server.Shutdown does not wait for hijacked connections.
	conns    sync.WaitGroup
	connMu   sync.Mutex
	closing  bool
	open     atomic.Int64
	ipMu     sync.Mutex
	perIP    map[string]int
	draining atomic.Bool
}

// New creates a Server with DefaultConfig. gatherer is the registry served on /metrics.
func New(mgr *match.Manager, met *metrics.Metrics, gatherer prometheus.Gatherer, log *slog.Logger) *Server {
	return NewWithConfig(mgr, met, gatherer, log, DefaultConfig())
}

// NewWithConfig creates a Server.
func NewWithConfig(mgr *match.Manager, met *metrics.Metrics, gatherer prometheus.Gatherer, log *slog.Logger, cfg Config) *Server {
	def := DefaultConfig()
	if cfg.HelloTimeout <= 0 {
		cfg.HelloTimeout = def.HelloTimeout
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = def.PingInterval
	}
	if cfg.PongWait <= cfg.PingInterval {
		cfg.PongWait = max(def.PongWait, 3*cfg.PingInterval)
	}
	s := &Server{
		mgr: mgr, queue: lobby.New(mgr.Create), metrics: met, gatherer: gatherer, log: log, cfg: cfg,
		origins: map[string]bool{}, perIP: map[string]int{},
		limits: connLimits{
			pingInterval: cfg.PingInterval, pongWait: cfg.PongWait,
			rate: cfg.InputRate, burst: cfg.InputBurst,
			wireDelta:   !cfg.BinaryFullFrames,
			rateLimited: met.Input("rate_limited"), kicked: met.Kicked,
			traffic: newTraffic(met),
		},
	}
	for _, o := range cfg.AllowedOrigins {
		if o = strings.ToLower(strings.TrimSpace(o)); o != "" {
			s.origins[o] = true
		}
	}
	// Default buffer sizes on purpose: gorilla then reuses the 4 KB buffers net/http
	// already allocated for the connection. A WriteBufferPool was measured to add heap
	// (≈ +8 MB at 4000 connections), not save it.
	s.upgrader = websocket.Upgrader{CheckOrigin: s.checkOrigin, Subprotocols: []string{wire.Subprotocol}}
	return s
}

func newTraffic(met *metrics.Metrics) *traffic {
	t := &traffic{}
	for d, dir := range [2]string{"in", "out"} {
		for f, format := range [2]string{"json", "binary"} {
			t.bytes[d][f] = met.WSBytes.WithLabelValues(dir, format)
			t.frames[d][f] = met.WSFrames.WithLabelValues(dir, format)
		}
	}
	return t
}

func (s *Server) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || len(s.origins) == 0 {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && s.origins[strings.ToLower(u.Host)]
}

// Handler returns the public routes: the WebSocket endpoint and health checks (plus
// /metrics if Config.PublicMetrics).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWS)
	s.health(mux)
	if s.cfg.PublicMetrics {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.gatherer, promhttp.HandlerOpts{}))
	}
	return mux
}

// AdminHandler returns the internal routes: metrics, health and pprof. Serve it on a
// private address only.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	s.health(mux)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func (s *Server) health(mux *http.ServeMux) {
	// healthz: the process is alive. readyz: it wants new players (503 while draining, so
	// the load balancer sends new connections elsewhere).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if s.draining.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"draining"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
}

// Drain stops matchmaking on this node: /readyz turns 503, queued players are told to
// go elsewhere, and new joins are refused. Running matches and resumes keep working, so
// they can finish before the process exits.
func (s *Server) Drain() {
	if s.draining.Swap(true) {
		return
	}
	s.queue.Close()
}

// Draining reports whether Drain was called.
func (s *Server) Draining() bool { return s.draining.Load() }

// UseMatchmaker replaces the in-process queue (call before serving).
func (s *Server) UseMatchmaker(mm Matchmaker) { s.queue = mm }

// Shutdown stops matchmaking, snapshots and stops all matches (which closes their
// sockets), then waits for connection handlers to exit. Call it after http.Server.Shutdown
// has stopped accepting new connections.
func (s *Server) Shutdown(ctx context.Context) error {
	s.Drain()
	s.connMu.Lock()
	s.closing = true
	s.connMu.Unlock()
	err := s.mgr.Shutdown(ctx)
	done := make(chan struct{})
	go func() { s.conns.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		err = errors.Join(err, ctx.Err())
	}
	return err
}

// track registers a connection handler, or reports false once Shutdown has begun.
func (s *Server) track() bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closing {
		return false
	}
	s.conns.Add(1)
	return true
}

// clientIP returns the address connection limits are keyed by.
//
// X-Forwarded-For is read from the right. Every proxy appends the address it received the
// request from, so only the last TrustedProxies entries were written by our proxies;
// everything to the left of them came from the client and is free to forge. (Taking the
// first entry, as before, let a client pick any IP it liked and bypass -max-conns-per-ip.)
func (s *Server) clientIP(r *http.Request) string {
	if n := s.cfg.TrustedProxies; n > 0 {
		var hops []string
		for _, v := range r.Header.Values("X-Forwarded-For") {
			hops = append(hops, strings.Split(v, ",")...)
		}
		if len(hops) > 0 {
			// Fewer entries than proxies: the client added none, the leftmost is real.
			hop := strings.TrimSpace(hops[max(0, len(hops)-n)])
			if ip := net.ParseIP(hop); ip != nil {
				return ip.String()
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// admit reserves a connection slot for ip, or returns the reason it is refused.
func (s *Server) admit(ip string) (release func(), reason string) {
	if n := s.open.Add(1); s.cfg.MaxConns > 0 && n > int64(s.cfg.MaxConns) {
		s.open.Add(-1)
		return nil, "max_conns"
	}
	if s.cfg.MaxConnsPerIP > 0 {
		s.ipMu.Lock()
		if s.perIP[ip] >= s.cfg.MaxConnsPerIP {
			s.ipMu.Unlock()
			s.open.Add(-1)
			return nil, "max_conns_per_ip"
		}
		s.perIP[ip]++
		s.ipMu.Unlock()
	}
	return func() {
		s.open.Add(-1)
		if s.cfg.MaxConnsPerIP > 0 {
			s.ipMu.Lock()
			if s.perIP[ip]--; s.perIP[ip] <= 0 {
				delete(s.perIP, ip)
			}
			s.ipMu.Unlock()
		}
	}, ""
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	ip := s.clientIP(r)
	release, reason := s.admit(ip)
	if release == nil {
		s.metrics.ConnsRejected.WithLabelValues(reason).Inc()
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	defer release()
	if !s.track() {
		s.metrics.ConnsRejected.WithLabelValues("shutting_down").Inc()
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer s.conns.Done()
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.metrics.ConnsRejected.WithLabelValues("upgrade").Inc()
		return // Upgrade already replied with an HTTP error
	}
	s.metrics.ConnectedPlayers.Inc()
	defer s.metrics.ConnectedPlayers.Dec()

	// A client that asked for wire.Subprotocol gets binary state frames; any other
	// client speaks JSON only.
	c := newClient(ws, s.limits, ws.Subprotocol() == wire.Subprotocol)
	defer func() { c.Close("bye"); <-c.done }()
	in := c.readLoop()
	log := s.log.With("remote", ip)

	// The first message decides the session: join the queue or resume a match.
	var hello protocol.ClientMsg
	hold := time.NewTimer(s.cfg.HelloTimeout)
	select {
	case msg, ok := <-in:
		hold.Stop()
		if !ok {
			return
		}
		hello = msg
	case <-hold.C:
		c.Send(errMsg("timeout", "expected join or resume"))
		return
	}

	var (
		actor *match.Actor
		seat  int
	)
	switch hello.Type {
	case protocol.TypeJoin:
		actor, seat, err = s.joinQueue(c, in, hello)
	case protocol.TypeResume:
		actor, seat, err = s.resume(c, hello.MatchID, hello.Token, hello.LastEventSeq)
	default:
		c.Send(errMsg("bad_request", "first message must be join or resume"))
		return
	}
	if err != nil {
		log.Debug("session not started", "err", err)
		return
	}
	log = log.With("match", actor.ID(), "seat", seat)
	log.Debug("player attached")
	defer actor.Detach(seat, c)

	for msg := range in {
		switch msg.Type {
		case protocol.TypeInput:
			actor.Input(seat, msg.Seq, msg.Action)
		case protocol.TypePing:
			c.Send(protocol.Pong{Type: protocol.TypePong, T: msg.T})
		default:
			c.Send(errMsg("bad_request", "unexpected message type "+msg.Type))
		}
	}
	log.Debug("player detached")
}

// validName accepts 1-24 printable characters (runes, not bytes).
func validName(name string) bool {
	if !utf8.ValidString(name) {
		return false
	}
	n := utf8.RuneCountInString(name)
	if n == 0 || n > 24 {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) || r == utf8.RuneError || unicode.Is(unicode.Cf, r) {
			return false
		}
	}
	return true
}

func (s *Server) joinQueue(c *client, in <-chan protocol.ClientMsg, hello protocol.ClientMsg) (*match.Actor, int, error) {
	if s.draining.Load() {
		c.Send(errMsg("unavailable", "server is draining, connect again"))
		return nil, 0, errors.New("draining")
	}
	name := strings.TrimSpace(hello.Name)
	if !validName(name) {
		c.Send(errMsg("bad_name", "name must be 1-24 printable characters"))
		return nil, 0, errors.New("bad name")
	}
	ticket := s.queue.JoinContext(c.ctx, name)
	c.Send(protocol.Queued{Type: protocol.TypeQueued})

	var a lobby.Assignment
	for waiting := true; waiting; {
		select {
		case a = <-ticket.C():
			waiting = false
		case msg, ok := <-in:
			if ok {
				// Nothing else to do while queued, but a client measuring latency (or
				// checking the link is alive) still gets its pong.
				if msg.Type == protocol.TypePing {
					c.Send(protocol.Pong{Type: protocol.TypePong, T: msg.T})
				}
				continue
			}
			if s.queue.Leave(ticket) {
				return nil, 0, errors.New("left queue")
			}
			select { // matched concurrently with the disconnect
			case a = <-ticket.C():
			case <-time.After(assignWait):
				return nil, 0, errors.New("left queue; assignment never arrived")
			}
			waiting = false
		}
	}
	if a.Err != nil {
		c.Send(errMsg("unavailable", a.Err.Error()))
		return nil, 0, a.Err
	}
	c.Send(protocol.Matched{Type: protocol.TypeMatched, MatchID: a.MatchID, Seat: a.Seat, Token: a.Token, Opponent: a.Opponent, URL: a.URL})
	if a.URL != "" {
		s.metrics.Matchmaking.WithLabelValues("redirected").Inc()
		c.Close("match on another server")
		return nil, 0, errRedirected
	}
	return s.resume(c, a.MatchID, a.Token, 0)
}

func (s *Server) resume(c *client, matchID, token string, lastEventSeq uint64) (*match.Actor, int, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	actor, ok := s.mgr.Get(matchID)
	if !ok {
		switch addr, moving := s.ownerURL(ctx, matchID); {
		case addr != "":
			c.Send(protocol.Error{Type: protocol.TypeError, Code: "wrong_server", Message: "match is on another server", URL: addr})
			return nil, 0, errors.New("match on another node")
		case moving:
			c.Send(errMsg("match_moving", "the match's server is down and another one is taking it over; retry in a moment"))
			return nil, 0, errors.New("match moving")
		}
		c.Send(errMsg("match_not_found", "no such match on this server"))
		return nil, 0, errors.New("match not found")
	}
	seat, err := actor.Attach(ctx, token, c, lastEventSeq)
	switch {
	case errors.Is(err, match.ErrMatchEnded):
		return actor, seat, nil // the snapshot carried the result; stay until closed
	case errors.Is(err, match.ErrBadToken):
		c.Send(errMsg("bad_token", "invalid session token"))
		return nil, 0, err
	case err != nil:
		c.Send(errMsg("unavailable", err.Error()))
		return nil, 0, err
	}
	return actor, seat, nil
}

// ownerURL asks the store's match directory which other node runs matchID. moving means
// its owner is dead and the match is being adopted by another node.
func (s *Server) ownerURL(ctx context.Context, matchID string) (addr string, moving bool) {
	dir, ok := s.mgr.Store().(store.Directory)
	if !ok || matchID == "" {
		return "", false
	}
	node, addr, found, err := dir.Owner(ctx, matchID)
	if err != nil || !found || node == s.cfg.NodeID {
		return "", false
	}
	return addr, addr == ""
}
