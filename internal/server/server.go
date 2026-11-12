// Package server exposes matches over WebSocket and the ops endpoints over HTTP.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Gegirhasut/sabostack-go-server/internal/lobby"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

const helloTimeout = 10 * time.Second

// Server wires the matchmaking queue and match manager to WebSocket clients.
type Server struct {
	mgr      *match.Manager
	queue    *lobby.Queue
	metrics  *metrics.Metrics
	gatherer prometheus.Gatherer
	log      *slog.Logger
	upgrader websocket.Upgrader

	conns sync.WaitGroup
}

// New creates a Server. gatherer is the registry served on /metrics.
func New(mgr *match.Manager, met *metrics.Metrics, gatherer prometheus.Gatherer, log *slog.Logger) *Server {
	return &Server{
		mgr: mgr, queue: lobby.New(mgr.Create), metrics: met, gatherer: gatherer, log: log,
		// This is a demo server: accept any origin. A real deployment would check Origin.
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.gatherer, promhttp.HandlerOpts{}))
	return mux
}

// Shutdown stops matchmaking, snapshots and stops all matches (which closes their
// sockets), then waits for connection handlers to exit. Call it after http.Server.Shutdown
// has stopped accepting new connections.
func (s *Server) Shutdown(ctx context.Context) error {
	s.queue.Close()
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

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already replied with an HTTP error
	}
	s.conns.Add(1)
	defer s.conns.Done()
	s.metrics.ConnectedPlayers.Inc()
	defer s.metrics.ConnectedPlayers.Dec()

	c := newClient(ws)
	defer func() { c.Close("bye"); <-c.done }()
	in := c.readLoop()
	log := s.log.With("remote", r.RemoteAddr)

	// The first message decides the session: join the queue or resume a match.
	var hello protocol.ClientMsg
	select {
	case msg, ok := <-in:
		if !ok {
			return
		}
		hello = msg
	case <-time.After(helloTimeout):
		c.Send(errMsg("timeout", "expected join or resume"))
		return
	}

	var (
		actor *match.Actor
		seat  int
	)
	switch hello.Type {
	case protocol.TypeJoin:
		actor, seat, err = s.joinQueue(r.Context(), c, in, hello)
	case protocol.TypeResume:
		actor, seat, err = s.resume(r.Context(), c, hello.MatchID, hello.Token, hello.LastEventSeq)
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

func (s *Server) joinQueue(ctx context.Context, c *client, in <-chan protocol.ClientMsg, hello protocol.ClientMsg) (*match.Actor, int, error) {
	name := strings.TrimSpace(hello.Name)
	if name == "" || len(name) > 24 {
		c.Send(errMsg("bad_name", "name must be 1-24 characters"))
		return nil, 0, errors.New("bad name")
	}
	ticket := s.queue.Join(name)
	c.Send(protocol.Queued{Type: protocol.TypeQueued})

	var a lobby.Assignment
	for waiting := true; waiting; {
		select {
		case a = <-ticket.C():
			waiting = false
		case _, ok := <-in:
			if ok {
				continue // nothing to do while queued; ignore
			}
			if s.queue.Leave(ticket) {
				return nil, 0, errors.New("left queue")
			}
			a = <-ticket.C() // matched concurrently with the disconnect
			waiting = false
		}
	}
	if a.Err != nil {
		c.Send(errMsg("unavailable", a.Err.Error()))
		return nil, 0, a.Err
	}
	c.Send(protocol.Matched{Type: protocol.TypeMatched, MatchID: a.MatchID, Seat: a.Seat, Token: a.Token, Opponent: a.Opponent})
	return s.resume(ctx, c, a.MatchID, a.Token, 0)
}

func (s *Server) resume(ctx context.Context, c *client, matchID, token string, lastEventSeq uint64) (*match.Actor, int, error) {
	actor, ok := s.mgr.Get(matchID)
	if !ok {
		c.Send(errMsg("match_not_found", "no such match on this server"))
		return nil, 0, errors.New("match not found")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
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
