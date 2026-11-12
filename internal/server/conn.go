package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

const (
	writeWait   = 5 * time.Second
	maxMsgBytes = 4 << 10
	// outboxSize bounds the queued messages that must not be dropped (events, snapshots,
	// errors). State messages are conflated and never count against it.
	outboxSize = 128
)

// connLimits are the per-connection knobs taken from Config.
type connLimits struct {
	pingInterval time.Duration
	pongWait     time.Duration
	rate, burst  float64 // client messages per second (token bucket); rate 0 = unlimited
	rateLimited  prometheus.Counter
	kicked       *prometheus.CounterVec // by reason; nil in unit tests
}

func (l connLimits) kick(reason string) {
	if l.kicked != nil {
		l.kicked.WithLabelValues(reason).Inc()
	}
}

// client wraps one WebSocket. It implements match.Sink: Send never blocks the match
// goroutine.
//
// The outbox conflates state messages: a state is a full authoritative picture, so a newer
// one replaces an older one that has not been written yet. A client on a bad mobile link
// therefore just sees fewer frames instead of being disconnected. Events, snapshots and
// errors are queued in order and never dropped; a client that falls outboxSize of those
// behind is disconnected (it can resume and get a fresh snapshot).
type client struct {
	ws     *websocket.Conn
	limits connLimits
	ctx    context.Context // cancelled when the socket is gone
	cancel context.CancelFunc

	mu       sync.Mutex
	queue    []any
	spare    []any
	stateIdx int // index of the pending state in queue, -1 if none
	pinned   int // queued messages that are not states
	wake     chan struct{}

	quit      chan struct{}
	closeOnce sync.Once
	reason    string
	done      chan struct{} // writer exited
}

func newClient(ws *websocket.Conn, limits connLimits) *client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		ws: ws, limits: limits, ctx: ctx, cancel: cancel, stateIdx: -1,
		wake: make(chan struct{}, 1), quit: make(chan struct{}), done: make(chan struct{}),
	}
	go c.writeLoop()
	return c
}

// Send queues msg for the writer goroutine.
func (c *client) Send(msg any) bool {
	select {
	case <-c.quit:
		return false
	default:
	}
	c.mu.Lock()
	if _, isState := msg.(protocol.State); isState {
		if c.stateIdx >= 0 { // superseded before it was written
			c.queue = append(c.queue[:c.stateIdx], c.queue[c.stateIdx+1:]...)
		}
		c.stateIdx = len(c.queue)
		c.queue = append(c.queue, msg)
	} else {
		if c.pinned >= outboxSize {
			c.mu.Unlock()
			c.limits.kick("slow_consumer")
			c.Close("slow consumer")
			return false
		}
		c.pinned++
		c.queue = append(c.queue, msg)
	}
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return true
}

// take swaps out everything queued so far (double-buffered: no allocation per drain).
func (c *client) take() []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.queue
	c.queue, c.spare = c.spare[:0], nil
	c.stateIdx, c.pinned = -1, 0
	return b
}

func (c *client) recycle(b []any) {
	clear(b)
	c.mu.Lock()
	if c.spare == nil {
		c.spare = b[:0]
	}
	c.mu.Unlock()
}

// Close asks the writer to send a close frame and shut the socket. Safe to call from any
// goroutine, any number of times.
func (c *client) Close(reason string) {
	c.closeOnce.Do(func() {
		c.reason = reason
		close(c.quit)
		c.cancel()
	})
}

func (c *client) writeLoop() {
	defer close(c.done)
	defer c.ws.Close()
	ping := time.NewTicker(c.limits.pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-c.wake:
			batch := c.take()
			for _, msg := range batch {
				if err := c.write(msg); err != nil {
					c.limits.kick("write_failed")
					c.Close("write failed")
					return
				}
			}
			c.recycle(batch)
		case <-ping.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.limits.kick("ping_failed")
				c.Close("ping failed")
				return
			}
		case <-c.quit:
			// Flush what is already queued (e.g. the final match_over event), then close.
			for _, msg := range c.take() {
				if c.write(msg) != nil {
					return
				}
			}
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, c.reason))
			return
		}
	}
}

func (c *client) write(msg any) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteMessage(websocket.TextMessage, b)
}

// bucket is a token bucket: rate tokens per second, up to burst.
type bucket struct {
	rate, burst, tokens float64
	last                time.Time
}

func newBucket(rate, burst float64) *bucket {
	return &bucket{rate: rate, burst: burst, tokens: burst}
}

func (b *bucket) allow(now time.Time) bool {
	if !b.last.IsZero() {
		b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// readLoop decodes client messages into the returned channel, which is closed when the
// socket fails or closes.
//
// Every message passes a token bucket (limits.rate/burst). Over the limit, messages are
// dropped (an un-acked input may be resent; inputs are idempotent). A client that keeps
// flooding exhausts a second "abuse" bucket and is disconnected.
func (c *client) readLoop() <-chan protocol.ClientMsg {
	in := make(chan protocol.ClientMsg, 16)
	c.ws.SetReadLimit(maxMsgBytes)
	_ = c.ws.SetReadDeadline(time.Now().Add(c.limits.pongWait))
	c.ws.SetPongHandler(func(string) error { return c.ws.SetReadDeadline(time.Now().Add(c.limits.pongWait)) })
	var limit, abuse *bucket
	if c.limits.rate > 0 {
		limit = newBucket(c.limits.rate, max(1, c.limits.burst))
		abuse = newBucket(c.limits.rate/4, 5*max(1, c.limits.burst))
	}
	go func() {
		defer close(in)
		defer c.cancel()
		for {
			var msg protocol.ClientMsg
			if err := c.ws.ReadJSON(&msg); err != nil {
				var syntax *json.SyntaxError
				var typ *json.UnmarshalTypeError
				switch {
				case errors.As(err, &syntax):
					c.Send(errMsg("bad_json", err.Error()))
					continue
				case errors.As(err, &typ):
					c.Send(errMsg("bad_request", err.Error()))
					continue
				}
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					c.limits.kick("pong_timeout") // silent peer: no message or pong within pongWait
				}
				return
			}
			now := time.Now()
			_ = c.ws.SetReadDeadline(now.Add(c.limits.pongWait))
			if limit != nil && !limit.allow(now) {
				if msg.Type == protocol.TypeInput && c.limits.rateLimited != nil {
					c.limits.rateLimited.Inc()
				}
				if !abuse.allow(now) {
					c.limits.kick("rate_limit")
					c.Send(errMsg("rate_limited", "too many messages"))
					c.Close("rate limit exceeded")
					return
				}
				continue
			}
			select {
			case in <- msg:
			case <-c.quit:
				return
			}
		}
	}()
	return in
}

func errMsg(code, text string) protocol.Error {
	return protocol.Error{Type: protocol.TypeError, Code: code, Message: text}
}
