package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/wire"
)

const (
	writeWait   = 5 * time.Second
	maxMsgBytes = 4 << 10
	// outboxSize bounds the queued messages that must not be dropped (events, snapshots,
	// errors). State messages are conflated and never count against it.
	outboxSize = 128
)

const (
	dirIn, dirOut      = 0, 1
	fmtJSON, fmtBinary = 0, 1
)

// traffic counts WebSocket payload bytes and frames by direction and format. A nil
// counter (unit tests) is skipped.
type traffic struct {
	bytes, frames [2][2]prometheus.Counter
}

func (t *traffic) add(dir, format, n int) {
	if t == nil || t.bytes[dir][format] == nil {
		return
	}
	t.bytes[dir][format].Add(float64(n))
	t.frames[dir][format].Inc()
}

// connLimits are the per-connection knobs taken from Config.
type connLimits struct {
	pingInterval time.Duration
	pongWait     time.Duration
	now          func() time.Time // the match clock, for when a client was last heard from
	rate, burst  float64          // client messages per second (token bucket); rate 0 = unlimited
	wireDelta    bool             // binary clients get state deltas (false: full binary frames)
	rateLimited  prometheus.Counter
	kicked       *prometheus.CounterVec // by reason; nil in unit tests
	traffic      *traffic
}

func (l connLimits) clock() time.Time {
	if l.now == nil {
		return time.Now()
	}
	return l.now()
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
//
// A binary client (subprotocol wire.Subprotocol) gets state and pong as binary frames.
// States are deltas against base, the last state this connection actually wrote: the
// writer is the only goroutine that knows what the client has, and computing the delta
// here keeps it consistent with conflation (a dropped state never becomes a base).
type client struct {
	ws     *websocket.Conn
	limits connLimits
	binary bool
	ctx    context.Context // cancelled when the socket is gone
	cancel context.CancelFunc

	mu       sync.Mutex
	queue    []any
	spare    []any
	stateIdx int // index of the pending state in queue, -1 if none
	pinned   int // queued messages that are not states
	wake     chan struct{}

	// Writer goroutine only.
	base *protocol.State
	enc  []byte

	quit      chan struct{}
	closeOnce sync.Once
	reason    string
	done      chan struct{} // writer exited

	heard atomic.Int64 // match-clock UnixNano of the last frame or pong from the client
}

func newClient(ws *websocket.Conn, limits connLimits, binary bool) *client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		ws: ws, limits: limits, binary: binary, ctx: ctx, cancel: cancel, stateIdx: -1,
		wake: make(chan struct{}, 1), quit: make(chan struct{}), done: make(chan struct{}),
	}
	c.heard.Store(limits.clock().UnixNano())
	go c.writeLoop()
	return c
}

// lastHeard is when the client last sent anything: a message of any kind, or a pong to
// the server's ping. Browsers and WebSocket libraries answer pings by themselves, so a
// live connection is heard at least once per ping interval even when the player is not
// touching anything.
func (c *client) lastHeard() time.Time { return time.Unix(0, c.heard.Load()) }

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
	if c.binary {
		switch m := msg.(type) {
		case protocol.State:
			var prev *protocol.State
			if c.limits.wireDelta {
				prev = c.base
			}
			if b, err := wire.AppendState(c.enc[:0], prev, &m); err == nil {
				c.enc, c.base = b, &m
				return c.send(websocket.BinaryMessage, b)
			}
			// Not encodable (never for a real match): fall through to JSON, which the
			// client reads as a full state.
			c.base = &m
		case protocol.Pong:
			c.enc = wire.AppendPong(c.enc[:0], m.T)
			return c.send(websocket.BinaryMessage, c.enc)
		case protocol.Snapshot:
			st := m.State
			c.base = &st // the client's state is now the snapshot's
		}
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.send(websocket.TextMessage, b)
}

func (c *client) send(messageType int, b []byte) error {
	_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	if err := c.ws.WriteMessage(messageType, b); err != nil {
		return err
	}
	format := fmtJSON
	if messageType == websocket.BinaryMessage {
		format = fmtBinary
	}
	c.limits.traffic.add(dirOut, format, len(b))
	return nil
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
// socket fails or closes. Text frames are JSON, binary frames are wire frames (input,
// ping); both decode to the same protocol.ClientMsg.
//
// Every frame passes a token bucket (limits.rate/burst) before it is decoded, so a flood
// of malformed frames is limited like any other. Over the limit, frames are dropped (an
// un-acked input may be resent; inputs are idempotent). A client that keeps flooding
// exhausts a second "abuse" bucket and is disconnected.
func (c *client) readLoop() <-chan protocol.ClientMsg {
	in := make(chan protocol.ClientMsg, 16)
	c.ws.SetReadLimit(maxMsgBytes)
	_ = c.ws.SetReadDeadline(time.Now().Add(c.limits.pongWait))
	c.ws.SetPongHandler(func(string) error {
		c.heard.Store(c.limits.clock().UnixNano())
		return c.ws.SetReadDeadline(time.Now().Add(c.limits.pongWait))
	})
	var limit, abuse *bucket
	if c.limits.rate > 0 {
		limit = newBucket(c.limits.rate, max(1, c.limits.burst))
		abuse = newBucket(c.limits.rate/4, 5*max(1, c.limits.burst))
	}
	go func() {
		defer close(in)
		defer c.cancel()
		var buf bytes.Buffer
		for {
			mt, r, err := c.ws.NextReader()
			if err == nil {
				buf.Reset()
				_, err = buf.ReadFrom(r)
			}
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					c.limits.kick("pong_timeout") // silent peer: no message or pong within pongWait
				}
				return
			}
			c.heard.Store(c.limits.clock().UnixNano())
			now := time.Now()
			_ = c.ws.SetReadDeadline(now.Add(c.limits.pongWait))
			format := fmtJSON
			if mt == websocket.BinaryMessage {
				format = fmtBinary
			}
			c.limits.traffic.add(dirIn, format, buf.Len())
			if limit != nil && !limit.allow(now) {
				if c.limits.rateLimited != nil {
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
			msg, err := decodeClient(format, buf.Bytes())
			if err != nil {
				code := "bad_request"
				var syntax *json.SyntaxError
				if errors.As(err, &syntax) {
					code = "bad_json"
				}
				c.Send(errMsg(code, err.Error()))
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

func decodeClient(format int, b []byte) (protocol.ClientMsg, error) {
	if format == fmtBinary {
		return wire.DecodeClient(b)
	}
	var msg protocol.ClientMsg
	err := json.Unmarshal(b, &msg)
	return msg, err
}

func errMsg(code, text string) protocol.Error {
	return protocol.Error{Type: protocol.TypeError, Code: code, Message: text}
}
