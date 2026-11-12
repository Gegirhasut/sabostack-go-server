package server

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

const (
	writeWait    = 5 * time.Second
	pongWait     = 30 * time.Second
	pingInterval = pongWait * 2 / 3
	maxMsgBytes  = 4 << 10
	outboxSize   = 128
)

// client wraps one WebSocket. It implements match.Sink: Send never blocks the match
// goroutine, and a client that falls outboxSize messages behind is disconnected (it can
// resume and get a fresh snapshot).
type client struct {
	ws        *websocket.Conn
	out       chan any
	quit      chan struct{}
	closeOnce sync.Once
	reason    string
	done      chan struct{} // writer exited
}

func newClient(ws *websocket.Conn) *client {
	c := &client{ws: ws, out: make(chan any, outboxSize), quit: make(chan struct{}), done: make(chan struct{})}
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
	select {
	case c.out <- msg:
		return true
	default:
		c.Close("slow consumer")
		return false
	}
}

// Close asks the writer to send a close frame and shut the socket. Safe to call from any
// goroutine, any number of times.
func (c *client) Close(reason string) {
	c.closeOnce.Do(func() {
		c.reason = reason
		close(c.quit)
	})
}

func (c *client) writeLoop() {
	defer close(c.done)
	defer c.ws.Close()
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case msg := <-c.out:
			if err := c.write(msg); err != nil {
				c.Close("write failed")
				return
			}
		case <-ping.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.Close("ping failed")
				return
			}
		case <-c.quit:
			// Flush what is already queued (e.g. the final match_over event), then close.
			for {
				select {
				case msg := <-c.out:
					if c.write(msg) != nil {
						return
					}
					continue
				default:
				}
				break
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

// readLoop decodes client messages into the returned channel, which is closed when the
// socket fails or closes.
func (c *client) readLoop() <-chan protocol.ClientMsg {
	in := make(chan protocol.ClientMsg, 16)
	c.ws.SetReadLimit(maxMsgBytes)
	_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error { return c.ws.SetReadDeadline(time.Now().Add(pongWait)) })
	go func() {
		defer close(in)
		for {
			var msg protocol.ClientMsg
			if err := c.ws.ReadJSON(&msg); err != nil {
				if _, ok := err.(*json.SyntaxError); ok {
					c.Send(errMsg("bad_json", err.Error()))
					continue
				}
				return
			}
			_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
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
