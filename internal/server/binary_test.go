package server

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
	"github.com/Gegirhasut/sabostack-go-server/internal/wire"
)

// binClient speaks the binary protocol and rebuilds the match state from deltas only.
type binClient struct {
	t  *testing.T
	ws *websocket.Conn

	mu      sync.Mutex
	state   protocol.State // snapshot, then every delta applied to it
	have    bool
	frames  int // binary state frames
	bytes   int // their total size
	pongs   []int64
	texts   chan []byte // JSON frames, in order
	decodeE error
}

func dialBinary(t *testing.T, url string) *binClient {
	t.Helper()
	d := websocket.Dialer{Subprotocols: []string{wire.Subprotocol}}
	ws, _, err := d.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ws.Subprotocol() != wire.Subprotocol {
		t.Fatalf("subprotocol = %q", ws.Subprotocol())
	}
	c := &binClient{t: t, ws: ws, texts: make(chan []byte, 4096)}
	go func() {
		defer close(c.texts)
		for {
			mt, b, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.TextMessage {
				var e envelope
				if json.Unmarshal(b, &e) == nil && e.Type == protocol.TypeSnapshot {
					snap := decode[protocol.Snapshot](t, b)
					c.mu.Lock()
					c.state, c.have = snap.State, true
					c.mu.Unlock()
				}
				c.texts <- b
				continue
			}
			c.mu.Lock()
			switch b[0] {
			case wire.TypeState:
				var base *protocol.State
				if c.have {
					base = &c.state
				}
				next, err := wire.DecodeState(base, b)
				if err != nil && c.decodeE == nil {
					c.decodeE = err
				}
				c.state, c.have = next, true
				c.frames++
				c.bytes += len(b)
			case wire.TypePong:
				v, _ := wire.DecodePong(b)
				c.pongs = append(c.pongs, v)
			}
			c.mu.Unlock()
		}
	}()
	t.Cleanup(func() { ws.Close() })
	return c
}

func (c *binClient) snapshotState() protocol.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.decodeE != nil {
		c.t.Fatalf("decode: %v", c.decodeE)
	}
	return c.state
}

func (c *binClient) waitState(what string, cond func(protocol.State) bool) protocol.State {
	c.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(2 * time.Millisecond) {
		st := c.snapshotState()
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s; last state %+v", what, st)
		}
	}
}

func (c *binClient) waitText(typ string, match func([]byte) bool) []byte {
	c.t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case b, ok := <-c.texts:
			if !ok {
				c.t.Fatalf("connection closed while waiting for %s", typ)
			}
			var e envelope
			if json.Unmarshal(b, &e) == nil && e.Type == typ && (match == nil || match(b)) {
				return b
			}
		case <-timeout:
			c.t.Fatalf("timed out waiting for %s", typ)
		}
	}
}

func boards(st protocol.State) []protocol.PlayerView {
	out := make([]protocol.PlayerView, len(st.Players))
	for i, p := range st.Players {
		out[i] = protocol.PlayerView{Rows: p.Rows, Piece: p.Piece, Score: p.Score, Lines: p.Lines, Pending: p.Pending, Wins: p.Wins, Dead: p.Dead}
	}
	return out
}

// A binary client that only ever applies deltas ends up with exactly the authoritative
// boards; inputs, acks and ping/pong work as binary frames; JSON and binary clients share
// one match.
func TestBinaryClientRebuildsExactStateFromDeltas(t *testing.T) {
	opts := fastOptions()
	opts.Match.Gravity = time.Hour // nothing moves unless a player moves it
	cfg := DefaultConfig()
	cfg.InputRate = 0
	ts := startServerOn(t, store.NewMemory(), clock.Real{}, opts, cfg)
	alice := dialBinary(t, ts.url)
	bob := dial(t, ts.url)
	if err := alice.ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "alice"}); err != nil {
		t.Fatal(err)
	}
	alice.waitText(protocol.TypeQueued, nil)
	bob.send(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "bob"})
	mb := decode[protocol.Matched](t, bob.waitFor(protocol.TypeMatched, nil))
	alice.waitText(protocol.TypeEvent, isPhase("playing"))
	bob.waitFor(protocol.TypeEvent, isPhase("playing"))

	// Both play: alice in binary, bob in JSON, so both boards change, lock and clear.
	acts := []game.Action{game.Left, game.Rotate, game.Right, game.HardDrop}
	for seq := uint64(1); seq <= 40; seq++ {
		a := acts[seq%uint64(len(acts))]
		if err := alice.ws.WriteMessage(websocket.BinaryMessage, wire.AppendInput(nil, seq, a)); err != nil {
			t.Fatal(err)
		}
		bob.send(protocol.ClientMsg{Type: protocol.TypeInput, Seq: seq, Action: acts[(seq+1)%uint64(len(acts))]})
		time.Sleep(3 * time.Millisecond)
	}
	alice.waitState("ack 40", func(st protocol.State) bool { return st.Ack == 40 })

	if err := alice.ws.WriteMessage(websocket.BinaryMessage, wire.AppendPing(nil, 777)); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(2 * time.Millisecond) {
		alice.mu.Lock()
		got := len(alice.pongs) > 0 && alice.pongs[0] == 777
		alice.mu.Unlock()
		if got {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no binary pong")
		}
	}

	// Bob drops. The match goes on (it never pauses), but nobody moves and gravity is
	// off in this test, so the boards stand still. Alice's delta-built state must then
	// equal the authoritative snapshot bob gets when he comes back.
	bob.ws.Close()
	alice.waitState("bob away", func(st protocol.State) bool {
		return !st.Paused && len(st.Players) == 2 && !st.Players[1].Connected
	})
	time.Sleep(100 * time.Millisecond) // let any last frame land
	frozen := alice.snapshotState()

	bob = dial(t, ts.url)
	bob.send(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: mb.MatchID, Token: mb.Token})
	snap := decode[protocol.Snapshot](t, bob.waitFor(protocol.TypeSnapshot, nil))
	if !reflect.DeepEqual(boards(frozen), boards(snap.State)) {
		t.Fatalf("boards rebuilt from deltas differ from the snapshot:\n deltas   %+v\n snapshot %+v", boards(frozen), boards(snap.State))
	}
	if frozen.Players[0].Name != "alice" || frozen.Players[1].Name != "bob" {
		t.Fatalf("names = %q, %q", frozen.Players[0].Name, frozen.Players[1].Name)
	}

	alice.mu.Lock()
	frames, size := alice.frames, alice.bytes
	alice.mu.Unlock()
	if frames == 0 || size/frames > 60 {
		t.Fatalf("%d binary state frames, %d B on average: deltas expected to be tiny", frames, size/max(frames, 1))
	}
	t.Logf("%d binary state frames, %.1f B on average", frames, float64(size)/float64(frames))

	// Server-side traffic metrics see both formats.
	resp := httpGetBody(t, ts.httpURL("/metrics"))
	for _, want := range []string{`sabostack_ws_bytes_total{dir="out",format="binary"}`, `sabostack_ws_bytes_total{dir="in",format="binary"}`, `sabostack_ws_frames_total{dir="out",format="json"}`} {
		if !strings.Contains(resp, want) {
			t.Fatalf("metrics lack %s", want)
		}
	}
}

// A JSON client (no subprotocol) never sees a binary frame, even in a match with a
// binary opponent.
func TestJSONClientGetsNoBinaryFrames(t *testing.T) {
	ts := startServer(t, store.NewMemory())
	bin := dialBinary(t, ts.url)
	_ = bin.ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "bin"})
	bin.waitText(protocol.TypeQueued, nil)
	ws, _, err := websocket.DefaultDialer.Dial(ts.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if ws.Subprotocol() != "" {
		t.Fatalf("subprotocol %q negotiated without asking", ws.Subprotocol())
	}
	_ = ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "json"})
	states := 0
	for states < 10 {
		_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
		mt, b, err := ws.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if mt != websocket.TextMessage {
			t.Fatalf("binary frame % x sent to a JSON client", b)
		}
		if strings.Contains(string(b), `"type":"state"`) {
			states++
		}
	}
}

// Malformed binary frames get an error reply and do not close the connection.
func TestMalformedBinaryFrame(t *testing.T) {
	ts := startServer(t, store.NewMemory())
	c := dialBinary(t, ts.url)
	_ = c.ws.WriteMessage(websocket.BinaryMessage, []byte{0x7f, 1, 2})
	if e := decode[protocol.Error](t, c.waitText(protocol.TypeError, nil)); e.Code != "bad_request" {
		t.Fatalf("error = %+v", e)
	}
	_ = c.ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeJoin, Name: "x"}) // still usable
	c.waitText(protocol.TypeQueued, nil)
}

func httpGetBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}
