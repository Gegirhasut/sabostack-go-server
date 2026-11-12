package wire

import (
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

func emptyRows() []string {
	rows := make([]string, game.Height)
	for i := range rows {
		rows[i] = strings.Repeat(".", game.Width)
	}
	return rows
}

func sampleState() protocol.State {
	st := protocol.State{
		Type: protocol.TypeState, Tick: 1000, Phase: "playing", Round: 2, RemainingMs: 61234,
		Ack: 77, EventSeq: 31, Players: make([]protocol.PlayerView, 2),
	}
	for i := range st.Players {
		rows := emptyRows()
		rows[19] = "GGGG.GGGGG"
		rows[18] = "IOTSZJL..G"
		st.Players[i] = protocol.PlayerView{
			Name: []string{"alice", "bob"}[i], Connected: true, Wins: 1, Rows: rows,
			Piece: game.Piece{Kind: game.T, Rot: 1, X: 4, Y: 3}, Score: 1300, Lines: 9, Pending: 2,
		}
	}
	return st
}

// mutate applies a random, game-like change: the piece moves, rows change, counters grow.
func mutate(r *rand.Rand, prev protocol.State) protocol.State {
	st := prev
	st.Players = make([]protocol.PlayerView, 2)
	copy(st.Players, prev.Players)
	st.Tick += uint64(1 + r.IntN(20))
	st.RemainingMs = int64(r.IntN(90000))
	if r.IntN(4) == 0 {
		st.Ack += uint64(r.IntN(5))
	}
	if r.IntN(10) == 0 {
		st.EventSeq += uint64(1 + r.IntN(3))
	}
	if r.IntN(30) == 0 {
		st.Phase = []string{"waiting", "countdown", "playing", "round_end", "match_end"}[r.IntN(5)]
		st.Round = r.IntN(4)
	}
	st.Paused = r.IntN(20) == 0
	for i := range st.Players {
		p := &st.Players[i]
		p.Rows = append([]string(nil), p.Rows...)
		if r.IntN(2) == 0 {
			p.Piece = game.Piece{Kind: game.Kind("IOTSZJL"[r.IntN(7)]), Rot: r.IntN(4), X: r.IntN(13) - 2, Y: r.IntN(22) - 1}
		}
		if r.IntN(5) == 0 {
			for n := r.IntN(4); n >= 0; n-- {
				b := []byte(p.Rows[r.IntN(game.Height)])
				b[r.IntN(game.Width)] = cells[r.IntN(len(cells))]
				p.Rows[r.IntN(game.Height)] = string(b)
			}
			p.Score += r.IntN(800)
			p.Lines += r.IntN(4)
			p.Pending = r.IntN(6)
		}
		p.Connected = r.IntN(15) != 0
		p.Dead = r.IntN(50) == 0
	}
	return st
}

func withoutNames(st protocol.State) protocol.State {
	st.Players = append([]protocol.PlayerView(nil), st.Players...)
	for i := range st.Players {
		st.Players[i].Name = ""
	}
	return st
}

// A long random sequence of deltas, each applied to the previously decoded state,
// reproduces every state exactly.
func TestStateDeltaRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	cur := sampleState()
	frame, err := AppendState(nil, nil, &cur)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeState(nil, frame)
	if err != nil {
		t.Fatal(err)
	}
	if want := withoutNames(cur); !reflect.DeepEqual(got, want) {
		t.Fatalf("full frame:\n got %+v\nwant %+v", got, want)
	}
	got.Players[0].Name, got.Players[1].Name = "alice", "bob" // what the snapshot gave the client
	for i := 0; i < 20000; i++ {
		next := mutate(r, cur)
		if frame, err = AppendState(frame[:0], &cur, &next); err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeState(&got, frame)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if !reflect.DeepEqual(decoded, next) {
			t.Fatalf("step %d:\n got %+v\nwant %+v", i, decoded, next)
		}
		cur, got = next, decoded
	}
}

// The decoder never aliases or modifies its base state.
func TestDecodeDoesNotTouchBase(t *testing.T) {
	base := sampleState()
	keep, _ := json.Marshal(base)
	next := sampleState()
	next.Players[1].Rows[0] = "GGGGGGGGG."
	frame, _ := AppendState(nil, &base, &next)
	got, err := DecodeState(&base, frame)
	if err != nil {
		t.Fatal(err)
	}
	got.Players[0].Rows[5] = "changed!!!"
	if after, _ := json.Marshal(base); string(after) != string(keep) {
		t.Fatal("DecodeState changed or shares data with its base")
	}
}

func TestFrameSizes(t *testing.T) {
	prev := sampleState()
	moved := sampleState()
	moved.Tick++
	moved.RemainingMs -= 50
	moved.Players[0].Piece.X++

	locked := moved
	locked.Players = append([]protocol.PlayerView(nil), moved.Players...)
	locked.Players[0].Rows = append([]string(nil), locked.Players[0].Rows...)
	locked.Players[0].Rows[17] = "..TTT....."
	locked.Players[0].Rows[16] = "...T......"
	locked.Players[0].Piece = game.Piece{Kind: game.L, X: 3}

	js, _ := json.Marshal(moved)
	full, _ := AppendState(nil, nil, &moved)
	move, _ := AppendState(nil, &prev, &moved)
	lock, _ := AppendState(nil, &prev, &locked)
	t.Logf("json %d B, full binary %d B, piece moved %d B, piece locked %d B", len(js), len(full), len(move), len(lock))
	if len(move) > 20 || len(lock) > 40 || len(full) > 250 || len(js) < 10*len(move) {
		t.Fatalf("frames larger than expected: json=%d full=%d move=%d lock=%d", len(js), len(full), len(move), len(lock))
	}
}

func TestAppendStateRejectsBadShapes(t *testing.T) {
	st := sampleState()
	st.Players = st.Players[:1]
	if _, err := AppendState(nil, nil, &st); err == nil {
		t.Fatal("one-player state encoded")
	}
	st = sampleState()
	st.Players[0].Rows[3] = "short"
	if out, err := AppendState([]byte("x"), nil, &st); err == nil || string(out) != "x" {
		t.Fatal("bad row encoded, or dst changed on error")
	}
}

func TestDeltaNeedsBase(t *testing.T) {
	a, b := sampleState(), sampleState()
	frame, _ := AppendState(nil, &a, &b)
	if _, err := DecodeState(nil, frame); err == nil {
		t.Fatal("delta decoded without a base")
	}
}

func TestClientFrames(t *testing.T) {
	for _, a := range actions {
		m, err := DecodeClient(AppendInput(nil, 123456789, a))
		if err != nil || m.Type != protocol.TypeInput || m.Seq != 123456789 || m.Action != a {
			t.Fatalf("input %s: %+v %v", a, m, err)
		}
	}
	if b := AppendInput(nil, 1, game.Left); len(b) != 3 {
		t.Fatalf("input frame is %d bytes", len(b))
	}
	m, err := DecodeClient(AppendPing(nil, -42))
	if err != nil || m.Type != protocol.TypePing || m.T != -42 {
		t.Fatalf("ping: %+v %v", m, err)
	}
	if v, err := DecodePong(AppendPong(nil, 1<<40)); err != nil || v != 1<<40 {
		t.Fatalf("pong: %v %v", v, err)
	}
	for _, bad := range [][]byte{nil, {0x99}, {TypeInput}, {TypeInput, 1}, {TypeInput, 1, 9}, {TypeInput, 1, 0, 0}, {TypePing}} {
		if _, err := DecodeClient(bad); err == nil {
			t.Fatalf("% x decoded", bad)
		}
	}
}

func FuzzDecodeClient(f *testing.F) {
	f.Add(AppendInput(nil, 5, game.Rotate))
	f.Add(AppendPing(nil, 99))
	f.Add([]byte{TypeInput, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := DecodeClient(b)
		if err == nil && m.Type == protocol.TypeInput && !m.Action.Valid() {
			t.Fatalf("decoded an invalid action from % x", b)
		}
	})
}

func FuzzDecodeState(f *testing.F) {
	base := sampleState()
	next := mutate(rand.New(rand.NewPCG(3, 4)), base)
	full, _ := AppendState(nil, nil, &base)
	delta, _ := AppendState(nil, &base, &next)
	f.Add(full)
	f.Add(delta)
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, prev := range []*protocol.State{nil, &base} {
			st, err := DecodeState(prev, b)
			if err != nil {
				continue
			}
			if len(st.Players) != 2 {
				t.Fatal("decoded state without two players")
			}
			for _, p := range st.Players {
				if len(p.Rows) != game.Height {
					t.Fatal("decoded board without 20 rows")
				}
				for _, r := range p.Rows {
					if len(r) != game.Width {
						t.Fatalf("decoded row %q", r)
					}
				}
			}
		}
	})
}

func BenchmarkStateJSON(b *testing.B) {
	st := sampleState()
	b.ReportAllocs()
	for range b.N {
		_, _ = json.Marshal(st)
	}
}

func BenchmarkStateDelta(b *testing.B) {
	prev, cur := sampleState(), sampleState()
	cur.Tick++
	cur.Players[0].Piece.X++
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for range b.N {
		buf, _ = AppendState(buf[:0], &prev, &cur)
	}
}

func BenchmarkStateFull(b *testing.B) {
	cur := sampleState()
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for range b.N {
		buf, _ = AppendState(buf[:0], nil, &cur)
	}
}
