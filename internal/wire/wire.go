// Package wire is the binary encoding of the hot-path messages: server state frames and
// client inputs and pings.
//
// A client opts in with the WebSocket subprotocol "sabostack.bin.v1". Without it the
// connection speaks the JSON protocol only. With it, state, pong (server to client) and
// input, ping (client to server) travel as binary frames; the rare messages (join,
// resume, queued, matched, snapshot, event, error) stay JSON text frames, because they are
// a negligible share of the traffic and readable frames are worth more there. The frame
// opcode (text or binary) tells the two apart.
//
// # State frames are deltas
//
// A state frame encodes the new state relative to the last state the client received:
// the snapshot sent on (re)attach, then every state frame after it. TCP keeps the stream
// ordered and lossless, so the client can always apply the next frame to what it has.
// A frame with the full flag set does not depend on any previous state.
//
//	byte    type = 0x01
//	byte    flags: bits 0-2 phase, bit 3 paused (always 0 now, kept for old clients), bit 4 full
//	svarint tick - previous tick
//	uvarint round
//	uvarint remainingMs
//	svarint ack - previous ack
//	svarint eventSeq - previous eventSeq
//	2 x player:
//	  byte    flags: bit 0 connected, bit 1 dead, bit 2 piece, bit 3 stats, bit 4 rows
//	  piece:  byte kind<<2 | rot, svarint x, svarint y          (if bit 2)
//	  stats:  uvarint score, lines, pending, wins               (if bit 3)
//	  rows:   uvarint mask of changed rows (bit i = row i),     (if bit 4)
//	          then 5 bytes per changed row: 10 cells x 4 bits, low nibble first
//
// Player names are not repeated: they never change during a match and come with the
// snapshot. A frame where only the falling piece moved is about 15 bytes; the same state
// as JSON is about 900.
//
// # Client frames
//
//	input: byte 0x10, uvarint seq, byte action (0 left, 1 right, 2 rotate, 3 soft_drop, 4 hard_drop)
//	ping:  byte 0x11, svarint t
//	pong:  byte 0x02, svarint t   (server to client)
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

// Subprotocol is the WebSocket subprotocol a client requests to use binary frames.
const Subprotocol = "sabostack.bin.v1"

// Frame types (the first byte of every binary frame).
const (
	TypeState byte = 0x01
	TypePong  byte = 0x02
	TypeInput byte = 0x10
	TypePing  byte = 0x11
)

const (
	flagPaused = 1 << 3
	flagFull   = 1 << 4

	pConnected = 1 << 0
	pDead      = 1 << 1
	pPiece     = 1 << 2
	pStats     = 1 << 3
	pRows      = 1 << 4
)

var phases = [...]string{"waiting", "countdown", "playing", "round_end", "match_end"}

// unknownPhase marks a phase name this version does not know (decoded as "").
const unknownPhase = 7

var actions = [...]game.Action{game.Left, game.Right, game.Rotate, game.SoftDrop, game.HardDrop}

var kinds = [...]game.Kind{game.I, game.O, game.T, game.S, game.Z, game.J, game.L}

// cells maps a 4-bit cell code to its board character ('.' empty, 'G' garbage).
const cells = ".IOTSZJLG"

var cellCode [256]byte

func init() {
	for i := range cellCode {
		cellCode[i] = 0xF
	}
	for i := 0; i < len(cells); i++ {
		cellCode[cells[i]] = byte(i)
	}
}

// ErrMalformed is returned for a frame that does not decode.
var ErrMalformed = errors.New("wire: malformed frame")

func phaseCode(p string) byte {
	for i, n := range phases {
		if n == p {
			return byte(i)
		}
	}
	return unknownPhase
}

func kindCode(k game.Kind) byte {
	for i, x := range kinds {
		if x == k {
			return byte(i)
		}
	}
	return 7
}

// AppendState appends the frame for cur, encoded relative to prev (nil: a full frame).
// It fails, writing nothing, if cur is not a two-player state with 10x20 boards.
func AppendState(dst []byte, prev, cur *protocol.State) ([]byte, error) {
	if len(cur.Players) != 2 || (prev != nil && len(prev.Players) != 2) {
		return dst, fmt.Errorf("wire: state needs 2 players, has %d", len(cur.Players))
	}
	for _, p := range cur.Players {
		if len(p.Rows) != game.Height {
			return dst, fmt.Errorf("wire: board has %d rows", len(p.Rows))
		}
		for _, r := range p.Rows {
			if len(r) != game.Width {
				return dst, fmt.Errorf("wire: row has %d cells", len(r))
			}
		}
	}
	var zero protocol.State
	base, flags := prev, phaseCode(cur.Phase)
	if base == nil {
		base, flags = &zero, flags|flagFull
	}
	if cur.Paused {
		flags |= flagPaused
	}
	dst = append(dst, TypeState, flags)
	dst = binary.AppendVarint(dst, int64(cur.Tick-base.Tick))
	dst = binary.AppendUvarint(dst, uint64(max(cur.Round, 0)))
	dst = binary.AppendUvarint(dst, uint64(max(cur.RemainingMs, 0)))
	dst = binary.AppendVarint(dst, int64(cur.Ack-base.Ack))
	dst = binary.AppendVarint(dst, int64(cur.EventSeq-base.EventSeq))
	for i := range cur.Players {
		var bp *protocol.PlayerView
		if prev != nil {
			bp = &prev.Players[i]
		}
		dst = appendPlayer(dst, bp, &cur.Players[i])
	}
	return dst, nil
}

func appendPlayer(dst []byte, prev, cur *protocol.PlayerView) []byte {
	var flags byte
	if cur.Connected {
		flags |= pConnected
	}
	if cur.Dead {
		flags |= pDead
	}
	if prev == nil || prev.Piece != cur.Piece {
		flags |= pPiece
	}
	if prev == nil || prev.Score != cur.Score || prev.Lines != cur.Lines || prev.Pending != cur.Pending || prev.Wins != cur.Wins {
		flags |= pStats
	}
	var mask uint64
	for y, row := range cur.Rows {
		if prev == nil || prev.Rows[y] != row {
			mask |= 1 << y
		}
	}
	if mask != 0 {
		flags |= pRows
	}
	dst = append(dst, flags)
	if flags&pPiece != 0 {
		dst = append(dst, kindCode(cur.Piece.Kind)<<2|byte(cur.Piece.Rot&3))
		dst = binary.AppendVarint(dst, int64(cur.Piece.X))
		dst = binary.AppendVarint(dst, int64(cur.Piece.Y))
	}
	if flags&pStats != 0 {
		for _, v := range [...]int{cur.Score, cur.Lines, cur.Pending, cur.Wins} {
			dst = binary.AppendUvarint(dst, uint64(max(v, 0)))
		}
	}
	if mask != 0 {
		dst = binary.AppendUvarint(dst, mask)
		for y, row := range cur.Rows {
			if mask&(1<<y) == 0 {
				continue
			}
			for x := 0; x < game.Width; x += 2 {
				dst = append(dst, cellCode[row[x]]|cellCode[row[x+1]]<<4)
			}
		}
	}
	return dst
}

// reader is a bounds-checked cursor; the first failure sticks.
type reader struct {
	b   []byte
	err error
}

func (r *reader) byte() byte {
	if r.err != nil || len(r.b) == 0 {
		r.err = ErrMalformed
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = ErrMalformed
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = ErrMalformed
		return 0
	}
	r.b = r.b[n:]
	return v
}

// int reads a non-negative uvarint that must fit a game counter.
func (r *reader) int() int {
	v := r.uvarint()
	if v > 1<<31 {
		r.err = ErrMalformed
		return 0
	}
	return int(v)
}

func (r *reader) done() error {
	if r.err == nil && len(r.b) != 0 {
		r.err = ErrMalformed
	}
	return r.err
}

// DecodeState applies a state frame to prev and returns the new state. prev may be nil
// only for a full frame. Names (not sent on the wire) are carried over from prev. prev is
// never modified, and the result shares no mutable data with it.
func DecodeState(prev *protocol.State, b []byte) (protocol.State, error) {
	r := reader{b: b}
	if r.byte() != TypeState {
		return protocol.State{}, ErrMalformed
	}
	flags := r.byte()
	if r.err != nil {
		return protocol.State{}, r.err // truncated: found by FuzzDecodeState
	}
	full := flags&flagFull != 0
	if !full && (prev == nil || len(prev.Players) != 2) {
		return protocol.State{}, errors.New("wire: delta frame without a base state")
	}
	var zero protocol.State
	base := prev
	if full {
		base = &zero
	}
	st := protocol.State{Type: protocol.TypeState, Paused: flags&flagPaused != 0, Players: make([]protocol.PlayerView, 2)}
	if pc := flags & 7; int(pc) < len(phases) {
		st.Phase = phases[pc]
	}
	st.Tick = base.Tick + uint64(r.varint())
	st.Round = r.int()
	st.RemainingMs = int64(r.int())
	st.Ack = base.Ack + uint64(r.varint())
	st.EventSeq = base.EventSeq + uint64(r.varint())
	for i := range st.Players {
		var bp *protocol.PlayerView
		if !full {
			bp = &prev.Players[i]
		}
		decodePlayer(&r, bp, &st.Players[i])
		if prev != nil && len(prev.Players) == 2 {
			st.Players[i].Name = prev.Players[i].Name
		}
	}
	if err := r.done(); err != nil {
		return protocol.State{}, err
	}
	return st, nil
}

func decodePlayer(r *reader, prev, p *protocol.PlayerView) {
	flags := r.byte()
	p.Connected, p.Dead = flags&pConnected != 0, flags&pDead != 0
	if prev != nil {
		p.Piece, p.Score, p.Lines, p.Pending, p.Wins = prev.Piece, prev.Score, prev.Lines, prev.Pending, prev.Wins
	}
	if flags&pPiece != 0 {
		kr := r.byte()
		if k := kr >> 2; int(k) < len(kinds) {
			p.Piece.Kind = kinds[k]
		} else {
			p.Piece.Kind = '?'
		}
		p.Piece.Rot = int(kr & 3)
		p.Piece.X, p.Piece.Y = int(r.varint()), int(r.varint())
	}
	if flags&pStats != 0 {
		p.Score, p.Lines, p.Pending, p.Wins = r.int(), r.int(), r.int(), r.int()
	}
	p.Rows = make([]string, game.Height)
	if prev != nil {
		copy(p.Rows, prev.Rows)
	}
	var mask uint64
	if flags&pRows != 0 {
		if mask = r.uvarint(); mask>>game.Height != 0 {
			r.err = ErrMalformed
		}
	}
	var row [game.Width]byte
	for y := range p.Rows {
		if mask&(1<<y) == 0 {
			if prev == nil && r.err == nil {
				r.err = ErrMalformed // a full frame must carry every row
			}
			continue
		}
		for x := 0; x < game.Width; x += 2 {
			c := r.byte()
			row[x], row[x+1] = cell(c&0xF), cell(c>>4)
		}
		p.Rows[y] = string(row[:])
	}
}

func cell(c byte) byte {
	if int(c) < len(cells) {
		return cells[c]
	}
	return '?'
}

// AppendInput appends a client input frame.
func AppendInput(dst []byte, seq uint64, a game.Action) []byte {
	code := byte(len(actions)) // invalid unless found
	for i, x := range actions {
		if x == a {
			code = byte(i)
		}
	}
	dst = append(dst, TypeInput)
	dst = binary.AppendUvarint(dst, seq)
	return append(dst, code)
}

// AppendPing appends a client ping frame.
func AppendPing(dst []byte, t int64) []byte {
	return binary.AppendVarint(append(dst, TypePing), t)
}

// AppendPong appends a server pong frame.
func AppendPong(dst []byte, t int64) []byte {
	return binary.AppendVarint(append(dst, TypePong), t)
}

// DecodePong decodes a server pong frame.
func DecodePong(b []byte) (int64, error) {
	r := reader{b: b}
	if r.byte() != TypePong {
		return 0, ErrMalformed
	}
	t := r.varint()
	return t, r.done()
}

// DecodeClient decodes a client binary frame (input or ping) into the same message the
// JSON protocol produces.
func DecodeClient(b []byte) (protocol.ClientMsg, error) {
	r := reader{b: b}
	var m protocol.ClientMsg
	switch r.byte() {
	case TypeInput:
		m.Type, m.Seq = protocol.TypeInput, r.uvarint()
		code := r.byte()
		if r.err == nil && int(code) >= len(actions) {
			return protocol.ClientMsg{}, fmt.Errorf("wire: unknown action %d", code)
		}
		if r.err == nil {
			m.Action = actions[code]
		}
	case TypePing:
		m.Type, m.T = protocol.TypePing, r.varint()
	default:
		if r.err == nil {
			return protocol.ClientMsg{}, fmt.Errorf("wire: unknown client frame type 0x%02x", b[0])
		}
	}
	if err := r.done(); err != nil {
		return protocol.ClientMsg{}, err
	}
	return m, nil
}
