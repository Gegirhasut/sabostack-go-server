package game

import "errors"

// Kind identifies one of the seven tetrominoes.
type Kind byte

const (
	I Kind = 'I'
	O Kind = 'O'
	T Kind = 'T'
	S Kind = 'S'
	Z Kind = 'Z'
	J Kind = 'J'
	L Kind = 'L'
)

var kinds = [...]Kind{I, O, T, S, Z, J, L}

// MarshalText encodes a Kind as its letter, so JSON shows "T" instead of 84.
func (k Kind) MarshalText() ([]byte, error) { return []byte{byte(k)}, nil }

func (k *Kind) UnmarshalText(b []byte) error {
	if len(b) != 1 {
		return errors.New("game: bad piece kind")
	}
	if _, ok := shapes[Kind(b[0])]; !ok {
		return errors.New("game: unknown piece kind")
	}
	*k = Kind(b[0])
	return nil
}

type cell struct{ X, Y int }

// shapes holds rotation state 0 (spawn) of each piece inside an n×n box (y grows
// downwards). Rotating the box clockwise gives states 1, 2 and 3: these are the SRS
// states, so the SRS kick tables below apply as they are.
var shapes = map[Kind]struct {
	n     int
	cells []cell
}{
	I: {4, []cell{{0, 1}, {1, 1}, {2, 1}, {3, 1}}},
	O: {2, []cell{{0, 0}, {1, 0}, {0, 1}, {1, 1}}},
	T: {3, []cell{{1, 0}, {0, 1}, {1, 1}, {2, 1}}},
	S: {3, []cell{{1, 0}, {2, 0}, {0, 1}, {1, 1}}},
	Z: {3, []cell{{0, 0}, {1, 0}, {1, 1}, {2, 1}}},
	J: {3, []cell{{0, 0}, {0, 1}, {1, 1}, {2, 1}}},
	L: {3, []cell{{2, 0}, {0, 1}, {1, 1}, {2, 1}}},
}

// Piece is the falling piece: kind, rotation (0..3) and the top-left of its box.
type Piece struct {
	Kind Kind `json:"kind"`
	Rot  int  `json:"rot"`
	X    int  `json:"x"`
	Y    int  `json:"y"`
}

// Cells returns the absolute board cells occupied by the piece.
func (p Piece) Cells() [4]cell {
	s := shapes[p.Kind]
	var out [4]cell
	for i, c := range s.cells {
		x, y := c.X, c.Y
		for r := 0; r < p.Rot%4; r++ {
			x, y = s.n-1-y, x // rotate 90° clockwise inside the box
		}
		out[i] = cell{p.X + x, p.Y + y}
	}
	return out
}

// SRS wall kicks: the offsets tried in order when a piece turns from one state to the
// next (0 = spawn, 1 = clockwise of it, 2, 3). They are written as in the SRS tables, with
// y pointing up; kicksFor flips y for this board, where it points down.
var kicksJLSTZ = map[[2]int][]cell{
	{0, 1}: {{0, 0}, {-1, 0}, {-1, 1}, {0, -2}, {-1, -2}},
	{1, 0}: {{0, 0}, {1, 0}, {1, -1}, {0, 2}, {1, 2}},
	{1, 2}: {{0, 0}, {1, 0}, {1, -1}, {0, 2}, {1, 2}},
	{2, 1}: {{0, 0}, {-1, 0}, {-1, 1}, {0, -2}, {-1, -2}},
	{2, 3}: {{0, 0}, {1, 0}, {1, 1}, {0, -2}, {1, -2}},
	{3, 2}: {{0, 0}, {-1, 0}, {-1, -1}, {0, 2}, {-1, 2}},
	{3, 0}: {{0, 0}, {-1, 0}, {-1, -1}, {0, 2}, {-1, 2}},
	{0, 3}: {{0, 0}, {1, 0}, {1, 1}, {0, -2}, {1, -2}},
}

var kicksI = map[[2]int][]cell{
	{0, 1}: {{0, 0}, {-2, 0}, {1, 0}, {-2, -1}, {1, 2}},
	{1, 0}: {{0, 0}, {2, 0}, {-1, 0}, {2, 1}, {-1, -2}},
	{1, 2}: {{0, 0}, {-1, 0}, {2, 0}, {-1, 2}, {2, -1}},
	{2, 1}: {{0, 0}, {1, 0}, {-2, 0}, {1, -2}, {-2, 1}},
	{2, 3}: {{0, 0}, {2, 0}, {-1, 0}, {2, 1}, {-1, -2}},
	{3, 2}: {{0, 0}, {-2, 0}, {1, 0}, {-2, -1}, {1, 2}},
	{3, 0}: {{0, 0}, {1, 0}, {-2, 0}, {1, -2}, {-2, 1}},
	{0, 3}: {{0, 0}, {-1, 0}, {2, 0}, {-1, 2}, {2, -1}},
}

// kicks[k][from][dir] holds the offsets in board coordinates (y down) for turning a piece
// of kind k from state from, clockwise (dir 0) or counter-clockwise (dir 1). Built once.
var kicks = func() map[Kind][4][2][]cell {
	out := map[Kind][4][2][]cell{}
	for _, k := range kinds {
		var t [4][2][]cell
		for from := 0; from < 4; from++ {
			for dir, to := range [2]int{(from + 1) % 4, (from + 3) % 4} {
				var srs []cell
				switch k {
				case O:
					srs = []cell{{0, 0}}
				case I:
					srs = kicksI[[2]int{from, to}]
				default:
					srs = kicksJLSTZ[[2]int{from, to}]
				}
				for _, c := range srs {
					t[from][dir] = append(t[from][dir], cell{c.X, -c.Y})
				}
			}
		}
		out[k] = t
	}
	return out
}()

// kicksFor returns the offsets to try, in order, for turning a piece of kind k from state
// from to state to (one quarter turn either way).
func kicksFor(k Kind, from, to int) []cell {
	dir := 0
	if to != (from+1)%4 {
		dir = 1
	}
	return kicks[k][from][dir]
}
