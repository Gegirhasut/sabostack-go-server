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

// shapes holds rotation state 0 of each piece inside an n×n box (y grows downwards).
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
