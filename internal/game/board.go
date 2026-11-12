// Package game is the deterministic, single-player falling-block simulation used by each
// seat of a match. It has no notion of time or networking: the match loop calls Apply for
// player actions and Gravity on the server-authoritative gravity timer.
package game

import (
	"encoding/json"
	"fmt"
)

const (
	Width  = 10
	Height = 20

	empty   = '.'
	garbage = 'G'
)

// Action is a player input.
type Action string

const (
	Left     Action = "left"
	Right    Action = "right"
	Rotate   Action = "rotate"
	SoftDrop Action = "soft_drop"
	HardDrop Action = "hard_drop"
)

// Valid reports whether a is a known action.
func (a Action) Valid() bool {
	switch a {
	case Left, Right, Rotate, SoftDrop, HardDrop:
		return true
	}
	return false
}

// Grid is the locked cells, row 0 at the top. It marshals to JSON as a list of strings.
type Grid [Height][Width]byte

func (g Grid) MarshalJSON() ([]byte, error) { return json.Marshal(g.Rows()) }

func (g *Grid) UnmarshalJSON(b []byte) error {
	var rows []string
	if err := json.Unmarshal(b, &rows); err != nil {
		return err
	}
	if len(rows) != Height {
		return fmt.Errorf("game: grid has %d rows, want %d", len(rows), Height)
	}
	for y, r := range rows {
		if len(r) != Width {
			return fmt.Errorf("game: row %d has %d cells, want %d", y, len(r), Width)
		}
		copy(g[y][:], r)
	}
	return nil
}

// Rows renders the grid as strings ('.' = empty).
func (g Grid) Rows() []string {
	out := make([]string, Height)
	for y := range g {
		out[y] = string(g[y][:])
	}
	return out
}

// Board is one player's playfield. All fields are exported so a Board round-trips through
// JSON for crash recovery.
type Board struct {
	Grid    Grid   `json:"grid"`
	Piece   Piece  `json:"piece"`
	Queue   []Kind `json:"queue"` // remaining pieces of the current 7-bag
	Bag     RNG    `json:"bag"`   // piece order (same seed for both seats = fair)
	Holes   RNG    `json:"holes"` // garbage hole columns
	Score   int    `json:"score"`
	Lines   int    `json:"lines"`
	Pending int    `json:"pending"` // incoming garbage rows, applied on the next lock without a clear
	Dead    bool   `json:"dead"`    // topped out
}

// Result describes what an action or gravity step did.
type Result struct {
	Moved   bool // the piece moved or rotated
	Locked  bool // the piece locked into the grid
	Cleared int  // lines cleared by that lock
	Garbage int  // garbage rows inserted on this board by that lock
	Died    bool // the next piece could not spawn
}

// NewBoard creates an empty board. pieceSeed decides the piece sequence; holeSeed the
// garbage holes.
func NewBoard(pieceSeed, holeSeed uint64) *Board {
	b := &Board{Bag: RNG{State: pieceSeed}, Holes: RNG{State: holeSeed}}
	for y := range b.Grid {
		for x := range b.Grid[y] {
			b.Grid[y][x] = empty
		}
	}
	b.spawn()
	return b
}

// Apply performs a player action. Actions on a dead board are no-ops.
func (b *Board) Apply(a Action) Result {
	if b.Dead {
		return Result{}
	}
	switch a {
	case Left:
		return Result{Moved: b.try(b.Piece.X-1, b.Piece.Y, b.Piece.Rot)}
	case Right:
		return Result{Moved: b.try(b.Piece.X+1, b.Piece.Y, b.Piece.Rot)}
	case Rotate:
		// Minimal wall kick: try in place, then one cell left/right, then two left (I piece).
		for _, dx := range []int{0, -1, 1, -2} {
			if b.try(b.Piece.X+dx, b.Piece.Y, b.Piece.Rot+1) {
				return Result{Moved: true}
			}
		}
		return Result{}
	case SoftDrop:
		return b.Gravity()
	case HardDrop:
		for b.try(b.Piece.X, b.Piece.Y+1, b.Piece.Rot) {
		}
		return b.lock()
	}
	return Result{}
}

// Gravity moves the piece down one row, locking it if it cannot move.
func (b *Board) Gravity() Result {
	if b.Dead {
		return Result{}
	}
	if b.try(b.Piece.X, b.Piece.Y+1, b.Piece.Rot) {
		return Result{Moved: true}
	}
	return b.lock()
}

// GarbageFor is the attack sent for clearing n lines at once.
func GarbageFor(n int) int {
	switch n {
	case 2:
		return 1
	case 3:
		return 2
	case 4:
		return 4
	}
	return 0
}

var lineScore = [...]int{0, 100, 300, 500, 800}

// StackHeight is the number of rows from the floor to the highest locked cell.
func (b *Board) StackHeight() int {
	for y := 0; y < Height; y++ {
		for x := 0; x < Width; x++ {
			if b.Grid[y][x] != empty {
				return Height - y
			}
		}
	}
	return 0
}

func (b *Board) fits(p Piece) bool {
	for _, c := range p.Cells() {
		if c.X < 0 || c.X >= Width || c.Y < 0 || c.Y >= Height || b.Grid[c.Y][c.X] != empty {
			return false
		}
	}
	return true
}

func (b *Board) try(x, y, rot int) bool {
	p := Piece{Kind: b.Piece.Kind, Rot: rot % 4, X: x, Y: y}
	if !b.fits(p) {
		return false
	}
	b.Piece = p
	return true
}

func (b *Board) lock() Result {
	for _, c := range b.Piece.Cells() {
		b.Grid[c.Y][c.X] = byte(b.Piece.Kind)
	}
	r := Result{Locked: true, Cleared: b.clearLines()}
	b.Lines += r.Cleared
	b.Score += lineScore[r.Cleared]
	if r.Cleared == 0 && b.Pending > 0 {
		r.Garbage = b.Pending
		b.insertGarbage(b.Pending)
		b.Pending = 0
	}
	if !b.spawn() {
		b.Dead = true
		r.Died = true
	}
	return r
}

func (b *Board) clearLines() int {
	n := 0
	for y := Height - 1; y >= 0; {
		full := true
		for x := 0; x < Width; x++ {
			if b.Grid[y][x] == empty {
				full = false
				break
			}
		}
		if !full {
			y--
			continue
		}
		copy(b.Grid[1:y+1], b.Grid[0:y])
		for x := range b.Grid[0] {
			b.Grid[0][x] = empty
		}
		n++
	}
	return n
}

// insertGarbage pushes n rows with a single hole in from the bottom. Cells pushed off the
// top are lost; the spawn check that follows decides whether the player survives.
func (b *Board) insertGarbage(n int) {
	if n > Height {
		n = Height
	}
	copy(b.Grid[0:Height-n], b.Grid[n:])
	hole := b.Holes.Intn(Width)
	for y := Height - n; y < Height; y++ {
		for x := range b.Grid[y] {
			b.Grid[y][x] = garbage
		}
		b.Grid[y][hole] = empty
	}
}

func (b *Board) nextKind() Kind {
	if len(b.Queue) == 0 {
		q := kinds
		for i := len(q) - 1; i > 0; i-- { // Fisher-Yates
			j := b.Bag.Intn(i + 1)
			q[i], q[j] = q[j], q[i]
		}
		b.Queue = q[:]
	}
	k := b.Queue[0]
	b.Queue = b.Queue[1:]
	return k
}

func (b *Board) spawn() bool {
	k := b.nextKind()
	b.Piece = Piece{Kind: k, X: (Width - shapes[k].n) / 2}
	return b.fits(b.Piece)
}
