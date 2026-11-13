// Package game is the deterministic, single-player falling-block simulation used by each
// seat of a match. It has no notion of time or networking: the match loop calls Apply for
// player actions, Gravity on the server-authoritative gravity timer, and Lock when a
// resting piece's lock delay runs out. The delay itself is the match's clock; the rule of
// what restarts it (Result.Extend, at most MaxLockResets times per piece) lives here.
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
	Left      Action = "left"
	Right     Action = "right"
	Rotate    Action = "rotate" // clockwise
	SoftDrop  Action = "soft_drop"
	HardDrop  Action = "hard_drop"
	RotateCCW Action = "rotate_ccw"
)

// MaxLockResets is how many times a move or rotation of a resting piece may restart its
// lock delay. Only reaching a new lowest row gives the piece its allowance back: the
// lowest row any of its cells has been in, so a turn in place is not one. The total is
// bounded by the board height: a player cannot hold a piece on the floor forever.
const MaxLockResets = 15

// Valid reports whether a is a known action.
func (a Action) Valid() bool {
	switch a {
	case Left, Right, Rotate, RotateCCW, SoftDrop, HardDrop:
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

	LockResets int `json:"lockResets"` // lock delay restarts used by the current piece
	LowestY    int `json:"lowestY"`    // lowest row any cell of the current piece has been in
}

// Result describes what an action or gravity step did.
type Result struct {
	Moved   bool // the piece moved or rotated
	Extend  bool // it did so while resting on something, within MaxLockResets: restart the lock delay
	Lower   bool // it reached a new lowest row: its lock delay starts afresh once it rests
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
	case Left, Right:
		dx := 1
		if a == Left {
			dx = -1
		}
		grounded := b.Grounded()
		if b.try(b.Piece.X+dx, b.Piece.Y, b.Piece.Rot) {
			return b.moved(grounded)
		}
		return Result{}
	case Rotate:
		return b.rotate(1)
	case RotateCCW:
		return b.rotate(3)
	case SoftDrop: // one row down; it never locks: a resting piece locks by its lock delay
		return b.Gravity()
	case HardDrop:
		for b.try(b.Piece.X, b.Piece.Y+1, b.Piece.Rot) {
		}
		return b.Lock()
	}
	return Result{}
}

// Gravity moves the piece down one row if it can. A piece that cannot fall stays where it
// is: it locks when the match's lock delay for it runs out (Lock), not here.
func (b *Board) Gravity() Result {
	if b.Dead {
		return Result{}
	}
	if b.try(b.Piece.X, b.Piece.Y+1, b.Piece.Rot) {
		return Result{Moved: true, Lower: b.fell()}
	}
	return Result{}
}

// Grounded reports whether the piece rests on the stack or the floor: the match runs its
// lock delay while it does.
func (b *Board) Grounded() bool {
	if b.Dead {
		return false
	}
	p := b.Piece
	p.Y++
	return !b.fits(p)
}

// Lock puts the piece into the grid where it is, clears lines, raises pending garbage and
// spawns the next piece.
func (b *Board) Lock() Result {
	if b.Dead {
		return Result{}
	}
	return b.lock()
}

// rotate turns the piece by turn quarters clockwise (1, or 3 for counter-clockwise),
// trying the SRS kick offsets in order: the first position that fits wins.
func (b *Board) rotate(turn int) Result {
	from := b.Piece.Rot % 4
	to := (from + turn) % 4
	grounded := b.Grounded()
	for _, k := range kicksFor(b.Piece.Kind, from, to) {
		if b.try(b.Piece.X+k.X, b.Piece.Y+k.Y, to) {
			return b.moved(grounded)
		}
	}
	return Result{}
}

// moved records a successful move or rotation for the lock delay.
func (b *Board) moved(grounded bool) Result {
	r := Result{Moved: true, Lower: b.fell()}
	if grounded && b.LockResets < MaxLockResets {
		b.LockResets++
		r.Extend = true
	}
	return r
}

// fell notes a new lowest row, by the piece's bottom cell (the top of its box moves when
// it turns in place): the piece gets its lock delay restarts back.
func (b *Board) fell() bool {
	if bottom := b.Piece.bottom(); bottom > b.LowestY {
		b.LowestY, b.LockResets = bottom, 0
		return true
	}
	return false
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
	overflow := false
	if r.Cleared == 0 && b.Pending > 0 {
		r.Garbage = b.Pending
		overflow = b.insertGarbage(b.Pending)
		b.Pending = 0
	}
	if overflow || !b.spawn() {
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

// insertGarbage pushes n rows with a single hole in from the bottom and reports whether
// that pushed any locked cell off the top. That is a top-out: the attack must not just
// shave the top of the stack.
func (b *Board) insertGarbage(n int) bool {
	if n > Height {
		n = Height
	}
	overflow := false
	for y := 0; y < n && !overflow; y++ {
		for x := 0; x < Width; x++ {
			if b.Grid[y][x] != empty {
				overflow = true
				break
			}
		}
	}
	copy(b.Grid[0:Height-n], b.Grid[n:])
	hole := b.Holes.Intn(Width)
	for y := Height - n; y < Height; y++ {
		for x := range b.Grid[y] {
			b.Grid[y][x] = garbage
		}
		b.Grid[y][hole] = empty
	}
	return overflow
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
	b.LowestY, b.LockResets = b.Piece.bottom(), 0
	return b.fits(b.Piece)
}
