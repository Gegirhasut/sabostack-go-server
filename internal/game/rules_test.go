package game

import "testing"

// The SRS kick tables are easy to mistype. Two properties catch that: every turn first
// tries the piece in place, and turning back undoes a turn, so each offset of a→b is
// the negation of the same offset of b→a.
func TestKicksFollowSRS(t *testing.T) {
	for _, k := range kinds {
		for from := 0; from < 4; from++ {
			to := (from + 1) % 4
			cw, back := kicksFor(k, from, to), kicksFor(k, to, from)
			if len(cw) == 0 || cw[0] != (cell{}) || len(cw) != len(back) {
				t.Fatalf("%c %d->%d: kicks %v, back %v", k, from, to, cw, back)
			}
			for i := range cw {
				if cw[i].X != -back[i].X || cw[i].Y != -back[i].Y {
					t.Fatalf("%c %d->%d kick %d is %v, but %d->%d has %v", k, from, to, i, cw[i], to, from, back[i])
				}
			}
		}
	}
	// And on an empty board a turn and the opposite turn bring every piece back.
	for _, k := range kinds {
		for rot := 0; rot < 4; rot++ {
			b := NewBoard(1, 1)
			b.Piece = Piece{Kind: k, Rot: rot, X: 3, Y: 8}
			before := b.Piece
			if !b.Apply(Rotate).Moved || !b.Apply(RotateCCW).Moved || b.Piece != before {
				t.Fatalf("%c from state %d: clockwise and back gave %+v, want %+v", k, rot, b.Piece, before)
			}
		}
	}
}

// Bug: the old kick only tried shifting sideways (0, -1, +1, -2), and only clockwise.
// SRS tries its offsets in order, including the upward ones that let a T turn out of a
// slot.
func TestRotationKicksInOrder(t *testing.T) {
	b := NewBoard(1, 1)
	b.Grid[12][4], b.Grid[12][5] = 'X', 'X' // blocks the turn in place and one left
	b.Piece = Piece{Kind: T, Rot: 0, X: 4, Y: 10}
	if !b.Apply(Rotate).Moved {
		t.Fatal("T could not turn")
	}
	if want := (Piece{Kind: T, Rot: 1, X: 3, Y: 9}); b.Piece != want { // third SRS offset: left and up
		t.Fatalf("T turned to %+v, want %+v", b.Piece, want)
	}

	b = NewBoard(1, 1)
	b.Piece = Piece{Kind: I, Rot: 1, X: 7, Y: 5} // vertical, against the right wall
	if !b.Apply(Rotate).Moved {
		t.Fatal("I could not turn off the wall")
	}
	if want := (Piece{Kind: I, Rot: 2, X: 6, Y: 5}); b.Piece != want {
		t.Fatalf("I turned to %+v, want %+v", b.Piece, want)
	}

	b = NewBoard(1, 1)
	b.Piece = Piece{Kind: T, Rot: 0, X: 4, Y: 8}
	if !b.Apply(RotateCCW).Moved || b.Piece.Rot != 3 {
		t.Fatalf("counter-clockwise turn gave %+v, want state 3", b.Piece)
	}
}

// Bug: gravity locked a piece on the step that found it resting, so a player had no time
// to slide or turn it along the floor. Now the piece stays and locks when told to.
func TestGravityLeavesRestingPiece(t *testing.T) {
	b := NewBoard(1, 1)
	for b.Gravity().Moved {
	}
	if r := b.Gravity(); r.Locked || !b.Grounded() {
		t.Fatalf("gravity on a resting piece: %+v, grounded=%v", r, b.Grounded())
	}
	if r := b.Apply(SoftDrop); r.Locked || r.Moved {
		t.Fatalf("soft drop on a resting piece: %+v, want nothing", r)
	}
	if r := b.Lock(); !r.Locked || b.Grounded() {
		t.Fatalf("lock: %+v, grounded=%v (the next piece should be falling)", r, b.Grounded())
	}
}

// A move or turn of a resting piece may restart its lock delay only MaxLockResets times;
// reaching a new lowest row gives the allowance back.
func TestLockResetsAreBounded(t *testing.T) {
	b := NewBoard(1, 1)
	b.Piece = Piece{Kind: O, X: 4, Y: 0}
	b.LowestY, b.LockResets = 0, 0
	b.Grid[2][4], b.Grid[2][5] = 'X', 'X' // a ledge under the O
	extends := 0
	for i := 0; i < 3*MaxLockResets; i++ {
		a := Left
		if b.Piece.X == 3 {
			a = Right
		}
		r := b.Apply(a)
		if !r.Moved {
			t.Fatalf("move %d failed", i)
		}
		if r.Extend {
			extends++
		}
	}
	if extends != MaxLockResets {
		t.Fatalf("%d moves of a resting piece restarted the lock delay %d times, want %d", 3*MaxLockResets, extends, MaxLockResets)
	}
	// Off the ledge (X 3 -> 2), down to the floor: a new lowest row, a new allowance.
	if b.Piece.X != 3 {
		b.Apply(Left)
	}
	b.Apply(Left)
	for b.Gravity().Moved {
	}
	if r := b.Apply(Left); !r.Extend {
		t.Fatalf("after reaching a new lowest row: %+v, want the lock delay restarted", r)
	}
}

// Bug: garbage that pushed locked cells off the top just deleted them: an attack shaved
// the top of the stack instead of topping the player out.
func TestGarbageAboveTheTopIsATopOut(t *testing.T) {
	for _, tc := range []struct {
		row  int
		dies bool
	}{{0, true}, {1, false}} {
		b := NewBoard(1, 1)
		b.Grid[tc.row][0] = 'X' // away from the spawn columns
		b.Pending = 1
		r := b.Apply(HardDrop) // no clear: the garbage row rises
		if r.Garbage != 1 || r.Died != tc.dies || b.Dead != tc.dies {
			t.Fatalf("cell in row %d, one garbage row: %+v dead=%v, want died=%v", tc.row, r, b.Dead, tc.dies)
		}
	}
}
