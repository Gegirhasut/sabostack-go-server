package game

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSameSeedSamePieces(t *testing.T) {
	a, b := NewBoard(42, 1), NewBoard(42, 2)
	for i := 0; i < 50; i++ {
		if a.Piece.Kind != b.Piece.Kind {
			t.Fatalf("piece %d differs: %c vs %c", i, a.Piece.Kind, b.Piece.Kind)
		}
		a.Apply(HardDrop)
		b.Apply(HardDrop)
		if a.Dead || b.Dead {
			return
		}
	}
}

func TestBagContainsEachKindOnce(t *testing.T) {
	b := NewBoard(7, 7)
	seen := map[Kind]int{b.Piece.Kind: 1}
	for i := 0; i < 6; i++ {
		seen[b.nextKind()]++
	}
	if len(seen) != 7 {
		t.Fatalf("first bag = %v, want all 7 kinds once", seen)
	}
}

func TestWallsBlockMovement(t *testing.T) {
	b := NewBoard(1, 1)
	for i := 0; i < Width; i++ {
		b.Apply(Left)
	}
	if b.Apply(Left).Moved {
		t.Fatal("piece moved through the left wall")
	}
	for _, c := range b.Piece.Cells() {
		if c.X < 0 {
			t.Fatalf("cell outside board: %+v", c)
		}
	}
}

func TestLineClearAndGarbage(t *testing.T) {
	b := NewBoard(1, 1)
	// Fill the two bottom rows except column 0, then drop a vertical I into column 0.
	for y := Height - 2; y < Height; y++ {
		for x := 1; x < Width; x++ {
			b.Grid[y][x] = 'X'
		}
	}
	b.Piece = Piece{Kind: I, Rot: 1, X: -2, Y: 0} // vertical I occupies box column 2 -> x=0
	if !b.fits(b.Piece) {
		t.Fatal("test setup: vertical I does not fit")
	}
	r := b.Apply(HardDrop)
	if !r.Locked || r.Cleared != 2 {
		t.Fatalf("result = %+v, want lock clearing 2 lines", r)
	}
	if b.Score != 300 || b.Lines != 2 {
		t.Fatalf("score=%d lines=%d", b.Score, b.Lines)
	}
	if GarbageFor(r.Cleared) != 1 {
		t.Fatal("2 lines should send 1 garbage row")
	}

	b.Pending = 3
	r = b.Apply(HardDrop) // no clear: pending garbage is inserted
	if r.Garbage != 3 || b.Pending != 0 {
		t.Fatalf("garbage=%d pending=%d", r.Garbage, b.Pending)
	}
	holes := 0
	for x := 0; x < Width; x++ {
		if b.Grid[Height-1][x] == empty {
			holes++
		}
	}
	if holes != 1 {
		t.Fatalf("garbage row has %d holes, want 1", holes)
	}
}

func TestTopOut(t *testing.T) {
	b := NewBoard(3, 3)
	for i := 0; i < 200 && !b.Dead; i++ {
		b.Apply(HardDrop)
	}
	if !b.Dead {
		t.Fatal("stacking pieces in the middle never topped out")
	}
	if r := b.Apply(Left); r != (Result{}) {
		t.Fatal("dead board accepted an action")
	}
}

func TestBoardJSONRoundTrip(t *testing.T) {
	b := NewBoard(9, 9)
	b.Apply(Left)
	b.Apply(HardDrop)
	b.Pending = 2
	raw, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var got Board
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*b, got) {
		t.Fatalf("round trip mismatch\n%+v\n%+v", *b, got)
	}
	// The restored board must continue the identical sequence.
	for i := 0; i < 10; i++ {
		b.Apply(HardDrop)
		got.Apply(HardDrop)
	}
	if !reflect.DeepEqual(*b, got) {
		t.Fatal("restored board diverged")
	}
}
