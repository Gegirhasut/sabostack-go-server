package match

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
)

func lockedCells(b *game.Board) int {
	n := 0
	for _, row := range b.Grid {
		for _, c := range row {
			if c != '.' {
				n++
			}
		}
	}
	return n
}

// restOnFloor drops seat's piece to the floor without the match noticing: the next
// gravity step is the one that finds it resting.
func restOnFloor(t *testing.T, m *Match, seat int) {
	t.Helper()
	b := m.Seats[seat].Board
	for b.Gravity().Moved {
	}
	if !b.Grounded() || lockedCells(b) != 0 {
		t.Fatal("test setup: piece not resting on an empty floor")
	}
}

// Bug: a piece locked on the gravity step that found it resting. The server is
// authoritative, so a player 100–150 ms away could neither slide a piece along the floor
// nor finish turning it. Now it locks LockDelay after coming to rest.
func TestPieceLocksAfterLockDelay(t *testing.T) {
	cfg := testConfig()
	m, _, _, start := newPlaying(t, cfg)
	restOnFloor(t, m, 0)
	rested := start.Add(cfg.Gravity) // the first gravity step finds it resting
	var lockedAt time.Time
	for now := start; now.Before(rested.Add(2 * cfg.LockDelay)); now = now.Add(cfg.TickInterval) {
		m.Advance(now)
		if lockedCells(m.Seats[0].Board) > 0 {
			lockedAt = now
			break
		}
	}
	if want := rested.Add(cfg.LockDelay); !lockedAt.Equal(want) {
		t.Fatalf("piece locked %s after coming to rest, want the lock delay %s", lockedAt.Sub(rested), cfg.LockDelay)
	}
}

// Sliding a resting piece back and forth restarts its lock delay, but only
// game.MaxLockResets times: it cannot be held on the floor forever.
func TestSlidingCannotHoldAPieceForever(t *testing.T) {
	cfg := testConfig()
	m, _, _, start := newPlaying(t, cfg)
	restOnFloor(t, m, 0)
	rested := start.Add(cfg.Gravity)
	var seq uint64
	var lockedAt time.Time
	for now := start; now.Before(rested.Add(10 * time.Second)); now = now.Add(cfg.TickInterval) {
		if now.After(rested) {
			seq++
			a := game.Left
			if seq%2 == 0 {
				a = game.Right
			}
			m.Input(0, seq, a, now)
		}
		m.Advance(now)
		if lockedCells(m.Seats[0].Board) > 0 {
			lockedAt = now
			break
		}
	}
	held := lockedAt.Sub(rested)
	lastReset := time.Duration(game.MaxLockResets) * cfg.TickInterval // one move per tick
	if lockedAt.IsZero() || held <= cfg.LockDelay || held > lastReset+cfg.LockDelay+cfg.TickInterval {
		t.Fatalf("piece slid along the floor locked %s after coming to rest, want after the lock delay %s and by %s",
			held, cfg.LockDelay, lastReset+cfg.LockDelay+cfg.TickInterval)
	}
}

// The lock timer and the piece's age are game clocks like gravity: a restore moves them
// by the time the match was down, and the resync stand-still moves them again, so the
// restart costs the resting piece none of its lock delay or its PieceLimit.
func TestLockDelaySurvivesRestore(t *testing.T) {
	cfg := testConfig()
	m, tokens, _, start := newPlaying(t, cfg)
	m.Advance(start) // the piece's age starts at the first play step
	restOnFloor(t, m, 0)
	now := start.Add(cfg.Gravity)
	m.Advance(now)
	left, age := m.Seats[0].LockAt.Sub(now), now.Sub(m.Seats[0].PieceAt)
	if left != cfg.LockDelay {
		t.Fatalf("lock timer %s after coming to rest, want %s", left, cfg.LockDelay)
	}
	data, err := m.Snapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	restoredAt := now.Add(time.Minute)
	r, err := Restore(data, cfg, restoredAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Attach(tokens[0], &fakeSink{}, 0, restoredAt); err != nil {
		t.Fatal(err)
	}
	back := restoredAt.Add(2 * time.Second)
	for at := restoredAt; at.Before(back); at = at.Add(cfg.TickInterval) {
		r.Advance(at)
	}
	if lockedCells(r.Seats[0].Board) != 0 {
		t.Fatal("the resting piece locked while the restored match stood still")
	}
	if _, err := r.Attach(tokens[1], &fakeSink{}, 0, back); err != nil {
		t.Fatal(err)
	}
	r.Advance(back)
	if got := r.Seats[0].LockAt.Sub(back); got != left {
		t.Fatalf("after the restore and the resync the lock timer has %s left, want the %s it had", got, left)
	}
	if got := back.Sub(r.Seats[0].PieceAt); got != age {
		t.Fatalf("after the restore and the resync the piece is %s old, want the %s it was", got, age)
	}
}

// Bug: a turn that lifted a resting piece cleared its lock timer, and landing again on
// the same row started a full new delay, so turning back and forth held any piece on the
// floor for ever: 400 turns gave 202 full delays. A player ahead on points could sit out
// the round, since incoming garbage only rises when a piece locks. Now, whatever the
// player does on the floor, every kind locks within (MaxLockResets+1) delays.
func TestSpinningCannotHoldAPieceForever(t *testing.T) {
	cfg := testConfig()
	patterns := [][]game.Action{
		{game.Rotate, game.RotateCCW},
		{game.Rotate, game.RotateCCW, game.Rotate, game.Rotate, game.Left, game.RotateCCW, game.Right},
	}
	for i := 0; i < 7*len(patterns); i++ {
		kind, pattern := []game.Kind{game.I, game.O, game.T, game.S, game.Z, game.J, game.L}[i%7], patterns[i/7]
		m, _, _, start := newPlaying(t, cfg)
		b := m.Seats[0].Board
		b.Piece = game.Piece{Kind: kind, X: 3}
		b.LowestY, b.LockResets = 0, 0
		restOnFloor(t, m, 0)
		rested := start.Add(cfg.Gravity)
		limit := time.Duration(game.MaxLockResets+1) * cfg.LockDelay
		var seq uint64
		var lockedAt time.Time
		for now := start; now.Before(rested.Add(limit + 2*time.Second)); now = now.Add(cfg.TickInterval) {
			if now.After(rested) {
				seq++
				m.Input(0, seq, pattern[int(seq)%len(pattern)], now)
			}
			m.Advance(now)
			if lockedCells(b) > 0 {
				lockedAt = now
				break
			}
		}
		if lockedAt.IsZero() || lockedAt.Sub(rested) > limit {
			held := "never locked"
			if !lockedAt.IsZero() {
				held = "locked after " + lockedAt.Sub(rested).String()
			}
			t.Fatalf("%c turned on the floor every tick (%v): %s, want within %s", kind, pattern, held, limit)
		}
		t.Logf("%c %v: locked %s after coming to rest", kind, pattern, lockedAt.Sub(rested))
	}
}

// Bug: the lock delay is bounded per row, and every new lowest row gives a fresh
// allowance, so a piece walked down a staircase of ledges lived on: here an O wiggles
// on each of three steps until its resets run out, then steps down to the next. That
// held it 25.5 s, and every further step adds up to (MaxLockResets+2) delays, so a tall
// enough staircase outlives the round. Now no piece lives longer than Config.PieceLimit
// (18.5 s with the defaults), however it is played.
func TestStaircaseCannotHoldAPieceForever(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = time.Hour // the other seat sends nothing
	m, _, _, start := newPlaying(t, cfg)
	b := m.Seats[0].Board
	// Ledges, one row lower every three columns: under columns 0-3, 4-6 and 7-9.
	for x := 0; x < game.Width; x++ {
		b.Grid[12+max(x-1, 0)/3][x] = 'X'
	}
	ledges := lockedCells(b)
	b.Piece = game.Piece{Kind: game.O, X: 1}
	b.LowestY, b.LockResets = b.Piece.Y+1, 0
	limit := cfg.PieceLimit()
	var seq uint64
	var moved time.Time
	for now := start; now.Before(start.Add(limit + 20*time.Second)); now = now.Add(cfg.TickInterval) {
		if b.Grounded() {
			a := game.Action("")
			switch x := b.Piece.X; {
			case b.LockResets == game.MaxLockResets || x%3 == 0:
				a = game.Right // resets used up: step down
			case now.Sub(moved) >= 9*cfg.TickInterval: // each reset near the full delay
				a = game.Right
				if x%3 == 2 {
					a = game.Left
				}
			}
			if a != "" {
				seq++
				m.Input(0, seq, a, now)
				moved = now
			}
		}
		row := b.LowestY
		m.Advance(now)
		if lockedCells(b) > ledges {
			if lived := now.Sub(start); lived > limit+cfg.TickInterval {
				t.Fatalf("an O walked down the stairs lived %s, want at most %s", lived, limit)
			}
			t.Logf("the O lived %s and reached row %d (limit %s)", now.Sub(start), row, limit)
			return
		}
	}
	t.Fatalf("an O walked down the stairs never locked, want within %s", limit)
}

// The invariant behind the fixes above, under random play: once a piece has stopped
// reaching new lowest rows, it locks within MaxLockResets+1 delays, plus one more if a
// turn left it in the air when its time ran out. Random inputs at 20 per second, mostly
// turns (the moves that lift a piece) and no hard drops, several seeds, until the round
// ends: pieces only lock by the delay here. And no piece lives longer than PieceLimit.
func TestRandomPlayLocksEveryPieceInTime(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout, cfg.RoundTime = time.Hour, 5*time.Minute
	actions := []game.Action{game.Rotate, game.RotateCCW, game.Rotate, game.RotateCCW, game.Left, game.Right, game.SoftDrop}
	limit := time.Duration(game.MaxLockResets+2)*cfg.LockDelay + cfg.TickInterval
	pieces := 0
	for seed := uint64(1); seed <= 8; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed))
		m, _, _, start := newPlaying(t, cfg)
		var seq uint64
		lowest, lowerAt, born := m.Seats[0].Board.LowestY, start, start
		for now := start; m.Phase == Playing && now.Before(start.Add(4*time.Minute)); now = now.Add(cfg.TickInterval) {
			seq++
			m.Input(0, seq, actions[rng.IntN(len(actions))], now)
			m.Advance(now)
			if m.Phase != Playing {
				break
			}
			b := m.Seats[0].Board
			switch {
			case b.LowestY < lowest: // a new piece
				pieces++
				lowest, lowerAt, born = b.LowestY, now, now
			case b.LowestY > lowest:
				lowest, lowerAt = b.LowestY, now
			}
			if held := now.Sub(lowerAt); held > limit {
				t.Fatalf("seed %d: a %c piece has been at its lowest row %d for %s, want it locked within %s",
					seed, b.Piece.Kind, b.LowestY, held, limit)
			}
			if lived := now.Sub(born); lived > cfg.PieceLimit()+cfg.TickInterval {
				t.Fatalf("seed %d: a %c piece has lived %s, want at most %s", seed, b.Piece.Kind, lived, cfg.PieceLimit())
			}
		}
	}
	t.Logf("%d pieces", pieces)
	if pieces < 50 {
		t.Fatalf("only %d pieces played: the test did not exercise locking", pieces)
	}
}
