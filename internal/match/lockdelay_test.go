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

// The lock timer is a game clock like gravity: a restore moves it by the time the match
// was down, and the resync stand-still moves it again, so the restart costs the resting
// piece none of its lock delay.
func TestLockDelaySurvivesRestore(t *testing.T) {
	cfg := testConfig()
	m, tokens, _, start := newPlaying(t, cfg)
	restOnFloor(t, m, 0)
	now := start.Add(cfg.Gravity)
	m.Advance(now)
	left := m.Seats[0].LockAt.Sub(now)
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

// The invariant behind the fixes above, under random play: once a piece has stopped
// reaching new lowest rows, it locks within MaxLockResets+1 delays, plus one more if a
// turn left it in the air when its time ran out. Random inputs at 20 per second, mostly
// turns (the moves that lift a piece) and no hard drops, several seeds, until the round
// ends: pieces only lock by the delay here.
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
		lowest, lowerAt := m.Seats[0].Board.LowestY, start
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
				lowest, lowerAt = b.LowestY, now
			case b.LowestY > lowest:
				lowest, lowerAt = b.LowestY, now
			}
			if held := now.Sub(lowerAt); held > limit {
				t.Fatalf("seed %d: a %c piece has been at its lowest row %d for %s, want it locked within %s",
					seed, b.Piece.Kind, b.LowestY, held, limit)
			}
		}
	}
	t.Logf("%d pieces", pieces)
	if pieces < 50 {
		t.Fatalf("only %d pieces played: the test did not exercise locking", pieces)
	}
}
