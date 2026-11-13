package match

// The disconnect rules: a disconnect never pauses the match. A disconnected player's
// board keeps running, the opponent keeps playing, and the disconnected player either
// resumes within the reconnect grace or forfeits. The one stand-still is the server's
// own: a restored match waits for its players (ResyncWait). Every test runs on explicit
// (fake) time.

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
	"github.com/Gegirhasut/sabostack-go-server/internal/testredis"
)

func kinds(evs []protocol.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}

func lastRoundOver(m *Match) protocol.Event {
	for i := len(m.Events) - 1; i >= 0; i-- {
		if m.Events[i].Kind == protocol.EvRoundOver {
			return m.Events[i]
		}
	}
	return protocol.Event{}
}

// (a) While one player is away the other's inputs are applied and the round timer runs
// to its original deadline.
func TestOpponentPlaysOnWhileOtherIsAway(t *testing.T) {
	cfg := testConfig()
	cfg.RoundTime = 10 * time.Second // ends well inside the 20 s grace
	m, _, sinks, start := newPlaying(t, cfg)
	ends := m.PhaseEnds
	m.Detach(1, sinks[1], start)

	var seq uint64
	for now := start; now.Before(ends); now = now.Add(cfg.TickInterval) {
		m.Advance(now)
		st := m.StateFor(0, now)
		if m.Phase != Playing || st.Paused || st.RemainingMs != ends.Sub(now).Milliseconds() {
			t.Fatalf("after %s: phase=%s paused=%v remaining=%dms, want playing with %dms left",
				now.Sub(start), m.Phase, st.Paused, st.RemainingMs, ends.Sub(now).Milliseconds())
		}
		if now.Sub(start)%time.Second == 0 {
			seq++
			act := []game.Action{game.Left, game.Right}[seq%2]
			if got := m.Input(0, seq, act, now); got != InputApplied || !m.Seats[0].LastInput.Equal(now) {
				t.Fatalf("opponent input at %s: %s", now.Sub(start), got)
			}
		}
	}
	if !m.PhaseEnds.Equal(ends) {
		t.Fatalf("round deadline moved by %s while a player was away", m.PhaseEnds.Sub(ends))
	}
	m.Advance(ends)
	if ev := lastRoundOver(m); m.Phase != RoundEnd || ev.Reason != "time_up" {
		t.Fatalf("at the round deadline: phase=%s last round_over=%+v", m.Phase, ev)
	}
	if m.Seats[1].Connected {
		t.Fatal("the away player is shown as connected")
	}
}

// (b) The away player's board keeps falling: pieces lock, incoming garbage rises, and
// a top-out loses the round like any other.
func TestAwayPlayersBoardFallsAndTopsOut(t *testing.T) {
	cfg := testConfig()
	cfg.ReconnectGrace, cfg.IdleTimeout = time.Hour, time.Hour // only the boards decide
	m, _, sinks, now := newPlaying(t, cfg)
	m.Detach(1, sinks[1], now)
	b := m.Seats[1].Board
	b.Pending = 8 // garbage the opponent sent: it rises when the next piece locks
	y := b.Piece.Y
	now = now.Add(cfg.Gravity)
	m.Advance(now)
	if b.Piece.Y != y+1 {
		t.Fatalf("away player's piece y %d -> %d, want it to fall", y, b.Piece.Y)
	}
	for i := 0; m.Phase == Playing && i < 20000; i++ {
		now = now.Add(cfg.TickInterval)
		m.Advance(now)
	}
	ev := lastRoundOver(m)
	if m.Phase != RoundEnd || ev.Reason != "top_out" || ev.Winner == nil || *ev.Winner != 0 {
		t.Fatalf("phase=%s round_over=%+v, want seat 1 topped out", m.Phase, ev)
	}
	if !b.Dead || m.Seats[0].Board.Dead || m.Seats[0].Wins != 1 || m.Seats[1].Connected {
		t.Fatalf("dead=%v/%v wins=%d connected=%v", m.Seats[0].Board.Dead, b.Dead, m.Seats[0].Wins, m.Seats[1].Connected)
	}
	rose := slices.ContainsFunc(m.Events, func(e protocol.Event) bool {
		return e.Kind == protocol.EvGarbage && e.Seat != nil && *e.Seat == 1 && e.Garbage == 8
	})
	if !rose {
		t.Fatalf("no garbage event for the away player: %v", kinds(m.Events))
	}
}

// (c) A player back within the grace gets a snapshot of the board as it is now, not as
// they left it, and their inputs apply at once.
func TestReconnectWithinGraceSeesTheAdvancedBoard(t *testing.T) {
	cfg := testConfig()
	m, tokens, sinks, start := newPlaying(t, cfg)
	ends, seen := m.PhaseEnds, m.EventSeq
	m.Detach(1, sinks[1], start)
	before := m.Seats[1].Board.Grid.Rows()
	m.Seats[1].Board.Pending = 2

	back := start.Add(cfg.ReconnectGrace - time.Second)
	var seq uint64
	for now := start; !now.After(back); now = now.Add(cfg.TickInterval) {
		m.Advance(now)
		if now.Sub(start)%time.Second == 0 {
			seq++
			m.Input(0, seq, game.Rotate, now)
		}
	}
	if m.Phase != Playing {
		t.Fatalf("phase = %s", m.Phase)
	}
	if slices.Equal(m.Seats[1].Board.Grid.Rows(), before) {
		t.Fatal("the away player's board did not change in 19 s")
	}

	fresh := &fakeSink{}
	if _, err := m.Attach(tokens[1], fresh, seen, back); err != nil {
		t.Fatal(err)
	}
	snap, ok := fresh.lastSnapshot()
	if !ok || !snap.EventsComplete || snap.State.Phase != "playing" || snap.State.Paused {
		t.Fatalf("bad snapshot: %+v", snap)
	}
	if got, want := snap.State.Players[1].Rows, m.Seats[1].Board.Grid.Rows(); !slices.Equal(got, want) {
		t.Fatalf("snapshot board %v, want the current one %v", got, want)
	}
	if snap.State.Players[1].Piece != m.Seats[1].Board.Piece || !snap.State.Players[1].Connected {
		t.Fatalf("snapshot piece %+v connected=%v", snap.State.Players[1].Piece, snap.State.Players[1].Connected)
	}
	if !m.PhaseEnds.Equal(ends) || snap.State.RemainingMs != ends.Sub(back).Milliseconds() {
		t.Fatalf("remaining %dms, want %dms: the round clock ran on", snap.State.RemainingMs, ends.Sub(back).Milliseconds())
	}
	k := kinds(snap.Events)
	if k[0] != protocol.EvPlayerLeft || k[len(k)-1] != protocol.EvPlayerBack || !slices.Contains(k, protocol.EvGarbage) {
		t.Fatalf("missed events = %v, want player_left ... garbage ... player_back", k)
	}
	if got := m.Input(1, 1, game.Left, back); got != InputApplied {
		t.Fatalf("input after the resume: %s", got)
	}
}

// (d) The grace runs out in the break between rounds: the match is forfeited, unless the
// rounds already decided it, in which case the result stands.
func TestGraceRunningOutBetweenRounds(t *testing.T) {
	cfg := testConfig()
	cfg.ReconnectGrace = time.Second // ends inside the 3 s break

	// Round 1, nothing decided yet: seat 1 drops, seat 0 tops out, seat 1 forfeits anyway.
	m, _, sinks, now := newPlaying(t, cfg)
	var seq uint64
	m.Detach(1, sinks[1], now)
	killSeat(t, m, 0, &seq, now)
	m.Advance(now.Add(cfg.ReconnectGrace))
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_disconnected" {
		t.Fatalf("undecided: phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}

	// Seat 1 wins round 1, drops in round 2 and wins it too: 2 wins, the match is theirs.
	m, _, sinks, now = newPlaying(t, cfg)
	seq = 0
	killSeat(t, m, 0, &seq, now)
	now = now.Add(cfg.RoundEndPause)
	m.Advance(now)
	now = now.Add(cfg.Countdown)
	m.Advance(now)
	if m.Phase != Playing || m.Round != 2 {
		t.Fatalf("phase=%s round=%d, want round 2", m.Phase, m.Round)
	}
	m.Detach(1, sinks[1], now)
	killSeat(t, m, 0, &seq, now)
	m.Advance(now.Add(cfg.ReconnectGrace))
	if m.Phase != RoundEnd {
		t.Fatalf("decided match forfeited in the break: phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
	m.Advance(now.Add(cfg.RoundEndPause))
	if m.Phase != MatchEnd || m.Winner != 1 || m.EndReason != "rounds" {
		t.Fatalf("decided: phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
}

// (e) Both players away: the match still runs, each player's grace is their own, and
// nobody wins if both run out together.
func TestBothPlayersAway(t *testing.T) {
	cfg := testConfig()

	t.Run("same time", func(t *testing.T) {
		m, _, sinks, start := newPlaying(t, cfg)
		ends := m.PhaseEnds
		y := m.Seats[0].Board.Piece.Y
		m.Detach(0, sinks[0], start)
		m.Detach(1, sinks[1], start)
		last := start.Add(cfg.ReconnectGrace - cfg.TickInterval)
		for now := start; !now.After(last); now = now.Add(cfg.TickInterval) {
			m.Advance(now)
		}
		if m.Phase != Playing || !m.PhaseEnds.Equal(ends) || m.Seats[0].Board.Piece.Y == y && m.Seats[0].Board.StackHeight() == 0 {
			t.Fatalf("with both away: phase=%s deadline moved %s, board still", m.Phase, m.PhaseEnds.Sub(ends))
		}
		m.Advance(start.Add(cfg.ReconnectGrace))
		if m.Phase != MatchEnd || m.Winner != NoSeat || m.EndReason != "both_disconnected" {
			t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
		}
	})

	t.Run("first to leave forfeits first", func(t *testing.T) {
		m, _, sinks, start := newPlaying(t, cfg)
		m.Detach(0, sinks[0], start)
		m.Detach(1, sinks[1], start.Add(5*time.Second))
		m.Advance(start.Add(cfg.ReconnectGrace))
		if m.Phase != MatchEnd || m.Winner != 1 || m.EndReason != "opponent_disconnected" {
			t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
		}
	})

	t.Run("one comes back", func(t *testing.T) {
		m, tokens, sinks, start := newPlaying(t, cfg)
		m.Detach(0, sinks[0], start)
		m.Detach(1, sinks[1], start)
		if _, err := m.Attach(tokens[0], &fakeSink{}, 0, start.Add(10*time.Second)); err != nil {
			t.Fatal(err)
		}
		m.Input(0, 1, game.Rotate, start.Add(10*time.Second))
		m.Advance(start.Add(cfg.ReconnectGrace))
		if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_disconnected" {
			t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
		}
	})
}

// The idle timeout counts only connected time: it never fires for a player who is
// away (the grace does), time away is not idle time after a resume, and a player who was
// idle before leaving is not given a fresh window for coming back.
func TestIdleAndGraceDoNotOverlap(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour
	cfg.ReconnectGrace = 2 * cfg.IdleTimeout // the grace outlasts the idle timeout

	// Away for longer than the idle timeout: still in the match, it is the grace that counts.
	m, tokens, sinks, start := newPlaying(t, cfg)
	m.Input(0, 1, game.Rotate, start)
	m.Detach(1, sinks[1], start)
	away := cfg.IdleTimeout + 5*time.Second
	for now := start; now.Before(start.Add(away)); now = now.Add(time.Second) {
		m.Input(0, uint64(now.Sub(start)/time.Second)+2, game.Rotate, now)
		m.Advance(now)
	}
	if m.Phase != Playing {
		t.Fatalf("away player forfeited by %s after %s away", m.EndReason, away)
	}

	// Idle 25 s, away 10 s, back: 5 s of idle time left, not 30 and not 0.
	m, tokens, sinks, start = newPlaying(t, cfg)
	left := start.Add(25 * time.Second)
	back := left.Add(10 * time.Second)
	seq := uint64(0)
	for now := start; now.Before(back); now = now.Add(time.Second) {
		seq++
		m.Input(0, seq, game.Rotate, now)
		if now.Equal(left) {
			m.Detach(1, sinks[1], now)
		}
		m.Advance(now)
	}
	if _, err := m.Attach(tokens[1], &fakeSink{}, 0, back); err != nil {
		t.Fatal(err)
	}
	for now := back; m.Phase == Playing && now.Before(back.Add(time.Minute)); now = now.Add(time.Second) {
		seq++
		m.Input(0, seq, game.Rotate, now)
		m.Advance(now)
		if m.Phase == MatchEnd && now.Sub(back) != 5*time.Second {
			t.Fatalf("idle forfeit %s after the resume, want 5s", now.Sub(back))
		}
	}
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_idle" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
}

// A silent connection (the server has heard nothing from it) counts as away while the
// socket stays attached, and it is the match, on its own tick, that notices. The seat
// comes back without a snapshot, but only once the connection has been heard from
// steadily for SilenceTimeout/2: one stray pong from a connection that keeps stalling
// does not bring it back (that would flap player_left/player_back). If the socket closes
// later, the grace still counts from the silence.
func TestSilentConnectionCountsAsAway(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour // only presence matters here
	m, tokens, sinks, start := newPlaying(t, cfg)
	bob := &heardSink{}
	bob.hear(start)
	if _, err := m.Attach(tokens[1], bob, 0, start); err != nil {
		t.Fatal(err)
	}
	silence, steady := cfg.SilenceTimeout, cfg.SilenceTimeout/2
	now, seq := start, uint64(0)
	// step runs the match tick by tick up to until; bob is heard on the ticks hear
	// picks. Alice plays on throughout, so only bob's presence changes.
	step := func(until time.Time, hear func(time.Time) bool) {
		for now.Before(until) {
			now = now.Add(cfg.TickInterval)
			if hear != nil && hear(now) {
				bob.hear(now)
			}
			if now.Sub(start)%time.Second == 0 {
				seq++
				m.Input(0, seq, game.Rotate, now)
			}
			m.Advance(now)
			m.Broadcast(now)
		}
	}
	count := func(kind string) (n int) {
		for _, e := range sinks[0].events() {
			if e.Kind == kind && e.Seat != nil && *e.Seat == 1 {
				n++
			}
		}
		return n
	}

	step(start.Add(silence-cfg.TickInterval), nil)
	if !m.Seats[1].Connected {
		t.Fatal("bob marked away before the silence timeout")
	}
	step(start.Add(silence), nil)
	silentAt := now
	if m.Seats[1].Connected || !m.Seats[1].LeftAt.Equal(silentAt) || count(protocol.EvPlayerLeft) != 1 {
		t.Fatalf("silent seat: connected=%v leftAt=%v player_left=%d", m.Seats[1].Connected, m.Seats[1].LeftAt, count(protocol.EvPlayerLeft))
	}
	n := bob.count()
	step(now.Add(time.Second), nil)
	if bob.count() == n {
		t.Fatal("a silent connection stopped getting the state")
	}

	// A stall: one message, then nothing again. The seat stays away.
	blip := now.Add(time.Second)
	step(blip.Add(2*steady), func(t time.Time) bool { return t.Equal(blip) })
	if m.Seats[1].Connected || count(protocol.EvPlayerBack) != 0 {
		t.Fatal("a single message brought a stalling connection back")
	}

	// Heard steadily again: back after SilenceTimeout/2 of it, on the same socket.
	lastInput, snaps := m.Seats[1].LastInput, bob.snapshots()
	run := now.Add(cfg.TickInterval)
	for !m.Seats[1].Connected && now.Before(run.Add(time.Minute)) {
		step(now.Add(cfg.TickInterval), func(time.Time) bool { return true })
	}
	if got := now.Sub(run); got != steady {
		t.Fatalf("back %s after being heard steadily again, want %s", got, steady)
	}
	// The idle clock stopped when bob was last heard (start), not when the silence was
	// noticed: the silence is no idle time either.
	if got, want := m.Seats[1].LastInput.Sub(lastInput), now.Sub(start); got != want {
		t.Fatalf("idle clock moved %s on the return, want the %s since bob was last heard", got, want)
	}
	if bob.snapshots() != snaps {
		t.Fatal("a connection heard again was sent a snapshot")
	}
	if count(protocol.EvPlayerLeft) != 1 || count(protocol.EvPlayerBack) != 1 {
		t.Fatalf("player_left=%d player_back=%d, want one of each for one stall", count(protocol.EvPlayerLeft), count(protocol.EvPlayerBack))
	}

	// Silent for good, and the socket closes later: the grace runs from the silence.
	step(now.Add(silence), nil)
	quiet := now
	if m.Seats[1].Connected {
		t.Fatal("bob not marked away the second time")
	}
	step(quiet.Add(5*time.Second), nil)
	m.Detach(1, bob, now)
	if !m.Seats[1].LeftAt.Equal(quiet) {
		t.Fatalf("grace restarted at the close: leftAt %s after the silence", m.Seats[1].LeftAt.Sub(quiet))
	}
	step(quiet.Add(cfg.ReconnectGrace-cfg.TickInterval), nil)
	if m.Phase != Playing {
		t.Fatalf("forfeited early: %s", m.EndReason)
	}
	step(quiet.Add(cfg.ReconnectGrace), nil)
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_disconnected" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
	if count(protocol.EvPlayerLeft) != 2 {
		t.Fatalf("%d player_left events, want 2 (two silences; the close adds none)", count(protocol.EvPlayerLeft))
	}
}

// Bug: after a restore or a takeover both players start disconnected, and the match ran
// on at once. A failover takes seconds, so whoever's client found the new node first
// played on while the other's board fell: the server's failure decided rounds. Now the
// restored match stands still (paused, inputs not applied, round time not running) until
// both players are back, then runs from where it stood, with the idle clocks not
// counting the wait either.
func TestRestoredMatchWaitsForBothPlayers(t *testing.T) {
	cfg := testConfig()
	m, tokens, _, start := newPlaying(t, cfg)
	m.Input(0, 1, game.Left, start)
	now := start.Add(2 * time.Second)
	m.Advance(now)
	left, idle := m.PhaseEnds.Sub(now), now.Sub(m.Seats[1].LastInput)
	data, err := m.Snapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	restoredAt := now.Add(time.Minute)
	r, err := Restore(data, cfg, restoredAt)
	if err != nil {
		t.Fatal(err)
	}
	y := r.Seats[0].Board.Piece.Y
	alice := &fakeSink{}
	if _, err := r.Attach(tokens[0], alice, 0, restoredAt); err != nil {
		t.Fatal(err)
	}
	if snap, _ := alice.lastSnapshot(); !snap.State.Paused || time.Duration(snap.State.RemainingMs)*time.Millisecond != left {
		t.Fatalf("snapshot on resume: paused=%v remaining=%dms, want paused with %s left", snap.State.Paused, snap.State.RemainingMs, left)
	}
	back := restoredAt.Add(cfg.ResyncWait / 2)
	for at := restoredAt; at.Before(back); at = at.Add(cfg.TickInterval) {
		r.Advance(at)
		st := r.StateFor(0, at)
		if !st.Paused || time.Duration(st.RemainingMs)*time.Millisecond != left || r.Seats[0].Board.Piece.Y != y {
			t.Fatalf("%s into the wait: paused=%v remaining=%dms piece y %d->%d", at.Sub(restoredAt), st.Paused, st.RemainingMs, y, r.Seats[0].Board.Piece.Y)
		}
	}
	if got := r.Input(0, 2, game.Left, back); got != InputNotPlaying || r.Seats[0].LastSeq != 1 {
		t.Fatalf("input while resyncing: %s, lastSeq %d (want it not acked, so the client resends it)", got, r.Seats[0].LastSeq)
	}
	if _, err := r.Attach(tokens[1], &fakeSink{}, 0, back); err != nil {
		t.Fatal(err)
	}
	r.Advance(back)
	if r.StateFor(0, back).Paused || r.PhaseEnds.Sub(back) != left {
		t.Fatalf("both back: paused=%v round time left %s, want %s", r.StateFor(0, back).Paused, r.PhaseEnds.Sub(back), left)
	}
	if evs := alice.events(); evs[len(evs)-1].Kind != protocol.EvResumed {
		t.Fatalf("last event %+v, want resumed", evs[len(evs)-1])
	}
	if got := back.Sub(r.Seats[1].LastInput); got != idle {
		t.Fatalf("idle %s after the wait, want the %s it had", got, idle)
	}
	if got := r.Input(0, 2, game.Left, back); got != InputApplied {
		t.Fatalf("the input resent after the resume: %s", got)
	}
}

// A restored match waits for its players only up to ResyncWait. A player who is not
// back by then has the rest of their grace, counted from the restore, on a running
// board; the one who came back plays on.
func TestResyncWaitIsBounded(t *testing.T) {
	cfg := testConfig()
	m, tokens, _, start := newPlaying(t, cfg)
	data, _ := m.Snapshot(start)
	r, err := Restore(data, cfg, start)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Attach(tokens[0], &fakeSink{}, 0, start); err != nil {
		t.Fatal(err)
	}
	for at := start; !at.After(start.Add(cfg.ReconnectGrace)); at = at.Add(cfg.TickInterval) {
		r.Advance(at)
		if paused := r.StateFor(0, at).Paused; paused != at.Before(start.Add(cfg.ResyncWait)) {
			t.Fatalf("%s after the restore: paused=%v", at.Sub(start), paused)
		}
		if r.Phase == MatchEnd {
			if at.Sub(start) != cfg.ReconnectGrace || r.Winner != 0 || r.EndReason != "opponent_disconnected" {
				t.Fatalf("ended %s after the restore: winner=%d reason=%s", at.Sub(start), r.Winner, r.EndReason)
			}
			return
		}
	}
	t.Fatal("the absent player never forfeited")
}

// (f) Restoring a match with a player away: the restored match waits ResyncWait for
// its players and then runs, the away player keeps the grace they had left (neither the
// downtime nor the wait extends it), and the player whose socket died with the process
// gets a full grace from the restore.
func TestRestoreWithAwayPlayer(t *testing.T) {
	cfg := testConfig()
	m, tokens, sinks, start := newPlaying(t, cfg)
	m.Input(0, 1, game.Left, start)
	m.Detach(1, sinks[1], start)
	away := 5 * time.Second
	now := start
	for ; now.Before(start.Add(away)); now = now.Add(cfg.TickInterval) {
		m.Advance(now)
	}
	data, err := m.Snapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{`"pausedAt"`, `"awayUsed"`} {
		if strings.Contains(string(data), gone) {
			t.Fatalf("snapshot still carries %s", gone)
		}
	}

	downtime := time.Minute
	later := now.Add(downtime)
	r, err := Restore(data, cfg, later)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Seats[1].LeftAt.Equal(m.Seats[1].LeftAt.Add(downtime)) || !r.Seats[0].LeftAt.Equal(later) || r.ConnectedCount() != 0 {
		t.Fatalf("restored leftAt: seat 0 %v, seat 1 %v", r.Seats[0].LeftAt, r.Seats[1].LeftAt)
	}
	if got := r.Input(0, 1, game.Left, later); got != InputDuplicate {
		t.Fatalf("retransmit after restore: %s", got)
	}
	if _, err := r.Attach(tokens[0], &fakeSink{}, 0, later); err != nil {
		t.Fatal(err)
	}
	forfeit := later.Add(cfg.ReconnectGrace - away)
	seq := uint64(1)
	for now := later; now.Before(forfeit); now = now.Add(cfg.TickInterval) {
		r.Advance(now)
		if r.Phase != Playing {
			t.Fatalf("%s after the restore: phase=%s reason=%s", now.Sub(later), r.Phase, r.EndReason)
		}
		if now.Sub(later)%time.Second == 0 {
			seq++
			want := InputApplied
			if now.Before(later.Add(cfg.ResyncWait)) {
				want = InputNotPlaying // seat 1 is not back: the match stands still
			}
			if got := r.Input(0, seq, game.Rotate, now); got != want {
				t.Fatalf("input %s after the restore: %s, want %s", now.Sub(later), got, want)
			}
		}
	}
	r.Advance(forfeit)
	if r.Phase != MatchEnd || r.Winner != 0 || r.EndReason != "opponent_disconnected" {
		t.Fatalf("phase=%s winner=%d reason=%s", r.Phase, r.Winner, r.EndReason)
	}
}

// A version-1 snapshot (from builds that paused matches on a disconnect) migrates to
// version 2: pausedAt becomes resyncFrom, so a match that was paused keeps the round
// time it had left when it froze, awayUsed (the pause budget) is dropped, and the idle
// clock of an away player stands from when they left. Like any restore, the match then
// waits for its players and runs.
func TestRestoreMigratesVersion1Snapshot(t *testing.T) {
	cfg := testConfig()
	m, tokens, _, start := newPlaying(t, cfg)
	var seq uint64
	frozeAt := start.Add(20 * time.Second)
	for now := start; !now.After(frozeAt); now = now.Add(cfg.TickInterval) {
		m.Advance(now)
		if now.Sub(start)%time.Second == 0 {
			seq++
			m.Input(0, seq, game.Rotate, now)
			m.Input(1, seq, game.Rotate, now)
		}
	}
	savedAt := frozeAt.Add(10 * time.Second) // the old build kept the clocks still since frozeAt
	data, err := m.FinalSnapshot(savedAt)
	if err != nil {
		t.Fatal(err)
	}
	pausedAt, _ := json.Marshal(frozeAt)
	old := strings.Replace(string(data), `"resyncFrom":`, `"pausedAt":`+string(pausedAt)+`,"oldResyncFrom":`, 1)
	old = strings.ReplaceAll(old, `"lastSeq":`, `"awayUsed":12000000000,"lastSeq":`)
	old = string(withVersion(t, []byte(old), 1))
	if !strings.Contains(old, `"pausedAt"`) || !strings.Contains(old, `"awayUsed"`) || !strings.Contains(old, `"version":1`) {
		t.Fatal("could not build the version-1 snapshot")
	}

	now := savedAt.Add(time.Minute)
	r, err := Restore([]byte(old), cfg, now)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := r.PhaseEnds.Sub(now), m.PhaseEnds.Sub(frozeAt); got != want {
		t.Fatalf("round time left %s, want the %s it had when it froze", got, want)
	}
	if got, want := r.NextGravity.Sub(now), m.NextGravity.Sub(frozeAt); got != want {
		t.Fatalf("next gravity in %s, want %s", got, want)
	}
	if got, want := now.Sub(r.Seats[0].LastInput), frozeAt.Sub(m.Seats[0].LastInput); got != want {
		t.Fatalf("idle for %s, want %s", got, want)
	}
	again, _ := r.Snapshot(now)
	for _, gone := range []string{`"pausedAt"`, `"awayUsed"`, `"version":1`} {
		if strings.Contains(string(again), gone) {
			t.Fatalf("%s survived the restore", gone)
		}
	}
	for i := range tokens {
		if _, err := r.Attach(tokens[i], &fakeSink{}, 0, now); err != nil {
			t.Fatal(err)
		}
	}
	y := r.Seats[0].Board.Piece.Y
	r.Advance(now)
	r.Advance(r.NextGravity)
	if r.Phase != Playing || r.StateFor(0, r.NextGravity).Paused || r.Seats[0].Board.Piece.Y == y && r.Seats[0].Board.StackHeight() == 0 {
		t.Fatal("restored version-1 match does not run")
	}
}

// (f) A node dies with one player away: another node takes the match over from Redis,
// the present player resumes and plays on there, the away player keeps the grace they
// had left and forfeits when it runs out, and the dead node can no longer write it.
func TestTakeoverWithAwayPlayer(t *testing.T) {
	ctx := context.Background()
	const lease = 10 * time.Second
	rdb := testredis.New(t)
	base := store.NewRedis(rdb.Client, "t:", time.Hour)
	nodeA, nodeB := base.ForNode("a", "ws://a/ws"), base.ForNode("b", "ws://b/ws")
	for _, n := range []*store.Redis{nodeA, nodeB} {
		if err := n.Heartbeat(ctx, lease); err != nil {
			t.Fatal(err)
		}
	}
	clk := clock.NewFake(t0)
	mgrA := newTestManager(t, clk, nodeA)
	defer mgrA.Shutdown(ctx)
	a, tokens, sinks := startPlaying(t, mgrA, clk)
	a.Input(0, 1, game.Left)
	a.Detach(1, sinks[1])
	var leftAt time.Time
	inspect(t, a, func(m *Match) { leftAt = m.Seats[1].LeftAt })

	clk.Advance(5 * time.Second)
	// Periodic snapshots fall on whole seconds here; the last one before the crash must
	// be in Redis. Its savedAt is the clock reading when it was taken, which on the fake
	// clock may already be the next tick, so the test uses whatever was stored.
	lastSnap := clk.Now().Truncate(time.Second)
	var saved time.Time
	eventually(t, "the last periodic snapshot in Redis", func() bool {
		all, _ := nodeA.LoadAll(ctx)
		var h struct {
			SavedAt time.Time `json:"savedAt"`
		}
		_ = json.Unmarshal(all[a.ID()], &h)
		saved = h.SavedAt
		return !saved.Before(lastSnap)
	})

	// Node a dies: no final snapshot. Its lease runs out and node b adopts the match.
	mgrA.EvictAll()
	<-a.Done()
	rdb.FastForward(lease + time.Second)
	if err := nodeB.Heartbeat(ctx, lease); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second) // the failover takes a while on the game clock too
	snaps, err := nodeB.AdoptOrphans(ctx, 10)
	if err != nil || len(snaps) != 1 {
		t.Fatalf("adopted %d matches, err %v", len(snaps), err)
	}
	mgrB := newTestManager(t, clk, nodeB)
	defer mgrB.Shutdown(ctx)
	if n := mgrB.Adopt(snaps); n != 1 {
		t.Fatalf("Adopt = %d", n)
	}
	adoptedAt := clk.Now()
	b, ok := mgrB.Get(a.ID())
	if !ok {
		t.Fatal("adopted match not running")
	}
	if err := nodeA.Save(ctx, a.ID(), []byte("zombie")); !errors.Is(err, store.ErrNotOwner) {
		t.Fatalf("the dead node could still write the match: %v", err)
	}

	sink := &fakeSink{}
	if _, err := b.Attach(ctx, tokens[0], sink, 0); err != nil {
		t.Fatal(err)
	}
	if snap, _ := sink.lastSnapshot(); snap.State.Phase != "playing" || snap.State.Ack != 1 || snap.State.Players[1].Connected {
		t.Fatalf("after the takeover: phase=%s ack=%d seat 1 connected=%v", snap.State.Phase, snap.State.Ack, snap.State.Players[1].Connected)
	}
	// Seat 1 had been away for saved-leftAt when the snapshot was taken; the failover is
	// not counted against them. (Every Advance below stops short of the forfeit tick:
	// the actor may read the clock one tick ahead while Advance delivers several.)
	forfeit := adoptedAt.Add(mgrB.opts.Match.ReconnectGrace - saved.Sub(leftAt))
	var y int
	inspect(t, b, func(m *Match) { y = m.Seats[1].Board.Piece.Y })
	tick := mgrB.opts.Match.TickInterval
	// Seat 1 is not coming back, so the match stands for the whole resync wait first.
	clk.Advance(mgrB.opts.Match.ResyncWait)
	inspect(t, b, func(m *Match) {
		if m.resyncing() {
			t.Fatal("still resyncing after the resync wait")
		}
	})
	for seq := uint64(2); clk.Now().Add(time.Second).Before(forfeit); seq++ {
		b.Input(0, seq, game.Rotate)
		inspect(t, b, func(m *Match) {
			if m.Seats[0].LastSeq != seq || m.Phase != Playing {
				t.Fatalf("input %d on the new node: lastSeq=%d phase=%s", seq, m.Seats[0].LastSeq, m.Phase)
			}
		})
		clk.Advance(time.Second)
	}
	clk.Advance(forfeit.Sub(clk.Now()) - tick)
	inspect(t, b, func(m *Match) {
		if m.Phase != Playing || m.Seats[1].Board.Piece.Y == y && m.Seats[1].Board.StackHeight() == 0 {
			t.Fatalf("one tick before the forfeit: phase=%s, away board fell: %v", m.Phase, m.Seats[1].Board.Piece.Y != y)
		}
	})
	clk.Advance(tick)
	inspect(t, b, func(m *Match) {
		if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_disconnected" {
			t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
		}
	})
}

// Bug: when one player's grace and the other's idle timeout ran out on the same tick, the
// reason was "both_idle" although one of them was not even connected.
func TestGraceAndIdleOnOneTick(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour
	m, _, sinks, start := newPlaying(t, cfg)
	end := start.Add(cfg.IdleTimeout) // seat 0 never moves: idle from the round start
	for now := start; !now.After(end); now = now.Add(cfg.TickInterval) {
		if now.Equal(end.Add(-cfg.ReconnectGrace)) {
			m.Detach(1, sinks[1], now) // seat 1's grace ends on the same tick
		}
		m.Advance(now)
		if m.Phase == MatchEnd && !now.Equal(end) {
			t.Fatalf("ended %s early: %s", end.Sub(now), m.EndReason)
		}
	}
	if m.Phase != MatchEnd || m.Winner != NoSeat || m.EndReason != "disconnected_and_idle" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
}

// Bug: the idle clock ran until the silence was noticed. A player who thought for 25 s
// and then lost the network lost by idle at 30 s, before the 8 s silence timeout marked
// them away, and the 8 s of silence counted as idle time even when it was caught in
// time. Now the idle clock of a quiet connection only counts to when it was last heard.
func TestSilenceIsNotIdleTime(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour
	m, tokens, sinks, start := newPlaying(t, cfg)
	bob := &heardSink{}
	bob.hear(start)
	if _, err := m.Attach(tokens[1], bob, 0, start); err != nil {
		t.Fatal(err)
	}
	dropped := start.Add(cfg.IdleTimeout - cfg.SilenceTimeout + 2*time.Second) // thinks, then the network goes
	noticed := dropped.Add(cfg.SilenceTimeout)
	var seq uint64
	for now := start; !now.After(noticed); now = now.Add(cfg.TickInterval) {
		if !now.After(dropped) {
			bob.hear(now) // pongs only: bob makes no move
		}
		if now.Sub(start)%time.Second == 0 {
			seq++
			m.Input(0, seq, game.Rotate, now)
		}
		m.Advance(now)
		if m.Phase != Playing {
			t.Fatalf("%s after the drop: %s (winner %d)", now.Sub(dropped), m.EndReason, m.Winner)
		}
	}
	s := m.Seats[1]
	if s.Connected || !s.LeftAt.Equal(noticed) || !s.IdleStop.Equal(dropped) {
		t.Fatalf("connected=%v leftAt=+%s idleStop=+%s, want away from the notice and idle stopped at the drop",
			s.Connected, s.LeftAt.Sub(dropped), s.IdleStop.Sub(dropped))
	}
	_ = sinks
}

// An input from a seat marked away by silence brings it back at once: the steady-contact
// wait is for pongs, which a stalling connection can still let through, not for moves.
func TestInputBringsSilentSeatBack(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour
	m, tokens, sinks, start := newPlaying(t, cfg)
	bob := &heardSink{}
	bob.hear(start)
	if _, err := m.Attach(tokens[1], bob, 0, start); err != nil {
		t.Fatal(err)
	}
	now := start.Add(cfg.SilenceTimeout)
	m.Advance(now)
	if m.Seats[1].Connected {
		t.Fatal("bob not marked away by silence")
	}
	snaps := bob.snapshots()
	now = now.Add(time.Second)
	bob.hear(now)
	if got := m.Input(1, 1, game.Left, now); got != InputApplied {
		t.Fatalf("input from the silent seat: %s", got)
	}
	if !m.Seats[1].Connected || m.Seats[1].LeftAt != (time.Time{}) {
		t.Fatal("an input did not bring the silent seat back")
	}
	if evs := sinks[0].events(); evs[len(evs)-1].Kind != protocol.EvPlayerBack {
		t.Fatalf("opponent's last event %+v, want player_back", evs[len(evs)-1])
	}
	if bob.snapshots() != snaps {
		t.Fatal("the seat brought back by an input was sent a snapshot")
	}
	m.Advance(now.Add(cfg.TickInterval))
	if !m.Seats[1].Connected {
		t.Fatal("the next tick marked the seat away again")
	}
}
