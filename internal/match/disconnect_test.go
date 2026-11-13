package match

// The disconnect rules: the match never pauses. A disconnected player's board keeps
// running, the opponent keeps playing, and the disconnected player either resumes within
// the reconnect grace or forfeits. Every test runs on explicit (fake) time.

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
// socket stays attached; hearing from it again brings the seat back without a snapshot,
// and if it closes later its grace still counts from the silence.
func TestSilentConnectionCountsAsAway(t *testing.T) {
	cfg := testConfig()
	m, _, sinks, silentAt := newPlaying(t, cfg)
	m.SetSilent(1, sinks[1], true, silentAt)
	if m.Seats[1].Connected || !m.Seats[1].LeftAt.Equal(silentAt) {
		t.Fatalf("silent seat: connected=%v leftAt=%v", m.Seats[1].Connected, m.Seats[1].LeftAt)
	}
	if evs := sinks[0].events(); evs[len(evs)-1].Kind != protocol.EvPlayerLeft {
		t.Fatalf("opponent's last event = %+v, want player_left", evs[len(evs)-1])
	}
	count := func() (n, snaps int) {
		sinks[1].mu.Lock()
		defer sinks[1].mu.Unlock()
		for _, msg := range sinks[1].msgs {
			if _, ok := msg.(protocol.Snapshot); ok {
				snaps++
			}
		}
		return len(sinks[1].msgs), snaps
	}
	n, snaps := count()
	now := silentAt.Add(cfg.Gravity)
	m.Advance(now)
	m.Broadcast(now)
	if got, _ := count(); got == n {
		t.Fatal("a silent connection stopped getting the state")
	}

	lastInput := m.Seats[1].LastInput
	back := now.Add(3 * time.Second)
	m.SetSilent(1, sinks[1], false, back)
	if !m.Seats[1].Connected || !m.Seats[1].LastInput.Equal(lastInput.Add(back.Sub(silentAt))) {
		t.Fatalf("heard again: connected=%v idle clock moved %s", m.Seats[1].Connected, m.Seats[1].LastInput.Sub(lastInput))
	}
	if _, s := count(); s != snaps {
		t.Fatal("a connection heard again was sent a snapshot")
	}
	if evs := sinks[0].events(); evs[len(evs)-1].Kind != protocol.EvPlayerBack {
		t.Fatalf("opponent's last event = %+v, want player_back", evs[len(evs)-1])
	}

	m.SetSilent(1, &fakeSink{}, true, back) // a connection that is not the seat's
	if !m.Seats[1].Connected {
		t.Fatal("a stale connection's silence marked the seat away")
	}

	quiet := back.Add(time.Second)
	m.SetSilent(1, sinks[1], true, quiet)
	m.Detach(1, sinks[1], quiet.Add(5*time.Second))
	if !m.Seats[1].LeftAt.Equal(quiet) {
		t.Fatalf("grace restarted at the close: leftAt %s after the silence", m.Seats[1].LeftAt.Sub(quiet))
	}
	m.Advance(quiet.Add(cfg.ReconnectGrace - time.Millisecond))
	if m.Phase != Playing {
		t.Fatalf("forfeited early: %s", m.EndReason)
	}
	m.Advance(quiet.Add(cfg.ReconnectGrace))
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_disconnected" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
	left := 0
	for _, e := range sinks[0].events() {
		if e.Kind == protocol.EvPlayerLeft {
			left++
		}
	}
	if left != 2 {
		t.Fatalf("%d player_left events, want 2 (silence, silence; the close adds none)", left)
	}
}

// (f) Restoring a match with a player away: the restored match runs at once, the away
// player keeps the grace they had left (the downtime is not counted), and the player
// whose socket died with the process gets a full grace from the restore.
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
			if got := r.Input(0, seq, game.Rotate, now); got != InputApplied {
				t.Fatalf("input after the restore: %s", got)
			}
		}
	}
	r.Advance(forfeit)
	if r.Phase != MatchEnd || r.Winner != 0 || r.EndReason != "opponent_disconnected" {
		t.Fatalf("phase=%s winner=%d reason=%s", r.Phase, r.Winner, r.EndReason)
	}
}

// Snapshots of builds that still paused matches carry pausedAt and awayUsed. They
// restore without them, and a match that was paused keeps the round time it had left
// when it froze (instead of losing the frozen stretch). Nothing pauses after the restore.
func TestRestoreIgnoresLegacyPauseFields(t *testing.T) {
	cfg := testConfig()
	m, _, _, start := newPlaying(t, cfg)
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
	old := strings.Replace(string(data), `"nextGravity":`, `"pausedAt":`+string(pausedAt)+`,"nextGravity":`, 1)
	old = strings.ReplaceAll(old, `"lastSeq":`, `"awayUsed":12000000000,"lastSeq":`)
	if !strings.Contains(old, `"pausedAt"`) || !strings.Contains(old, `"awayUsed"`) {
		t.Fatal("could not build the legacy snapshot")
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
	if strings.Contains(string(again), `"pausedAt"`) || strings.Contains(string(again), `"awayUsed"`) {
		t.Fatal("pause fields survived the restore")
	}
	y := r.Seats[0].Board.Piece.Y
	r.Advance(r.NextGravity)
	if r.Phase != Playing || r.StateFor(0, r.NextGravity).Paused || r.Seats[0].Board.Piece.Y == y && r.Seats[0].Board.StackHeight() == 0 {
		t.Fatal("restored legacy match does not run")
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
