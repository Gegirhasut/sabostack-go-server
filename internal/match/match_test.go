package match

import (
	"errors"
	"testing"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

func TestTransitionTable(t *testing.T) {
	legal := map[[2]Phase]bool{
		{Waiting, Countdown}: true, {Waiting, MatchEnd}: true,
		{Countdown, Playing}: true, {Countdown, MatchEnd}: true,
		{Playing, RoundEnd}: true, {Playing, MatchEnd}: true,
		{RoundEnd, Countdown}: true, {RoundEnd, MatchEnd}: true,
	}
	all := []Phase{Waiting, Countdown, Playing, RoundEnd, MatchEnd}
	for _, from := range all {
		for _, to := range all {
			m := &Match{Phase: from}
			err := m.Transition(to)
			if legal[[2]Phase{from, to}] {
				if err != nil || m.Phase != to {
					t.Errorf("%s -> %s rejected: %v", from, to, err)
				}
				continue
			}
			var ill *ErrIllegalTransition
			if !errors.As(err, &ill) || m.Phase != from {
				t.Errorf("%s -> %s accepted (err=%v, phase=%s)", from, to, err, m.Phase)
			}
		}
	}
}

// killSeat hard-drops pieces for one seat until it tops out.
func killSeat(t *testing.T, m *Match, seat int, seq *uint64, now time.Time) {
	t.Helper()
	for i := 0; i < 500 && m.Phase == Playing; i++ {
		*seq++
		m.Input(seat, *seq, game.HardDrop, now)
	}
	if m.Phase != RoundEnd {
		t.Fatalf("seat %d never topped out; phase = %s", seat, m.Phase)
	}
}

func TestFullMatchFlow(t *testing.T) {
	cfg := testConfig()
	m, _, sinks, now := newPlaying(t, cfg)
	var seq uint64

	killSeat(t, m, 1, &seq, now)
	if m.Seats[0].Wins != 1 || m.Round != 1 {
		t.Fatalf("after round 1: wins=%d round=%d", m.Seats[0].Wins, m.Round)
	}
	m.Advance(now.Add(cfg.RoundEndPause - time.Millisecond))
	if m.Phase != RoundEnd {
		t.Fatalf("round-end pause ended early: %s", m.Phase)
	}
	now = now.Add(cfg.RoundEndPause)
	m.Advance(now)
	if m.Phase != Countdown || m.Round != 2 {
		t.Fatalf("phase=%s round=%d, want countdown round 2", m.Phase, m.Round)
	}
	if m.Seats[1].Board.Dead || m.Seats[1].Board.StackHeight() != 0 {
		t.Fatal("boards were not reset for round 2")
	}
	now = now.Add(cfg.Countdown)
	m.Advance(now)
	killSeat(t, m, 1, &seq, now)
	now = now.Add(cfg.RoundEndPause)
	m.Advance(now)

	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "rounds" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
	if err := m.Transition(Countdown); err == nil {
		t.Fatal("MatchEnd is terminal but accepted a transition")
	}
	// Both clients saw the same ordered, gap-free event stream ending in match_over.
	for i, s := range sinks {
		ev := s.events()
		for j := 1; j < len(ev); j++ {
			if ev[j].Seq != ev[j-1].Seq+1 {
				t.Fatalf("seat %d: event seq gap %d -> %d", i, ev[j-1].Seq, ev[j].Seq)
			}
		}
		if last := ev[len(ev)-1]; last.Kind != protocol.EvMatchOver || *last.Winner != 0 {
			t.Fatalf("seat %d last event = %+v", i, last)
		}
	}
}

func TestInputIdempotency(t *testing.T) {
	m, _, _, now := newPlaying(t, testConfig())
	steps := []struct {
		seq  uint64
		want InputResult
	}{
		{1, InputApplied},
		{1, InputDuplicate}, // retransmit
		{0, InputDuplicate},
		{3, InputApplied},   // gaps are fine (client may drop its own inputs)
		{2, InputDuplicate}, // reordered/stale: older than what we already applied
		{4, InputApplied},
	}
	for _, s := range steps {
		x := m.Seats[0].Board.Piece.X
		got := m.Input(0, s.seq, game.Left, now)
		if got != s.want {
			t.Fatalf("seq %d: got %s, want %s", s.seq, got, s.want)
		}
		moved := m.Seats[0].Board.Piece.X != x
		if moved != (got == InputApplied) && x > 0 {
			t.Fatalf("seq %d: moved=%v but result %s", s.seq, moved, got)
		}
	}
	if m.Seats[0].LastSeq != 4 {
		t.Fatalf("LastSeq = %d", m.Seats[0].LastSeq)
	}
	if got := m.Input(0, 5, game.Action("teleport"), now); got != InputInvalid {
		t.Fatalf("unknown action: %s", got)
	}
	if st := m.StateFor(0, now); st.Ack != 4 {
		t.Fatalf("ack = %d, want 4", st.Ack)
	}
}

func TestInputsOutsidePlayingAreConsumedNotApplied(t *testing.T) {
	cfg := testConfig()
	m, _, _ := newConnected(t, cfg)
	m.Advance(t0) // countdown
	x := m.Seats[0].Board.Piece.X
	if got := m.Input(0, 1, game.Left, t0); got != InputNotPlaying {
		t.Fatalf("countdown input: %s", got)
	}
	if m.Seats[0].Board.Piece.X != x {
		t.Fatal("countdown input moved the piece")
	}
	m.Advance(t0.Add(cfg.Countdown))
	// The same packet retransmitted after the round started must not apply now.
	if got := m.Input(0, 1, game.Left, t0.Add(cfg.Countdown)); got != InputDuplicate {
		t.Fatalf("retransmit after countdown: %s", got)
	}
}

func TestGravityTimer(t *testing.T) {
	cfg := testConfig()
	m, _, _, now := newPlaying(t, cfg)
	y := m.Seats[0].Board.Piece.Y
	m.Advance(now.Add(cfg.Gravity - time.Millisecond))
	if m.Seats[0].Board.Piece.Y != y {
		t.Fatal("piece fell before the gravity interval")
	}
	m.Advance(now.Add(cfg.Gravity))
	if got := m.Seats[0].Board.Piece.Y; got != y+1 {
		t.Fatalf("after one interval Y=%d, want %d", got, y+1)
	}
	m.Advance(now.Add(3 * cfg.Gravity)) // a late tick catches up
	if got := m.Seats[1].Board.Piece.Y; got != y+3 {
		t.Fatalf("after catch-up Y=%d, want %d", got, y+3)
	}
}

func TestRoundTimerDecidesByScore(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = time.Hour
	m, _, _, now := newPlaying(t, cfg)
	m.Seats[1].Board.Score = 300
	m.Advance(now.Add(cfg.RoundTime - time.Millisecond))
	if m.Phase != Playing {
		t.Fatalf("round ended early: %s", m.Phase)
	}
	m.Advance(now.Add(cfg.RoundTime))
	if m.Phase != RoundEnd || m.Seats[1].Wins != 1 {
		t.Fatalf("phase=%s wins=%d", m.Phase, m.Seats[1].Wins)
	}
	ev := m.Events[len(m.Events)-1]
	if ev.Kind != protocol.EvRoundOver || ev.Reason != "time_up" || *ev.Winner != 1 {
		t.Fatalf("last event %+v", ev)
	}
}

func TestJoinTimeout(t *testing.T) {
	cfg := testConfig()
	m, tokens := New("m", cfg, [2]string{"a", "b"}, 1, t0)
	if _, err := m.Attach(tokens[0], &fakeSink{}, 0, t0); err != nil {
		t.Fatal(err)
	}
	m.Advance(t0.Add(cfg.JoinTimeout - time.Millisecond))
	if m.Phase != Waiting {
		t.Fatalf("phase = %s", m.Phase)
	}
	m.Advance(t0.Add(cfg.JoinTimeout))
	if m.Phase != MatchEnd || m.Winner != NoSeat || m.EndReason != "opponent_never_joined" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
}

func TestIdleTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.Gravity = time.Hour // keep gravity out of the picture
	m, _, _, now := newPlaying(t, cfg)
	m.Input(0, 1, game.Rotate, now.Add(cfg.IdleTimeout/2)) // seat 0 is active
	m.Advance(now.Add(cfg.IdleTimeout - time.Millisecond))
	if m.Phase != Playing {
		t.Fatalf("idle fired early: %s", m.Phase)
	}
	m.Advance(now.Add(cfg.IdleTimeout))
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_idle" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
}

// A player who does not come back within the grace forfeits the match, exactly when it
// runs out, while the opponent plays on tick by tick.
func TestReconnectGraceExpires(t *testing.T) {
	cfg := testConfig()
	m, _, sinks, start := newPlaying(t, cfg)
	m.Detach(1, sinks[1], start)
	deadline := start.Add(cfg.ReconnectGrace)
	var seq uint64
	for now := start; now.Before(deadline); now = now.Add(cfg.TickInterval) {
		m.Advance(now)
		if m.Phase != Playing {
			t.Fatalf("after %s away: phase=%s reason=%s, want playing until the grace runs out", now.Sub(start), m.Phase, m.EndReason)
		}
		if now.Sub(start)%time.Second == 0 {
			seq++
			if got := m.Input(0, seq, game.Rotate, now); got != InputApplied {
				t.Fatalf("opponent input at %s: %s", now.Sub(start), got)
			}
		}
	}
	m.Advance(deadline)
	if m.Phase != MatchEnd || m.Winner != 0 || m.EndReason != "opponent_disconnected" {
		t.Fatalf("phase=%s winner=%d reason=%s", m.Phase, m.Winner, m.EndReason)
	}
	evs := sinks[0].events()
	if last := evs[len(evs)-1]; last.Kind != protocol.EvMatchOver || *last.Winner != 0 || last.Reason != "opponent_disconnected" {
		t.Fatalf("opponent's last event = %+v", last)
	}
}

func TestNewerConnectionReplacesOld(t *testing.T) {
	m, tokens, sinks, now := newPlaying(t, testConfig())
	fresh := &fakeSink{}
	if _, err := m.Attach(tokens[0], fresh, 0, now); err != nil {
		t.Fatal(err)
	}
	if sinks[0].closed == "" {
		t.Fatal("old connection was not closed")
	}
	m.Detach(0, sinks[0], now) // the old socket's reader exits late
	if !m.Seats[0].Connected {
		t.Fatal("stale detach disconnected the new connection")
	}
	if _, err := m.Attach("nope", &fakeSink{}, 0, now); !errors.Is(err, ErrBadToken) {
		t.Fatalf("bad token: %v", err)
	}
}

func TestEventLogIsBounded(t *testing.T) {
	cfg := testConfig()
	cfg.EventLogSize = 4
	m, _ := New("m", cfg, [2]string{"a", "b"}, 1, t0)
	for i := 0; i < 10; i++ {
		m.emit(t0, protocol.Event{Kind: protocol.EvGarbage})
	}
	if len(m.Events) != 4 || m.Events[0].Seq != 7 {
		t.Fatalf("log = %d events starting at %d", len(m.Events), m.Events[0].Seq)
	}
	if ev, complete := m.EventsSince(2); complete || len(ev) != 4 {
		t.Fatalf("since 2: complete=%v n=%d", complete, len(ev))
	}
	if ev, complete := m.EventsSince(8); !complete || len(ev) != 2 {
		t.Fatalf("since 8: complete=%v n=%d", complete, len(ev))
	}
	if ev, complete := m.EventsSince(10); !complete || len(ev) != 0 {
		t.Fatalf("since 10: complete=%v n=%d", complete, len(ev))
	}
}

func TestSnapshotRestore(t *testing.T) {
	cfg := testConfig()
	m, tokens, _, now := newPlaying(t, cfg)
	for seq := uint64(1); seq <= 5; seq++ {
		m.Input(0, seq, game.HardDrop, now)
	}
	data, err := m.Snapshot(now)
	if err != nil {
		t.Fatal(err)
	}

	downtime := time.Minute
	later := now.Add(downtime)
	r, err := Restore(data, cfg, later)
	if err != nil {
		t.Fatal(err)
	}
	// A periodic (non-final) snapshot: the counters skip ahead so numbers a client may
	// have seen after it are never reused (see TestRestoreFromCrashSnapshotNeverReusesEventNumbers).
	if r.Phase != Playing || r.Round != 1 || r.EventSeq != m.EventSeq+rewindGap {
		t.Fatalf("restored phase=%s round=%d eventSeq=%d", r.Phase, r.Round, r.EventSeq)
	}
	if got := r.PhaseEnds.Sub(m.PhaseEnds); got != downtime {
		t.Fatalf("deadline shifted by %v, want %v", got, downtime)
	}
	if r.Seats[0].Connected || r.Seats[1].Connected {
		t.Fatal("restored players should start disconnected")
	}
	if r.Seats[0].Board.Grid != m.Seats[0].Board.Grid || r.Seats[0].Board.Piece != m.Seats[0].Board.Piece {
		t.Fatal("board not restored")
	}
	// Idempotency survives the restart: the client retransmits its last input.
	if got := r.Input(0, 5, game.HardDrop, later); got != InputDuplicate {
		t.Fatalf("retransmit after restore: %s", got)
	}

	// The restored match waits for its players (ResyncWait), then runs from where it stood.
	y := r.Seats[1].Board.Piece.Y
	back := later.Add(cfg.Gravity * 2)
	r.Advance(back)
	if !r.StateFor(0, back).Paused || r.Seats[1].Board.Piece.Y != y {
		t.Fatalf("restored match ran before its players were back: piece y %d -> %d", y, r.Seats[1].Board.Piece.Y)
	}
	for i := range tokens {
		if _, err := r.Attach(tokens[i], &fakeSink{}, 0, back); err != nil {
			t.Fatal(err)
		}
	}
	r.Advance(back)
	next := back.Add(cfg.Gravity)
	r.Advance(next)
	if r.Phase != Playing || r.StateFor(0, next).Paused || r.Seats[1].Board.Piece.Y != y+1 {
		t.Fatalf("restored match: phase=%s piece y %d -> %d, want it falling", r.Phase, y, r.Seats[1].Board.Piece.Y)
	}
	if r.ConnectedCount() != 2 || r.Input(0, 6, game.Left, next) != InputApplied {
		t.Fatal("restored match does not take the players' inputs")
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	if _, err := Restore([]byte(`{"version":99}`), testConfig(), t0); err == nil {
		t.Fatal("accepted unknown version")
	}
	if _, err := Restore([]byte(`not json`), testConfig(), t0); err == nil {
		t.Fatal("accepted invalid json")
	}
}
