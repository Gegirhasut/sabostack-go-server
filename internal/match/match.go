// Package match holds the round/match state machine and the actor that runs it.
//
// Match is a plain, synchronous struct: every method takes "now" explicitly and nothing in
// it is safe for concurrent use. Concurrency lives one level up, in Actor, which owns a
// Match inside a single goroutine and feeds it commands and clock ticks over channels.
package match

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

// Config holds the match timings and rules.
type Config struct {
	TickInterval  time.Duration // server tick: timers are checked and state is broadcast
	Gravity       time.Duration // the piece falls one row per Gravity
	Countdown     time.Duration
	RoundTime     time.Duration // round timer; on expiry the higher score wins
	RoundEndPause time.Duration
	JoinTimeout   time.Duration // both players must connect within this after matchmaking
	// ReconnectGrace: a disconnected player who is not back within this forfeits. The
	// match does not wait for them meanwhile: their board keeps falling, the round timer
	// keeps running, and the opponent keeps playing.
	ReconnectGrace time.Duration
	// IdleTimeout: a connected player who sends no input for this long while Playing
	// forfeits. Only connected time counts; time away is the grace's business.
	IdleTimeout time.Duration
	// SilenceTimeout: a player whose connection the server has heard nothing from (no
	// message, no pong) for this long is marked away: the reconnect grace starts and the
	// idle clock stops. The socket stays attached, and once it has been heard from
	// steadily for SilenceTimeout/2 (no gap of SilenceTimeout/2 or more) the seat is back.
	// The steady part is the hysteresis: a connection that keeps stalling stays away
	// instead of flapping away/back. 0 disables silence detection (only a closed socket
	// counts as gone). Needs a sink that implements Heard.
	SilenceTimeout time.Duration
	// ResyncWait: a match restored after a restart or taken over from a dead node stands
	// still (paused) until both players are back or this passes, so nobody loses round
	// time to the server's failure while the clients find the new node. Each player's
	// reconnect grace still runs from the restore. 0 = no wait.
	ResyncWait time.Duration
	// LockDelay: a piece that comes to rest locks this long later, not on the gravity
	// step that found it resting, so a player 100–150 ms away still has time to slide or
	// turn it along the floor. A move or turn of the resting piece restarts the delay, at
	// most game.MaxLockResets times per piece (more after it reaches a new lowest row).
	// Nothing else restarts it: a turn that lifts the piece leaves the timer running, and
	// a piece still in the air when it runs out drops and locks LockDelay later at the
	// latest. 0 = lock on contact.
	LockDelay      time.Duration
	WinsNeeded     int // rounds needed to win the match
	MaxRounds      int
	EventLogSize   int // events kept for reconnect catch-up
	SnapshotEvents int // newest events kept in a persisted snapshot (0 = all of the log)
}

// DefaultConfig is used by the server unless overridden by flags.
func DefaultConfig() Config {
	return Config{
		TickInterval:   50 * time.Millisecond,
		Gravity:        500 * time.Millisecond,
		Countdown:      3 * time.Second,
		RoundTime:      90 * time.Second,
		RoundEndPause:  3 * time.Second,
		JoinTimeout:    15 * time.Second,
		ReconnectGrace: 20 * time.Second,
		IdleTimeout:    30 * time.Second,
		SilenceTimeout: 8 * time.Second,
		ResyncWait:     10 * time.Second,
		LockDelay:      500 * time.Millisecond,
		WinsNeeded:     2,
		MaxRounds:      3,
		EventLogSize:   256,
		SnapshotEvents: 32,
	}
}

// Sink receives messages for one connected player. Send must not block: it returns false
// if the client cannot keep up, and the connection layer is expected to drop it.
type Sink interface {
	Send(msg any) bool
	Close(reason string)
}

// Heard is implemented by a sink that knows when its client last sent anything: a
// message or a pong to the server's ping. The match polls it on every tick to notice a
// connection that went silent (Config.SilenceTimeout).
type Heard interface {
	LastHeard() time.Time
}

// InputResult says what happened to a player input.
type InputResult int

const (
	InputApplied    InputResult = iota
	InputDuplicate              // seq <= last consumed seq: retransmit or reordered packet
	InputNotPlaying             // valid but arrived outside the Playing phase
	InputInvalid                // unknown action
)

func (r InputResult) String() string {
	return [...]string{"applied", "duplicate", "not_playing", "invalid"}[r]
}

// Errors returned by Attach.
var (
	ErrBadToken   = errors.New("match: invalid session token")
	ErrMatchEnded = errors.New("match: match has ended")
)

// NoSeat marks "no winner" (draw or abort).
const NoSeat = -1

// Seat is one player's slot. Everything except the sink is persisted in snapshots.
type Seat struct {
	Name      string      `json:"name"`
	TokenHash string      `json:"tokenHash"` // sha256 of the session token; the token itself is never stored
	Connected bool        `json:"connected"` // false while away: no connection, or a silent one
	LeftAt    time.Time   `json:"leftAt"`    // when the player went away (the grace runs from it); zero while connected
	LastInput time.Time   `json:"lastInput"` // idle clock: last applied input, moved past the time the clock stood
	IdleStop  time.Time   `json:"idleStop"`  // since when the idle clock stands (away, or the match resyncing); zero while it runs
	LastSeq   uint64      `json:"lastSeq"`   // highest input seq consumed (idempotency)
	Wins      int         `json:"wins"`
	Board     *game.Board `json:"board"`
	LockAt    time.Time   `json:"lockAt"` // when the resting piece locks (Config.LockDelay); zero while it falls

	sink       Sink
	rewound    bool      // restored from a non-final snapshot; cleared by the next Attach
	heardSince time.Time // start of the current steady run of hearing from sink (silence hysteresis)
}

// Match is the full state of one 1v1 match. Exported fields form the snapshot.
type Match struct {
	ID          string           `json:"id"`
	Seed        uint64           `json:"seed"`
	Phase       Phase            `json:"phase"`
	Round       int              `json:"round"`
	Seats       [2]*Seat         `json:"seats"`
	PhaseEnds   time.Time        `json:"phaseEnds"`
	NextGravity time.Time        `json:"nextGravity"`
	Tick        uint64           `json:"tick"`
	EventSeq    uint64           `json:"eventSeq"`
	Events      []protocol.Event `json:"events"`
	Winner      int              `json:"winner"`
	EndReason   string           `json:"endReason"`
	EndedAt     time.Time        `json:"endedAt"`
	// ResyncFrom and ResyncUntil bound the stand-still after a restore (Config.ResyncWait);
	// both are zero while the match runs.
	ResyncFrom  time.Time `json:"resyncFrom"`
	ResyncUntil time.Time `json:"resyncUntil"`

	cfg   Config
	dirty bool   // state changed since the last broadcast
	rev   uint64 // bumped on every change; lets the actor skip identical snapshots
}

// touch marks the state as changed (for the next broadcast and the next snapshot).
func (m *Match) touch() {
	m.dirty = true
	m.rev++
}

// Rev is a counter that changes whenever the match state changes.
func (m *Match) Rev() uint64 { return m.rev }

// New creates a match in the Waiting phase and returns the session tokens for both seats.
func New(id string, cfg Config, names [2]string, seed uint64, now time.Time) (*Match, [2]string) {
	tokens := [2]string{NewToken(), NewToken()}
	return NewWithTokenHashes(id, cfg, names, [2]string{HashToken(tokens[0]), HashToken(tokens[1])}, seed, now), tokens
}

// NewWithTokenHashes creates a match whose players hold tokens this process never sees:
// only their hashes (HashToken). In a cluster the token is made by the node the player is
// connected to and never leaves it; the hosting node gets the hash.
func NewWithTokenHashes(id string, cfg Config, names, tokenHashes [2]string, seed uint64, now time.Time) *Match {
	m := &Match{ID: id, Seed: seed, Phase: Waiting, Winner: NoSeat, cfg: cfg, PhaseEnds: now.Add(cfg.JoinTimeout)}
	for i := range m.Seats {
		m.Seats[i] = &Seat{Name: names[i], TokenHash: tokenHashes[i]}
	}
	m.newBoards()
	return m
}

// SetConfig replaces the (unexported, non-persisted) config, e.g. after a restore.
func (m *Match) SetConfig(cfg Config) { m.cfg = cfg }

// Attach connects sink to the seat that owns token and sends it a snapshot plus the
// events after lastEventSeq. A previous connection for the same seat is closed (the
// newest connection wins, which handles "reconnected before the old socket timed out").
func (m *Match) Attach(token string, sink Sink, lastEventSeq uint64, now time.Time) (int, error) {
	seat := m.seatByToken(token)
	if seat < 0 {
		return NoSeat, ErrBadToken
	}
	s := m.Seats[seat]
	if s.sink != nil && s.sink != sink {
		s.sink.Close("replaced by a newer connection")
	}
	// The new sink is attached only after its snapshot is sent: player_back below is
	// delivered inside the snapshot, never as a separate event ahead of it (or twice).
	s.sink, s.heardSince = nil, time.Time{}
	if !s.Connected {
		m.back(seat, now)
	}
	events, complete := m.EventsSince(lastEventSeq)
	if s.rewound {
		complete, s.rewound = false, false
	}
	sink.Send(protocol.Snapshot{
		Type: protocol.TypeSnapshot, MatchID: m.ID, Seat: seat,
		State: m.StateFor(seat, now), Events: events, EventsComplete: complete,
	})
	s.sink = sink
	m.touch()
	if m.Phase == MatchEnd {
		return seat, ErrMatchEnded // the snapshot still told the client the result
	}
	return seat, nil
}

// away marks a connected seat away: its grace starts and its idle clock stops.
func (m *Match) away(seat int, now time.Time) {
	s := m.Seats[seat]
	s.Connected, s.LeftAt = false, now
	if s.IdleStop.IsZero() {
		s.IdleStop = now
	}
	if m.Phase != MatchEnd {
		m.emit(now, protocol.Event{Kind: protocol.EvPlayerLeft, Seat: intp(seat)})
	}
}

// back marks an away seat connected again.
func (m *Match) back(seat int, now time.Time) {
	s := m.Seats[seat]
	s.Connected, s.LeftAt = true, time.Time{}
	if !m.resyncing() {
		m.runIdle(s, now) // while resyncing the clock stands for everyone; resync restarts it
	}
	if m.Phase != Waiting && m.Phase != MatchEnd {
		m.emit(now, protocol.Event{Kind: protocol.EvPlayerBack, Seat: intp(seat)})
	}
}

// runIdle restarts a seat's stopped idle clock. Time away is not idle time (the
// reconnect grace covered it), nor is time the match stood still, so LastInput moves
// forward by exactly the stretch the clock stood after it. Never to "now": a reset gave
// a player who never moved a fresh idle window per reconnect. Replacing a live
// connection (not away) leaves the clock alone for the same reason.
func (m *Match) runIdle(s *Seat, now time.Time) {
	if s.IdleStop.IsZero() {
		return
	}
	if d := now.Sub(later(s.IdleStop, s.LastInput)); d > 0 {
		s.LastInput = s.LastInput.Add(d)
	}
	s.IdleStop = time.Time{}
}

// resyncing reports whether the match stands still after a restore (Config.ResyncWait).
func (m *Match) resyncing() bool { return !m.ResyncFrom.IsZero() }

// resync ends the stand-still after a restore once both players are back or
// ResyncUntil has passed, and reports whether the match runs. Every game clock (phase
// deadline, gravity, the idle clocks of the players present) moves forward by the time
// it stood, so the restart costs nobody round time. An absent player's grace is not
// moved: it has run since the restore.
func (m *Match) resync(now time.Time) bool {
	if now.Before(m.ResyncUntil) && (!m.Seats[0].Connected || !m.Seats[1].Connected) {
		return false
	}
	stood := now.Sub(m.ResyncFrom)
	shiftTime(&m.PhaseEnds, stood)
	shiftTime(&m.NextGravity, stood)
	m.ResyncFrom, m.ResyncUntil = time.Time{}, time.Time{}
	for _, s := range m.Seats {
		shiftTime(&s.LockAt, stood)
		if s.Connected {
			m.runIdle(s, now)
		}
	}
	m.emit(now, protocol.Event{Kind: protocol.EvResumed})
	return true
}

// watchSilence marks away a seat whose connection has been silent for SilenceTimeout,
// and back one that has been heard from steadily again (see Config.SilenceTimeout). It
// runs in the actor on every tick, so it is ordered with the inputs, and it only looks
// at the seat's current connection: a socket that closed went through Detach instead.
func (m *Match) watchSilence(now time.Time) {
	limit := m.cfg.SilenceTimeout
	if limit <= 0 || m.Phase == MatchEnd {
		return
	}
	for i, s := range m.Seats {
		h, ok := s.sink.(Heard)
		if !ok {
			continue
		}
		last := h.LastHeard()
		quiet := now.Sub(last)
		if quiet >= limit/2 {
			s.heardSince = time.Time{} // a gap: the steady run starts over
		} else if s.heardSince.IsZero() {
			s.heardSince = last
		}
		switch {
		case s.Connected && quiet >= limit:
			m.away(i, now)
			// The grace runs from now (the player gets the benefit of the doubt), but the
			// idle clock stopped when the connection did: the silence is not idle time.
			if last.Before(s.IdleStop) {
				s.IdleStop = last
			}
			m.touch()
		case !s.Connected && !s.heardSince.IsZero() && now.Sub(s.heardSince) >= limit/2:
			m.back(i, now)
			m.touch()
		}
	}
}

// Detach marks the seat disconnected if sink is still its current connection. A stale
// connection closing after it was replaced is ignored, and so is one whose seat already
// went silent: its reconnect grace has been running since then.
func (m *Match) Detach(seat int, sink Sink, now time.Time) {
	s := m.Seats[seat]
	if sink == nil || s.sink != sink {
		return
	}
	s.sink = nil
	if s.Connected {
		m.away(seat, now)
	}
	m.touch()
}

// Input applies a player action. seq makes inputs idempotent: anything at or below the
// last consumed seq is dropped. A seq is consumed even when the action is ignored because
// of the phase, so a retransmit can never be applied later in a different round. The
// same holds while a restored match resyncs: the input is acked and dropped, as in
// Countdown. Were it left unacked, the client would resend everything the player pressed
// at the frozen board and it would all apply at once on the first tick after resumed.
//
// An input is proof the player is there: one from a seat marked away by silence brings
// it back at once, without the steady-contact wait that pongs need (see watchSilence).
func (m *Match) Input(seat int, seq uint64, a game.Action, now time.Time) InputResult {
	s := m.Seats[seat]
	if seq <= s.LastSeq {
		return InputDuplicate
	}
	if !a.Valid() {
		return InputInvalid
	}
	if !s.Connected && s.sink != nil { // silent, socket still attached
		m.back(seat, now)
		m.touch()
	}
	s.LastSeq = seq
	m.touch() // the ack changed even if nothing else did
	if m.Phase != Playing || m.resyncing() {
		return InputNotPlaying
	}
	s.LastInput = now
	m.handle(seat, s.Board.Apply(a), now)
	return InputApplied
}

// Advance runs all timers up to now. The actor calls it once per server tick.
//
// A player's disconnect never pauses the match. Their board keeps falling (pieces lock,
// incoming garbage rises, a top-out loses the round), the round timer keeps running and
// the opponent keeps playing. The disconnected player may resume within ReconnectGrace
// and carries on from whatever their board is by then. The one stand-still is the
// server's own: after a restore the match waits up to ResyncWait for both players.
func (m *Match) Advance(now time.Time) {
	m.Tick++
	m.watchSilence(now)
	if m.resyncing() && !m.resync(now) {
		return
	}
	switch m.Phase {
	case Waiting:
		if m.Seats[0].Connected && m.Seats[1].Connected {
			m.startCountdown(now)
		} else if !now.Before(m.PhaseEnds) {
			m.finish(now, NoSeat, "opponent_never_joined")
		}
	case Countdown:
		if m.forfeit(now) {
			return
		}
		if !now.Before(m.PhaseEnds) {
			m.startRound(now)
		}
	case Playing:
		if m.forfeit(now) {
			return
		}
		m.play(now)
	case RoundEnd:
		// Once the rounds played decide the match, the result stands: a player who drops
		// in the break before it is announced cannot lose a match they have already won.
		if _, _, decided := m.outcome(); !decided && m.forfeit(now) {
			return
		}
		if !now.Before(m.PhaseEnds) {
			m.afterRound(now)
		}
	}
}

// forfeit ends the match if a player has gone: disconnected for ReconnectGrace, or (in
// Playing) connected without an input for IdleTimeout. A seat is either connected or
// not, so exactly one of the two clocks applies to it at any moment; they cannot both
// fire for one player. If both players go on the same tick nobody wins. It reports
// whether the match ended.
func (m *Match) forfeit(now time.Time) bool {
	var gone, away [2]bool
	for i, s := range m.Seats {
		away[i] = !s.Connected
		switch {
		case away[i]:
			gone[i] = now.Sub(s.LeftAt) >= m.cfg.ReconnectGrace
		case m.Phase == Playing:
			gone[i] = m.heardUntil(s, now).Sub(s.LastInput) >= m.cfg.IdleTimeout
		}
	}
	switch {
	case gone[0] && gone[1]: // nobody is playing: no winner (seat 0 used to lose by default)
		switch {
		case away[0] && away[1]:
			m.finish(now, NoSeat, "both_disconnected")
		case !away[0] && !away[1]:
			m.finish(now, NoSeat, "both_idle")
		default:
			m.finish(now, NoSeat, "disconnected_and_idle")
		}
	case gone[0]:
		m.finish(now, 1, forfeitReason(away[0]))
	case gone[1]:
		m.finish(now, 0, forfeitReason(away[1]))
	default:
		return false
	}
	return true
}

// heardUntil is how far the idle clock of a connected seat may count: to now, unless its
// connection has been quiet for SilenceTimeout/2 or more, and then only to when it was
// last heard. Such a connection may have dropped without the silence timeout having
// caught it yet; without this a player who thought for 25 s and then lost the network
// would lose by idle a few seconds before being marked away. A live client answers the
// server's pings every couple of seconds, so for it this is always now.
func (m *Match) heardUntil(s *Seat, now time.Time) time.Time {
	h, ok := s.sink.(Heard)
	if !ok || m.cfg.SilenceTimeout <= 0 {
		return now
	}
	if last := h.LastHeard(); now.Sub(last) >= m.cfg.SilenceTimeout/2 {
		return last
	}
	return now
}

func forfeitReason(away bool) string {
	if away {
		return "opponent_disconnected"
	}
	return "opponent_idle"
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// play runs gravity and the lock delays on both boards, whether their players are
// connected or not, and the round timer.
func (m *Match) play(now time.Time) {
	// Gravity steps and locks run in the order they fall due, catching up if a tick ran
	// late, but never looping unboundedly.
	for steps := 0; m.Phase == Playing && steps < 20; steps++ {
		at, lock := m.NextGravity, NoSeat
		for i, s := range m.Seats {
			if s.LockAt.IsZero() {
				continue
			}
			due := s.LockAt
			if !s.Board.Grounded() {
				// A turn lifted it: it locks as soon as it lands, and if a player keeps it
				// in the air past one more delay, it is dropped.
				due = due.Add(m.cfg.LockDelay)
			}
			if due.Before(at) {
				at, lock = due, i
			}
		}
		if now.Before(at) {
			break
		}
		if lock != NoSeat {
			m.handle(lock, m.Seats[lock].Board.Apply(game.HardDrop), now) // a resting piece drops 0 rows
			continue
		}
		m.NextGravity = m.NextGravity.Add(m.cfg.Gravity)
		for i, s := range m.Seats {
			m.handle(i, s.Board.Gravity(), now)
			if m.Phase != Playing {
				return
			}
		}
	}
	if m.Phase != Playing {
		return
	}
	if !now.Before(m.PhaseEnds) {
		a, b := m.Seats[0].Board, m.Seats[1].Board
		switch {
		case a.Score > b.Score:
			m.endRound(now, 0, "time_up")
		case b.Score > a.Score:
			m.endRound(now, 1, "time_up")
		default:
			m.endRound(now, NoSeat, "time_up")
		}
	}
}

// handle turns a board result into attacks, events and round decisions.
func (m *Match) handle(seat int, r game.Result, now time.Time) {
	if r.Moved || r.Locked {
		m.touch()
	}
	m.settle(seat, r, now)
	me, opp := m.Seats[seat].Board, m.Seats[1-seat].Board
	if r.Cleared > 0 {
		attack := game.GarbageFor(r.Cleared)
		cancel := min(attack, me.Pending) // clearing first cancels your own incoming garbage
		me.Pending -= cancel
		opp.Pending += attack - cancel
		m.emit(now, protocol.Event{Kind: protocol.EvCleared, Seat: intp(seat), Lines: r.Cleared, Garbage: attack - cancel})
	}
	if r.Garbage > 0 {
		m.emit(now, protocol.Event{Kind: protocol.EvGarbage, Seat: intp(seat), Garbage: r.Garbage})
	}
	if r.Died {
		m.endRound(now, 1-seat, "top_out")
	}
}

func (m *Match) startCountdown(now time.Time) {
	m.transition(Countdown)
	m.Round++
	if m.Round > 1 {
		m.newBoards()
	}
	m.PhaseEnds = now.Add(m.cfg.Countdown)
	m.emit(now, protocol.Event{Kind: protocol.EvPhase, Phase: Countdown.String(), Round: m.Round, DurationMs: m.cfg.Countdown.Milliseconds()})
}

func (m *Match) startRound(now time.Time) {
	m.transition(Playing)
	m.PhaseEnds = now.Add(m.cfg.RoundTime)
	m.NextGravity = now.Add(m.cfg.Gravity)
	for _, s := range m.Seats {
		s.LastInput = now
	}
	m.emit(now, protocol.Event{Kind: protocol.EvPhase, Phase: Playing.String(), Round: m.Round, DurationMs: m.cfg.RoundTime.Milliseconds()})
}

func (m *Match) endRound(now time.Time, winner int, reason string) {
	m.transition(RoundEnd)
	if winner != NoSeat {
		m.Seats[winner].Wins++
	}
	m.PhaseEnds = now.Add(m.cfg.RoundEndPause)
	m.emit(now, protocol.Event{Kind: protocol.EvRoundOver, Round: m.Round, Winner: intp(winner), Reason: reason})
}

func (m *Match) afterRound(now time.Time) {
	if winner, reason, decided := m.outcome(); decided {
		m.finish(now, winner, reason)
		return
	}
	m.startCountdown(now)
}

// outcome reports the match result if the rounds played so far already decide it.
func (m *Match) outcome() (winner int, reason string, decided bool) {
	w0, w1 := m.Seats[0].Wins, m.Seats[1].Wins
	switch {
	case w0 >= m.cfg.WinsNeeded:
		return 0, "rounds", true
	case w1 >= m.cfg.WinsNeeded:
		return 1, "rounds", true
	case m.Round >= m.cfg.MaxRounds && w0 > w1:
		return 0, "rounds", true
	case m.Round >= m.cfg.MaxRounds && w1 > w0:
		return 1, "rounds", true
	case m.Round >= m.cfg.MaxRounds:
		return NoSeat, "draw", true
	}
	return NoSeat, "", false
}

func (m *Match) finish(now time.Time, winner int, reason string) {
	m.transition(MatchEnd)
	m.Winner, m.EndReason, m.EndedAt = winner, reason, now
	m.emit(now, protocol.Event{Kind: protocol.EvMatchOver, Winner: intp(winner), Reason: reason})
}

// transition enforces the state machine. An illegal edge is a programming error, so it
// panics; Transition is the checked variant for callers outside this file.
func (m *Match) transition(to Phase) {
	if err := m.Transition(to); err != nil {
		panic(err)
	}
}

// Transition moves to phase to if the edge is legal.
func (m *Match) Transition(to Phase) error {
	if !CanTransition(m.Phase, to) {
		return &ErrIllegalTransition{From: m.Phase, To: to}
	}
	m.Phase = to
	m.touch()
	return nil
}

func (m *Match) newBoards() {
	// Both seats get the same piece sequence per round; holes differ per seat.
	pieceSeed := m.Seed + uint64(m.Round)*0x9e3779b97f4a7c15
	for i, s := range m.Seats {
		s.Board = game.NewBoard(pieceSeed, pieceSeed^uint64(i+1)*0xbf58476d1ce4e5b9)
		s.LockAt = time.Time{}
	}
}

// settle runs the lock delay of a seat after anything happened to its piece. The timer
// starts when the piece first comes to rest and starts afresh only for a new piece or a
// new lowest row (game.Result.Lower); a move or turn of the resting piece restarts it
// within game.MaxLockResets (game.Result.Extend). Nothing else touches it. In particular
// a turn that lifts the piece off the floor does not stop it: clearing the timer there
// gave every landing on the same row a full new delay, and turning back and forth held
// a piece on the floor for ever (and a player ahead on points could sit out the round,
// since incoming garbage only rises when a piece locks).
func (m *Match) settle(seat int, r game.Result, now time.Time) {
	s := m.Seats[seat]
	b := s.Board
	switch {
	case b.Dead:
		s.LockAt = time.Time{}
	case r.Locked || r.Lower:
		s.LockAt = time.Time{}
		if b.Grounded() {
			s.LockAt = now.Add(m.cfg.LockDelay)
		}
	case r.Extend:
		s.LockAt = now.Add(m.cfg.LockDelay)
	case s.LockAt.IsZero() && b.Grounded():
		s.LockAt = now.Add(m.cfg.LockDelay)
	}
}

// emit appends an event to the bounded log and pushes it to connected players.
func (m *Match) emit(now time.Time, e protocol.Event) {
	m.EventSeq++
	e.Seq, e.AtMs = m.EventSeq, now.UnixMilli()
	m.Events = append(m.Events, e)
	if n := len(m.Events) - m.cfg.EventLogSize; n > 0 && m.cfg.EventLogSize > 0 {
		// Trim in place: no new allocation per event once the log is full.
		k := copy(m.Events, m.Events[n:])
		clear(m.Events[k:])
		m.Events = m.Events[:k]
	}
	m.touch()
	msg := protocol.EventMsg{Type: protocol.TypeEvent, Event: e}
	for _, s := range m.Seats {
		if s.sink != nil {
			s.sink.Send(msg)
		}
	}
}

// EventsSince returns events with Seq > after. complete is false if some were already
// evicted from the log.
func (m *Match) EventsSince(after uint64) (events []protocol.Event, complete bool) {
	events = []protocol.Event{}
	complete = after >= m.EventSeq || (len(m.Events) > 0 && m.Events[0].Seq <= after+1)
	for _, e := range m.Events {
		if e.Seq > after {
			events = append(events, e)
		}
	}
	return events, complete
}

// Broadcast sends the current state to connected players if anything changed since the
// last call. It is called once per tick, which batches all inputs of that tick.
func (m *Match) Broadcast(now time.Time) {
	if !m.dirty {
		return
	}
	m.dirty = false
	// The two recipients' states differ only in Ack, so the player views (two boards of
	// row strings) are built once and shared read-only by both messages.
	st := m.StateFor(NoSeat, now)
	for i, s := range m.Seats {
		if s.sink != nil {
			st.Ack = m.Seats[i].LastSeq
			s.sink.Send(st)
		}
	}
}

// StateFor builds the state message for seat (the ack is per recipient).
func (m *Match) StateFor(seat int, now time.Time) protocol.State {
	st := protocol.State{
		Type: protocol.TypeState, Tick: m.Tick, Phase: m.Phase.String(), Round: m.Round,
		EventSeq: m.EventSeq, Players: make([]protocol.PlayerView, 2),
	}
	if seat >= 0 {
		st.Ack = m.Seats[seat].LastSeq
	}
	if m.resyncing() {
		st.Paused = true
		now = m.ResyncFrom // the clocks stand still: the time left is what it was
	}
	if m.Phase != MatchEnd {
		st.RemainingMs = max(0, m.PhaseEnds.Sub(now).Milliseconds())
	}
	for i, s := range m.Seats {
		b := s.Board
		st.Players[i] = protocol.PlayerView{
			Name: s.Name, Connected: s.Connected, Wins: s.Wins, Rows: b.Grid.Rows(), Piece: b.Piece,
			Score: b.Score, Lines: b.Lines, Pending: b.Pending, Dead: b.Dead,
		}
	}
	return st
}

// CloseSinks disconnects every attached client (used on shutdown and after the match).
func (m *Match) CloseSinks(reason string) {
	for _, s := range m.Seats {
		if s.sink != nil {
			s.sink.Close(reason)
			s.sink = nil
		}
	}
}

// ConnectedCount is the number of attached players.
func (m *Match) ConnectedCount() int {
	n := 0
	for _, s := range m.Seats {
		if s.Connected {
			n++
		}
	}
	return n
}

func (m *Match) seatByToken(token string) int {
	h := []byte(HashToken(token))
	for i, s := range m.Seats {
		if subtle.ConstantTimeCompare(h, []byte(s.TokenHash)) == 1 {
			return i
		}
	}
	return NoSeat
}

// NewToken returns a new random session token.
func NewToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("match: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// HashToken is what a match stores (and compares) instead of a token.
func HashToken(t string) string {
	sum := sha256.Sum256([]byte(t))
	return hex.EncodeToString(sum[:])
}

func intp(i int) *int { return &i }
