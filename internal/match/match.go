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
	TickInterval   time.Duration // server tick: timers are checked and state is broadcast
	Gravity        time.Duration // the piece falls one row per Gravity
	Countdown      time.Duration
	RoundTime      time.Duration // round timer; on expiry the higher score wins
	RoundEndPause  time.Duration
	JoinTimeout    time.Duration // both players must connect within this after matchmaking
	ReconnectGrace time.Duration // a disconnected player may come back within this
	IdleTimeout    time.Duration // a connected player sending no input for this long forfeits
	// PauseBudget caps the total time one player may keep the match paused by being away
	// (summed over all disconnects). Without it a losing player could freeze the game
	// forever by reconnecting just before the grace runs out. 0 disables the cap.
	PauseBudget    time.Duration
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
		PauseBudget:    45 * time.Second,
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

// InputResult says what happened to a player input.
type InputResult int

const (
	InputApplied    InputResult = iota
	InputDuplicate              // seq <= last consumed seq: retransmit or reordered packet
	InputNotPlaying             // valid but arrived outside the Playing phase (or while paused)
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
	Name      string        `json:"name"`
	TokenHash string        `json:"tokenHash"` // sha256 of the session token; the token itself is never stored
	Connected bool          `json:"connected"`
	LeftAt    time.Time     `json:"leftAt"`
	LastInput time.Time     `json:"lastInput"`
	LastSeq   uint64        `json:"lastSeq"`  // highest input seq consumed (idempotency)
	AwayUsed  time.Duration `json:"awayUsed"` // pause time this player has caused so far
	Wins      int           `json:"wins"`
	Board     *game.Board   `json:"board"`

	sink    Sink
	rewound bool // restored from a non-final snapshot; cleared by the next Attach
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
	PausedAt    time.Time        `json:"pausedAt"` // zero when not paused
	Tick        uint64           `json:"tick"`
	EventSeq    uint64           `json:"eventSeq"`
	Events      []protocol.Event `json:"events"`
	Winner      int              `json:"winner"`
	EndReason   string           `json:"endReason"`
	EndedAt     time.Time        `json:"endedAt"`

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
	s.sink = nil
	wasAway := !s.Connected
	if wasAway && !m.PausedAt.IsZero() && (m.Phase == Countdown || m.Phase == Playing) {
		s.AwayUsed += now.Sub(later(s.LeftAt, m.PausedAt))
	}
	s.Connected, s.LeftAt = true, time.Time{}
	// LastInput is deliberately left alone. Time away is not idle time, but that is
	// already handled: being away pauses the match and the end of the pause shifts every
	// LastInput by the pause length (checkPause). Resetting it here as well gave a free
	// pause-length of idle credit per reconnect, and let a player who never moved dodge
	// the idle forfeit by opening a new socket every so often.
	if wasAway && m.Phase != Waiting && m.Phase != MatchEnd {
		m.emit(now, protocol.Event{Kind: protocol.EvPlayerBack, Seat: intp(seat)})
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

// Detach marks the seat disconnected if sink is still its current connection. A stale
// connection closing after it was replaced is ignored.
func (m *Match) Detach(seat int, sink Sink, now time.Time) {
	s := m.Seats[seat]
	if s.sink != sink || !s.Connected {
		return
	}
	s.sink, s.Connected, s.LeftAt = nil, false, now
	if m.Phase != MatchEnd {
		m.emit(now, protocol.Event{Kind: protocol.EvPlayerLeft, Seat: intp(seat)})
	}
	m.touch()
}

// Input applies a player action. seq makes inputs idempotent: anything at or below the
// last consumed seq is dropped. A seq is consumed even when the action is ignored because
// of the phase, so a retransmit can never be applied later in a different round.
func (m *Match) Input(seat int, seq uint64, a game.Action, now time.Time) InputResult {
	s := m.Seats[seat]
	if seq <= s.LastSeq {
		return InputDuplicate
	}
	if !a.Valid() {
		return InputInvalid
	}
	s.LastSeq = seq
	m.touch() // the ack changed even if nothing else did
	if m.Phase != Playing || !m.PausedAt.IsZero() {
		return InputNotPlaying
	}
	s.LastInput = now
	m.handle(seat, s.Board.Apply(a), now)
	return InputApplied
}

// Advance runs all timers up to now. The actor calls it once per server tick.
func (m *Match) Advance(now time.Time) {
	m.Tick++
	switch m.Phase {
	case Waiting:
		if m.Seats[0].Connected && m.Seats[1].Connected {
			m.startCountdown(now)
		} else if !now.Before(m.PhaseEnds) {
			m.finish(now, NoSeat, "opponent_never_joined")
		}
	case Countdown, Playing:
		if m.checkPause(now) {
			return
		}
		if m.Phase == Countdown {
			if !now.Before(m.PhaseEnds) {
				m.startRound(now)
			}
			return
		}
		m.play(now)
	case RoundEnd:
		if !now.Before(m.PhaseEnds) {
			m.afterRound(now)
		}
	}
}

// checkPause freezes Countdown/Playing while a player is disconnected (so a dropped
// connection is not a free loss) and forfeits whoever outstays the reconnect grace.
// It returns true while the match is paused.
func (m *Match) checkPause(now time.Time) bool {
	anyAway := false
	for _, s := range m.Seats {
		anyAway = anyAway || !s.Connected
	}
	if !anyAway {
		if !m.PausedAt.IsZero() {
			shift := now.Sub(m.PausedAt)
			m.PhaseEnds = m.PhaseEnds.Add(shift)
			m.NextGravity = m.NextGravity.Add(shift)
			for _, s := range m.Seats {
				s.LastInput = s.LastInput.Add(shift)
			}
			m.PausedAt = time.Time{}
			m.emit(now, protocol.Event{Kind: protocol.EvResumed})
		}
		return false
	}
	if m.PausedAt.IsZero() {
		m.PausedAt = now
		m.emit(now, protocol.Event{Kind: protocol.EvPaused})
	}
	var expired [2]bool
	for i, s := range m.Seats {
		if s.Connected {
			continue
		}
		overBudget := m.cfg.PauseBudget > 0 && s.AwayUsed+now.Sub(later(s.LeftAt, m.PausedAt)) >= m.cfg.PauseBudget
		expired[i] = overBudget || now.Sub(s.LeftAt) >= m.cfg.ReconnectGrace
	}
	switch {
	case expired[0] && expired[1]:
		m.finish(now, NoSeat, "both_disconnected")
	case expired[0]:
		m.finish(now, 1, "opponent_disconnected")
	case expired[1]:
		m.finish(now, 0, "opponent_disconnected")
	}
	return true
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (m *Match) play(now time.Time) {
	// Gravity catches up if a tick ran late, but never loops unboundedly.
	for steps := 0; !now.Before(m.NextGravity) && m.Phase == Playing && steps < 10; steps++ {
		m.NextGravity = m.NextGravity.Add(m.cfg.Gravity)
		for i, s := range m.Seats {
			m.handle(i, s.Board.Gravity(), now)
			if m.Phase != Playing {
				return
			}
		}
	}
	idle0 := now.Sub(m.Seats[0].LastInput) >= m.cfg.IdleTimeout
	idle1 := now.Sub(m.Seats[1].LastInput) >= m.cfg.IdleTimeout
	switch {
	case idle0 && idle1: // nobody is playing: no winner (seat 0 used to lose by default)
		m.finish(now, NoSeat, "both_idle")
		return
	case idle0:
		m.finish(now, 1, "opponent_idle")
		return
	case idle1:
		m.finish(now, 0, "opponent_idle")
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
	w0, w1 := m.Seats[0].Wins, m.Seats[1].Wins
	switch {
	case w0 >= m.cfg.WinsNeeded:
		m.finish(now, 0, "rounds")
	case w1 >= m.cfg.WinsNeeded:
		m.finish(now, 1, "rounds")
	case m.Round >= m.cfg.MaxRounds && w0 > w1:
		m.finish(now, 0, "rounds")
	case m.Round >= m.cfg.MaxRounds && w1 > w0:
		m.finish(now, 1, "rounds")
	case m.Round >= m.cfg.MaxRounds:
		m.finish(now, NoSeat, "draw")
	default:
		m.startCountdown(now)
	}
}

func (m *Match) finish(now time.Time, winner int, reason string) {
	m.transition(MatchEnd)
	m.Winner, m.EndReason, m.EndedAt = winner, reason, now
	m.PausedAt = time.Time{}
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
		Paused: !m.PausedAt.IsZero(), EventSeq: m.EventSeq, Players: make([]protocol.PlayerView, 2),
	}
	if seat >= 0 {
		st.Ack = m.Seats[seat].LastSeq
	}
	if m.Phase != MatchEnd {
		ref := now
		if st.Paused {
			ref = m.PausedAt
		}
		st.RemainingMs = max(0, m.PhaseEnds.Sub(ref).Milliseconds())
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
