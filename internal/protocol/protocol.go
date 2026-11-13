// Package protocol defines the JSON messages exchanged over the WebSocket.
//
// Every message is a JSON object with a "type" field. See README.md for the full
// description and an example session.
package protocol

import "github.com/Gegirhasut/sabostack-go-server/internal/game"

// Client -> server message types.
const (
	TypeJoin   = "join"   // enter the matchmaking queue
	TypeResume = "resume" // reattach to a match with a session token
	TypeInput  = "input"  // a game action
	TypePing   = "ping"
)

// Server -> client message types.
const (
	TypeQueued   = "queued"
	TypeMatched  = "matched"
	TypeSnapshot = "snapshot"
	TypeState    = "state"
	TypeEvent    = "event"
	TypeError    = "error"
	TypePong     = "pong"
)

// ClientMsg is the union of all client -> server messages.
type ClientMsg struct {
	Type string `json:"type"`

	// join
	Name string `json:"name,omitempty"`

	// resume
	MatchID      string `json:"matchId,omitempty"`
	Token        string `json:"token,omitempty"`
	LastEventSeq uint64 `json:"lastEventSeq,omitempty"`

	// input: Seq must increase per player; repeats and older values are ignored.
	Seq    uint64      `json:"seq,omitempty"`
	Action game.Action `json:"action,omitempty"`

	// ping: echoed back in pong
	T int64 `json:"t,omitempty"`
}

// Queued confirms the player is waiting for an opponent.
type Queued struct {
	Type string `json:"type"`
}

// Matched tells a queued player which match and seat they got. Token is the secret the
// client presents in a resume message after a disconnect.
type Matched struct {
	Type     string `json:"type"`
	MatchID  string `json:"matchId"`
	Seat     int    `json:"seat"`
	Token    string `json:"token"`
	Opponent string `json:"opponent"`
	// URL is set when the match runs on another server: the client connects there and
	// sends resume with this matchId and token.
	URL string `json:"url,omitempty"`
}

// Snapshot is sent on every (re)attach: the full current state plus the events the
// client has not seen. EventsComplete is false when the log no longer reaches back to
// lastEventSeq; the state is still authoritative.
type Snapshot struct {
	Type           string  `json:"type"`
	MatchID        string  `json:"matchId"`
	Seat           int     `json:"seat"`
	State          State   `json:"state"`
	Events         []Event `json:"events"`
	EventsComplete bool    `json:"eventsComplete"`
}

// State is the authoritative match state, broadcast once per server tick when something
// changed. Ack is the highest input seq the server has consumed from the recipient.
type State struct {
	Type  string `json:"type"`
	Tick  uint64 `json:"tick"`
	Phase string `json:"phase"`
	Round int    `json:"round"`
	// Paused is always false: matches no longer pause when a player disconnects. The
	// field (and bit 3 of a binary state frame) stays on the wire so that clients which
	// read it keep working; new clients should ignore it.
	Paused      bool         `json:"paused"`
	RemainingMs int64        `json:"remainingMs"` // until the current phase deadline
	Ack         uint64       `json:"ack"`
	EventSeq    uint64       `json:"eventSeq"` // latest event seq
	Players     []PlayerView `json:"players"`
}

// PlayerView is the public view of one seat.
type PlayerView struct {
	Name string `json:"name"`
	// Connected is false while the player is away (disconnected, or silent for the
	// server's silence timeout); clients show "reconnecting" for it.
	Connected bool       `json:"connected"`
	Wins      int        `json:"wins"`
	Rows      []string   `json:"rows"`
	Piece     game.Piece `json:"piece"`
	Score     int        `json:"score"`
	Lines     int        `json:"lines"`
	Pending   int        `json:"pending"`
	Dead      bool       `json:"dead"`
}

// Event kinds.
const (
	EvPhase      = "phase"       // Phase, Round, DurationMs
	EvCleared    = "cleared"     // Seat, Lines, Garbage (rows sent to the opponent)
	EvGarbage    = "garbage"     // Seat, Garbage (rows that rose on Seat's board)
	EvRoundOver  = "round_over"  // Winner (-1 = draw), Reason
	EvMatchOver  = "match_over"  // Winner (-1 = no winner), Reason
	EvPlayerLeft = "player_left" // Seat: disconnected or silent; the match goes on without them
	EvPlayerBack = "player_back" // Seat
	// The "paused" and "resumed" events are no longer sent. A match restored from a
	// snapshot written by an older build may still replay them from its log in
	// snapshot.events; clients should ignore event kinds they do not know.
)

// Event is a discrete, sequence-numbered thing that happened in a match. Events are kept
// in a bounded log so reconnecting clients can catch up.
type Event struct {
	Seq        uint64 `json:"seq"`
	AtMs       int64  `json:"atMs"` // unix milliseconds
	Kind       string `json:"kind"`
	Seat       *int   `json:"seat,omitempty"`
	Phase      string `json:"phase,omitempty"`
	Round      int    `json:"round,omitempty"`
	DurationMs int64  `json:"durationMs,omitempty"`
	Lines      int    `json:"lines,omitempty"`
	Garbage    int    `json:"garbage,omitempty"`
	Winner     *int   `json:"winner,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// EventMsg wraps an Event for the wire.
type EventMsg struct {
	Type  string `json:"type"`
	Event Event  `json:"event"`
}

// Error reports a rejected request. URL is set with code "wrong_server": the match lives
// on another game server and the client should resume there.
type Error struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
	URL     string `json:"url,omitempty"`
}

// Pong answers a ping.
type Pong struct {
	Type string `json:"type"`
	T    int64  `json:"t"`
}
