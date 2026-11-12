package match

import (
	"encoding/json"
	"fmt"
)

// Phase is the match state-machine state.
//
//	Waiting ──► Countdown ──► Playing ──► RoundEnd ──► MatchEnd
//	   │            ▲  │          │           │
//	   │            └──┼──────────┼───────────┘ (next round)
//	   └───────────────┴──────────┴──► MatchEnd (abort / forfeit)
type Phase int

const (
	Waiting   Phase = iota // seats assigned, waiting for both players to connect
	Countdown              // short pause before a round; inputs are ignored
	Playing                // gravity runs, inputs are applied
	RoundEnd               // round decided, brief pause before the next round
	MatchEnd               // terminal
)

var phaseNames = [...]string{"waiting", "countdown", "playing", "round_end", "match_end"}

func (p Phase) String() string {
	if p < 0 || int(p) >= len(phaseNames) {
		return fmt.Sprintf("phase(%d)", int(p))
	}
	return phaseNames[p]
}

func (p Phase) MarshalJSON() ([]byte, error) { return json.Marshal(p.String()) }

func (p *Phase) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	for i, n := range phaseNames {
		if n == s {
			*p = Phase(i)
			return nil
		}
	}
	return fmt.Errorf("match: unknown phase %q", s)
}

// transitions lists every legal edge. Anything else is a bug and is rejected.
var transitions = map[Phase][]Phase{
	Waiting:   {Countdown, MatchEnd},
	Countdown: {Playing, MatchEnd},
	Playing:   {RoundEnd, MatchEnd},
	RoundEnd:  {Countdown, MatchEnd},
	MatchEnd:  nil,
}

// ErrIllegalTransition is returned (wrapped) for a transition not in the table.
type ErrIllegalTransition struct{ From, To Phase }

func (e *ErrIllegalTransition) Error() string {
	return fmt.Sprintf("match: illegal transition %s -> %s", e.From, e.To)
}

// CanTransition reports whether from -> to is a legal edge.
func CanTransition(from, to Phase) bool {
	for _, p := range transitions[from] {
		if p == to {
			return true
		}
	}
	return false
}
