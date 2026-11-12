package main

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
)

// checker validates everything one bot sees during one match, so that a load test is
// also a correctness test: any violation is counted and the first few are printed.
//
// Phase order is checked on events, not on state frames: the server conflates state
// frames (a slow reader may skip some), but never drops or reorders events.
type checker struct {
	bot        string
	violations *atomic.Int64
	printed    *atomic.Int64

	events     uint64 // last event seq received
	matchOver  bool
	phase      string // from phase events
	round      int
	roundOpen  bool // a round_over is due before the next countdown
	tick       uint64
	ack        uint64
	sentSeq    uint64 // highest input seq sent
	eventSeq   uint64 // from the last state
	stateRound int
	score      [2]int
	lines      [2]int
	wins       [2]int
}

const maxPrinted = 20

func (c *checker) fail(format string, args ...any) {
	c.violations.Add(1)
	if c.printed.Add(1) <= maxPrinted {
		fmt.Fprintf(os.Stderr, "%s: VIOLATION: %s\n", c.bot, fmt.Sprintf(format, args...))
	}
}

func (c *checker) sent(seq uint64) { c.sentSeq = max(c.sentSeq, seq) }

func (c *checker) event(e protocol.Event) {
	if e.Seq != c.events+1 {
		c.fail("event seq %d after %d (gap or duplicate)", e.Seq, c.events)
	}
	c.events = max(c.events, e.Seq)
	if c.matchOver {
		c.fail("event %s after match_over", e.Kind)
	}
	switch e.Kind {
	case protocol.EvPhase:
		switch e.Phase {
		case "countdown":
			if c.roundOpen || e.Round != c.round+1 {
				c.fail("countdown for round %d after round %d (round over: %v)", e.Round, c.round, !c.roundOpen)
			}
			c.round, c.roundOpen = e.Round, false
		case "playing":
			if c.phase != "countdown" || e.Round != c.round {
				c.fail("playing round %d after phase %q round %d", e.Round, c.phase, c.round)
			}
			c.roundOpen = true
		default:
			c.fail("unexpected phase event %q", e.Phase)
		}
		c.phase = e.Phase
	case protocol.EvRoundOver:
		if !c.roundOpen || e.Round != c.round {
			c.fail("round_over for round %d while round %d open=%v", e.Round, c.round, c.roundOpen)
		}
		c.roundOpen = false
		c.phase = "round_end"
	case protocol.EvMatchOver:
		c.matchOver = true
	}
}

// snapshot checks a (re)attach snapshot: its events must continue the stream exactly.
func (c *checker) snapshot(s protocol.Snapshot) {
	switch {
	case !s.EventsComplete && len(s.Events) > 0:
		// The bounded log no longer reaches back to what we saw: the server says so, and
		// the stream continues from the oldest event it still has.
		c.events = max(c.events, s.Events[0].Seq-1)
		c.resync(s.State)
	case !s.EventsComplete:
		c.events = max(c.events, s.State.EventSeq)
		c.resync(s.State)
	case len(s.Events) > 0 && s.Events[0].Seq != c.events+1:
		c.fail("snapshot events start at %d, last seen %d", s.Events[0].Seq, c.events)
	}
	for _, e := range s.Events {
		if e.Seq <= c.events {
			continue // already seen before the reconnect
		}
		c.event(e)
	}
	c.state(s.State)
}

// resync takes the phase from an authoritative state after events were lost to the
// bounded log (the phase checks cannot bridge the gap).
func (c *checker) resync(st protocol.State) {
	c.phase, c.round, c.roundOpen = st.Phase, st.Round, st.Phase == "playing"
}

func (c *checker) state(st protocol.State) {
	if st.Tick < c.tick {
		c.fail("tick went back %d -> %d", c.tick, st.Tick)
	}
	if st.Ack < c.ack {
		c.fail("ack went back %d -> %d", c.ack, st.Ack)
	}
	if st.Ack > c.sentSeq {
		c.fail("ack %d for an input never sent (highest %d)", st.Ack, c.sentSeq)
	}
	if st.EventSeq < c.eventSeq {
		c.fail("eventSeq went back %d -> %d", c.eventSeq, st.EventSeq)
	}
	if st.EventSeq > c.events {
		c.fail("state reflects event %d before it was delivered (last event %d)", st.EventSeq, c.events)
	}
	c.tick, c.ack, c.eventSeq = st.Tick, st.Ack, st.EventSeq
	if len(st.Players) != 2 {
		c.fail("state has %d players", len(st.Players))
		return
	}
	newRound := st.Round != c.stateRound
	if st.Round < c.stateRound {
		c.fail("round went back %d -> %d", c.stateRound, st.Round)
	}
	c.stateRound = st.Round
	winsSum := 0
	for i, p := range st.Players {
		if len(p.Rows) != game.Height {
			c.fail("player %d board has %d rows", i, len(p.Rows))
		}
		for y, r := range p.Rows {
			if len(r) != game.Width || strings.Trim(r, ".IOTSZJLG") != "" {
				c.fail("player %d row %d = %q", i, y, r)
				break
			}
		}
		if !strings.ContainsRune("IOTSZJL", rune(p.Piece.Kind)) || p.Piece.Rot < 0 || p.Piece.Rot > 3 {
			c.fail("player %d piece %+v", i, p.Piece)
		}
		if !newRound && (p.Score < c.score[i] || p.Lines < c.lines[i]) {
			c.fail("player %d score/lines went back within round %d: %d/%d -> %d/%d", i, st.Round, c.score[i], c.lines[i], p.Score, p.Lines)
		}
		if p.Wins < c.wins[i] {
			c.fail("player %d wins went back %d -> %d", i, c.wins[i], p.Wins)
		}
		if p.Score < 0 || p.Lines < 0 || p.Pending < 0 {
			c.fail("player %d negative counters %+v", i, p)
		}
		c.score[i], c.lines[i], c.wins[i] = p.Score, p.Lines, p.Wins
		winsSum += p.Wins
	}
	if winsSum > st.Round {
		c.fail("%d round wins after %d rounds", winsSum, st.Round)
	}
}

// end is called when the bot leaves the match.
func (c *checker) end(finished bool) {
	if finished && !c.matchOver {
		c.fail("match left without match_over")
	}
}
