package match

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// snapshotVersion is the format this build writes. Bump it when the snapshot's meaning
// changes, and register a migration from the previous version in snapshotMigrations, so a
// rolling deploy can restore and adopt the matches the old build left behind.
//
// Version 2: a disconnect no longer pauses the match (a match only stands still while it
// resyncs after a restore: resyncFrom/resyncUntil), and the idle clock stops at its own
// idleStop instead of at leftAt. The same version-1 snapshot meant a paused match to one
// build and a running one to the next, so a mixed cluster would have run one match two
// ways; with the bump a version-1 node leaves version-2 matches alone.
//
// Version 3: a resting piece locks after the lock delay (seat lockAt, board lockResets
// and lowestY), not on the gravity step that found it resting. A version-2 build would
// ignore those fields and lock at once: one match, two sets of rules.
const snapshotVersion = 3

// snapshotMigrations[v] turns the "match" object of a version-v snapshot into version
// v+1. A snapshot older than the oldest registered migration, or newer than this build,
// is not ours to run: it is left alone (the store's TTL removes it), never deleted, since
// a node of the right version may still need it.
var snapshotMigrations = map[int]func(json.RawMessage) (json.RawMessage, error){
	1: migrateV1,
	2: migrateV2,
}

// migrateV2 turns a version-2 match into version 3. Nothing to rewrite: without lockAt a
// resting piece gets its lock delay on the next gravity step, and the reset counters
// start at zero.
func migrateV2(raw json.RawMessage) (json.RawMessage, error) { return raw, nil }

// migrateV1 turns a version-1 match into version 2. A version-1 build that paused
// matches kept their clocks still from "pausedAt": that is exactly what resyncFrom means
// now, so Restore resumes the clocks from there and the round keeps the time it had left
// when it froze. The pause budget ("awayUsed") is gone. The idle clock of an away
// player stood from the moment they left, so idleStop starts as leftAt.
func migrateV1(raw json.RawMessage) (json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if at, ok := m["pausedAt"]; ok {
		m["resyncFrom"] = at
		delete(m, "pausedAt")
	}
	var seats []map[string]json.RawMessage
	if err := json.Unmarshal(m["seats"], &seats); err != nil {
		return nil, err
	}
	for _, seat := range seats {
		if seat == nil {
			continue
		}
		delete(seat, "awayUsed")
		if left, ok := seat["leftAt"]; ok {
			seat["idleStop"] = left
		}
	}
	var err error
	if m["seats"], err = json.Marshal(seats); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

// ErrSnapshotVersion means the snapshot was written in a format this build cannot run.
var ErrSnapshotVersion = errors.New("match: unsupported snapshot version")

// SnapshotSupported reports whether this build can run a match from data's format. It
// only looks at the version, so a node can decide before taking a match over.
func SnapshotSupported(data []byte) bool {
	var h struct {
		Version int `json:"version"`
	}
	if json.Unmarshal(data, &h) != nil {
		return false
	}
	return supportedVersion(h.Version)
}

func supportedVersion(v int) bool {
	for ; v < snapshotVersion; v++ {
		if snapshotMigrations[v] == nil {
			return false
		}
	}
	return v == snapshotVersion
}

type snapshot struct {
	Version int `json:"version"`
	// Match is decoded only after the version check (and any migration), from the raw
	// form in snapshotHeader.
	SavedAt time.Time `json:"savedAt"`
	// Final: taken by a graceful stop, after which the match did not change. A match
	// restored from any other snapshot has rewound to it (a crash loses the progress
	// since the last periodic snapshot).
	Final bool   `json:"final"`
	Match *Match `json:"match"`
}

// snapshotHeader is a snapshot as read back: the match stays raw until its version is
// known to be runnable.
type snapshotHeader struct {
	Version int             `json:"version"`
	SavedAt time.Time       `json:"savedAt"`
	Final   bool            `json:"final"`
	Match   json.RawMessage `json:"match"`
}

// rewindGap is added to the event and tick counters of a match restored from a
// non-final snapshot, so the numbers a client already saw (for events that were lost with
// the crashed process) are never reused for different events.
const rewindGap = 10000

// Snapshot serialises the full match state. Only the newest cfg.SnapshotEvents events of
// the reconnect log are kept: the log dominated the snapshot size, and after a restore a
// client that is further behind still gets the authoritative state (eventsComplete=false).
func (m *Match) Snapshot(now time.Time) ([]byte, error) { return m.snapshot(now, false) }

// FinalSnapshot is Snapshot for a graceful stop: restoring it loses nothing.
func (m *Match) FinalSnapshot(now time.Time) ([]byte, error) { return m.snapshot(now, true) }

func (m *Match) snapshot(now time.Time, final bool) ([]byte, error) {
	c := *m
	if n := m.cfg.SnapshotEvents; n > 0 && len(c.Events) > n {
		c.Events = c.Events[len(c.Events)-n:]
	}
	return json.Marshal(snapshot{Version: snapshotVersion, SavedAt: now, Final: final, Match: &c})
}

// Restore rebuilds a match from a snapshot taken at some earlier time.
//
// Every deadline is shifted forward by the downtime, so the process being dead does not
// eat into round or countdown time. All players start disconnected (their sockets died
// with the old process), and each has the normal reconnect grace, counted from now, to
// resume. Until both are back, or for at most cfg.ResyncWait, a match in Countdown,
// Playing or RoundEnd stands still (state.paused): a failover takes seconds, and
// without the wait the player whose client finds the new node first would get free
// round time on a falling board the other cannot touch. A disconnect alone never
// pauses a match; this stand-still is the server's own failure.
//
// A snapshot taken while a match stood still (resyncFrom set) resumes its clocks from
// resyncFrom, not from the snapshot time: the round keeps the time it had left.
func Restore(data []byte, cfg Config, now time.Time) (*Match, error) {
	var s snapshotHeader
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("match: decode snapshot: %w", err)
	}
	if !supportedVersion(s.Version) {
		return nil, fmt.Errorf("%w: %d (this build writes %d)", ErrSnapshotVersion, s.Version, snapshotVersion)
	}
	raw := s.Match
	for v := s.Version; v < snapshotVersion; v++ {
		var err error
		if raw, err = snapshotMigrations[v](raw); err != nil {
			return nil, fmt.Errorf("match: migrate snapshot from version %d: %w", v, err)
		}
	}
	var m *Match
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("match: decode snapshot: %w", err)
	}
	if m == nil || m.Seats[0] == nil || m.Seats[1] == nil || m.Seats[0].Board == nil || m.Seats[1].Board == nil {
		return nil, fmt.Errorf("match: incomplete snapshot")
	}
	m.cfg = cfg
	shift := now.Sub(s.SavedAt)
	play := shift // for the game clocks: round/countdown deadline, gravity, idle
	if !m.ResyncFrom.IsZero() && !m.ResyncFrom.After(s.SavedAt) {
		play = now.Sub(m.ResyncFrom) // they stood still since then
	}
	m.ResyncFrom, m.ResyncUntil = time.Time{}, time.Time{}
	shiftTime(&m.PhaseEnds, play)
	shiftTime(&m.NextGravity, play)
	shiftTime(&m.EndedAt, shift)
	for _, seat := range m.Seats {
		shiftTime(&seat.LastInput, play)
		shiftTime(&seat.IdleStop, play)
		shiftTime(&seat.LockAt, play)
		if seat.Connected || seat.LeftAt.IsZero() {
			seat.Connected, seat.LeftAt = false, now
		} else {
			shiftTime(&seat.LeftAt, shift)
		}
		if seat.IdleStop.IsZero() || seat.IdleStop.After(now) {
			seat.IdleStop = now
		}
	}
	if cfg.ResyncWait > 0 && (m.Phase == Countdown || m.Phase == Playing || m.Phase == RoundEnd) {
		m.ResyncFrom, m.ResyncUntil = now, now.Add(cfg.ResyncWait)
	}
	if !s.Final {
		// The state rewound to this snapshot: whatever happened after it is gone. Skip the
		// counters ahead so lost event and tick numbers are never reused, and tell each
		// player on their next attach that their history is invalid (eventsComplete=false:
		// trust the snapshot state, resend un-acked inputs).
		m.EventSeq += rewindGap
		m.Tick += rewindGap
		for _, seat := range m.Seats {
			seat.rewound = true
		}
	}
	m.dirty = true
	return m, nil
}

func shiftTime(t *time.Time, d time.Duration) {
	if !t.IsZero() {
		*t = t.Add(d)
	}
}
