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
const snapshotVersion = 1

// snapshotMigrations[v] turns the "match" object of a version-v snapshot into version
// v+1. A snapshot older than the oldest registered migration, or newer than this build,
// is not ours to run: it is left alone (the store's TTL removes it), never deleted, since
// a node of the right version may still need it.
var snapshotMigrations = map[int]func(json.RawMessage) (json.RawMessage, error){}

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
// with the old process); the normal reconnect grace then applies, and Countdown/Playing
// stay paused until both are back.
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
	for _, t := range []*time.Time{&m.PhaseEnds, &m.NextGravity, &m.PausedAt, &m.EndedAt} {
		shiftTime(t, shift)
	}
	for _, seat := range m.Seats {
		shiftTime(&seat.LastInput, shift)
		if seat.Connected || seat.LeftAt.IsZero() {
			seat.Connected, seat.LeftAt = false, now
		} else {
			shiftTime(&seat.LeftAt, shift)
		}
	}
	if (m.Phase == Countdown || m.Phase == Playing) && m.PausedAt.IsZero() {
		m.PausedAt = now // frozen until both players are back (see checkPause)
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
