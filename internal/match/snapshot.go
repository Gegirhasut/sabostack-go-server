package match

import (
	"encoding/json"
	"fmt"
	"time"
)

// snapshotVersion guards against loading a snapshot written by an incompatible build.
const snapshotVersion = 1

type snapshot struct {
	Version int       `json:"version"`
	SavedAt time.Time `json:"savedAt"`
	Match   *Match    `json:"match"`
}

// Snapshot serialises the full match state. Only the newest cfg.SnapshotEvents events of
// the reconnect log are kept: the log dominated the snapshot size, and after a restore a
// client that is further behind still gets the authoritative state (eventsComplete=false).
func (m *Match) Snapshot(now time.Time) ([]byte, error) {
	c := *m
	if n := m.cfg.SnapshotEvents; n > 0 && len(c.Events) > n {
		c.Events = c.Events[len(c.Events)-n:]
	}
	return json.Marshal(snapshot{Version: snapshotVersion, SavedAt: now, Match: &c})
}

// Restore rebuilds a match from a snapshot taken at some earlier time.
//
// Every deadline is shifted forward by the downtime, so the process being dead does not
// eat into round or countdown time. All players start disconnected (their sockets died
// with the old process); the normal reconnect grace then applies, and Countdown/Playing
// stay paused until both are back.
func Restore(data []byte, cfg Config, now time.Time) (*Match, error) {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("match: decode snapshot: %w", err)
	}
	if s.Version != snapshotVersion {
		return nil, fmt.Errorf("match: snapshot version %d, want %d", s.Version, snapshotVersion)
	}
	m := s.Match
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
	m.dirty = true
	return m, nil
}

func shiftTime(t *time.Time, d time.Duration) {
	if !t.IsZero() {
		*t = t.Add(d)
	}
}
