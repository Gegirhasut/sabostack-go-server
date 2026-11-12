// Package metrics defines the Prometheus collectors exported on /metrics.
package metrics

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics groups every collector so it can be passed around (and isolated per test).
type Metrics struct {
	ActiveMatches    prometheus.Gauge
	LiveMatches      prometheus.Gauge
	ConnectedPlayers prometheus.Gauge
	TickDuration     prometheus.Histogram
	Inputs           *prometheus.CounterVec
	MatchesFinished  *prometheus.CounterVec
	SnapshotWrites   *prometheus.CounterVec
	SnapshotBytes    prometheus.Histogram
	PersistPending   prometheus.Gauge
	ActorPanics      prometheus.Counter
	ConnsRejected    *prometheus.CounterVec
	Kicked           *prometheus.CounterVec
	WSBytes          *prometheus.CounterVec // payload bytes by dir (in/out) and format (json/binary)
	WSFrames         *prometheus.CounterVec

	tickMaxNanos atomic.Int64
}

// ObserveTick records one tick's processing time in the histogram and the running max.
// The max is a plain load in the common case (no new maximum), so it adds no contention.
func (m *Metrics) ObserveTick(d time.Duration) {
	m.TickDuration.Observe(d.Seconds())
	for {
		cur := m.tickMaxNanos.Load()
		if int64(d) <= cur || m.tickMaxNanos.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// Input returns the counter for one input outcome. Callers on hot paths should resolve it
// once and keep it, instead of a label lookup per input.
func (m *Metrics) Input(result string) prometheus.Counter { return m.Inputs.WithLabelValues(result) }

// New creates the collectors and registers them on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ActiveMatches: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sabostack_active_matches", Help: "Matches currently running in this process.",
		}),
		LiveMatches: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sabostack_live_matches", Help: "Matches not yet finished (active matches minus those lingering after match end).",
		}),
		ConnectedPlayers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sabostack_connected_players", Help: "Open WebSocket connections.",
		}),
		TickDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "sabostack_tick_duration_seconds",
			Help:    "Time a match goroutine spends processing one server tick (timers, gravity, broadcast).",
			Buckets: []float64{5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 5e-3, 1e-2},
		}),
		Inputs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_inputs_total", Help: "Player inputs by outcome (applied, duplicate, not_playing, invalid, overflow, rate_limited).",
		}, []string{"result"}),
		MatchesFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_matches_finished_total", Help: "Finished matches by end reason.",
		}, []string{"reason"}),
		SnapshotWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_snapshot_writes_total", Help: "Snapshot store operations by result (ok, error, not_owner, superseded, skipped).",
		}, []string{"op", "result"}),
		SnapshotBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "sabostack_snapshot_bytes",
			Help:    "Encoded size of match snapshots.",
			Buckets: prometheus.ExponentialBuckets(512, 2, 10), // 512 B .. 256 KB
		}),
		PersistPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sabostack_persist_pending", Help: "Snapshot writes queued and not yet sent to the store.",
		}),
		ActorPanics: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sabostack_actor_panics_total", Help: "Match goroutines that panicked (the match is aborted, the process keeps running).",
		}),
		ConnsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_connections_rejected_total", Help: "WebSocket upgrades refused, by reason.",
		}, []string{"reason"}),
		Kicked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_connections_kicked_total", Help: "Connections closed by the server, by reason.",
		}, []string{"reason"}),
		WSBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_ws_bytes_total", Help: "WebSocket message payload bytes, by direction (in/out) and format (json/binary).",
		}, []string{"dir", "format"}),
		WSFrames: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_ws_frames_total", Help: "WebSocket messages, by direction (in/out) and format (json/binary).",
		}, []string{"dir", "format"}),
	}
	tickMax := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "sabostack_tick_duration_max_seconds", Help: "Longest single tick processed since the process started.",
	}, func() float64 { return time.Duration(m.tickMaxNanos.Load()).Seconds() })
	reg.MustRegister(tickMax, m.ActiveMatches, m.LiveMatches, m.ConnectedPlayers, m.TickDuration, m.Inputs,
		m.MatchesFinished, m.SnapshotWrites, m.SnapshotBytes, m.PersistPending, m.ActorPanics, m.ConnsRejected, m.Kicked, m.WSBytes, m.WSFrames)
	return m
}
