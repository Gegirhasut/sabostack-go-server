// Package metrics defines the Prometheus collectors exported on /metrics.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics groups every collector so it can be passed around (and isolated per test).
type Metrics struct {
	ActiveMatches    prometheus.Gauge
	ConnectedPlayers prometheus.Gauge
	TickDuration     prometheus.Histogram
	Inputs           *prometheus.CounterVec
	MatchesFinished  *prometheus.CounterVec
	SnapshotWrites   *prometheus.CounterVec
}

// New creates the collectors and registers them on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ActiveMatches: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sabostack_active_matches", Help: "Matches currently running in this process.",
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
			Name: "sabostack_inputs_total", Help: "Player inputs by outcome (applied, duplicate, not_playing, invalid, overflow).",
		}, []string{"result"}),
		MatchesFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_matches_finished_total", Help: "Finished matches by end reason.",
		}, []string{"reason"}),
		SnapshotWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sabostack_snapshot_writes_total", Help: "Snapshot store operations by result.",
		}, []string{"op", "result"}),
	}
	reg.MustRegister(m.ActiveMatches, m.ConnectedPlayers, m.TickDuration, m.Inputs, m.MatchesFinished, m.SnapshotWrites)
	return m
}
