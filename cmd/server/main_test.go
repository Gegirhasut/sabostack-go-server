package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/server"
)

// Bug: the disconnect rules rest on how the timings relate (a dropped connection must
// be noticed before the idle forfeit, pings must come often enough for the silence
// timeout, the resync wait must fit in the grace), and a flag could break any of them
// with at most a warning in the log. Now the server refuses to start.
func TestTimingFlagsAreChecked(t *testing.T) {
	if err := checkTimings(server.DefaultConfig(), match.DefaultConfig()); err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
	cases := []struct {
		name string
		edit func(*server.Config, *match.Config)
		want string
	}{
		{"silence not before idle", func(_ *server.Config, m *match.Config) { m.SilenceTimeout = 40 * time.Second }, "-idle-timeout"},
		{"silence off, pong-wait as long as idle", func(_ *server.Config, m *match.Config) { m.SilenceTimeout = 0 }, "-idle-timeout"},
		{"pings too rare for silence", func(s *server.Config, _ *match.Config) { s.PingInterval = 5 * time.Second }, "-silence-timeout"},
		{"ping not under pong-wait", func(s *server.Config, _ *match.Config) { s.PingInterval = s.PongWait }, "-pong-wait"},
		{"resync wait not under grace", func(_ *server.Config, m *match.Config) { m.ResyncWait = m.ReconnectGrace }, "-resync-wait"},
		{"no grace", func(_ *server.Config, m *match.Config) { m.ReconnectGrace = 0 }, "-reconnect-grace"},
	}
	for _, tc := range cases {
		srv, m := server.DefaultConfig(), match.DefaultConfig()
		tc.edit(&srv, &m)
		err := checkTimings(srv, m)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want one naming %s", tc.name, err, tc.want)
		}
	}
	srv, m := server.DefaultConfig(), match.DefaultConfig()
	m.SilenceTimeout, srv.PongWait = 0, 20*time.Second // silence off but the socket closes in time
	if err := checkTimings(srv, m); err != nil {
		t.Fatalf("silence off with -pong-wait under -idle-timeout rejected: %v", err)
	}
}
