// Command server runs the real-time 1v1 game server.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/cluster"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/server"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

type config struct {
	addr, adminAddr        string
	redisAddr, prefix      string
	redisTTL               time.Duration
	nodeID, publicURL      string
	logLevel               string
	logJSON, publicMetrics bool
	drainTimeout           time.Duration
	origins                string
	sharedQueue            bool
	cluster                cluster.Options
	opts                   match.Options
	srv                    server.Config
}

func main() {
	c := config{opts: match.DefaultOptions(), srv: server.DefaultConfig(), cluster: cluster.DefaultOptions()}
	flag.StringVar(&c.addr, "addr", envOr("ADDR", ":8080"), "public listen address (WebSocket and health checks)")
	flag.StringVar(&c.adminAddr, "admin-addr", os.Getenv("ADMIN_ADDR"), "private listen address for /metrics and /debug/pprof (empty = metrics on the public address, no pprof)")
	flag.BoolVar(&c.publicMetrics, "public-metrics", envBool("PUBLIC_METRICS", true), "with -admin-addr: also keep /metrics on the public address")
	flag.StringVar(&c.redisAddr, "redis", os.Getenv("REDIS_ADDR"), "Redis address for snapshots (empty = in-memory store, no crash recovery)")
	flag.StringVar(&c.prefix, "redis-prefix", "sabostack:", "Redis key prefix")
	flag.DurationVar(&c.redisTTL, "redis-ttl", time.Hour, "how long an orphaned snapshot is kept")
	flag.StringVar(&c.nodeID, "node-id", os.Getenv("NODE_ID"), "stable node id; required when several servers share one Redis (each restores only its own matches)")
	flag.StringVar(&c.publicURL, "public-url", os.Getenv("PUBLIC_URL"), "this node's public WebSocket URL; other nodes send resuming clients here")
	flag.DurationVar(&c.cluster.Lease, "lease", c.cluster.Lease, "with -node-id: a node silent this long is dead and its matches move to other nodes")
	flag.BoolVar(&c.sharedQueue, "shared-queue", envBool("SHARED_QUEUE", true), "with -node-id: one matchmaking queue for all nodes (false: per-node queue)")
	flag.StringVar(&c.logLevel, "log-level", "info", "debug, info, warn or error")
	flag.BoolVar(&c.logJSON, "log-json", true, "log as JSON (false = human-readable text)")
	flag.DurationVar(&c.drainTimeout, "drain-timeout", envDuration("DRAIN_TIMEOUT", 0), "on SIGTERM, stop matchmaking and let running matches finish for up to this long before snapshotting the rest (0 = snapshot immediately)")
	flag.StringVar(&c.origins, "allowed-origins", os.Getenv("ALLOWED_ORIGINS"), "comma-separated browser Origin hosts to accept (empty = any)")
	flag.IntVar(&c.srv.MaxConns, "max-conns", 0, "max open WebSockets per process (0 = unlimited)")
	flag.IntVar(&c.srv.MaxConnsPerIP, "max-conns-per-ip", 0, "max open WebSockets per client IP (0 = unlimited)")
	flag.BoolVar(&c.srv.TrustProxy, "trust-proxy", false, "take the client IP from X-Forwarded-For (only behind a trusted proxy)")
	flag.Float64Var(&c.srv.InputRate, "input-rate", c.srv.InputRate, "client messages per second per connection (0 = unlimited)")
	flag.Float64Var(&c.srv.InputBurst, "input-burst", c.srv.InputBurst, "burst allowance for -input-rate")
	flag.DurationVar(&c.srv.PingInterval, "ping-interval", c.srv.PingInterval, "server WebSocket ping interval")
	flag.DurationVar(&c.srv.PongWait, "pong-wait", c.srv.PongWait, "a silent socket is considered dead after this")
	flag.BoolVar(&c.srv.BinaryFullFrames, "binary-full-frames", false, "send binary clients full state frames instead of deltas (for measurements)")
	flag.DurationVar(&c.opts.Match.TickInterval, "tick", c.opts.Match.TickInterval, "server tick interval")
	flag.DurationVar(&c.opts.Match.Gravity, "gravity", c.opts.Match.Gravity, "gravity interval")
	flag.DurationVar(&c.opts.Match.Countdown, "countdown", c.opts.Match.Countdown, "countdown before each round")
	flag.DurationVar(&c.opts.Match.RoundTime, "round-time", c.opts.Match.RoundTime, "round timer")
	flag.DurationVar(&c.opts.Match.RoundEndPause, "round-end-pause", c.opts.Match.RoundEndPause, "pause between rounds")
	flag.DurationVar(&c.opts.Match.ReconnectGrace, "reconnect-grace", c.opts.Match.ReconnectGrace, "how long a disconnected player may come back")
	flag.DurationVar(&c.opts.Match.PauseBudget, "pause-budget", c.opts.Match.PauseBudget, "total time one player may keep a match paused by disconnecting (0 = unlimited)")
	flag.DurationVar(&c.opts.Match.IdleTimeout, "idle-timeout", c.opts.Match.IdleTimeout, "forfeit after this long without input")
	flag.DurationVar(&c.opts.SnapshotEvery, "snapshot-every", c.opts.SnapshotEvery, "periodic snapshot interval")
	flag.IntVar(&c.opts.PersistWorkers, "persist-workers", c.opts.PersistWorkers, "parallel snapshot writers")
	flag.Parse()

	for _, o := range strings.Split(c.origins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			c.srv.AllowedOrigins = append(c.srv.AllowedOrigins, o)
		}
	}
	c.srv.NodeID = c.nodeID
	c.srv.PublicMetrics = c.adminAddr == "" || c.publicMetrics

	log := newLogger(c.logLevel, c.logJSON)
	if err := run(log, c); err != nil {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, c config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	met := metrics.New(reg)

	var (
		st  store.Store = store.NewMemory()
		rdb *redis.Client
		rs  *store.Redis
	)
	if c.redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: c.redisAddr})
		defer rdb.Close()
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			return err
		}
		rs = store.NewRedis(rdb, c.prefix, c.redisTTL)
		if c.nodeID != "" {
			rs = rs.ForNode(c.nodeID, c.publicURL)
			log.Info("using redis snapshot store", "addr", c.redisAddr, "node", c.nodeID)
		} else {
			log.Warn("using redis snapshot store without -node-id: only one server may use this Redis prefix", "addr", c.redisAddr)
		}
		st = rs
	} else {
		log.Warn("using in-memory snapshot store: matches will not survive a restart")
	}
	if len(c.srv.AllowedOrigins) == 0 {
		log.Warn("accepting WebSocket upgrades from any Origin (set -allowed-origins in production)")
	}

	mgr := match.NewManager(c.opts, clock.Real{}, st, met, log)

	// Cluster mode: announce this node before restoring, so nobody adopts its matches
	// while it restarts; then adopt dead nodes' matches in the background.
	var (
		node       *cluster.Node
		srv        *server.Server
		nodeCancel = func() {}
	)
	if c.nodeID != "" && rs != nil {
		node = cluster.NewNode(rs, mgr, met, log, c.cluster, func() bool { return srv != nil && !srv.Draining() })
		if err := node.Join(ctx); err != nil {
			return err
		}
	}
	n, err := mgr.Restore(ctx)
	if err != nil {
		return err
	}
	log.Info("restored matches", "count", n)

	srv = server.NewWithConfig(mgr, met, reg, log, c.srv)
	if node != nil {
		if c.sharedQueue {
			mm, err := cluster.NewMatchmaker(rdb, c.prefix, c.nodeID, c.publicURL, mgr.Create, met, log)
			if err != nil {
				return err
			}
			srv.UseMatchmaker(mm)
		}
		var nodeCtx context.Context
		nodeCtx, nodeCancel = context.WithCancel(context.Background())
		go node.Run(nodeCtx)
		log.Info("cluster mode", "node", c.nodeID, "url", c.publicURL, "lease", c.cluster.Lease, "sharedQueue", c.sharedQueue)
	}
	defer nodeCancel()
	httpSrv := &http.Server{Addr: c.addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 16 << 10}
	errc := make(chan error, 2)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Info("listening", "addr", c.addr)
	var adminSrv *http.Server
	if c.adminAddr != "" {
		adminSrv = &http.Server{Addr: c.adminAddr, Handler: srv.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { errc <- adminSrv.ListenAndServe() }()
		log.Info("admin listening", "addr", c.adminAddr)
	}

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	stop() // a second signal now kills the process the default way

	if c.drainTimeout > 0 {
		// Rolling deploys: take no new players, let running matches end naturally. The
		// listener stays up so disconnected players can still resume here.
		log.Info("draining: matchmaking stopped, waiting for live matches", "live", mgr.LiveCount(), "timeout", c.drainTimeout)
		srv.Drain()
		deadline := time.Now().Add(c.drainTimeout)
		for mgr.LiveCount() > 0 && time.Now().Before(deadline) {
			time.Sleep(250 * time.Millisecond)
		}
		log.Info("drain finished", "live", mgr.LiveCount())
	}

	log.Info("shutting down: snapshotting matches")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http shutdown", "err", err)
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if node != nil {
		// Final snapshots are written: hand the matches over now instead of after a lease.
		nodeCancel()
		if err := node.Leave(shutdownCtx); err != nil {
			log.Warn("cluster leave", "err", err)
		}
	}
	if adminSrv != nil {
		_ = adminSrv.Shutdown(shutdownCtx)
	}
	log.Info("shutdown complete")
	return nil
}

func newLogger(level string, json bool) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	ho := &slog.HandlerOptions{Level: lvl}
	if json {
		return slog.New(slog.NewJSONHandler(os.Stdout, ho))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, ho))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
