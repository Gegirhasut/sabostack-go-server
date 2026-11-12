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
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/Gegirhasut/sabostack-go-server/internal/clock"
	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/server"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

func main() {
	opts := match.DefaultOptions()
	var (
		addr      = flag.String("addr", envOr("ADDR", ":8080"), "listen address")
		redisAddr = flag.String("redis", os.Getenv("REDIS_ADDR"), "Redis address for snapshots (empty = in-memory store, no crash recovery)")
		prefix    = flag.String("redis-prefix", "sabostack:", "Redis key prefix")
		logLevel  = flag.String("log-level", "info", "debug, info, warn or error")
		logJSON   = flag.Bool("log-json", true, "log as JSON (false = human-readable text)")
	)
	flag.DurationVar(&opts.Match.TickInterval, "tick", opts.Match.TickInterval, "server tick interval")
	flag.DurationVar(&opts.Match.Gravity, "gravity", opts.Match.Gravity, "gravity interval")
	flag.DurationVar(&opts.Match.Countdown, "countdown", opts.Match.Countdown, "countdown before each round")
	flag.DurationVar(&opts.Match.RoundTime, "round-time", opts.Match.RoundTime, "round timer")
	flag.DurationVar(&opts.Match.RoundEndPause, "round-end-pause", opts.Match.RoundEndPause, "pause between rounds")
	flag.DurationVar(&opts.Match.ReconnectGrace, "reconnect-grace", opts.Match.ReconnectGrace, "how long a disconnected player may come back")
	flag.DurationVar(&opts.Match.IdleTimeout, "idle-timeout", opts.Match.IdleTimeout, "forfeit after this long without input")
	flag.DurationVar(&opts.SnapshotEvery, "snapshot-every", opts.SnapshotEvery, "periodic snapshot interval")
	flag.Parse()

	log := newLogger(*logLevel, *logJSON)
	if err := run(log, *addr, *redisAddr, *prefix, opts); err != nil {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, redisAddr, prefix string, opts match.Options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	met := metrics.New(reg)

	var st store.Store = store.NewMemory()
	if redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		defer rdb.Close()
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			return err
		}
		st = store.NewRedis(rdb, prefix, time.Hour)
		log.Info("using redis snapshot store", "addr", redisAddr)
	} else {
		log.Warn("using in-memory snapshot store: matches will not survive a restart")
	}

	mgr := match.NewManager(opts, clock.Real{}, st, met, log)
	n, err := mgr.Restore(ctx)
	if err != nil {
		return err
	}
	log.Info("restored matches", "count", n)

	srv := server.New(mgr, met, reg, log)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Info("listening", "addr", addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
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
