// Package cluster runs several game servers as one: node liveness, takeover of a dead
// node's matches, and matchmaking across nodes. All of it goes through the Redis that
// already holds the match snapshots; there is no other coordination service.
package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/Gegirhasut/sabostack-go-server/internal/match"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
	"github.com/Gegirhasut/sabostack-go-server/internal/store"
)

// Options configures a Node.
type Options struct {
	Lease      time.Duration // a node that misses its heartbeat this long is dead
	AdoptEvery time.Duration // how often live nodes look for dead nodes' matches
	AdoptBatch int           // most matches adopted per scan, so a dead node's load spreads
}

// DefaultOptions: a crashed node's matches resume elsewhere within about Lease +
// AdoptEvery (≈ 12 s), inside the 20 s reconnect grace.
func DefaultOptions() Options {
	return Options{Lease: 10 * time.Second, AdoptEvery: 2 * time.Second, AdoptBatch: 200}
}

// Node keeps this server's membership alive and adopts the matches of dead nodes.
type Node struct {
	st    store.Cluster
	mgr   *match.Manager
	met   *metrics.Metrics
	log   *slog.Logger
	opts  Options
	adopt func() bool // false while draining: a node on its way out takes no new matches
}

// NewNode creates a Node. canAdopt may be nil (always adopt).
func NewNode(st store.Cluster, mgr *match.Manager, met *metrics.Metrics, log *slog.Logger, opts Options, canAdopt func() bool) *Node {
	if canAdopt == nil {
		canAdopt = func() bool { return true }
	}
	return &Node{st: st, mgr: mgr, met: met, log: log, opts: opts, adopt: canAdopt}
}

// Join sends the first heartbeat. Call it before restoring this node's own matches, so
// no other node mistakes a restarting node for a dead one.
func (n *Node) Join(ctx context.Context) error {
	return n.st.Heartbeat(ctx, n.opts.Lease)
}

// Run heartbeats and adopts until ctx ends.
//
// Self-fencing: if this node cannot renew its heartbeat for a whole lease (say, it lost
// Redis), other nodes will treat it as dead and adopt its matches, so it stops them
// itself. If it is merely partitioned and comes back, the store's owner checks reject its
// writes anyway (ErrNotOwner), which also stops any match it was still driving.
func (n *Node) Run(ctx context.Context) {
	beat := time.NewTicker(n.opts.Lease / 3)
	defer beat.Stop()
	scan := time.NewTicker(n.opts.AdoptEvery)
	defer scan.Stop()
	lastBeat, fenced := time.Now(), false
	for {
		select {
		case <-ctx.Done():
			return
		case <-beat.C:
			op, cancel := context.WithTimeout(ctx, n.opts.Lease/3)
			err := n.st.Heartbeat(op, n.opts.Lease)
			cancel()
			if err == nil {
				lastBeat = time.Now()
				if fenced {
					// Back in the cluster. Matches this node stopped and nobody adopted
					// (Redis was down for everyone, so nobody could) are still ours: bring
					// them back from their last snapshots; their players resume.
					fenced = false
					n.met.ClusterFenced.Set(0)
					op, cancel := context.WithTimeout(ctx, 10*time.Second)
					restored, err := n.mgr.Restore(op)
					cancel()
					if err != nil {
						n.met.ClusterErrors.WithLabelValues("restore").Inc()
					}
					n.log.Info("cluster lease renewed again; restored own matches", "count", restored, "err", err)
				}
				continue
			}
			n.met.ClusterErrors.WithLabelValues("heartbeat").Inc()
			n.log.Warn("cluster heartbeat failed", "err", err)
			if !fenced && time.Since(lastBeat) >= n.opts.Lease {
				fenced = true
				n.met.ClusterFenced.Set(1)
				stopped := n.mgr.EvictAll()
				n.log.Error("cluster lease lost: stopped local matches so other nodes can adopt them", "matches", stopped)
			}
		case <-scan.C:
			if fenced || !n.adopt() {
				continue
			}
			op, cancel := context.WithTimeout(ctx, 5*time.Second)
			snaps, err := n.st.AdoptOrphans(op, n.opts.AdoptBatch)
			cancel()
			if err != nil {
				n.met.ClusterErrors.WithLabelValues("adopt").Inc()
				n.log.Warn("adopting orphaned matches failed", "err", err)
			}
			if len(snaps) > 0 {
				started := n.mgr.Adopt(snaps)
				n.log.Info("adopted matches from dead nodes", "count", started)
			}
		}
	}
}

// Leave ends this node's membership at once (after a graceful shutdown wrote the final
// snapshots), so the other nodes adopt its matches on their next scan instead of after a
// full lease.
func (n *Node) Leave(ctx context.Context) error {
	return n.st.Release(ctx)
}
