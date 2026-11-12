// Package cluster runs several game servers as one: node liveness, takeover of a dead
// node's matches, and matchmaking across nodes. All of it goes through the Redis that
// already holds the match snapshots; there is no other coordination service.
package cluster

import (
	"context"
	"log/slog"
	"sync/atomic"
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
// AdoptEvery (≈ 12 s), inside the 20 s reconnect grace. AdoptBatch is small on purpose:
// one survivor taking every match of a dead node at once would double its load, while
// batches spread them over the live nodes, one scan (2 s) apart.
func DefaultOptions() Options {
	return Options{Lease: 10 * time.Second, AdoptEvery: 2 * time.Second, AdoptBatch: 50}
}

// Node keeps this server's membership alive and adopts the matches of dead nodes.
type Node struct {
	st    store.Cluster
	mgr   *match.Manager
	met   *metrics.Metrics
	log   *slog.Logger
	opts  Options
	adopt func() bool // false while draining: a node on its way out takes no new matches

	joinSent time.Time // when Join's heartbeat was sent
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
	sent := time.Now()
	if err := n.st.Heartbeat(ctx, n.opts.Lease); err != nil {
		return err
	}
	n.joinSent = sent
	return nil
}

// Run heartbeats and adopts until ctx ends.
//
// Self-fencing: other nodes may adopt this node's matches as soon as its alive key
// expires, which is Lease after Redis applied the last heartbeat that succeeded. If this
// node cannot renew in time (say, it lost Redis), it must stop its matches before that,
// or a match would run on two nodes at once: the store's owner checks keep the data
// safe, but the moves players make on the stale copy are lost. So:
//
//   - a heartbeat counts from the moment it was sent, not when Redis answered: the key's
//     TTL starts somewhere in between, and only the earlier bound is safe;
//   - the node fences itself at fenceAfter (2/3 of the lease) without a renewal, which
//     leaves a third of the lease as margin for scheduling and clock-rate differences;
//   - no heartbeat may outlive the fencing deadline, and nothing else runs on the
//     heartbeat goroutine: adoption scans and restores (which can take seconds) used to
//     share its loop and could hold the heartbeat back past the lease.
//
// If the node is merely partitioned and comes back, the store's owner checks reject its
// writes anyway (ErrNotOwner), which also stops any match it was still driving.
func (n *Node) Run(ctx context.Context) {
	restore := make(chan struct{}, 1)
	var fenced atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.adoptLoop(ctx, &fenced, restore)
	}()
	n.heartbeatLoop(ctx, &fenced, restore)
	<-done
}

func (n *Node) fenceAfter() time.Duration { return n.opts.Lease * 2 / 3 }

func (n *Node) heartbeatLoop(ctx context.Context, fenced *atomic.Bool, restore chan<- struct{}) {
	every := n.opts.Lease / 3
	lastSent := n.joinSent
	if lastSent.IsZero() {
		lastSent = time.Now()
	}
	// Beats are spaced from the previous send, so a slow Restore between Join and Run
	// gets an immediate first beat instead of a full interval later.
	timer := time.NewTimer(max(0, every-time.Since(lastSent)))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		sent := time.Now()
		timer.Reset(every)
		// A heartbeat that answers after the fencing deadline is useless: give up on it
		// in time to fence. The floor keeps a late beat from being a certain failure.
		timeout := min(every, max(every/4, time.Until(lastSent.Add(n.fenceAfter()))))
		op, cancel := context.WithTimeout(ctx, timeout)
		err := n.st.Heartbeat(op, n.opts.Lease)
		cancel()
		if err == nil {
			lastSent = sent
			if fenced.Swap(false) {
				n.met.ClusterFenced.Set(0)
				// Back in the cluster. Matches this node stopped and nobody adopted (Redis
				// was down for everyone, so nobody could) are still ours: the adopt loop
				// brings them back from their last snapshots and their players resume.
				select {
				case restore <- struct{}{}:
				default:
				}
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		n.met.ClusterErrors.WithLabelValues("heartbeat").Inc()
		n.log.Warn("cluster heartbeat failed", "err", err)
		if !fenced.Load() && time.Since(lastSent) >= n.fenceAfter() {
			fenced.Store(true)
			n.met.ClusterFenced.Set(1)
			stopped := n.mgr.EvictAll()
			n.log.Error("cluster lease about to lapse: stopped local matches so other nodes can adopt them",
				"matches", stopped, "since_last_heartbeat", time.Since(lastSent).Round(time.Millisecond))
		}
	}
}

func (n *Node) adoptLoop(ctx context.Context, fenced *atomic.Bool, restore <-chan struct{}) {
	scan := time.NewTicker(n.opts.AdoptEvery)
	defer scan.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-restore:
			if fenced.Load() {
				continue // lost the lease again meanwhile; the next renewal asks again
			}
			op, cancel := context.WithTimeout(ctx, 10*time.Second)
			restored, err := n.mgr.Restore(op)
			cancel()
			if err != nil {
				n.met.ClusterErrors.WithLabelValues("restore").Inc()
			}
			n.log.Info("cluster lease renewed again; restored own matches", "count", restored, "err", err)
		case <-scan.C:
			if fenced.Load() || !n.adopt() {
				continue
			}
			op, cancel := context.WithTimeout(ctx, 5*time.Second)
			snaps, err := n.st.AdoptOrphans(op, n.opts.AdoptBatch)
			cancel()
			if err != nil {
				n.met.ClusterErrors.WithLabelValues("adopt").Inc()
				n.log.Warn("adopting orphaned matches failed", "err", err)
			}
			if len(snaps) > 0 && !fenced.Load() {
				started := n.mgr.Adopt(snaps)
				n.log.Info("adopted matches from dead nodes", "count", started)
			}
		}
	}
}

// Leaving tells the other nodes this one is shutting down. Call it before closing the
// players' sockets: they reconnect elsewhere at once, and from then on other nodes answer
// "match_moving" (retry shortly) instead of redirecting them back to a node whose
// listener is already gone. The matches are adopted after Leave.
func (n *Node) Leaving(ctx context.Context) error {
	return n.st.Leaving(ctx)
}

// Leave ends this node's membership at once (after a graceful shutdown wrote the final
// snapshots), so the other nodes adopt its matches on their next scan instead of after a
// full lease.
func (n *Node) Leave(ctx context.Context) error {
	return n.st.Release(ctx)
}
