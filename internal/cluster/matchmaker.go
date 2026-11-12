package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Gegirhasut/sabostack-go-server/internal/lobby"
	"github.com/Gegirhasut/sabostack-go-server/internal/metrics"
)

// Matchmaker is a FIFO matchmaking queue shared by every node.
//
//   - A joining player's ticket "<id>\t<node>\t<name>" is pushed onto one Redis list.
//   - Any node pops the two oldest tickets in one Lua script, skipping tickets of nodes
//     whose heartbeat has expired, and creates the match locally (it is alive, so it can
//     host it).
//   - Each player's node learns its assignment: directly if it is the popping node, else
//     through a Redis pub/sub channel per node. A player connected to another node gets
//     "matched" with the url of the hosting node, reconnects there and resumes.
//
// Pub/sub is at most once, so it is only the fast path. The pair script also records a
// claim per ticket ("node X popped it"), and the popping node writes each remote
// assignment to Redis before publishing it. A waiting node whose ticket left the queue but
// got no message looks there:
//
//   - an assignment is recorded: the message was lost or is late; deliver the record;
//   - the claimer is alive and still working: wait, however slow it is;
//   - the claimer is dead (or its claim expired) and nothing was recorded: the match was
//     never announced, so the ticket goes back into the queue.
//
// A ticket is therefore requeued only when no assignment can still arrive, and a player
// is never put into two matches because a message was slow. (Before, any ticket missing
// for 3 s was requeued; a late message then left one of the two matches waiting for a
// player who never came.)
type Matchmaker struct {
	rdb    redis.UniversalClient
	prefix string
	node   string
	url    string
	create lobby.CreateFunc
	met    *metrics.Metrics
	log    *slog.Logger

	mu      sync.Mutex
	waiting map[string]*waiter // by ticket id
	byT     map[*lobby.Ticket]string
	closed  bool

	sub    *redis.PubSub
	stop   chan struct{}
	stopWG sync.WaitGroup
}

type waiter struct {
	id           string
	t            *lobby.Ticket
	raw          string
	missingSince time.Time // popped but not assigned yet; zero while queued
}

// assignMsg is published to the channel of the node a player is connected to.
type assignMsg struct {
	Ticket   string `json:"ticket"`
	MatchID  string `json:"matchId"`
	Seat     int    `json:"seat"`
	Token    string `json:"token"`
	Opponent string `json:"opponent"`
	URL      string `json:"url"`
}

// Timings (variables so tests can shorten them).
var (
	pollEvery      = 200 * time.Millisecond
	requeueAfter   = 3 * time.Second // popped, unclaimed or claimer dead, nothing recorded
	checkLostEvery = time.Second
	claimTTL       = 30 * time.Second // a live popper that has not announced by then failed
	assignTTL      = time.Minute
)

// KEYS: queue. ARGV: alive key prefix, claim key prefix, this node, claim ttl ms. Pops the
// two oldest tickets whose node is alive and claims them for this node.
var pairScript = redis.NewScript(`
local function alive(raw)
  local a = string.find(raw, '\t', 1, true)
  if not a then return false end
  local b = string.find(raw, '\t', a + 1, true)
  if not b then return false end
  return redis.call('EXISTS', ARGV[1] .. string.sub(raw, a + 1, b - 1)) == 1
end
local picked = {}
while #picked < 2 do
  local raw = redis.call('LPOP', KEYS[1])
  if not raw then break end
  if alive(raw) then picked[#picked + 1] = raw end
end
if #picked == 1 then
  redis.call('LPUSH', KEYS[1], picked[1])
  return {}
end
for _, raw in ipairs(picked) do
  local a = string.find(raw, '\t', 1, true)
  redis.call('SET', ARGV[2] .. string.sub(raw, 1, a - 1), ARGV[3], 'PX', ARGV[4])
end
return picked
`)

// NewMatchmaker starts the subscription and the background pairing loop. create starts a
// match on this node. url is this node's public WebSocket URL.
func NewMatchmaker(rdb redis.UniversalClient, prefix, node, url string, create lobby.CreateFunc, met *metrics.Metrics, log *slog.Logger) (*Matchmaker, error) {
	m := &Matchmaker{
		rdb: rdb, prefix: prefix, node: node, url: url, create: create, met: met, log: log,
		waiting: map[string]*waiter{}, byT: map[*lobby.Ticket]string{}, stop: make(chan struct{}),
	}
	m.sub = rdb.Subscribe(context.Background(), m.channel(node))
	// Wait for the subscription to be live before any ticket can be assigned to us.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := m.sub.Receive(ctx); err != nil {
		m.sub.Close()
		return nil, err
	}
	m.stopWG.Add(2)
	go m.receive()
	go m.loop()
	return m, nil
}

func (m *Matchmaker) queueKey() string           { return m.prefix + "mm:queue" }
func (m *Matchmaker) channel(node string) string { return m.prefix + "mm:to:" + node }
func (m *Matchmaker) alivePrefix() string        { return m.prefix + "alive:" }
func (m *Matchmaker) claimKey(id string) string  { return m.prefix + "mm:claim:" + id }
func (m *Matchmaker) assignKey(id string) string { return m.prefix + "mm:assign:" + id }

func ticketID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// JoinContext implements the server's matchmaker interface.
func (m *Matchmaker) JoinContext(ctx context.Context, name string) *lobby.Ticket {
	t := lobby.NewTicket(ctx, name)
	id := ticketID()
	w := &waiter{id: id, t: t, raw: id + "\t" + m.node + "\t" + name}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		t.Deliver(lobby.Assignment{Err: lobby.ErrClosed})
		return t
	}
	m.waiting[id], m.byT[t] = w, id
	m.mu.Unlock()

	op, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.rdb.RPush(op, m.queueKey(), w.raw).Err(); err != nil {
		m.forget(id)
		m.met.ClusterErrors.WithLabelValues("queue").Inc()
		t.Deliver(lobby.Assignment{Err: err})
		return t
	}
	m.tryPair(op)
	return t
}

// Leave removes a ticket that is still queued. False means it was already popped: its
// assignment is on the way (or was lost, and the match will time out).
func (m *Matchmaker) Leave(t *lobby.Ticket) bool {
	m.mu.Lock()
	id, ok := m.byT[t]
	var raw string
	if ok {
		raw = m.waiting[id].raw
	}
	m.mu.Unlock()
	if !ok {
		return false
	}
	op, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	n, err := m.rdb.LRem(op, m.queueKey(), 1, raw).Result()
	if err != nil || n == 0 {
		return false
	}
	m.forget(id)
	return true
}

// Close stops matchmaking on this node: its queued players are taken out of the shared
// queue and told to go elsewhere.
func (m *Matchmaker) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	ws := make([]*waiter, 0, len(m.waiting))
	for _, w := range m.waiting {
		ws = append(ws, w)
	}
	m.waiting, m.byT = map[string]*waiter{}, map[*lobby.Ticket]string{}
	m.mu.Unlock()
	op, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, w := range ws {
		m.rdb.LRem(op, m.queueKey(), 1, w.raw)
		w.t.Deliver(lobby.Assignment{Err: lobby.ErrClosed})
	}
	close(m.stop)
	m.sub.Close()
	m.stopWG.Wait()
}

func (m *Matchmaker) forget(id string) *waiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := m.waiting[id]
	if w != nil {
		delete(m.waiting, id)
		delete(m.byT, w.t)
	}
	return w
}

// tryPair pops and starts matches while there are two live tickets in the queue.
func (m *Matchmaker) tryPair(ctx context.Context) {
	for {
		m.mu.Lock()
		closed := m.closed
		m.mu.Unlock()
		if closed {
			return
		}
		pair, err := pairScript.Run(ctx, m.rdb, []string{m.queueKey()},
			m.alivePrefix(), m.claimKey(""), m.node, claimTTL.Milliseconds()).StringSlice()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				m.met.ClusterErrors.WithLabelValues("pair").Inc()
			}
			return
		}
		if len(pair) != 2 {
			return
		}
		m.start(ctx, pair)
	}
}

type ticket struct{ id, node, name string }

func parseTicket(raw string) (ticket, bool) {
	f := strings.SplitN(raw, "\t", 3)
	if len(f) != 3 {
		return ticket{}, false
	}
	return ticket{f[0], f[1], f[2]}, true
}

// start hosts a match for two popped tickets and tells both players where it is.
func (m *Matchmaker) start(ctx context.Context, pair []string) {
	a, okA := parseTicket(pair[0])
	b, okB := parseTicket(pair[1])
	if !okA || !okB {
		m.log.Warn("dropping malformed matchmaking tickets", "a", pair[0], "b", pair[1])
		return
	}
	id, tokens, err := m.create([2]string{a.name, b.name})
	if err != nil {
		// Cannot host (shutting down): give the pair back, oldest first, to another node.
		m.rdb.LPush(ctx, m.queueKey(), pair[1], pair[0])
		m.rdb.Del(ctx, m.claimKey(a.id), m.claimKey(b.id))
		return
	}
	for seat, t := range []ticket{a, b} {
		opp := []ticket{b, a}[seat]
		as := assignMsg{Ticket: t.id, MatchID: id, Seat: seat, Token: tokens[seat], Opponent: opp.name}
		if t.node == m.node {
			m.met.Matchmaking.WithLabelValues("local").Inc()
			m.deliver(as)
			continue
		}
		as.URL = m.url
		m.met.Matchmaking.WithLabelValues("remote").Inc()
		body, _ := json.Marshal(as)
		// Record first, then publish: if the message is lost, the record is still there.
		_, err := m.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, m.assignKey(t.id), body, assignTTL)
			p.Publish(ctx, m.channel(t.node), body)
			return nil
		})
		if err != nil {
			m.met.ClusterErrors.WithLabelValues("publish").Inc()
		}
	}
}

func (m *Matchmaker) deliver(as assignMsg) {
	w := m.forget(as.Ticket)
	if w == nil {
		m.met.Matchmaking.WithLabelValues("stale").Inc() // the player left meanwhile
		return
	}
	w.t.Deliver(lobby.Assignment{MatchID: as.MatchID, Seat: as.Seat, Token: as.Token, Opponent: as.Opponent, URL: as.URL})
}

func (m *Matchmaker) receive() {
	defer m.stopWG.Done()
	for msg := range m.sub.Channel() {
		var as assignMsg
		if json.Unmarshal([]byte(msg.Payload), &as) != nil {
			continue
		}
		m.deliver(as)
	}
}

// loop pairs tickets pushed by any node and re-queues tickets whose assignment was lost.
func (m *Matchmaker) loop() {
	defer m.stopWG.Done()
	poll := time.NewTicker(pollEvery)
	defer poll.Stop()
	lost := time.NewTicker(checkLostEvery)
	defer lost.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-poll.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			m.tryPair(ctx)
			cancel()
		case <-lost.C:
			m.requeueLost()
		}
	}
}

func (m *Matchmaker) requeueLost() {
	m.mu.Lock()
	ws := make([]*waiter, 0, len(m.waiting))
	for _, w := range m.waiting {
		ws = append(ws, w)
	}
	m.mu.Unlock()
	if len(ws) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := time.Now()
	for _, w := range ws {
		if w.t.Gone() {
			if n, _ := m.rdb.LRem(ctx, m.queueKey(), 1, w.raw).Result(); n > 0 {
				m.forgetTicket(w)
			}
			continue
		}
		_, err := m.rdb.LPos(ctx, m.queueKey(), w.raw, redis.LPosArgs{}).Result()
		switch {
		case err == nil: // still queued
			m.setMissing(w, time.Time{})
		case errors.Is(err, redis.Nil):
			if m.popped(ctx, w) != poppedLost {
				m.setMissing(w, time.Time{})
				continue
			}
			since := m.setMissing(w, now)
			if now.Sub(since) >= requeueAfter && m.stillWaiting(w) {
				m.met.Matchmaking.WithLabelValues("requeued").Inc()
				m.rdb.Del(ctx, m.claimKey(w.id))
				m.rdb.RPush(ctx, m.queueKey(), w.raw)
				m.setMissing(w, time.Time{})
			}
		}
	}
}

type poppedState int

const (
	poppedLost     poppedState = iota // nobody will announce it: requeue (after requeueAfter)
	poppedAssigned                    // the assignment was recorded, and is now delivered
	poppedPending                     // a live node claimed it and is still working
	poppedUnknown                     // Redis did not answer: decide next time
)

// popped finds out what happened to a ticket that left the queue without its assignment
// reaching this node.
func (m *Matchmaker) popped(ctx context.Context, w *waiter) poppedState {
	var assign, claim *redis.StringCmd
	_, err := m.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		assign = p.Get(ctx, m.assignKey(w.id))
		claim = p.Get(ctx, m.claimKey(w.id))
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return poppedUnknown
	}
	if body, err := assign.Bytes(); err == nil {
		var as assignMsg
		if json.Unmarshal(body, &as) == nil {
			m.met.Matchmaking.WithLabelValues("recovered").Inc()
			m.deliver(as)
			return poppedAssigned
		}
	}
	claimer, err := claim.Result()
	if err != nil {
		return poppedLost // never claimed, or the claim expired
	}
	alive, err := m.rdb.Exists(ctx, m.alivePrefix()+claimer).Result()
	switch {
	case err != nil:
		return poppedUnknown
	case alive == 1:
		return poppedPending
	}
	return poppedLost
}

func (m *Matchmaker) forgetTicket(w *waiter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.byT[w.t]; ok {
		delete(m.waiting, id)
		delete(m.byT, w.t)
	}
}

func (m *Matchmaker) stillWaiting(w *waiter) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.byT[w.t]
	return ok
}

// setMissing records since when w has been neither queued nor assigned (zero clears it)
// and returns that time.
func (m *Matchmaker) setMissing(w *waiter, at time.Time) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if at.IsZero() || w.missingSince.IsZero() {
		w.missingSince = at
	}
	return w.missingSince
}
