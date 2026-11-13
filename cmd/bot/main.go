// Command bot is a load generator. It starts N pairs of WebSocket clients that queue, get
// matched and play random moves. With -duration they re-queue after every match, which
// keeps the load constant for a fixed time. While running it scrapes the server's
// /metrics once per second, and at the end prints a summary (optionally also as JSON).
//
// With -proto bin the bots use the binary wire protocol (WebSocket subprotocol
// sabostack.bin.v1): inputs are sent as binary frames and every state delta is decoded
// and applied to the bot's copy of the match state, as a real client would.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Gegirhasut/sabostack-go-server/internal/game"
	"github.com/Gegirhasut/sabostack-go-server/internal/protocol"
	"github.com/Gegirhasut/sabostack-go-server/internal/wire"
)

type config struct {
	URL        string        `json:"url"`
	MetricsURL string        `json:"metricsUrl"`
	Pairs      int           `json:"pairs"`
	Rate       float64       `json:"inputsPerSecondPerBot"`
	Ramp       time.Duration `json:"rampDelayPerBot"`
	Duration   time.Duration `json:"duration"` // 0 = play one match per bot and stop
	Warmup     time.Duration `json:"warmup"`   // after the ramp, before measuring
	StormAt    time.Duration `json:"stormAt"`  // 0 = no reconnect storm
	StormFrac  float64       `json:"stormFraction"`
	Proto      string        `json:"proto"` // "json" or "bin"
	// ResumeOnDrop: a socket dropped mid-match is resumed (on any server) instead of
	// counting as a dropped connection.
	ResumeOnDrop bool   `json:"resumeOnDrop"`
	Label        string `json:"label"`
}

var actions = []game.Action{game.Left, game.Right, game.Rotate, game.SoftDrop, game.HardDrop}

// stats is shared by all bots; counters only count after measuring starts.
type stats struct {
	measuring atomic.Bool

	matches, errors, dropped  atomic.Int64
	msgs, bytes, inputs       atomic.Int64
	bytesSent                 atomic.Int64
	redirects                 atomic.Int64 // matched.url or wrong_server followed
	dropsRecovered, dropsLost atomic.Int64
	violations, printed       atomic.Int64 // protocol/game invariant violations (check.go)
	stormTried, stormOK       atomic.Int64
	stormFailed, stormSkipped atomic.Int64

	mu    sync.Mutex
	ack   []time.Duration // input sent -> first state acknowledging it
	recon []time.Duration // socket dropped -> resume snapshot received
	dropR []time.Duration // unexpected drop -> resumed (possibly on another server)
}

// reconnected records a finished reconnect of the given kind ("" = none pending).
func (s *stats) reconnected(kind string, d time.Duration) {
	switch kind {
	case "storm":
		s.mu.Lock()
		s.recon = append(s.recon, d)
		s.mu.Unlock()
		s.stormOK.Add(1)
	case "drop":
		s.mu.Lock()
		s.dropR = append(s.dropR, d)
		s.mu.Unlock()
		s.dropsRecovered.Add(1)
	}
}

func (s *stats) addAck(d time.Duration) {
	if !s.measuring.Load() {
		return
	}
	s.mu.Lock()
	s.ack = append(s.ack, d)
	s.mu.Unlock()
}

func main() {
	var c config
	flag.StringVar(&c.URL, "url", "ws://localhost:8080/ws", "server WebSocket URL; a comma-separated list spreads the bots over several servers")
	flag.StringVar(&c.MetricsURL, "metrics", "http://localhost:8080/metrics", "server metrics URL, or a comma-separated list summed over all servers (empty to skip)")
	flag.BoolVar(&c.ResumeOnDrop, "resume-on-drop", false, "resume a match after an unexpected disconnect (through any server) instead of giving it up")
	flag.IntVar(&c.Pairs, "pairs", 50, "number of bot pairs")
	flag.Float64Var(&c.Rate, "rate", 10, "inputs per second per bot")
	flag.DurationVar(&c.Ramp, "ramp", 2*time.Millisecond, "delay between starting bots")
	flag.DurationVar(&c.Duration, "duration", 0, "keep playing (re-queue after each match) for this long; 0 = one match")
	flag.DurationVar(&c.Warmup, "warmup", 10*time.Second, "with -duration: settle time after the ramp before measuring")
	flag.DurationVar(&c.StormAt, "storm-at", 0, "with -duration: drop and resume -storm-frac of the bots this long after measuring starts")
	flag.Float64Var(&c.StormFrac, "storm-frac", 0.2, "fraction of bots dropped in the reconnect storm")
	flag.StringVar(&c.Proto, "proto", "json", "wire protocol: json, or bin (binary state deltas)")
	flag.StringVar(&c.Label, "label", "", "run label stored in the JSON output")
	jsonOut := flag.String("json", "", "also write the result as JSON to this file")
	flag.Parse()
	if c.Proto != "json" && c.Proto != "bin" {
		fmt.Fprintln(os.Stderr, "-proto must be json or bin")
		os.Exit(2)
	}

	res := run(c)
	res.print()
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func run(c config) *result {
	st := &stats{}
	bots := c.Pairs * 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var urls []string
	for _, u := range strings.Split(c.URL, ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	storms := make([]chan struct{}, bots)
	stormEvery := 0
	if c.StormAt > 0 && c.StormFrac > 0 {
		stormEvery = max(1, int(1/c.StormFrac+0.5))
	}

	var sampler *sampler
	if c.MetricsURL != "" {
		sampler = startSampler(ctx, c.MetricsURL, st)
	}

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < bots; i++ {
		storms[i] = make(chan struct{})
		var storm <-chan struct{}
		if stormEvery > 0 && i%stormEvery == 0 {
			storm = storms[i]
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := &bot{cfg: c, name: fmt.Sprintf("bot%d", i), st: st, storm: storm, urls: urls, entry: urls[i%len(urls)]}
			b.loop(ctx)
		}()
		time.Sleep(c.Ramp)
	}
	rampDone := time.Now()

	var measureStart, measureEnd time.Time
	if c.Duration > 0 {
		time.Sleep(c.Warmup)
		measureStart = time.Now()
		st.measuring.Store(true)
		if c.StormAt > 0 && stormEvery > 0 {
			go func() {
				time.Sleep(c.StormAt)
				for i := 0; i < bots; i += stormEvery {
					close(storms[i])
				}
			}()
		}
		time.Sleep(c.Duration)
		measureEnd = time.Now()
		st.measuring.Store(false)
		if sampler != nil {
			sampler.scrapeNow() // end-of-window sample, taken while all bots are still connected
		}
		cancel()
		wg.Wait()
	} else {
		measureStart = start
		st.measuring.Store(true)
		wg.Wait()
		measureEnd = time.Now()
	}
	if sampler != nil && c.Duration == 0 {
		sampler.scrapeNow() // final sample
	}
	cancel()
	return summarize(c, st, sampler, start, rampDone, measureStart, measureEnd)
}

// ---------------------------------------------------------------------------- bots

type bot struct {
	cfg   config
	urls  []string // every server, for drop recovery
	entry string   // the server this bot joins through (its "load balancer" pick)
	name  string
	st    *stats
	storm <-chan struct{} // closed when this bot should drop its socket and resume
}

var errDropped = errors.New("connection dropped by server")

// loop plays matches until ctx ends (or once, without -duration).
func (b *bot) loop(ctx context.Context) {
	every := time.Duration(float64(time.Second) / b.cfg.Rate)
	for {
		err := b.playMatch(ctx, every)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if b.st.measuring.Load() {
				if errors.Is(err, errDropped) {
					b.st.dropped.Add(1)
				} else {
					b.st.errors.Add(1)
				}
			}
			fmt.Fprintf(os.Stderr, "%s: %v\n", b.name, err)
			time.Sleep(time.Second)
		}
		if b.cfg.Duration == 0 {
			return
		}
	}
}

type frame struct {
	binary bool
	b      []byte
}

type conn struct {
	ws     *websocket.Conn
	binary bool
	in     chan frame
	done   chan struct{}
}

func dial(ctx context.Context, url string, binary bool) (*conn, error) {
	d := *websocket.DefaultDialer
	// A dead server's address may not even refuse the connection (its IP is gone): give
	// up after a few seconds like a real client, not after the OS connect timeout.
	d.HandshakeTimeout = 3 * time.Second
	d.NetDialContext = (&net.Dialer{Timeout: 3 * time.Second}).DialContext
	if binary {
		d.Subprotocols = []string{wire.Subprotocol}
	}
	ws, _, err := d.DialContext(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	if binary && ws.Subprotocol() != wire.Subprotocol {
		ws.Close()
		return nil, errors.New("server did not accept the binary subprotocol")
	}
	c := &conn{ws: ws, binary: binary, in: make(chan frame, 64), done: make(chan struct{})}
	go func() {
		defer close(c.in)
		for {
			mt, b, err := ws.ReadMessage()
			if err != nil {
				return
			}
			select {
			case c.in <- frame{mt == websocket.BinaryMessage, b}:
			case <-c.done:
				return
			}
		}
	}()
	return c, nil
}

// sendInput sends one input in the connection's protocol and returns the bytes written.
func (c *conn) sendInput(seq uint64, a game.Action, buf []byte) ([]byte, int, error) {
	if c.binary {
		buf = wire.AppendInput(buf[:0], seq, a)
		return buf, len(buf), c.ws.WriteMessage(websocket.BinaryMessage, buf)
	}
	b, err := json.Marshal(protocol.ClientMsg{Type: protocol.TypeInput, Seq: seq, Action: a})
	if err != nil {
		return buf, 0, err
	}
	return buf, len(b), c.ws.WriteMessage(websocket.TextMessage, b)
}

func (c *conn) close() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	c.ws.Close()
}

type serverMsg struct {
	Type    string         `json:"type"`
	Phase   string         `json:"phase"`
	Ack     uint64         `json:"ack"`
	MatchID string         `json:"matchId"`
	Token   string         `json:"token"`
	Event   protocol.Event `json:"event"`
	Code    string         `json:"code"` // error
	URL     string         `json:"url"`  // matched (match elsewhere), error wrong_server
	State   protocol.State `json:"state"`
	// state (decoded in the same pass: a real client needs the boards to draw them)
	Tick        uint64                `json:"tick"`
	Round       int                   `json:"round"`
	RemainingMs int64                 `json:"remainingMs"`
	EventSeq    uint64                `json:"eventSeq"`
	Players     []protocol.PlayerView `json:"players"`
	// snapshot
	Events         []protocol.Event `json:"events"`
	EventsComplete bool             `json:"eventsComplete"`
}

// playMatch runs one match. A single goroutine owns all bot state; a reader goroutine per
// socket only forwards raw frames.
//
// The bot moves between servers like a real client: it follows matched.url and
// wrong_server redirects, and with -resume-on-drop it treats a dropped socket in the
// middle of a match (its server died, say) as a reason to reconnect through its entry
// server and resume, not as a lost match.
func (b *bot) playMatch(ctx context.Context, every time.Duration) error {
	bin := b.cfg.Proto == "bin"
	chk := &checker{bot: b.name, violations: &b.st.violations, printed: &b.st.printed}
	// Join through the entry server; if it is down, through the next one, as a load
	// balancer would.
	var c *conn
	var err error
	for i, start := 0, slices.Index(b.urls, b.entry); i < len(b.urls); i++ {
		if c, err = dial(ctx, b.urls[(start+i)%len(b.urls)], bin); err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	defer func() { c.close() }()
	if err := c.ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeJoin, Name: b.name}); err != nil {
		return err
	}

	var (
		matchID, token string
		lastEvent      uint64
		playing        bool
		seq            uint64
		sent           = map[uint64]time.Time{}
		storm          = b.storm
		state          protocol.State // binary mode: base for the next delta
		haveState      bool
		inBuf          []byte
		// A reconnect in progress: why, since when, and the deadline for drop recovery.
		pending   string // "", "storm", "redirect", "drop"
		pendingAt time.Time
		nextURL   = 0
	)
	ack := func(upto uint64, now time.Time) {
		for s, at := range sent {
			if s <= upto {
				b.st.addAck(now.Sub(at))
				delete(sent, s)
			}
		}
	}
	// reattach moves to url (a new socket) and resumes the match there.
	reattach := func(url, why string) error {
		c.close()
		haveState, playing = false, false // the new connection starts from its snapshot
		if pending == "" || why != "drop" {
			pending, pendingAt = why, time.Now()
		}
		nc, err := dial(ctx, url, bin)
		if err != nil {
			return err
		}
		c = nc
		return c.ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeResume, MatchID: matchID, Token: token, LastEventSeq: lastEvent})
	}
	// recoverDrop retries a resume on the servers in turn until one answers or 30 s pass.
	recoverDrop := func() error {
		for {
			if time.Since(pendingAt) > dropRecoveryWindow {
				b.st.dropsLost.Add(1)
				return errDropped
			}
			url := b.urls[nextURL%len(b.urls)]
			nextURL++
			if err := reattach(url, "drop"); err == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	// The match is created the instant the second bot joins, so a plain ticker started
	// here would fire in lockstep with the server tick. Start at a random offset instead.
	tick := time.NewTimer(rand.N(every))
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case fr, ok := <-c.in:
			if !ok {
				if matchID == "" || !b.cfg.ResumeOnDrop {
					return errDropped
				}
				if pending == "" {
					pending, pendingAt = "drop", time.Now()
				}
				if err := recoverDrop(); err != nil {
					return err
				}
				continue
			}
			raw := fr.b
			now := time.Now()
			if b.st.measuring.Load() {
				b.st.msgs.Add(1)
				b.st.bytes.Add(int64(len(raw)))
			}
			if fr.binary {
				if len(raw) == 0 || raw[0] != wire.TypeState {
					continue // pong
				}
				var base *protocol.State
				if haveState {
					base = &state
				}
				next, err := wire.DecodeState(base, raw)
				if err != nil {
					return fmt.Errorf("bad state frame: %w", err)
				}
				state, haveState = next, true
				chk.state(state)
				// Matches never pause (state.paused is always false), so a bot plays
				// whenever the round is on, opponent connected or not.
				playing = state.Phase == "playing"
				ack(state.Ack, now)
				continue
			}
			var m serverMsg
			if json.Unmarshal(raw, &m) != nil {
				continue
			}
			switch m.Type {
			case protocol.TypeMatched:
				matchID, token = m.MatchID, m.Token
				if m.URL != "" { // the match runs on another server
					b.st.redirects.Add(1)
					if err := reattach(m.URL, "redirect"); err != nil {
						return err
					}
				}
			case protocol.TypeState:
				chk.state(protocol.State{
					Type: m.Type, Tick: m.Tick, Phase: m.Phase, Round: m.Round,
					RemainingMs: m.RemainingMs, Ack: m.Ack, EventSeq: m.EventSeq, Players: m.Players,
				})
				playing = m.Phase == "playing"
				ack(m.Ack, now)
			case protocol.TypeSnapshot:
				chk.snapshot(protocol.Snapshot{State: m.State, Events: m.Events, EventsComplete: m.EventsComplete})
				state, haveState = m.State, true
				playing = m.State.Phase == "playing"
				ack(m.State.Ack, now)
				b.st.reconnected(pending, now.Sub(pendingAt))
				pending = ""
			case protocol.TypeEvent:
				chk.event(m.Event)
				lastEvent = m.Event.Seq
				if m.Event.Kind == protocol.EvMatchOver {
					chk.end(true)
					if b.st.measuring.Load() {
						b.st.matches.Add(1)
					}
					return nil
				}
			case protocol.TypeError:
				switch {
				case m.Code == "wrong_server" && m.URL != "":
					b.st.redirects.Add(1)
					if err := reattach(m.URL, pendingOr(pending, "redirect")); err != nil && pending != "drop" {
						return err
					}
				case pending == "drop" && (m.Code == "match_moving" || m.Code == "match_not_found" || m.Code == "unavailable"):
					// The match is between nodes (its server died, no one adopted it yet):
					// keep trying until the recovery window closes.
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(500 * time.Millisecond):
					}
					if err := recoverDrop(); err != nil {
						return err
					}
				default:
					if pending == "storm" {
						b.st.stormFailed.Add(1)
					}
					return fmt.Errorf("server error: %s", raw)
				}
			}

		case <-storm:
			storm, b.storm = nil, nil // a bot takes part in one storm only
			if matchID == "" {
				b.st.stormSkipped.Add(1) // still queued: nothing to resume
				continue
			}
			b.st.stormTried.Add(1)
			if err := reattach(b.entry, "storm"); err != nil {
				b.st.stormFailed.Add(1)
				return err
			}

		case <-tick.C:
			tick.Reset(every)
			if pending != "" && pending != "drop" && time.Since(pendingAt) > 10*time.Second {
				if pending == "storm" {
					b.st.stormFailed.Add(1)
				}
				return errors.New("resume timed out")
			}
			if !playing {
				continue
			}
			seq++
			sent[seq] = time.Now()
			chk.sent(seq)
			a := actions[rand.IntN(len(actions))]
			if rand.IntN(3) == 0 { // bias towards drops so rounds finish
				a = game.HardDrop
			}
			var n int
			if inBuf, n, err = c.sendInput(seq, a, inBuf); err != nil {
				continue // the reader sees the broken socket and recovers or gives up
			}
			if b.st.measuring.Load() {
				b.st.inputs.Add(1)
				b.st.bytesSent.Add(int64(n))
			}
		}
	}
}

func pendingOr(p, def string) string {
	if p != "" {
		return p
	}
	return def
}

// dropRecoveryWindow is how long a bot keeps trying to resume after its server vanished.
const dropRecoveryWindow = 30 * time.Second

// ---------------------------------------------------------------------------- metrics

// sample is one scrape of the server's /metrics plus the bot-side counters at that time.
type sample struct {
	T             float64 `json:"t"` // seconds since the bot started
	CPUSeconds    float64 `json:"cpuSeconds"`
	RSSBytes      float64 `json:"rssBytes"`
	Goroutines    float64 `json:"goroutines"`
	ActiveMatches float64 `json:"activeMatches"`
	LiveMatches   float64 `json:"liveMatches"`
	Connections   float64 `json:"connections"`
	TickSum       float64 `json:"tickSum"`
	TickCount     float64 `json:"tickCount"`
	TickMax       float64 `json:"tickMax"`
	MsgsReceived  int64   `json:"msgsReceived"` // bot-side, measuring window only
	WSBytesOut    float64 `json:"wsBytesOut"`   // server-side payload bytes sent (all formats)
	WSBytesIn     float64 `json:"wsBytesIn"`
}

type sampler struct {
	url   string
	st    *stats
	start time.Time

	mu      sync.Mutex
	samples []sample
}

func startSampler(ctx context.Context, url string, st *stats) *sampler {
	s := &sampler{url: url, st: st, start: time.Now()}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.scrapeNow()
			}
		}
	}()
	return s
}

// scrapeNow takes one sample. With several metrics URLs (one per server) the values are
// summed over the servers (the tick maximum is the largest); a server that does not answer
// is left out of that sample.
func (s *sampler) scrapeNow() {
	smp := sample{T: time.Since(s.start).Seconds(), MsgsReceived: s.st.msgs.Load()}
	for _, u := range strings.Split(s.url, ",") {
		if u = strings.TrimSpace(u); u != "" {
			scrapeInto(u, &smp)
		}
	}
	s.mu.Lock()
	s.samples = append(s.samples, smp)
	s.mu.Unlock()
}

func scrapeInto(url string, smp *sample) {
	cl := http.Client{Timeout: 3 * time.Second}
	resp, err := cl.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 2 || strings.HasPrefix(f[0], "#") {
			continue
		}
		v, _ := strconv.ParseFloat(f[1], 64)
		if strings.HasPrefix(f[0], "sabostack_ws_bytes_total{") {
			if strings.Contains(f[0], `dir="out"`) {
				smp.WSBytesOut += v
			} else {
				smp.WSBytesIn += v
			}
			continue
		}
		switch f[0] {
		case "process_cpu_seconds_total":
			smp.CPUSeconds += v
		case "process_resident_memory_bytes":
			smp.RSSBytes += v
		case "go_goroutines":
			smp.Goroutines += v
		case "sabostack_active_matches":
			smp.ActiveMatches += v
		case "sabostack_live_matches":
			smp.LiveMatches += v
		case "sabostack_connected_players":
			smp.Connections += v
		case "sabostack_tick_duration_seconds_sum":
			smp.TickSum += v
		case "sabostack_tick_duration_seconds_count":
			smp.TickCount += v
		case "sabostack_tick_duration_max_seconds":
			smp.TickMax = max(smp.TickMax, v)
		}
	}
}

// ---------------------------------------------------------------------------- results

type result struct {
	Config config `json:"config"`

	Bots            int     `json:"bots"`
	RampSeconds     float64 `json:"rampSeconds"`
	MeasureSeconds  float64 `json:"measureSeconds"`
	MatchesFinished int64   `json:"matchesFinished"` // match_over seen by bots, /2
	Errors          int64   `json:"errors"`
	Violations      int64   `json:"violations"` // protocol/game invariant violations seen by bots
	Redirects       int64   `json:"redirects"`
	DropsRecovered  int64   `json:"dropsRecovered"`
	DropsLost       int64   `json:"dropsLost"`
	DropRecP50Ms    float64 `json:"dropRecoveryP50Ms"`
	DropRecP95Ms    float64 `json:"dropRecoveryP95Ms"`
	DropRecMaxMs    float64 `json:"dropRecoveryMaxMs"`
	Dropped         int64   `json:"droppedConnections"`
	InputsSent      int64   `json:"inputsSent"`
	MsgsReceived    int64   `json:"msgsReceived"`
	MsgsPerSecond   float64 `json:"msgsPerSecond"`
	MBReceived      float64 `json:"mbReceived"`
	MBSent          float64 `json:"mbSent"` // input frames, bot-side

	// Server-side traffic over the measuring window, per connected client.
	ServerOutKBps float64 `json:"serverOutKBpsPerClient"`
	ServerInKBps  float64 `json:"serverInKBpsPerClient"`

	AckSamples int     `json:"ackSamples"`
	AckP50Ms   float64 `json:"ackP50Ms"`
	AckP95Ms   float64 `json:"ackP95Ms"`
	AckP99Ms   float64 `json:"ackP99Ms"`
	AckMaxMs   float64 `json:"ackMaxMs"`

	// Server-side, over the measuring window (from /metrics scrapes).
	ConnectionsAvg   float64 `json:"connectionsAvg"`
	ConnectionsMin   float64 `json:"connectionsMin"`
	ActiveMatchesAvg float64 `json:"activeMatchesAvg"` // incl. finished matches lingering for resumes
	LiveMatchesAvg   float64 `json:"liveMatchesAvg"`   // not yet finished
	CPUPercent       float64 `json:"cpuPercentOfOneCore"`
	RSSMBAvg         float64 `json:"rssMbAvg"`
	RSSMBMax         float64 `json:"rssMbMax"`
	RSSMBStart       float64 `json:"rssMbStart"`
	RSSMBEnd         float64 `json:"rssMbEnd"`
	GoroutinesAvg    float64 `json:"goroutinesAvg"`
	GoroutinesStart  float64 `json:"goroutinesStart"`
	GoroutinesEnd    float64 `json:"goroutinesEnd"`
	TickMeanUs       float64 `json:"tickMeanUs"`
	TickMaxMs        float64 `json:"tickMaxMsProcessLifetime"`

	Storm *stormResult `json:"storm,omitempty"`

	Samples []sample `json:"samples"`
}

type stormResult struct {
	Selected   int64   `json:"selected"`
	Skipped    int64   `json:"skippedNotInMatch"`
	Attempted  int64   `json:"attempted"`
	Recovered  int64   `json:"recovered"`
	Failed     int64   `json:"failed"`
	SuccessPct float64 `json:"successPct"`
	P50Ms      float64 `json:"recoveryP50Ms"`
	P95Ms      float64 `json:"recoveryP95Ms"`
	MaxMs      float64 `json:"recoveryMaxMs"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, len(sorted)*p/100)]
}

func summarize(c config, st *stats, smp *sampler, start, rampDone, mStart, mEnd time.Time) *result {
	r := &result{Config: c, Bots: c.Pairs * 2}
	r.RampSeconds = rampDone.Sub(start).Seconds()
	r.MeasureSeconds = mEnd.Sub(mStart).Seconds()
	r.MatchesFinished = st.matches.Load() / 2
	r.Errors, r.Dropped = st.errors.Load(), st.dropped.Load()
	r.Violations = st.violations.Load()
	r.Redirects, r.DropsRecovered, r.DropsLost = st.redirects.Load(), st.dropsRecovered.Load(), st.dropsLost.Load()
	r.InputsSent, r.MsgsReceived = st.inputs.Load(), st.msgs.Load()
	r.MsgsPerSecond = float64(r.MsgsReceived) / r.MeasureSeconds
	r.MBReceived = float64(st.bytes.Load()) / 1e6
	r.MBSent = float64(st.bytesSent.Load()) / 1e6

	st.mu.Lock()
	slices.Sort(st.ack)
	slices.Sort(st.recon)
	lat, recon := st.ack, st.recon
	slices.Sort(st.dropR)
	if dr := st.dropR; len(dr) > 0 {
		r.DropRecP50Ms, r.DropRecP95Ms, r.DropRecMaxMs = ms(pct(dr, 50)), ms(pct(dr, 95)), ms(dr[len(dr)-1])
	}
	st.mu.Unlock()
	r.AckSamples = len(lat)
	if len(lat) > 0 {
		r.AckP50Ms, r.AckP95Ms, r.AckP99Ms, r.AckMaxMs = ms(pct(lat, 50)), ms(pct(lat, 95)), ms(pct(lat, 99)), ms(lat[len(lat)-1])
	}
	if c.StormAt > 0 {
		s := &stormResult{
			Skipped: st.stormSkipped.Load(), Attempted: st.stormTried.Load(),
			Recovered: st.stormOK.Load(), Failed: st.stormFailed.Load(),
		}
		s.Selected = s.Skipped + s.Attempted
		if s.Attempted > 0 {
			s.SuccessPct = float64(s.Recovered) / float64(s.Attempted) * 100
		}
		if len(recon) > 0 {
			s.P50Ms, s.P95Ms, s.MaxMs = ms(pct(recon, 50)), ms(pct(recon, 95)), ms(recon[len(recon)-1])
		}
		r.Storm = s
	}

	if smp == nil {
		return r
	}
	smp.mu.Lock()
	r.Samples = smp.samples
	smp.mu.Unlock()
	from, to := mStart.Sub(smp.start).Seconds(), mEnd.Sub(smp.start).Seconds()+0.5
	var win []sample
	for _, s := range r.Samples {
		if s.T >= from && s.T <= to {
			win = append(win, s)
		}
	}
	if len(win) < 2 {
		return r
	}
	first, last := win[0], win[len(win)-1]
	dt := last.T - first.T
	r.CPUPercent = (last.CPUSeconds - first.CPUSeconds) / dt * 100
	if n := last.TickCount - first.TickCount; n > 0 {
		r.TickMeanUs = (last.TickSum - first.TickSum) / n * 1e6
	}
	r.TickMaxMs = last.TickMax * 1000
	r.RSSMBStart, r.RSSMBEnd = first.RSSBytes/1e6, last.RSSBytes/1e6
	r.GoroutinesStart, r.GoroutinesEnd = first.Goroutines, last.Goroutines
	r.ConnectionsMin = first.Connections
	for _, s := range win {
		r.ConnectionsAvg += s.Connections
		r.ActiveMatchesAvg += s.ActiveMatches
		r.LiveMatchesAvg += s.LiveMatches
		r.RSSMBAvg += s.RSSBytes / 1e6
		r.GoroutinesAvg += s.Goroutines
		r.RSSMBMax = max(r.RSSMBMax, s.RSSBytes/1e6)
		r.ConnectionsMin = min(r.ConnectionsMin, s.Connections)
	}
	n := float64(len(win))
	r.ConnectionsAvg /= n
	r.ActiveMatchesAvg /= n
	r.LiveMatchesAvg /= n
	r.RSSMBAvg /= n
	r.GoroutinesAvg /= n
	if r.ConnectionsAvg > 0 {
		r.ServerOutKBps = (last.WSBytesOut - first.WSBytesOut) / dt / r.ConnectionsAvg / 1024
		r.ServerInKBps = (last.WSBytesIn - first.WSBytesIn) / dt / r.ConnectionsAvg / 1024
	}
	return r
}

func (r *result) print() {
	fmt.Printf("bots:               %d (%d pairs)\n", r.Bots, r.Config.Pairs)
	fmt.Printf("ramp / measured:    %.1fs / %.1fs\n", r.RampSeconds, r.MeasureSeconds)
	fmt.Printf("matches finished:   %d\n", r.MatchesFinished)
	fmt.Printf("errors / dropped:   %d / %d\n", r.Errors, r.Dropped)
	fmt.Printf("violations:         %d\n", r.Violations)
	if r.Redirects+r.DropsRecovered+r.DropsLost > 0 {
		fmt.Printf("cross-server:       %d redirects; drops resumed %d, lost %d; drop->resumed p50=%.0fms p95=%.0fms max=%.0fms\n",
			r.Redirects, r.DropsRecovered, r.DropsLost, r.DropRecP50Ms, r.DropRecP95Ms, r.DropRecMaxMs)
	}
	fmt.Printf("inputs sent:        %d\n", r.InputsSent)
	fmt.Printf("messages received:  %d (%.0f/s, %.1f MB); inputs sent: %.1f MB (%s)\n", r.MsgsReceived, r.MsgsPerSecond, r.MBReceived, r.MBSent, r.Config.Proto)
	fmt.Printf("input->ack latency: p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms (n=%d)\n", r.AckP50Ms, r.AckP95Ms, r.AckP99Ms, r.AckMaxMs, r.AckSamples)
	if r.Samples != nil {
		fmt.Printf("server connections: avg %.0f, min %.0f; live matches avg %.0f (%.0f incl. lingering)\n", r.ConnectionsAvg, r.ConnectionsMin, r.LiveMatchesAvg, r.ActiveMatchesAvg)
		fmt.Printf("server CPU:         %.1f%% of one core\n", r.CPUPercent)
		fmt.Printf("server RSS:         avg %.1f MB, max %.1f MB (start %.1f, end %.1f)\n", r.RSSMBAvg, r.RSSMBMax, r.RSSMBStart, r.RSSMBEnd)
		fmt.Printf("server goroutines:  avg %.0f (start %.0f, end %.0f)\n", r.GoroutinesAvg, r.GoroutinesStart, r.GoroutinesEnd)
		fmt.Printf("server tick:        mean %.1fµs, max %.2fms\n", r.TickMeanUs, r.TickMaxMs)
		if r.ServerOutKBps > 0 {
			fmt.Printf("server traffic:     %.2f KB/s out, %.2f KB/s in per client\n", r.ServerOutKBps, r.ServerInKBps)
		}
	}
	if s := r.Storm; s != nil {
		fmt.Printf("reconnect storm:    %d selected, %d in a match, %d recovered (%.1f%%), %d failed; recovery p50=%.1fms p95=%.1fms max=%.1fms\n",
			s.Selected, s.Attempted, s.Recovered, s.SuccessPct, s.Failed, s.P50Ms, s.P95Ms, s.MaxMs)
	}
}
