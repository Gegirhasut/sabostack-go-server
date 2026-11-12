// Command bot is a load generator: it opens N pairs of WebSocket clients that queue,
// get matched and play random moves until their match ends, then prints what it measured.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
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
)

var actions = []game.Action{game.Left, game.Right, game.Rotate, game.SoftDrop, game.HardDrop}

type stats struct {
	matches, errors     atomic.Int64
	msgs, bytes, inputs atomic.Int64

	mu  sync.Mutex
	ack []time.Duration // input sent -> first state acknowledging it
}

func main() {
	var (
		url     = flag.String("url", "ws://localhost:8080/ws", "server WebSocket URL")
		metrics = flag.String("metrics", "http://localhost:8080/metrics", "server metrics URL (empty to skip)")
		pairs   = flag.Int("pairs", 50, "number of bot pairs (matches)")
		rate    = flag.Float64("rate", 10, "inputs per second per bot")
		ramp    = flag.Duration("ramp", 2*time.Millisecond, "delay between starting bots")
		timeout = flag.Duration("timeout", 5*time.Minute, "give up after this long")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	st := &stats{}
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *pairs*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runBot(ctx, *url, fmt.Sprintf("bot%d", i), *rate, st); err != nil {
				st.errors.Add(1)
				fmt.Fprintf(os.Stderr, "bot%d: %v\n", i, err)
			}
		}()
		time.Sleep(*ramp)
	}
	wg.Wait()
	elapsed := time.Since(start)

	st.mu.Lock()
	slices.Sort(st.ack)
	lat := st.ack
	st.mu.Unlock()

	fmt.Printf("bots:              %d (%d pairs)\n", *pairs*2, *pairs)
	fmt.Printf("matches finished:  %d (seen by both bots: %d)\n", st.matches.Load()/2, st.matches.Load())
	fmt.Printf("bot errors:        %d\n", st.errors.Load())
	fmt.Printf("wall time:         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("inputs sent:       %d (%.0f/s)\n", st.inputs.Load(), float64(st.inputs.Load())/elapsed.Seconds())
	fmt.Printf("messages received: %d (%.0f/s, %.1f MB)\n", st.msgs.Load(), float64(st.msgs.Load())/elapsed.Seconds(), float64(st.bytes.Load())/1e6)
	if len(lat) > 0 {
		fmt.Printf("input->ack latency (includes up to one server tick of batching): p50=%s p95=%s p99=%s max=%s (n=%d)\n",
			pct(lat, 50), pct(lat, 95), pct(lat, 99), lat[len(lat)-1].Round(time.Microsecond), len(lat))
	}
	if *metrics != "" {
		printServerMetrics(*metrics)
	}
}

func pct(sorted []time.Duration, p int) time.Duration {
	return sorted[min(len(sorted)-1, len(sorted)*p/100)].Round(time.Microsecond)
}

func runBot(ctx context.Context, url, name string, rate float64, st *stats) error {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer ws.Close()
	go func() { <-ctx.Done(); ws.Close() }()

	if err := ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeJoin, Name: name}); err != nil {
		return err
	}

	var (
		mu      sync.Mutex
		playing bool
		sent    = map[uint64]time.Time{}
		done    = make(chan struct{})
	)
	readErr := make(chan error, 1)
	go func() {
		defer close(done)
		for {
			_, b, err := ws.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			st.msgs.Add(1)
			st.bytes.Add(int64(len(b)))
			var env struct {
				Type  string         `json:"type"`
				Phase string         `json:"phase"`
				Ack   uint64         `json:"ack"`
				Event protocol.Event `json:"event"`
				State protocol.State `json:"state"`
			}
			if json.Unmarshal(b, &env) != nil {
				continue
			}
			now := time.Now()
			mu.Lock()
			switch env.Type {
			case protocol.TypeState:
				playing = env.Phase == "playing"
				for seq, at := range sent {
					if seq <= env.Ack {
						st.mu.Lock()
						st.ack = append(st.ack, now.Sub(at))
						st.mu.Unlock()
						delete(sent, seq)
					}
				}
			case protocol.TypeSnapshot:
				playing = env.State.Phase == "playing"
			case protocol.TypeEvent:
				if env.Event.Kind == protocol.EvMatchOver {
					st.matches.Add(1)
					mu.Unlock()
					readErr <- nil
					return
				}
			case protocol.TypeError:
				mu.Unlock()
				readErr <- fmt.Errorf("server error: %s", b)
				return
			}
			mu.Unlock()
		}
	}()

	// Random phase so bot inputs are not aligned with the server tick (all bots and the
	// match are created within a millisecond, which would otherwise flatter the latency).
	every := time.Duration(float64(time.Second) / rate)
	time.Sleep(rand.N(every))
	tick := time.NewTicker(every)
	defer tick.Stop()
	var seq uint64
	for {
		select {
		case <-done:
			return <-readErr
		case <-tick.C:
			mu.Lock()
			ok := playing
			mu.Unlock()
			if !ok {
				continue
			}
			seq++
			mu.Lock()
			sent[seq] = time.Now()
			mu.Unlock()
			// Bias towards drops so rounds finish.
			a := actions[rand.IntN(len(actions))]
			if rand.IntN(3) == 0 {
				a = game.HardDrop
			}
			if err := ws.WriteJSON(protocol.ClientMsg{Type: protocol.TypeInput, Seq: seq, Action: a}); err != nil {
				return err
			}
			st.inputs.Add(1)
		}
	}
}

// printServerMetrics scrapes a few server-side numbers from the Prometheus endpoint.
func printServerMetrics(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("metrics scrape failed:", err)
		return
	}
	defer resp.Body.Close()
	var sum, count float64
	buckets := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		v, _ := strconv.ParseFloat(f[1], 64)
		switch {
		case strings.HasPrefix(line, "sabostack_tick_duration_seconds_sum"):
			sum = v
		case strings.HasPrefix(line, "sabostack_tick_duration_seconds_count"):
			count = v
		case strings.HasPrefix(line, "sabostack_tick_duration_seconds_bucket"):
			le := f[0][strings.Index(f[0], `le="`)+4 : len(f[0])-2]
			buckets[le] = v
		case strings.HasPrefix(line, "sabostack_inputs_total"),
			strings.HasPrefix(line, "process_resident_memory_bytes"),
			strings.HasPrefix(line, "go_goroutines"):
			fmt.Println("server", line)
		}
	}
	if count > 0 {
		fmt.Printf("server ticks: %.0f, mean tick processing %.1fµs\n", count, sum/count*1e6)
		for _, le := range []string{"5e-05", "0.0001", "0.00025", "0.001"} {
			if b, ok := buckets[le]; ok {
				fmt.Printf("server ticks <= %ss: %.2f%%\n", le, b/count*100)
			}
		}
	}
}
