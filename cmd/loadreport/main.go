// Command loadreport regenerates the tables and charts in docs/LOAD_TESTING.md from the raw
// bot results in docs/loadtest/*.json. Generated regions are delimited by
//
//	<!-- gen:NAME -->
//	...
//	<!-- /gen -->
//
// and everything outside them is left alone.
//
//	go run ./cmd/loadreport            # rewrites docs/LOAD_TESTING.md in place
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// result mirrors the fields of cmd/bot's JSON output that the report uses.
type result struct {
	Config struct {
		Pairs     int     `json:"pairs"`
		Label     string  `json:"label"`
		Rate      float64 `json:"inputsPerSecondPerBot"`
		Duration  int64   `json:"duration"`
		StormFrac float64 `json:"stormFraction"`
	} `json:"config"`
	Bots             int     `json:"bots"`
	MeasureSeconds   float64 `json:"measureSeconds"`
	MatchesFinished  int64   `json:"matchesFinished"`
	Errors           int64   `json:"errors"`
	Dropped          int64   `json:"droppedConnections"`
	InputsSent       int64   `json:"inputsSent"`
	MsgsPerSecond    float64 `json:"msgsPerSecond"`
	MBReceived       float64 `json:"mbReceived"`
	AckP50Ms         float64 `json:"ackP50Ms"`
	AckP95Ms         float64 `json:"ackP95Ms"`
	AckP99Ms         float64 `json:"ackP99Ms"`
	AckMaxMs         float64 `json:"ackMaxMs"`
	ConnectionsAvg   float64 `json:"connectionsAvg"`
	ConnectionsMin   float64 `json:"connectionsMin"`
	LiveMatchesAvg   float64 `json:"liveMatchesAvg"`
	ActiveMatchesAvg float64 `json:"activeMatchesAvg"`
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
	Storm            *struct {
		Selected   int64   `json:"selected"`
		Skipped    int64   `json:"skippedNotInMatch"`
		Attempted  int64   `json:"attempted"`
		Recovered  int64   `json:"recovered"`
		Failed     int64   `json:"failed"`
		SuccessPct float64 `json:"successPct"`
		P50Ms      float64 `json:"recoveryP50Ms"`
		P95Ms      float64 `json:"recoveryP95Ms"`
		MaxMs      float64 `json:"recoveryMaxMs"`
	} `json:"storm"`
	Samples []struct {
		T           float64 `json:"t"`
		RSSBytes    float64 `json:"rssBytes"`
		Goroutines  float64 `json:"goroutines"`
		Connections float64 `json:"connections"`
	} `json:"samples"`
}

func load(path string) *result {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var r result
	if err := json.Unmarshal(b, &r); err != nil {
		log.Fatalf("%s: %v", path, err)
	}
	return &r
}

func main() {
	dir := flag.String("dir", "docs/loadtest", "raw results directory")
	doc := flag.String("doc", "docs/LOAD_TESTING.md", "markdown file to update")
	flag.Parse()

	paths, _ := filepath.Glob(filepath.Join(*dir, "ramp-*.json"))
	var ramp []*result
	for _, p := range paths {
		ramp = append(ramp, load(p))
	}
	slices.SortFunc(ramp, func(a, b *result) int { return a.Config.Pairs - b.Config.Pairs })
	if len(ramp) == 0 {
		log.Fatal("no ramp-*.json results")
	}
	soak := load(filepath.Join(*dir, "soak.json"))
	storm := load(filepath.Join(*dir, "storm.json"))

	gen := map[string]string{
		"ramp-table":    rampTable(ramp),
		"chart-rss":     chart("RSS сервера и число подключений", "RSS (МБ)", ramp, func(r *result) float64 { return r.RSSMBAvg }),
		"chart-cpu":     chart("CPU сервера и число подключений", "CPU (% одного ядра)", ramp, func(r *result) float64 { return r.CPUPercent }),
		"chart-latency": latencyChart(ramp),
		"slopes":        slopes(ramp),
		"soak":          soakSection(soak),
		"storm":         stormSection(storm),
		"compare-hardening": compareTable(filepath.Join(*dir, "hardening"), []string{"before", "after"},
			map[string]string{"before": "до", "after": "после"}),
	}

	b, err := os.ReadFile(*doc)
	if err != nil {
		log.Fatal(err)
	}
	re := regexp.MustCompile(`(?s)<!-- gen:([a-z-]+) -->.*?<!-- /gen -->`)
	out := re.ReplaceAllStringFunc(string(b), func(m string) string {
		name := re.FindStringSubmatch(m)[1]
		body, ok := gen[name]
		if !ok {
			log.Fatalf("unknown generated region %q", name)
		}
		return "<!-- gen:" + name + " -->\n" + strings.TrimRight(body, "\n") + "\n<!-- /gen -->"
	})
	if err := os.WriteFile(*doc, []byte(out), 0o644); err != nil {
		log.Fatal(err)
	}
}

// compareTable summarises a before/after (or any multi-variant) comparison. Files are
// <variant>-<pairs>[-r<k>].json and <variant>-storm-<pairs>[-r<k>].json; repeats of one
// variant and step are reduced to their median, because single runs on a shared host are
// noisy in the tail.
func compareTable(dir string, variants []string, names map[string]string) string {
	type key struct {
		variant string
		storm   bool
		pairs   int
	}
	re := regexp.MustCompile(`^([a-z0-9-]+?)-(storm-)?(\d{4})(-r\d+)?\.json$`)
	runs := map[key][]*result{}
	paths, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, p := range paths {
		m := re.FindStringSubmatch(filepath.Base(p))
		if m == nil {
			continue
		}
		pairs, _ := strconv.Atoi(m[3]) // not fmt.Sscan: it reads "0500" as octal
		k := key{m[1], m[2] != "", pairs}
		runs[k] = append(runs[k], load(p))
	}
	var steps []key
	for k := range runs {
		if k.variant == variants[0] {
			steps = append(steps, k)
		}
	}
	slices.SortFunc(steps, func(a, b key) int {
		if a.storm != b.storm {
			if a.storm {
				return 1
			}
			return -1
		}
		return a.pairs - b.pairs
	})
	med := func(rs []*result, f func(*result) float64) float64 {
		vs := make([]float64, len(rs))
		for i, r := range rs {
			vs[i] = f(r)
		}
		slices.Sort(vs)
		if len(vs)%2 == 1 {
			return vs[len(vs)/2]
		}
		return (vs[len(vs)/2-1] + vs[len(vs)/2]) / 2
	}
	var b strings.Builder
	b.WriteString("| Пар | Версия | Прогонов | Input→ack p50 / p95 / p99 (мс) | CPU сервера, % ядра | RSS сервера, МБ (сред.) | Трафик на клиента, КБ/с | Матчей | Ошибок / обрывов |\n")
	b.WriteString("|---:|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, st := range steps {
		for _, v := range variants {
			rs := runs[key{v, st.storm, st.pairs}]
			if len(rs) == 0 {
				continue
			}
			label := fmt.Sprintf("%d", st.pairs)
			if st.storm {
				label += " (шторм)"
			}
			var errs, drops int64
			for _, r := range rs {
				errs += r.Errors
				drops += r.Dropped
			}
			kbps := med(rs, func(r *result) float64 { return r.MBReceived * 1e6 / 1024 / r.MeasureSeconds / float64(r.Bots) })
			fmt.Fprintf(&b, "| %s | %s | %d | %.1f / %.1f / %.1f | %.0f | %.0f | %.2f | %.0f | %d / %d |\n",
				label, names[v], len(rs),
				med(rs, func(r *result) float64 { return r.AckP50Ms }),
				med(rs, func(r *result) float64 { return r.AckP95Ms }),
				med(rs, func(r *result) float64 { return r.AckP99Ms }),
				med(rs, func(r *result) float64 { return r.CPUPercent }),
				med(rs, func(r *result) float64 { return r.RSSMBAvg }),
				kbps,
				med(rs, func(r *result) float64 { return float64(r.MatchesFinished) }),
				errs, drops)
		}
	}
	return b.String()
}

func rampTable(rs []*result) string {
	var b strings.Builder
	b.WriteString("| Пар | WS-подключений (сред. / мин.) | Живых матчей (сред.) | RSS сервера, МБ (сред. / макс.) | CPU сервера, % одного ядра | Goroutines (сред.) | Input→ack p50 / p95 / p99 / max (мс) | Тик: среднее мкс / макс. мс | Сообщений/с | Входов отправлено | Ошибок / обрывов |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rs {
		lat := "–"
		if r.AckP50Ms > 0 {
			lat = fmt.Sprintf("%.1f / %.1f / %.1f / %.1f", r.AckP50Ms, r.AckP95Ms, r.AckP99Ms, r.AckMaxMs)
		}
		fmt.Fprintf(&b, "| %d | %.0f / %.0f | %.0f | %.1f / %.1f | %.1f | %.0f | %s | %.1f / %.2f | %.0f | %d | %d / %d |\n",
			r.Config.Pairs, r.ConnectionsAvg, r.ConnectionsMin, r.LiveMatchesAvg, r.RSSMBAvg, r.RSSMBMax, r.CPUPercent,
			r.GoroutinesAvg, lat, r.TickMeanUs, r.TickMaxMs, r.MsgsPerSecond, r.InputsSent, r.Errors, r.Dropped)
	}
	return b.String()
}

func xAxis(rs []*result) string {
	var xs []string
	for _, r := range rs {
		xs = append(xs, fmt.Sprintf("%.0f", r.ConnectionsAvg))
	}
	return "x-axis \"WebSocket-подключения\" [" + strings.Join(xs, ", ") + "]"
}

func series(rs []*result, f func(*result) float64) (string, float64) {
	var vs []string
	top := 0.0
	for _, r := range rs {
		v := f(r)
		top = max(top, v)
		vs = append(vs, fmt.Sprintf("%.1f", v))
	}
	return "[" + strings.Join(vs, ", ") + "]", top
}

func chart(title, ylabel string, rs []*result, f func(*result) float64) string {
	s, top := series(rs, f)
	return fmt.Sprintf("```mermaid\nxychart-beta\n    title \"%s\"\n    %s\n    y-axis \"%s\" 0 --> %.0f\n    line %s\n```\n", title, xAxis(rs), ylabel, niceTop(top), s)
}

func latencyChart(rs []*result) string {
	var withLat []*result
	for _, r := range rs {
		if r.AckP50Ms > 0 {
			withLat = append(withLat, r)
		}
	}
	p95, t1 := series(withLat, func(r *result) float64 { return r.AckP95Ms })
	p99, t2 := series(withLat, func(r *result) float64 { return r.AckP99Ms })
	return fmt.Sprintf("```mermaid\nxychart-beta\n    title \"Задержка input→ack: p95 (нижняя линия) и p99 (верхняя)\"\n    %s\n    y-axis \"мс\" 0 --> %.0f\n    line %s\n    line %s\n```\n",
		xAxis(withLat), niceTop(max(t1, t2)), p95, p99)
}

func niceTop(v float64) float64 {
	for _, step := range []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000} {
		if v*1.1 <= step {
			return step
		}
	}
	return v * 1.2
}

// fit is an ordinary least-squares line y = a + b*x.
func fit(xs, ys []float64) (a, b float64) {
	n := float64(len(xs))
	var sx, sy, sxx, sxy float64
	for i := range xs {
		sx += xs[i]
		sy += ys[i]
		sxx += xs[i] * xs[i]
		sxy += xs[i] * ys[i]
	}
	b = (n*sxy - sx*sy) / (n*sxx - sx*sx)
	a = (sy - b*sx) / n
	return a, b
}

func slopes(rs []*result) string {
	var conns, rss, cpu []float64
	for _, r := range rs {
		conns = append(conns, r.ConnectionsAvg)
		rss = append(rss, r.RSSMBAvg)
		cpu = append(cpu, r.CPUPercent)
	}
	ra, rb := fit(conns, rss)
	ca, cb := fit(conns, cpu)
	return fmt.Sprintf("Линейная регрессия (МНК) по всем %d шагам, x = среднее число WebSocket-подключений:\n\n"+
		"- **RSS ≈ %.1f МБ + %.1f КБ × подключения**\n"+
		"- **CPU ≈ %.1f %% + %.3f %% одного ядра × подключения**\n", len(rs), ra, rb*1000, ca, cb)
}

func soakSection(r *result) string {
	if r == nil {
		return "_Soak-прогон не записан._\n"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| Метрика | Значение |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Пар / ботов | %d / %d |\n", r.Config.Pairs, r.Bots)
	fmt.Fprintf(&b, "| Длительность измерения | %.0f с |\n", r.MeasureSeconds)
	fmt.Fprintf(&b, "| Завершено матчей | %d |\n", r.MatchesFinished)
	fmt.Fprintf(&b, "| WS-подключений (сред. / мин.) | %.0f / %.0f |\n", r.ConnectionsAvg, r.ConnectionsMin)
	fmt.Fprintf(&b, "| RSS сервера: начало → конец (макс.) | %.1f → %.1f МБ (%.1f) |\n", r.RSSMBStart, r.RSSMBEnd, r.RSSMBMax)
	fmt.Fprintf(&b, "| Goroutines: начало → конец (сред.) | %.0f → %.0f (%.0f) |\n", r.GoroutinesStart, r.GoroutinesEnd, r.GoroutinesAvg)
	fmt.Fprintf(&b, "| CPU сервера | %.1f %% одного ядра |\n", r.CPUPercent)
	fmt.Fprintf(&b, "| Input→ack p50 / p95 / p99 / max | %.1f / %.1f / %.1f / %.1f мс |\n", r.AckP50Ms, r.AckP95Ms, r.AckP99Ms, r.AckMaxMs)
	fmt.Fprintf(&b, "| Тик: среднее / макс. | %.1f мкс / %.2f мс |\n", r.TickMeanUs, r.TickMaxMs)
	fmt.Fprintf(&b, "| Ошибок / обрывов соединения | %d / %d |\n\n", r.Errors, r.Dropped)

	// RSS and goroutines over time, one point per ~10% of the run.
	var pts []int
	for i := range r.Samples {
		if r.Samples[i].Connections > 0 {
			pts = append(pts, i)
		}
	}
	if len(pts) > 2 {
		step := max(1, len(pts)/12)
		var xs, rss, gor []string
		top, gtop := 0.0, 0.0
		for i := 0; i < len(pts); i += step {
			s := r.Samples[pts[i]]
			xs = append(xs, fmt.Sprintf("%.0f", s.T))
			rss = append(rss, fmt.Sprintf("%.1f", s.RSSBytes/1e6))
			gor = append(gor, fmt.Sprintf("%.0f", s.Goroutines))
			top, gtop = max(top, s.RSSBytes/1e6), max(gtop, s.Goroutines)
		}
		fmt.Fprintf(&b, "```mermaid\nxychart-beta\n    title \"Soak: RSS сервера во времени\"\n    x-axis \"секунд от старта\" [%s]\n    y-axis \"RSS (МБ)\" 0 --> %.0f\n    line [%s]\n```\n\n",
			strings.Join(xs, ", "), niceTop(top), strings.Join(rss, ", "))
		fmt.Fprintf(&b, "```mermaid\nxychart-beta\n    title \"Soak: число goroutines во времени\"\n    x-axis \"секунд от старта\" [%s]\n    y-axis \"goroutines\" 0 --> %.0f\n    line [%s]\n```\n",
			strings.Join(xs, ", "), niceTop(gtop), strings.Join(gor, ", "))
	}
	return b.String()
}

func stormSection(r *result) string {
	if r == nil || r.Storm == nil {
		return "_Шторм переподключений не записан._\n"
	}
	s := r.Storm
	var b strings.Builder
	fmt.Fprintf(&b, "| Метрика | Значение |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Пар / ботов | %d / %d |\n", r.Config.Pairs, r.Bots)
	fmt.Fprintf(&b, "| Выбрано ботов (%.0f %%) | %d |\n", r.Config.StormFrac*100, s.Selected)
	fmt.Fprintf(&b, "| …из них были в матче (сокет оборван, затем resume) | %d |\n", s.Attempted)
	fmt.Fprintf(&b, "| …были между матчами или в очереди (пропущены) | %d |\n", s.Skipped)
	fmt.Fprintf(&b, "| Успешно восстановились | %d (%.1f %%) |\n", s.Recovered, s.SuccessPct)
	fmt.Fprintf(&b, "| Неудачно | %d |\n", s.Failed)
	fmt.Fprintf(&b, "| Время восстановления (обрыв → snapshot) p50 / p95 / max | %.1f / %.1f / %.1f мс |\n", s.P50Ms, s.P95Ms, s.MaxMs)
	fmt.Fprintf(&b, "| Ошибок / обрывов за прогон | %d / %d |\n", r.Errors, r.Dropped)
	fmt.Fprintf(&b, "| Input→ack p99 / max за прогон | %.1f / %.1f мс |\n", r.AckP99Ms, r.AckMaxMs)
	return b.String()
}
