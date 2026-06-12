package loadgen

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// StepRecord is one point on the capacity curve. Latencies are milliseconds;
// -1 means "no data" (no samples in the window, or scraping disabled).
//
// Two vantage points, deliberately both kept:
//   - E2E*: client-observed wall time (includes VAD hangover/min-speech,
//     network, playout of the measurement itself) — what a user would feel.
//   - Srv*: server-side histograms diffed across the window — the kernel's
//     own accounting (first response from utterance end; barge-in cancel
//     from cancel request, the 200ms-budget metric).
type StepRecord struct {
	Concurrency int     `json:"concurrency"`
	LiveWorkers int     `json:"liveWorkers"`
	DurationS   float64 `json:"durationS"`
	Turns       int     `json:"turns"`  // client-side completed turns in window
	Errors      int     `json:"errors"` // timeouts + connection failures in window

	E2EFirstP50 float64 `json:"e2eFirstP50"`
	E2EFirstP95 float64 `json:"e2eFirstP95"`
	E2EFirstP99 float64 `json:"e2eFirstP99"`
	E2EBargeP50 float64 `json:"e2eBargeP50"`
	E2EBargeP99 float64 `json:"e2eBargeP99"`

	SrvFirstP50  float64 `json:"srvFirstP50"`
	SrvFirstP99  float64 `json:"srvFirstP99"`
	SrvKernelP99 float64 `json:"srvKernelP99"`
	SrvBargeP99  float64 `json:"srvBargeP99"`

	ServerTurnRate  float64 `json:"serverTurnRate"`  // completed turns/s
	UplinkFPS       float64 `json:"uplinkFPS"`       // frames/s the harness actually sent
	IngressDropRate float64 `json:"ingressDropRate"` // dropped / sent (0..1); -1 unknown
	EgressDropPerS  float64 `json:"egressDropPerS"`
	CPUUtil         float64 `json:"cpuUtil"` // busy/capacity (0..1); -1 unknown
	Goroutines      float64 `json:"goroutines"`
	HeapMB          float64 `json:"heapMB"`
	SessionsActive  float64 `json:"sessionsActive"`
}

// percentile returns the q-th (0..1) percentile of xs in milliseconds,
// nearest-rank on a sorted copy; -1 when empty.
func percentile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return -1
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	idx := int(q*float64(len(s)) + 0.5)
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx] * 1000
}

func buildRecord(n int, elapsed time.Duration, agg windowAgg, s0, s1 *snapshot, live int) StepRecord {
	rec := StepRecord{
		Concurrency: n,
		LiveWorkers: live,
		DurationS:   elapsed.Seconds(),
		Turns:       agg.turns,
		Errors:      agg.errs,
		E2EFirstP50: percentile(agg.first, 0.50),
		E2EFirstP95: percentile(agg.first, 0.95),
		E2EFirstP99: percentile(agg.first, 0.99),
		E2EBargeP50: percentile(agg.barge, 0.50),
		E2EBargeP99: percentile(agg.barge, 0.99),
		UplinkFPS:   float64(agg.frames) / elapsed.Seconds(),

		SrvFirstP50: -1, SrvFirstP99: -1, SrvKernelP99: -1, SrvBargeP99: -1,
		ServerTurnRate: -1, IngressDropRate: -1, EgressDropPerS: -1,
		CPUUtil: -1, Goroutines: -1, HeapMB: -1, SessionsActive: -1,
	}
	if s0 == nil || s1 == nil {
		return rec
	}
	histQ := func(name string, q float64) float64 {
		bounds, counts := histDelta(s0, s1, name)
		v := histQuantile(bounds, counts, q)
		if v < 0 {
			return -1
		}
		return v * 1000
	}
	rec.SrvFirstP50 = histQ("voicestream_first_response_seconds", 0.50)
	rec.SrvFirstP99 = histQ("voicestream_first_response_seconds", 0.99)
	rec.SrvKernelP99 = histQ("voicestream_kernel_overhead_seconds", 0.99)
	rec.SrvBargeP99 = histQ("voicestream_barge_in_cancel_seconds", 0.99)

	secs := elapsed.Seconds()
	rec.ServerTurnRate = (scalarDelta(s0, s1, "voicestream_turns_completed_total") +
		scalarDelta(s0, s1, "voicestream_turns_cancelled_total")) / secs
	if agg.frames > 0 {
		rec.IngressDropRate = scalarDelta(s0, s1, "voicestream_ingress_dropped_frames_total") / float64(agg.frames)
	}
	rec.EgressDropPerS = scalarDelta(s0, s1, "voicestream_egress_dropped_frames_total") / secs
	if dc := scalarDelta(s0, s1, "voicestream_cpu_capacity_seconds_total"); dc > 0 {
		rec.CPUUtil = scalarDelta(s0, s1, "voicestream_cpu_busy_seconds_total") / dc
	}
	rec.Goroutines = s1.scalars["voicestream_goroutines"]
	rec.HeapMB = s1.scalars["voicestream_heap_alloc_bytes"] / (1024 * 1024)
	rec.SessionsActive = s1.scalars["voicestream_sessions_active"]
	return rec
}

// Analysis is the harness's mechanical reading of the curve (10.4). It is a
// first pass for the report; the design doc's final attribution is reviewed
// by a human against the full data.
type Analysis struct {
	KneeConcurrency int    `json:"kneeConcurrency"` // 0 = knee not reached
	Wall            string `json:"wall"`            // cpu | drops | errors | scheduling | none
	Reason          string `json:"reason"`
}

// Analyze finds the capacity knee: the first step whose server-side first
// response p99 leaves the baseline band (>2x baseline +20ms), or that sheds
// >1% of ingress frames, or that records errors — and whose degradation
// *persists into the next step* (or is the final step). A single degraded
// window with clean steps after it is a transient (GC pause, OS hiccup,
// background process), not a capacity knee: capacity exhaustion does not
// recover by adding more load. The wall is whichever limiting signal is
// loudest at the knee step.
func Analyze(recs []StepRecord) Analysis {
	if len(recs) == 0 {
		return Analysis{Wall: "none", Reason: "无数据"}
	}
	lat := func(r StepRecord) float64 { // prefer server view, fall back to client
		if r.SrvFirstP99 >= 0 {
			return r.SrvFirstP99
		}
		return r.E2EFirstP99
	}
	base := lat(recs[0])
	degraded := func(r StepRecord) bool {
		return (base >= 0 && lat(r) >= 0 && lat(r) > base*2+20) ||
			r.IngressDropRate > 0.01 ||
			r.Errors > 0
	}
	for i, r := range recs {
		if !degraded(r) {
			continue
		}
		if i+1 < len(recs) && !degraded(recs[i+1]) {
			continue // transient: the next, heavier step is clean again
		}
		a := Analysis{KneeConcurrency: r.Concurrency}
		switch {
		case r.Errors > 0:
			a.Wall = "errors"
			a.Reason = fmt.Sprintf("并发 %d 时出现 %d 个超时/连接错误（超出 %s 截止）", r.Concurrency, r.Errors, "TurnTimeout")
		case r.CPUUtil >= 0.85:
			a.Wall = "cpu"
			a.Reason = fmt.Sprintf("并发 %d 时 CPU 利用率 %.0f%%，延迟 p99 %.0fms（基线 %.0fms）", r.Concurrency, r.CPUUtil*100, lat(r), base)
		case r.IngressDropRate > 0.01:
			a.Wall = "drops"
			a.Reason = fmt.Sprintf("并发 %d 时入口丢帧率 %.2f%% —— 背压设计性卸载先于其它资源耗尽", r.Concurrency, r.IngressDropRate*100)
		default:
			a.Wall = "scheduling"
			a.Reason = fmt.Sprintf("并发 %d 时延迟 p99 %.0fms（基线 %.0fms）而 CPU 仅 %.0f%% —— 指向调度/系统调用竞争（含同机 loadgen 争用）", r.Concurrency, lat(r), base, max0(r.CPUUtil)*100)
		}
		return a
	}
	last := recs[len(recs)-1]
	return Analysis{Wall: "none", Reason: fmt.Sprintf("至并发 %d 未见拐点：延迟平稳、丢帧率 %.2f%%、无错误", last.Concurrency, max0(last.IngressDropRate)*100)}
}

func max0(v float64) float64 {
	if v < 0 {
		return 0
	}
	return v
}

// Report is the run's full artifact set.
type Report struct {
	Config     string       `json:"config"`
	StartedAt  time.Time    `json:"startedAt"`
	FinishedAt time.Time    `json:"finishedAt"`
	Records    []StepRecord `json:"records"`
	Analysis   Analysis     `json:"analysis"`
}

// WriteFiles emits capacity.csv, capacity.json and capacity.html into dir.
func (r *Report) WriteFiles(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "capacity.csv"), []byte(r.CSV()), 0o644); err != nil {
		return err
	}
	js, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "capacity.json"), js, 0o644); err != nil {
		return err
	}
	html, err := r.HTML()
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "capacity.html"), []byte(html), 0o644)
}

// CSV renders the records as a flat table.
func (r *Report) CSV() string {
	var b strings.Builder
	b.WriteString("concurrency,turns,errors,e2e_first_p50_ms,e2e_first_p95_ms,e2e_first_p99_ms," +
		"e2e_barge_p50_ms,e2e_barge_p99_ms,srv_first_p50_ms,srv_first_p99_ms,srv_kernel_p99_ms," +
		"srv_barge_p99_ms,turn_rate,uplink_fps,ingress_drop_rate,egress_drop_per_s,cpu_util," +
		"goroutines,heap_mb,sessions_active\n")
	for _, x := range r.Records {
		fmt.Fprintf(&b, "%d,%d,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.2f,%.0f,%.5f,%.2f,%.3f,%.0f,%.1f,%.0f\n",
			x.Concurrency, x.Turns, x.Errors,
			x.E2EFirstP50, x.E2EFirstP95, x.E2EFirstP99,
			x.E2EBargeP50, x.E2EBargeP99,
			x.SrvFirstP50, x.SrvFirstP99, x.SrvKernelP99, x.SrvBargeP99,
			x.ServerTurnRate, x.UplinkFPS, x.IngressDropRate, x.EgressDropPerS,
			x.CPUUtil, x.Goroutines, x.HeapMB, x.SessionsActive)
	}
	return b.String()
}

// Table renders a human-readable summary for stdout.
func (r *Report) Table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-6s %-6s %-5s %9s %9s %9s %9s %8s %7s %6s\n",
		"conc", "turns", "errs", "e2e_p50", "e2e_p99", "srv_p99", "barge_p99", "drop%", "cpu%", "goro")
	ms := func(v float64) string {
		if v < 0 {
			return "-"
		}
		return fmt.Sprintf("%.0fms", v)
	}
	pct := func(v float64) string {
		if v < 0 {
			return "-"
		}
		return fmt.Sprintf("%.1f", v*100)
	}
	for _, x := range r.Records {
		fmt.Fprintf(&b, "%-6d %-6d %-5d %9s %9s %9s %9s %8s %7s %6.0f\n",
			x.Concurrency, x.Turns, x.Errors,
			ms(x.E2EFirstP50), ms(x.E2EFirstP99), ms(x.SrvFirstP99), ms(x.E2EBargeP99),
			pct(x.IngressDropRate), pct(x.CPUUtil), x.Goroutines)
	}
	fmt.Fprintf(&b, "\n拐点: ")
	if r.Analysis.KneeConcurrency > 0 {
		fmt.Fprintf(&b, "并发 %d（墙: %s）\n", r.Analysis.KneeConcurrency, r.Analysis.Wall)
	} else {
		fmt.Fprintf(&b, "未达\n")
	}
	fmt.Fprintf(&b, "%s\n", r.Analysis.Reason)
	return b.String()
}
