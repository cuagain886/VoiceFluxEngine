package loadgen

import (
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	if got := percentile(nil, 0.5); got != -1 {
		t.Fatalf("empty percentile = %v, want -1", got)
	}
	xs := make([]float64, 100)
	for i := range xs {
		xs[i] = float64(i+1) / 1000 // 1ms .. 100ms in seconds
	}
	if got := percentile(xs, 0.50); got < 49 || got > 52 {
		t.Fatalf("p50 = %vms, want ~50ms", got)
	}
	if got := percentile(xs, 0.99); got < 98 || got > 100 {
		t.Fatalf("p99 = %vms, want ~99ms", got)
	}
}

func TestHistQuantile(t *testing.T) {
	bounds := []float64{0.005, 0.01, inf}
	counts := []float64{0, 2, 2}
	// q=0.5: target 2 falls exactly at the (0.005,0.01] bucket's end.
	if got := histQuantile(bounds, counts, 0.5); got != 0.01 {
		t.Fatalf("q50 = %v, want 0.01", got)
	}
	// q=0.99 lands in +Inf: report the highest finite bound, never invent.
	if got := histQuantile(bounds, counts, 0.99); got != 0.01 {
		t.Fatalf("q99 = %v, want 0.01 (clamped at last finite bound)", got)
	}
	if got := histQuantile(nil, nil, 0.5); got != -1 {
		t.Fatalf("empty quantile = %v, want -1", got)
	}
}

func recordsFixture(mutate func(recs []StepRecord)) []StepRecord {
	recs := []StepRecord{
		{Concurrency: 5, SrvFirstP99: 50, IngressDropRate: 0, CPUUtil: 0.10},
		{Concurrency: 20, SrvFirstP99: 55, IngressDropRate: 0, CPUUtil: 0.30},
		{Concurrency: 80, SrvFirstP99: 60, IngressDropRate: 0.001, CPUUtil: 0.55},
	}
	if mutate != nil {
		mutate(recs)
	}
	return recs
}

func TestAnalyzeNoKnee(t *testing.T) {
	a := Analyze(recordsFixture(nil))
	if a.KneeConcurrency != 0 || a.Wall != "none" {
		t.Fatalf("analysis = %+v, want no knee", a)
	}
}

func TestAnalyzeCPUWall(t *testing.T) {
	a := Analyze(recordsFixture(func(r []StepRecord) {
		r[2].SrvFirstP99 = 250 // > 2*50+20
		r[2].CPUUtil = 0.92
	}))
	if a.KneeConcurrency != 80 || a.Wall != "cpu" {
		t.Fatalf("analysis = %+v, want cpu wall at 80", a)
	}
}

func TestAnalyzeDropWall(t *testing.T) {
	a := Analyze(recordsFixture(func(r []StepRecord) {
		r[1].IngressDropRate = 0.05
		r[2].IngressDropRate = 0.12 // persists into the heavier step
	}))
	if a.KneeConcurrency != 20 || a.Wall != "drops" {
		t.Fatalf("analysis = %+v, want drops wall at 20", a)
	}
}

// TestAnalyzeIgnoresTransientSpike pins the persistence rule: one degraded
// window followed by clean heavier steps (GC pause, OS hiccup) is not a
// capacity knee — exhausted capacity does not recover under more load.
func TestAnalyzeIgnoresTransientSpike(t *testing.T) {
	a := Analyze(recordsFixture(func(r []StepRecord) {
		r[1].SrvFirstP99 = 430 // spike at 20...
		// ...but the 80-concurrency step is clean again (fixture default).
	}))
	if a.KneeConcurrency != 0 || a.Wall != "none" {
		t.Fatalf("analysis = %+v, want transient ignored", a)
	}
}

func TestAnalyzeErrorsTrumpCPU(t *testing.T) {
	a := Analyze(recordsFixture(func(r []StepRecord) {
		r[2].Errors = 3
		r[2].CPUUtil = 0.95
		r[2].SrvFirstP99 = 400
	}))
	if a.Wall != "errors" {
		t.Fatalf("analysis = %+v, want errors wall", a)
	}
}

func TestAnalyzeSchedulingWallWhenCPUIdle(t *testing.T) {
	a := Analyze(recordsFixture(func(r []StepRecord) {
		r[2].SrvFirstP99 = 300
		r[2].CPUUtil = 0.40
	}))
	if a.Wall != "scheduling" {
		t.Fatalf("analysis = %+v, want scheduling wall", a)
	}
}

func TestReportCSVAndHTML(t *testing.T) {
	rep := &Report{
		Config:    "steps=[5 20 80]",
		StartedAt: time.Now(),
		Records:   recordsFixture(nil),
		Analysis:  Analyze(recordsFixture(nil)),
	}
	csv := rep.CSV()
	if lines := strings.Count(csv, "\n"); lines != 4 { // header + 3 rows
		t.Fatalf("CSV has %d lines, want 4:\n%s", lines, csv)
	}
	if !strings.HasPrefix(csv, "concurrency,") {
		t.Fatalf("CSV header malformed: %q", csv[:40])
	}
	html, err := rep.HTML()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"容量曲线", `"concurrency":5`, "__REPORT_JSON__"} {
		has := strings.Contains(html, want)
		if want == "__REPORT_JSON__" {
			if has {
				t.Fatal("HTML still contains the placeholder — data not injected")
			}
			continue
		}
		if !has {
			t.Fatalf("HTML missing %q", want)
		}
	}
	if rep.Table() == "" {
		t.Fatal("empty table")
	}
}
