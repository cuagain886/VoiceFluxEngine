package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"voicestream/internal/pipeline"
)

func scrape(t *testing.T, r *Registry) string {
	t.Helper()
	ts := httptest.NewServer(r.Handler())
	defer ts.Close()
	resp, err := ts.Client().Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCounterAndGaugeExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("test_events_total", "Events.")
	c.Add(3)
	r.NewGaugeFunc("test_live", "Live things.", func() float64 { return 7 })

	out := scrape(t, r)
	for _, want := range []string{
		"# TYPE test_events_total counter",
		"test_events_total 3",
		"# TYPE test_live gauge",
		"test_live 7",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q:\n%s", want, out)
		}
	}
}

func TestHistogramBucketsCumulative(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("test_latency_seconds", "Latency.", []float64{0.01, 0.1, 1})
	for _, v := range []float64{0.005, 0.05, 0.05, 0.5, 2} {
		h.Observe(v)
	}
	out := scrape(t, r)
	for _, want := range []string{
		`test_latency_seconds_bucket{le="0.01"} 1`,
		`test_latency_seconds_bucket{le="0.1"} 3`,
		`test_latency_seconds_bucket{le="1"} 4`,
		`test_latency_seconds_bucket{le="+Inf"} 5`,
		"test_latency_seconds_count 5",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q:\n%s", want, out)
		}
	}
	if h.Count() != 5 {
		t.Fatalf("count = %d", h.Count())
	}
	// Negative and NaN observations are ignored, never corrupting counts.
	h.Observe(-1)
	if h.Count() != 5 {
		t.Fatal("negative observation was counted")
	}
}

func TestHubReplayAndLiveDelivery(t *testing.T) {
	h := NewHub()
	h.Publish(map[string]int{"n": 1})
	h.Publish(map[string]int{"n": 2})

	ts := httptest.NewServer(h)
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := httptest.NewRequest("GET", ts.URL, nil), 0
	_ = req
	resp, err := ts.Client().Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// History replays first; then a live publish arrives.
	go func() {
		time.Sleep(50 * time.Millisecond)
		h.Publish(map[string]int{"n": 3})
	}()

	buf := make([]byte, 0, 256)
	tmp := make([]byte, 64)
	deadline := time.Now().Add(4 * time.Second)
	for !strings.Contains(string(buf), `{"n":3}`) {
		if time.Now().After(deadline) {
			t.Fatalf("SSE stream incomplete: %q", buf)
		}
		n, err := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}
	got := string(buf)
	for _, want := range []string{`data: {"n":1}`, `data: {"n":2}`, `data: {"n":3}`} {
		if !strings.Contains(got, want) {
			t.Fatalf("stream missing %q:\n%s", want, got)
		}
	}
	_ = ctx
}

func TestRecordTurnSerialAndRelative(t *testing.T) {
	base := time.Now()
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	ts := pipeline.TurnStats{
		Prompt:          "你好",
		Reply:           "echo: 你好",
		UtteranceEndAt:  base,
		ASRFinalAt:      at(20),
		LLMStartAt:      at(21),
		LLMFirstTokenAt: at(60),
		LLMLastTokenAt:  at(220),
		TTSStartAt:      at(61),
		TTSFirstFrameAt: at(80),
		TTSLastFrameAt:  at(240),
		EndedAt:         at(245),
	}
	m := New()
	m.RecordTurn("abcdef1234567890", ts)

	if m.TurnsCompleted.Value() != 1 || m.FirstResponse.Count() != 1 {
		t.Fatal("turn not recorded in instruments")
	}
	rec := toRecord("abcdef1234567890", ts)
	if rec.Session != "abcdef12" {
		t.Fatalf("session = %q", rec.Session)
	}
	if rec.WallMs != 245 {
		t.Fatalf("wall = %v", rec.WallMs)
	}
	// serial = asr 20 + llm (220-21=199) + tts (240-61=179) = 398 > wall 245:
	// the overlap the dashboard visualizes.
	if rec.SerialMs != 398 {
		t.Fatalf("serial = %v, want 398", rec.SerialMs)
	}
	if rec.SerialMs <= rec.WallMs {
		t.Fatal("pipelining should beat naive serial in this fixture")
	}
}
