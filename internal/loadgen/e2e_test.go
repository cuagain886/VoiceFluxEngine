package loadgen

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/metrics"
	"voicestream/internal/session"
	"voicestream/internal/transport"
)

// TestRampE2E runs the whole harness against a real in-process server stack
// (transport + session + VAD + pipeline + latency-shaped mocks): a 2-step
// ramp, faster than real time (5ms frame interval; PTS stays nominal 20ms),
// with mild netem perturbation and barge-ins. It asserts the mechanics —
// every step produces turns and first-response samples, barge-ins land, the
// server-side scrape yields data, and nothing errors.
func TestRampE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := config.Default()
	cfg.Server.StaticDir = ""
	cfg.VAD.MinSpeech = 60 * time.Millisecond  // 3 nominal frames
	cfg.VAD.Hangover = 100 * time.Millisecond // 5 nominal frames
	cfg.Session.IdleTimeout = 2 * time.Second
	cfg.Adapters.Mock = config.MockAdaptersConfig{
		LLMTokenDelay: 20 * time.Millisecond,
		TTSFrameDelay: 3 * time.Millisecond, // keeps each response in flight ~200ms+
	}
	set, err := adapter.Build(cfg)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	mgr := session.NewManager(cfg, set, logger, m)
	go mgr.Run(ctx)
	srv := transport.NewServerWithHandler(cfg.Server, logger, mgr.Handler())
	srv.RegisterRoute("/metrics", m.Registry.Handler())
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()

	rep, err := Run(ctx, Config{
		URL:           "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws",
		MetricsURL:    ts.URL + "/metrics",
		Steps:         []int{2, 3},
		StepDuration:  2 * time.Second,
		Warmup:        400 * time.Millisecond,
		FrameInterval: 5 * time.Millisecond,
		SpeechFrames:  16,
		BargeEvery:    2,
		BargeDelay:    40 * time.Millisecond,
		QuietGap:      150 * time.Millisecond,
		TurnTimeout:   15 * time.Second,
		Netem:         Netem{Delay: 3 * time.Millisecond, Jitter: 2 * time.Millisecond},
		Logger:        logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(rep.Records))
	}

	var sawBarge, sawServerHist bool
	for i, rec := range rep.Records {
		if rec.Turns < 1 {
			t.Errorf("step %d: %d turns, want >= 1", i, rec.Turns)
		}
		if rec.Errors != 0 {
			t.Errorf("step %d: %d errors, want 0", i, rec.Errors)
		}
		if rec.E2EFirstP50 <= 0 {
			t.Errorf("step %d: first-response p50 = %v, want > 0", i, rec.E2EFirstP50)
		}
		if rec.UplinkFPS <= 0 {
			t.Errorf("step %d: uplink fps = %v", i, rec.UplinkFPS)
		}
		if rec.E2EBargeP99 > 0 {
			sawBarge = true
		}
		if rec.SrvFirstP99 >= 0 {
			sawServerHist = true
		}
		if rec.Goroutines <= 0 {
			t.Errorf("step %d: runtime gauges not scraped (goroutines = %v)", i, rec.Goroutines)
		}
	}
	if !sawBarge {
		t.Error("no barge-in sample landed in any step")
	}
	if !sawServerHist {
		t.Error("server-side first-response histogram never had samples in a window")
	}
	if rep.Analysis.Reason == "" {
		t.Error("analysis missing")
	}
}
