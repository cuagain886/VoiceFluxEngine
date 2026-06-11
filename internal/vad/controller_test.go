package vad

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/pipeline"
)

// vadTestConfig: 20ms frames, min speech 60ms (3 frames), hangover 100ms
// (5 frames) — small enough to keep tests fast, real enough to exercise the
// filters.
func vadTestConfig() config.Config {
	cfg := config.Default()
	cfg.VAD.MinSpeech = 60 * time.Millisecond
	cfg.VAD.Hangover = 100 * time.Millisecond
	return cfg
}

// rig wires a real pipeline (mock models) to a Controller exactly as the
// transport will in M7: Ingest on the uplink path, turn hooks to the machine.
type rig struct {
	p    *pipeline.Pipeline
	ctrl *Controller
	stop func()
}

func newRig(t *testing.T, cfg config.Config, llm *adapter.MockLLM) *rig {
	t.Helper()
	if llm == nil {
		llm = &adapter.MockLLM{}
	}
	set := adapter.Set{
		ASR: &adapter.MockASR{Script: "what is the weather"},
		LLM: llm,
		TTS: &adapter.MockTTS{SampleRate: 16000, FrameDuration: 20 * time.Millisecond},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p, err := pipeline.New(cfg, set, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctrl := NewController(cfg, nil, p, logger)
	p.OnTurnStart = ctrl.ResponseStarted
	p.OnTurnEnd = func(bool) { ctrl.ResponseDone() }

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.Run(ctx) }()
	stop := func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("pipeline did not stop")
		}
	}
	return &rig{p: p, ctrl: ctrl, stop: stop}
}

func (r *rig) ingest(amplitude float64, frames int) {
	pcm := pcmFrame(amplitude)
	for i := 0; i < frames; i++ {
		r.ctrl.Ingest(adapter.AudioFrame{PCM: pcm, TsUs: int64(i) * 20_000})
	}
}

func (r *rig) awaitCancelledTurn(t *testing.T, within time.Duration) pipeline.TurnStats {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s, ok := r.p.LastTurn(); ok && s.Cancelled {
			return s
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("no cancelled turn observed")
	return pipeline.TurnStats{}
}

// TestVoiceDrivenConversation runs a full turn with no explicit
// EndUtterance/BargeIn calls — the VAD events drive everything.
func TestVoiceDrivenConversation(t *testing.T) {
	r := newRig(t, vadTestConfig(), nil)
	defer r.stop()

	r.ingest(0.5, 5) // speech (start fires on frame 3)
	if got := r.ctrl.State(); got != SpeakingUser {
		t.Fatalf("state after speech = %v, want SPEAKING_USER", got)
	}
	r.ingest(0.0, 6) // silence past hangover: speech_end -> EndUtterance

	// The turn must complete and the machine return to LISTENING.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := r.p.LastTurn(); ok && !s.Cancelled {
			if s.Prompt != "what is the weather" {
				t.Fatalf("prompt = %q", s.Prompt)
			}
			for r.ctrl.State() != Listening && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
			if got := r.ctrl.State(); got != Listening {
				t.Fatalf("state after turn = %v, want LISTENING", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("voice-driven turn never completed")
}

// TestBargeInLatencyUnder200ms is the M6 acceptance benchmark: from the
// user's first interrupting frame to the response sub-chain cancelled and
// flushed, on the mock chain, in under 200ms (60ms of which is the
// configured min-speech filter doing its anti-false-trigger job).
func TestBargeInLatencyUnder200ms(t *testing.T) {
	llm := &adapter.MockLLM{Latency: adapter.Latency{Delay: 60 * time.Millisecond}}
	r := newRig(t, vadTestConfig(), llm)
	defer r.stop()

	r.ingest(0.5, 5)
	r.ingest(0.0, 6)

	// Wait for the agent to actually be speaking.
	dlCtx, dlCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if _, err := r.p.AwaitDownlink(dlCtx); err != nil {
		t.Fatalf("no downlink: %v", err)
	}
	dlCancel()

	start := time.Now()
	r.ingest(0.5, 3) // interrupting speech: barge-in on the 3rd frame
	s := r.awaitCancelledTurn(t, time.Second)
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("barge-in took %v, budget 200ms", elapsed)
	}
	if got := r.ctrl.State(); got != SpeakingUser {
		t.Fatalf("state after barge-in = %v, want SPEAKING_USER", got)
	}
	if s.FlushedFrames < 0 {
		t.Fatalf("flush not recorded")
	}
	// Egress must be empty after the flush.
	quiet, qCancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer qCancel()
	if _, err := r.p.AwaitDownlink(quiet); err == nil {
		t.Fatal("stale downlink audio after barge-in flush")
	}
}

// TestNoiseDoesNotBargeIn: short bursts and sub-threshold noise during
// RESPONDING_AGENT must not cancel the response (task 6.5 / 6.6).
func TestNoiseDoesNotBargeIn(t *testing.T) {
	llm := &adapter.MockLLM{Latency: adapter.Latency{Delay: 40 * time.Millisecond}}
	r := newRig(t, vadTestConfig(), llm)
	defer r.stop()

	r.ingest(0.5, 5)
	r.ingest(0.0, 6)
	dlCtx, dlCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if _, err := r.p.AwaitDownlink(dlCtx); err != nil {
		t.Fatalf("no downlink: %v", err)
	}
	dlCancel()

	r.ingest(0.002, 10) // noise floor: below both thresholds
	r.ingest(0.5, 2)    // a thud: loud but shorter than min speech
	r.ingest(0.0, 2)

	// The turn must run to natural completion, uncancelled.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := r.p.LastTurn(); ok {
			if s.Cancelled {
				t.Fatal("noise triggered a barge-in")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("turn never completed")
}
