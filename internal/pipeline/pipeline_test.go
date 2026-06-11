package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/ringbuf"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig() config.Config {
	cfg := config.Default()
	cfg.RingBuf.IngressCapacity = 64
	cfg.RingBuf.EgressCapacity = 64
	cfg.Pipeline.TokenChanCap = 4
	cfg.Pipeline.TranscriptChanCap = 2
	cfg.Pipeline.AudioChanCap = 4
	return cfg
}

func mockSet(asr *adapter.MockASR, llm *adapter.MockLLM, tts *adapter.MockTTS) adapter.Set {
	if asr == nil {
		asr = &adapter.MockASR{Script: "hello kernel"}
	}
	if llm == nil {
		llm = &adapter.MockLLM{}
	}
	if tts == nil {
		tts = &adapter.MockTTS{SampleRate: 16000, FrameDuration: 20 * time.Millisecond}
	}
	return adapter.Set{ASR: asr, LLM: llm, TTS: tts}
}

// startPipeline runs p.Run in the background and returns a stopper that
// cancels and waits for clean exit.
func startPipeline(t *testing.T, p *Pipeline) (context.Context, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.Run(ctx) }()
	return ctx, func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not exit after cancel")
		}
	}
}

func pushFrames(p *Pipeline, n int, startTs int64) {
	for i := 0; i < n; i++ {
		p.PushAudio(adapter.AudioFrame{PCM: make([]byte, 640), TsUs: startTs + int64(i)*20_000})
	}
}

// awaitTurn waits until a turn whose prompt matches has been published.
func awaitTurn(t *testing.T, p *Pipeline, prompt string, within time.Duration) TurnStats {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s, ok := p.LastTurn(); ok && s.Prompt == prompt {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no completed turn for prompt %q within %v", prompt, within)
	return TurnStats{}
}

// TestThreeStageStreaming is the happy path: audio in, three stages overlap,
// audio out — and the first downlink frame arrives while the LLM is still
// emitting (streaming, not batch).
func TestThreeStageStreaming(t *testing.T) {
	llm := &adapter.MockLLM{
		Reply:   strings.Repeat("tok ", 20), // 20 tokens
		Latency: adapter.Latency{Delay: 15 * time.Millisecond},
	}
	var tokens atomic.Int32
	p, err := New(testConfig(), mockSet(nil, llm, nil), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	p.OnToken = func(adapter.Token) { tokens.Add(1) }

	ctx, stop := startPipeline(t, p)
	defer stop()

	pushFrames(p, 10, 0)
	p.EndUtterance()

	// First downlink frame must arrive long before the ~300ms token stream
	// finishes.
	dlCtx, dlCancel := context.WithTimeout(ctx, 2*time.Second)
	defer dlCancel()
	if _, err := p.AwaitDownlink(dlCtx); err != nil {
		t.Fatalf("no downlink audio: %v", err)
	}
	if got := tokens.Load(); got >= 20 {
		t.Fatalf("first audio only after all %d tokens: not streaming", got)
	}

	s := awaitTurn(t, p, "hello kernel", 3*time.Second)
	if s.Reply != llm.Reply {
		t.Fatalf("reply = %q, want %q", s.Reply, llm.Reply)
	}
	if int(tokens.Load()) != 20 {
		t.Fatalf("OnToken fired %d times, want 20", tokens.Load())
	}
	if s.FirstResponse() <= 0 || s.FirstResponse() >= s.EndedAt.Sub(s.UtteranceEndAt) {
		t.Fatalf("first response %v should be positive and well before turn end", s.FirstResponse())
	}
}

// TestBackpressurePropagatesToIngressDrop drives the spec's chain: slow TTS
// -> token channel fills -> LLM blocks -> finals queue fills -> ASR runner
// blocks -> nobody drains the ingress ring -> drop-oldest sheds frames, and
// every buffer stays at its configured bound.
func TestBackpressurePropagatesToIngressDrop(t *testing.T) {
	llm := &adapter.MockLLM{Reply: strings.Repeat("word ", 50)}
	tts := &adapter.MockTTS{
		SampleRate:    16000,
		FrameDuration: 20 * time.Millisecond,
		Latency:       adapter.Latency{Delay: 30 * time.Millisecond}, // ~30ms per frame: very slow
	}
	cfg := testConfig()
	p, err := New(cfg, mockSet(nil, llm, tts), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startPipeline(t, p)
	defer stop()

	// Utterance 1 starts the slow turn; 2 and 3 queue; the 4th final blocks
	// the ASR runner.
	for i := 0; i < 4; i++ {
		pushFrames(p, 5, int64(i)*1_000_000)
		p.EndUtterance()
		time.Sleep(20 * time.Millisecond) // let each utterance finalize
	}

	// With the pipe clogged, sustained audio must overflow the ingress ring
	// (cap 64) and shed.
	pushFrames(p, 500, 10_000_000)

	deadline := time.Now().Add(3 * time.Second)
	for p.IngressDropped() == 0 && time.Now().Before(deadline) {
		pushFrames(p, 50, 20_000_000)
		time.Sleep(10 * time.Millisecond)
	}
	if p.IngressDropped() == 0 {
		t.Fatal("ingress never dropped: backpressure did not reach the audio edge")
	}
	// Memory is bounded by construction (fixed rings, bounded channels);
	// goroutine count must also stay flat: 4 turn goroutines + ASR loop +
	// orchestrator, not one per utterance.
	if n := runtime.NumGoroutine(); n > 50 {
		t.Fatalf("goroutines ballooned to %d under load", n)
	}
}

// countingASR finalizes each utterance as "question N", so consecutive turns
// are distinguishable without mutating shared adapter state mid-test.
type countingASR struct{ n atomic.Int32 }

func (a *countingASR) Stream(ctx context.Context, in <-chan adapter.AudioFrame, out chan<- adapter.Transcript) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-in:
			if !ok {
				n := a.n.Add(1)
				tr := adapter.Transcript{Text: fmt.Sprintf("question %d", n), Final: true}
				select {
				case out <- tr:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

// TestBargeInCancelsFlushesAndRestarts covers tasks 5.4 + 5.5: cancel stops
// the sub-chain and flushes in-flight downlink audio, ASR keeps listening,
// and the next turn runs with no residue from the cancelled one.
func TestBargeInCancelsFlushesAndRestarts(t *testing.T) {
	// 3 echo tokens x 60ms: the turn runs ~180ms, leaving a wide window to
	// barge in after the first downlink frame (~60ms in).
	llm := &adapter.MockLLM{Latency: adapter.Latency{Delay: 60 * time.Millisecond}}
	set := mockSet(nil, llm, nil)
	set.ASR = &countingASR{}
	p, err := New(testConfig(), set, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := startPipeline(t, p)
	defer stop()

	pushFrames(p, 10, 0)
	p.EndUtterance()

	// Wait until the agent is mid-response.
	dlCtx, dlCancel := context.WithTimeout(ctx, 2*time.Second)
	if _, err := p.AwaitDownlink(dlCtx); err != nil {
		t.Fatalf("no downlink before barge-in: %v", err)
	}
	dlCancel()

	start := time.Now()
	p.BargeIn()
	s := awaitTurn(t, p, "question 1", 2*time.Second)
	if !s.Cancelled {
		t.Fatal("turn not marked cancelled")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("barge-in took %v, budget is 200ms", elapsed)
	}
	// Egress must hold nothing stale.
	quiet, qCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer qCancel()
	if f, err := p.AwaitDownlink(quiet); err == nil {
		t.Fatalf("stale downlink frame after flush: ts=%d", f.TsUs)
	}

	// ASR kept listening: a new utterance gets a fresh, complete answer.
	pushFrames(p, 10, 5_000_000)
	p.EndUtterance()
	s2 := awaitTurn(t, p, "question 2", 3*time.Second)
	if s2.Cancelled {
		t.Fatal("restarted turn unexpectedly cancelled")
	}
	if s2.Reply != "echo: question 2" {
		t.Fatalf("residue in restarted turn: reply = %q", s2.Reply)
	}
}

// TestLatencyDecomposition checks task 5.6: with known injected model
// latencies, FirstResponse ≈ model spans and the kernel's own overhead is
// small and separable.
func TestLatencyDecomposition(t *testing.T) {
	asr := &adapter.MockASR{
		Script:  "measure me",
		Latency: adapter.Latency{Delay: 20 * time.Millisecond},
	}
	llm := &adapter.MockLLM{Latency: adapter.Latency{Delay: 20 * time.Millisecond}}
	p, err := New(testConfig(), mockSet(asr, llm, nil), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startPipeline(t, p)
	defer stop()

	pushFrames(p, 10, 0)
	p.EndUtterance()
	s := awaitTurn(t, p, "measure me", 3*time.Second)

	if s.ModelLatency() < 35*time.Millisecond {
		t.Fatalf("model latency %v should reflect ~40ms of injected delay", s.ModelLatency())
	}
	if s.KernelOverhead() > 60*time.Millisecond {
		t.Fatalf("kernel overhead %v is implausibly high", s.KernelOverhead())
	}
	if s.FirstResponse() != s.ModelLatency()+s.KernelOverhead() {
		t.Fatal("decomposition does not sum to first-response latency")
	}
	for i, ts := range []time.Time{s.UtteranceEndAt, s.ASRFinalAt, s.LLMFirstTokenAt, s.TTSFirstFrameAt} {
		if ts.IsZero() {
			t.Fatalf("boundary timestamp %d not captured", i)
		}
	}
}

// TestShutdownLeavesNoGoroutines: Run's exit must tear down the ASR loop and
// any in-flight turn.
func TestShutdownLeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	llm := &adapter.MockLLM{
		Reply:   strings.Repeat("slow ", 100),
		Latency: adapter.Latency{Delay: 20 * time.Millisecond},
	}
	p, err := New(testConfig(), mockSet(nil, llm, nil), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startPipeline(t, p)
	pushFrames(p, 10, 0)
	p.EndUtterance()
	time.Sleep(50 * time.Millisecond) // mid-turn
	stop()

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("goroutines leaked: %d -> %d", before, n)
	}
}

func TestEdgeDoorbell(t *testing.T) {
	e, err := newEdge(8, ringbuf.DropOldest)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan adapter.AudioFrame, 1)
	go func() {
		f, _ := e.await(context.Background())
		got <- f
	}()
	time.Sleep(20 * time.Millisecond) // consumer is parked
	e.push(adapter.AudioFrame{TsUs: 42})
	select {
	case f := <-got:
		if f.TsUs != 42 {
			t.Fatalf("TsUs = %d", f.TsUs)
		}
	case <-time.After(time.Second):
		t.Fatal("doorbell never woke the consumer")
	}
}
