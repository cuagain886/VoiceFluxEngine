package adapter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMockASRPartialsThenFinal(t *testing.T) {
	asr := &MockASR{Script: "hello streaming world", PartialEvery: 2}
	in := make(chan AudioFrame)
	out := make(chan Transcript, 16)

	go func() {
		for i := 0; i < 6; i++ {
			in <- AudioFrame{PCM: make([]byte, 640), TsUs: int64(i) * 20_000}
		}
		close(in)
	}()

	if err := asr.Stream(context.Background(), in, out); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	close(out)

	var got []Transcript
	for tr := range out {
		got = append(got, tr)
	}
	if len(got) < 2 {
		t.Fatalf("want partials + final, got %d transcripts", len(got))
	}
	for i, tr := range got[:len(got)-1] {
		if tr.Final {
			t.Fatalf("transcript %d marked final before the end", i)
		}
		if !strings.HasPrefix("hello streaming world", tr.Text) {
			t.Fatalf("partial %q is not a prefix of the script", tr.Text)
		}
	}
	last := got[len(got)-1]
	if !last.Final || last.Text != "hello streaming world" {
		t.Fatalf("final = %+v, want full script with Final=true", last)
	}
	if last.TsUs != 5*20_000 {
		t.Fatalf("final TsUs = %d, want anchored to last frame (100000)", last.TsUs)
	}
}

func TestMockLLMIncremental(t *testing.T) {
	llm := &MockLLM{}
	out := make(chan Token, 64)
	if err := llm.Stream(context.Background(), Turn{Prompt: "how are you"}, out); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	close(out)

	var parts []string
	for tok := range out {
		parts = append(parts, tok.Text)
	}
	if len(parts) < 2 {
		t.Fatalf("want incremental tokens, got %d", len(parts))
	}
	if joined := strings.Join(parts, ""); joined != "echo: how are you" {
		t.Fatalf("concatenation = %q, want %q", joined, "echo: how are you")
	}
}

func TestMockLLMCJKTokenization(t *testing.T) {
	llm := &MockLLM{Reply: "你好流式语音内核"}
	out := make(chan Token, 64)
	if err := llm.Stream(context.Background(), Turn{}, out); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	close(out)
	var parts []string
	for tok := range out {
		parts = append(parts, tok.Text)
	}
	if len(parts) != 3 { // 8 runes in chunks of 3 -> 3+3+2
		t.Fatalf("token count = %d, want 3", len(parts))
	}
	if joined := strings.Join(parts, ""); joined != "你好流式语音内核" {
		t.Fatalf("concatenation = %q", joined)
	}
}

func TestMockTTSSampleClockPTS(t *testing.T) {
	tts := &MockTTS{SampleRate: 16000, FrameDuration: 20 * time.Millisecond}
	in := make(chan Token, 2)
	in <- Token{Text: "hi"} // 2 runes * 60ms = 120ms -> 6 frames
	close(in)
	out := make(chan AudioFrame, 64)
	if err := tts.Stream(context.Background(), in, out); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	close(out)

	var frames []AudioFrame
	for f := range out {
		frames = append(frames, f)
	}
	if len(frames) != 6 {
		t.Fatalf("frames = %d, want 6", len(frames))
	}
	for i, f := range frames {
		if len(f.PCM) != 640 {
			t.Fatalf("frame %d size = %d, want 640", i, len(f.PCM))
		}
		if want := int64(i) * 20_000; f.TsUs != want {
			t.Fatalf("frame %d TsUs = %d, want %d (sample clock)", i, f.TsUs, want)
		}
	}
}

// TestCancellationStopsOutput is the barge-in contract at adapter level:
// cancel mid-stream and the adapter must return promptly with ctx.Err()
// even if nobody is draining its output channel.
func TestCancellationStopsOutput(t *testing.T) {
	llm := &MockLLM{
		Reply:   strings.Repeat("word ", 100),
		Latency: Latency{Delay: 5 * time.Millisecond},
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Token) // unbuffered and undrained after the first token

	errc := make(chan error, 1)
	go func() { errc <- llm.Stream(ctx, Turn{}, out) }()

	<-out // accept exactly one token, then abandon the channel
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not return after cancel — barge-in would hang")
	}
}

func TestLatencyDeterministicJitter(t *testing.T) {
	a := Latency{Jitter: time.Hour, Seed: 7}.newRNG()
	b := Latency{Jitter: time.Hour, Seed: 7}.newRNG()
	for i := 0; i < 10; i++ {
		if a.Int64N(1000) != b.Int64N(1000) {
			t.Fatal("same seed must reproduce the same jitter schedule")
		}
	}
}

// TestFullMockChain runs audio -> ASR -> LLM -> TTS -> audio end-to-end
// through nothing but the adapter interfaces: the "全 mock 跑通端到端"
// scenario, with no orchestration layer yet (that is M5).
func TestFullMockChain(t *testing.T) {
	ctx := context.Background()
	asr := &MockASR{Script: "turn on the lights", PartialEvery: 2}
	llm := &MockLLM{}
	tts := &MockTTS{SampleRate: 16000, FrameDuration: 20 * time.Millisecond}

	// ASR
	audioIn := make(chan AudioFrame)
	go func() {
		for i := 0; i < 10; i++ {
			audioIn <- AudioFrame{PCM: make([]byte, 640), TsUs: int64(i) * 20_000}
		}
		close(audioIn)
	}()
	transcripts := make(chan Transcript, 16)
	if err := asr.Stream(ctx, audioIn, transcripts); err != nil {
		t.Fatalf("ASR: %v", err)
	}
	close(transcripts)
	var final string
	for tr := range transcripts {
		if tr.Final {
			final = tr.Text
		}
	}
	if final == "" {
		t.Fatal("no final transcript")
	}

	// LLM
	tokens := make(chan Token, 64)
	if err := llm.Stream(ctx, Turn{Prompt: final}, tokens); err != nil {
		t.Fatalf("LLM: %v", err)
	}
	close(tokens)

	// TTS
	audioOut := make(chan AudioFrame, 256)
	if err := tts.Stream(ctx, tokens, audioOut); err != nil {
		t.Fatalf("TTS: %v", err)
	}
	close(audioOut)

	n, prevTs := 0, int64(-1)
	for f := range audioOut {
		if f.TsUs <= prevTs {
			t.Fatalf("PTS not monotonic: %d after %d", f.TsUs, prevTs)
		}
		prevTs = f.TsUs
		n++
	}
	if n == 0 {
		t.Fatal("chain produced no audio")
	}
}
