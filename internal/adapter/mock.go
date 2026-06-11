package adapter

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// Latency injects a configurable fixed delay plus bounded jitter before each
// emission. Jitter is drawn from a seeded PRNG so a given seed always
// reproduces the same schedule — benchmarks stay deterministic while still
// exercising irregular timing (the point of the mock per the spec: measure
// kernel overhead separately from model-inherent latency).
type Latency struct {
	Delay  time.Duration // fixed component, applied before every emission
	Jitter time.Duration // max extra delay, uniformly drawn per emission
	Seed   uint64        // PRNG seed; same seed -> same jitter schedule
}

func (l Latency) newRNG() *rand.Rand {
	return rand.New(rand.NewPCG(l.Seed, l.Seed))
}

// wait sleeps for the next scheduled delay, aborting early on ctx cancel.
func (l Latency) wait(ctx context.Context, rng *rand.Rand) error {
	d := l.Delay
	if l.Jitter > 0 {
		d += time.Duration(rng.Int64N(int64(l.Jitter) + 1))
	}
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// send delivers v without ever outliving a cancelled context.
func send[T any](ctx context.Context, out chan<- T, v T) error {
	select {
	case out <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MockASR pretends to recognize one utterance: every PartialEvery consumed
// frames it emits a growing prefix of Script as a partial, and when the input
// closes it emits Script as the final. Transcripts are anchored to the PTS of
// the most recent audio frame.
type MockASR struct {
	Script       string // the "recognized" text
	PartialEvery int    // emit a partial every N frames; 0 disables partials
	Latency      Latency
}

func (m *MockASR) Stream(ctx context.Context, in <-chan AudioFrame, out chan<- Transcript) error {
	words := strings.Fields(m.Script)
	rng := m.Latency.newRNG()
	frames, emitted := 0, 0
	var lastTs int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-in:
			if !ok {
				if err := m.Latency.wait(ctx, rng); err != nil {
					return err
				}
				return send(ctx, out, Transcript{Text: m.Script, Final: true, TsUs: lastTs})
			}
			lastTs = f.TsUs
			frames++
			if m.PartialEvery > 0 && frames%m.PartialEvery == 0 && emitted < len(words) {
				emitted++
				if err := m.Latency.wait(ctx, rng); err != nil {
					return err
				}
				partial := strings.Join(words[:emitted], " ")
				if err := send(ctx, out, Transcript{Text: partial, TsUs: lastTs}); err != nil {
					return err
				}
			}
		}
	}
}

// MockLLM emits a deterministic reply as a token stream. With Reply unset it
// echoes the prompt ("echo: <prompt>"), so tests can derive expected output
// from input alone. Tokenization is whitespace words when present, otherwise
// fixed-size rune chunks (CJK-ish granularity).
type MockLLM struct {
	Reply   string // fixed reply; empty -> "echo: <prompt>"
	Latency Latency
}

func (m *MockLLM) Stream(ctx context.Context, turn Turn, out chan<- Token) error {
	reply := m.Reply
	if reply == "" {
		reply = "echo: " + turn.Prompt
	}
	rng := m.Latency.newRNG()
	for _, tok := range tokenize(reply) {
		if err := m.Latency.wait(ctx, rng); err != nil {
			return err
		}
		if err := send(ctx, out, Token{Text: tok}); err != nil {
			return err
		}
	}
	return nil
}

// tokenize splits text into word tokens (space-separated languages) or
// three-rune chunks (no spaces), preserving exact concatenation either way.
func tokenize(text string) []string {
	if strings.ContainsRune(text, ' ') {
		fields := strings.SplitAfter(text, " ")
		out := fields[:0]
		for _, f := range fields {
			if f != "" {
				out = append(out, f)
			}
		}
		return out
	}
	runes := []rune(text)
	var toks []string
	for i := 0; i < len(runes); i += 3 {
		end := min(i+3, len(runes))
		toks = append(toks, string(runes[i:end]))
	}
	return toks
}

// MockTTS synthesizes silence at a fixed rate: each input token becomes
// MsPerRune milliseconds of zeroed PCM per rune, cut into frame-sized pieces.
// PTS advances on the synthesis sample clock (cumulative samples / rate), so
// downstream timing logic sees realistic, gapless audio timestamps.
type MockTTS struct {
	SampleRate    int           // e.g. 16000
	FrameDuration time.Duration // e.g. 20ms
	MsPerRune     int           // speech tempo; default 60ms per rune
	Latency       Latency
}

func (m *MockTTS) Stream(ctx context.Context, in <-chan Token, out chan<- AudioFrame) error {
	if m.SampleRate <= 0 || m.FrameDuration <= 0 {
		return fmt.Errorf("adapter: MockTTS requires SampleRate and FrameDuration")
	}
	msPerRune := m.MsPerRune
	if msPerRune <= 0 {
		msPerRune = 60
	}
	samplesPerFrame := int(int64(m.SampleRate) * int64(m.FrameDuration) / int64(time.Second))
	rng := m.Latency.newRNG()
	var totalSamples int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tok, ok := <-in:
			if !ok {
				return nil
			}
			dur := time.Duration(len([]rune(tok.Text))*msPerRune) * time.Millisecond
			frames := int((dur + m.FrameDuration - 1) / m.FrameDuration)
			for i := 0; i < frames; i++ {
				if err := m.Latency.wait(ctx, rng); err != nil {
					return err
				}
				f := AudioFrame{
					PCM:  make([]byte, samplesPerFrame*2), // 16-bit mono silence
					TsUs: totalSamples * 1_000_000 / int64(m.SampleRate),
				}
				if err := send(ctx, out, f); err != nil {
					return err
				}
				totalSamples += int64(samplesPerFrame)
			}
		}
	}
}
