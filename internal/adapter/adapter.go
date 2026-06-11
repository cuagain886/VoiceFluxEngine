package adapter

import "context"

// AudioFrame is one PCM frame moving through an adapter boundary. TsUs is the
// sample-clock PTS (D7): for ASR input it is stamped at ingress; for TTS
// output it is derived from the cumulative samples synthesized.
type AudioFrame struct {
	PCM  []byte
	TsUs int64
}

// Transcript is an incremental ASR result. A stream is zero or more partials
// (Final=false, each replacing the previous hypothesis) followed by exactly
// one final. TsUs anchors the transcript to the source audio's PTS.
type Transcript struct {
	Text  string
	Final bool
	TsUs  int64
}

// Token is one increment of LLM output text. It is also the TTS input unit;
// the pipeline may aggregate tokens into larger chunks before synthesis
// without changing the interface.
type Token struct {
	Text string
}

// Message is one turn of conversation history.
type Message struct {
	Role string // "system" | "user" | "assistant"
	Text string
}

// Turn is the input to one LLM generation.
type Turn struct {
	Prompt  string
	History []Message
}

// Streaming contract shared by all three interfaces:
//
//   - Stream runs synchronously in the caller's goroutine (the pipeline gives
//     each stage its own) and returns when the stream completes, the input
//     channel closes, or ctx is cancelled.
//   - The adapter never closes out; the caller owns it and closes it after
//     Stream returns. Every send must abort if ctx is cancelled, so a
//     cancelled adapter can never block on a full channel — that is what
//     makes barge-in's sub-chain cancel prompt.
//   - Blocking on a full out channel is intentional: it is the pipeline's
//     text backpressure (D3) propagating upstream.

// ASR consumes one utterance of PCM frames and emits incremental transcripts.
// The caller closes in to mark the end of the utterance; the adapter then
// emits the final transcript and returns.
type ASR interface {
	Stream(ctx context.Context, in <-chan AudioFrame, out chan<- Transcript) error
}

// LLM generates a token stream for one conversational turn.
type LLM interface {
	Stream(ctx context.Context, turn Turn, out chan<- Token) error
}

// TTS consumes text increments and emits synthesized PCM frames as they
// become available, so playback can start before the full text is known.
type TTS interface {
	Stream(ctx context.Context, in <-chan Token, out chan<- AudioFrame) error
}
