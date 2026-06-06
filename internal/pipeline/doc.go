// Package pipeline orchestrates the ASR -> LLM -> TTS streaming stages with
// dual backpressure (drop-oldest at audio edges, blocking channels for text)
// and cancellable sub-chains for barge-in (M5).
package pipeline
