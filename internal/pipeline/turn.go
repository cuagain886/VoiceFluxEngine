package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"voicestream/internal/adapter"
)

// maxHistory bounds the conversation memory passed to the LLM.
const maxHistory = 16

// TurnStats is the per-turn latency decomposition (task 5.6). Boundary
// timestamps are captured at stage handoffs so the total splits into "time
// inside model calls" vs "time inside the kernel's plumbing".
type TurnStats struct {
	Prompt string
	Reply  string

	UtteranceEndAt  time.Time // t0: user stopped speaking
	ASRFinalAt      time.Time // final transcript available
	LLMStartAt      time.Time // LLM stream opened
	LLMFirstTokenAt time.Time // first token out of the LLM
	TTSStartAt      time.Time // first token handed to TTS
	TTSFirstFrameAt time.Time // first synthesized frame in the egress ring
	EndedAt         time.Time

	Cancelled     bool
	FlushedFrames int // egress frames drained by a barge-in flush
}

// FirstResponse is the user-perceived latency: speech end to first downlink
// audio frame being available to the transport.
func (s TurnStats) FirstResponse() time.Duration {
	if s.TTSFirstFrameAt.IsZero() {
		return 0
	}
	return s.TTSFirstFrameAt.Sub(s.UtteranceEndAt)
}

// ModelLatency sums the time spent waiting on the models themselves: ASR
// finalization, LLM first token, TTS first frame.
func (s TurnStats) ModelLatency() time.Duration {
	if s.TTSFirstFrameAt.IsZero() {
		return 0
	}
	return s.ASRFinalAt.Sub(s.UtteranceEndAt) +
		s.LLMFirstTokenAt.Sub(s.LLMStartAt) +
		s.TTSFirstFrameAt.Sub(s.TTSStartAt)
}

// KernelOverhead is what this project is accountable for: first-response
// latency minus the model-inherent spans — queueing and handoff costs in the
// transport/orchestration layer.
func (s TurnStats) KernelOverhead() time.Duration {
	if s.TTSFirstFrameAt.IsZero() {
		return 0
	}
	return s.FirstResponse() - s.ModelLatency()
}

// turnHandle owns one LLM->TTS response sub-chain. Its goroutines share the
// turn context; cancelling it is the barge-in mechanism. Stats fields are
// written by the sub-chain goroutines and read only after done closes
// (wg.Wait provides the happens-before edge).
type turnHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	stats  TurnStats
	err    error // first non-cancellation stage error
}

// startTurn launches the response sub-chain for one finalized utterance:
//
//	LLM ──tokens──▶ forwarder ──ttsIn──▶ TTS ──ttsOut──▶ egress pump ─▶ ring
//
// Four goroutines, all under the turn context. The forwarder tees tokens to
// the OnToken observer and the reply accumulator; its blocking send into
// ttsIn is the LLM-side backpressure. The pump's ring push never blocks —
// egress is real-time and sheds instead.
func (p *Pipeline) startTurn(ctx context.Context, u utterance) *turnHandle {
	tctx, cancel := context.WithCancel(ctx)
	h := &turnHandle{cancel: cancel, done: make(chan struct{})}
	h.stats.Prompt = u.text
	h.stats.UtteranceEndAt = u.endAt
	h.stats.ASRFinalAt = u.finalAt

	turn := adapter.Turn{Prompt: u.text, History: append([]adapter.Message(nil), p.history...)}

	tokens := make(chan adapter.Token, p.cfg.Pipeline.TokenChanCap)
	ttsIn := make(chan adapter.Token, p.cfg.Pipeline.TokenChanCap)
	ttsOut := make(chan adapter.AudioFrame, p.cfg.Pipeline.AudioChanCap)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	fail := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		errMu.Lock()
		if h.err == nil {
			h.err = err
		}
		errMu.Unlock()
		cancel() // a dead stage takes the whole sub-chain down
	}

	// LLM stage.
	wg.Add(1)
	h.stats.LLMStartAt = time.Now()
	go func() {
		defer wg.Done()
		fail(p.set.LLM.Stream(tctx, turn, tokens))
		close(tokens)
	}()

	// Token forwarder: observe, accumulate the reply, feed TTS.
	wg.Add(1)
	var reply strings.Builder
	go func() {
		defer wg.Done()
		defer close(ttsIn)
		for tok := range tokens {
			if h.stats.LLMFirstTokenAt.IsZero() {
				h.stats.LLMFirstTokenAt = time.Now()
			}
			reply.WriteString(tok.Text)
			if p.OnToken != nil {
				p.OnToken(tok)
			}
			select {
			case ttsIn <- tok:
				if h.stats.TTSStartAt.IsZero() {
					h.stats.TTSStartAt = time.Now()
				}
			case <-tctx.Done():
				return
			}
		}
	}()

	// TTS stage.
	wg.Add(1)
	go func() {
		defer wg.Done()
		fail(p.set.TTS.Stream(tctx, ttsIn, ttsOut))
		close(ttsOut)
	}()

	// Egress pump: the only writer into the egress ring during this turn.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for f := range ttsOut {
			p.egress.push(f)
			if h.stats.TTSFirstFrameAt.IsZero() {
				h.stats.TTSFirstFrameAt = time.Now()
			}
		}
	}()

	go func() {
		wg.Wait()
		h.stats.Reply = reply.String()
		h.stats.EndedAt = time.Now()
		close(h.done)
	}()
	if p.OnTurnStart != nil {
		p.OnTurnStart()
	}
	return h
}

// finishTurn records a naturally completed turn and appends it to the
// conversation history.
func (p *Pipeline) finishTurn(h *turnHandle) {
	<-h.done
	if h.err != nil {
		p.logger.Error("turn stage failed", "err", h.err)
	}
	p.appendHistory(h.stats.Prompt, h.stats.Reply)
	p.publish(h.stats)
	if p.OnTurnEnd != nil {
		p.OnTurnEnd(false)
	}
}

// cancelTurn is the barge-in path (task 5.4): cancel the sub-chain, wait for
// every stage to exit (the adapters' cancel contract makes this prompt), then
// flush the in-flight downlink audio so the egress holds nothing stale. The
// ASR loop is untouched — it keeps listening throughout.
func (p *Pipeline) cancelTurn(h *turnHandle) {
	h.cancel()
	<-h.done
	h.stats.Cancelled = true
	h.stats.FlushedFrames = p.egress.drain()
	// What the agent managed to say still happened; keep it in history so
	// the next turn's context reflects reality.
	p.appendHistory(h.stats.Prompt, h.stats.Reply)
	p.publish(h.stats)
	if p.OnTurnEnd != nil {
		p.OnTurnEnd(true)
	}
}

func (p *Pipeline) appendHistory(prompt, reply string) {
	p.history = append(p.history,
		adapter.Message{Role: "user", Text: prompt},
		adapter.Message{Role: "assistant", Text: reply},
	)
	if len(p.history) > maxHistory {
		p.history = append(p.history[:0], p.history[len(p.history)-maxHistory:]...)
	}
}
