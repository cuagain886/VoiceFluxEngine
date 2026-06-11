package metrics

import (
	"sync/atomic"
	"time"

	"voicestream/internal/pipeline"
)

// latencyBuckets cover the conversational range: 5ms .. 5s.
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5}

// cancelBuckets cover the barge-in budget: 1ms .. 500ms (budget is 200ms).
var cancelBuckets = []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5}

// M bundles the kernel's standard instruments, the scrape registry and the
// dashboard hub. One per process.
type M struct {
	Registry *Registry
	Hub      *Hub

	TurnsCompleted *Counter
	TurnsCancelled *Counter

	FirstResponse  *Histogram // utterance end -> first downlink frame
	KernelOverhead *Histogram // FirstResponse minus model spans
	StageASRFinal  *Histogram // utterance end -> final transcript
	StageQueueWait *Histogram // final produced -> turn picked up
	StageLLMFirst  *Histogram // LLM start -> first token
	StageTTSFirst  *Histogram // first token to TTS -> first frame
	BargeInCancel  *Histogram // cancel requested -> sub-chain down + flushed

	// Frame-hygiene totals accumulated from sessions when they are
	// reclaimed; live values are added at scrape time via gauge funcs.
	AccIngressDropped atomic.Uint64
	AccEgressDropped  atomic.Uint64
	AccDupFrames      atomic.Uint64
	AccStaleFrames    atomic.Uint64
	AccSubtitleDrops  atomic.Uint64
	AccIllegalEvents  atomic.Uint64
}

// New builds the process-wide instrument set.
func New() *M {
	r := NewRegistry()
	return &M{
		Registry:       r,
		Hub:            NewHub(),
		TurnsCompleted: r.NewCounter("voicestream_turns_completed_total", "Turns that ran to natural completion."),
		TurnsCancelled: r.NewCounter("voicestream_turns_cancelled_total", "Turns cancelled by barge-in or supersession."),
		FirstResponse:  r.NewHistogram("voicestream_first_response_seconds", "Utterance end to first downlink audio frame.", latencyBuckets),
		KernelOverhead: r.NewHistogram("voicestream_kernel_overhead_seconds", "First-response latency minus model-inherent spans.", latencyBuckets),
		StageASRFinal:  r.NewHistogram("voicestream_stage_asr_final_seconds", "Utterance end to final transcript.", latencyBuckets),
		StageQueueWait: r.NewHistogram("voicestream_stage_queue_wait_seconds", "Final transcript produced to turn pickup (orchestrator queue).", latencyBuckets),
		StageLLMFirst:  r.NewHistogram("voicestream_stage_llm_first_token_seconds", "LLM stream open to first token.", latencyBuckets),
		StageTTSFirst:  r.NewHistogram("voicestream_stage_tts_first_frame_seconds", "First token into TTS to first synthesized frame.", latencyBuckets),
		BargeInCancel:  r.NewHistogram("voicestream_barge_in_cancel_seconds", "Barge-in cancel request to sub-chain torn down and egress flushed.", cancelBuckets),
	}
}

// TurnRecord is the dashboard's per-turn waterfall datum. All *Ms fields are
// milliseconds relative to utterance end (t0); -1 marks an absent boundary
// (e.g. a turn cancelled before TTS produced anything).
type TurnRecord struct {
	Session   string  `json:"session"`
	Time      string  `json:"time"` // wall clock, HH:MM:SS
	Prompt    string  `json:"prompt"`
	Reply     string  `json:"reply"`
	Cancelled bool    `json:"cancelled"`
	WallMs    float64 `json:"wallMs"` // t0 -> turn end

	ASRFinalMs      float64 `json:"asrFinalMs"`
	LLMStartMs      float64 `json:"llmStartMs"`
	LLMFirstTokenMs float64 `json:"llmFirstTokenMs"`
	LLMLastTokenMs  float64 `json:"llmLastTokenMs"`
	TTSStartMs      float64 `json:"ttsStartMs"`
	TTSFirstFrameMs float64 `json:"ttsFirstFrameMs"`
	TTSLastFrameMs  float64 `json:"ttsLastFrameMs"`

	FirstResponseMs float64 `json:"firstResponseMs"`
	KernelMs        float64 `json:"kernelMs"`
	BargeInMs       float64 `json:"bargeInMs"`
	// SerialMs is what a naive sequential ASR->LLM->TTS run of the same
	// spans would have taken — the dashboard's overlap comparison bar.
	SerialMs float64 `json:"serialMs"`
}

// RecordTurn feeds one finished turn into histograms and the dashboard hub.
// It runs on the pipeline orchestrator goroutine: everything below is
// atomics, arithmetic and a non-blocking publish.
func (m *M) RecordTurn(sessionID string, ts pipeline.TurnStats) {
	if ts.Cancelled {
		m.TurnsCancelled.Add(1)
		if ts.BargeInLatency > 0 {
			m.BargeInCancel.Observe(ts.BargeInLatency.Seconds())
		}
	} else {
		m.TurnsCompleted.Add(1)
	}
	if !ts.TTSFirstFrameAt.IsZero() {
		m.FirstResponse.Observe(ts.FirstResponse().Seconds())
		m.KernelOverhead.Observe(ts.KernelOverhead().Seconds())
		m.StageASRFinal.Observe(ts.ASRFinalAt.Sub(ts.UtteranceEndAt).Seconds())
		m.StageQueueWait.Observe(ts.LLMStartAt.Sub(ts.ASRFinalAt).Seconds())
		m.StageLLMFirst.Observe(ts.LLMFirstTokenAt.Sub(ts.LLMStartAt).Seconds())
		m.StageTTSFirst.Observe(ts.TTSFirstFrameAt.Sub(ts.TTSStartAt).Seconds())
	}
	m.Hub.Publish(toRecord(sessionID, ts))
}

func toRecord(sessionID string, ts pipeline.TurnStats) TurnRecord {
	t0 := ts.UtteranceEndAt
	rel := func(t time.Time) float64 {
		if t.IsZero() {
			return -1
		}
		return float64(t.Sub(t0).Microseconds()) / 1000
	}
	rec := TurnRecord{
		Session:         shortID(sessionID),
		Time:            ts.EndedAt.Format("15:04:05"),
		Prompt:          truncate(ts.Prompt, 60),
		Reply:           truncate(ts.Reply, 60),
		Cancelled:       ts.Cancelled,
		WallMs:          rel(ts.EndedAt),
		ASRFinalMs:      rel(ts.ASRFinalAt),
		LLMStartMs:      rel(ts.LLMStartAt),
		LLMFirstTokenMs: rel(ts.LLMFirstTokenAt),
		LLMLastTokenMs:  rel(ts.LLMLastTokenAt),
		TTSStartMs:      rel(ts.TTSStartAt),
		TTSFirstFrameMs: rel(ts.TTSFirstFrameAt),
		TTSLastFrameMs:  rel(ts.TTSLastFrameAt),
		FirstResponseMs: float64(ts.FirstResponse().Microseconds()) / 1000,
		KernelMs:        float64(ts.KernelOverhead().Microseconds()) / 1000,
		BargeInMs:       float64(ts.BargeInLatency.Microseconds()) / 1000,
	}
	// Naive serial = each stage's full span, one after another.
	if rec.ASRFinalMs >= 0 && rec.LLMLastTokenMs >= 0 && rec.LLMStartMs >= 0 {
		serial := rec.ASRFinalMs + (rec.LLMLastTokenMs - rec.LLMStartMs)
		if rec.TTSLastFrameMs >= 0 && rec.TTSStartMs >= 0 {
			serial += rec.TTSLastFrameMs - rec.TTSStartMs
		}
		rec.SerialMs = serial
	}
	return rec
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
