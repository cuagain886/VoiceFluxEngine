package metrics

import (
	"sync/atomic"
	"time"

	"voicestream/internal/pipeline"
)

// latencyBuckets 覆盖对话尺度的范围：5ms .. 5s。
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5}

// cancelBuckets 覆盖打断预算：1ms .. 500ms（预算是 200ms）。
var cancelBuckets = []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5}

// M 把内核的标准仪表、抓取注册表和仪表盘 hub 打成一束。每进程一个。
type M struct {
	Registry *Registry
	Hub      *Hub

	TurnsCompleted *Counter
	TurnsCancelled *Counter

	FirstResponse  *Histogram // 语句结束 -> 第一帧下行
	KernelOverhead *Histogram // 首响减去模型跨度
	StageASRFinal  *Histogram // 语句结束 -> final 转写
	StageQueueWait *Histogram // final 产出 -> 轮被领取
	StageLLMFirst  *Histogram // LLM 开始 -> 第一个 token
	StageTTSFirst  *Histogram // 第一个 token 进 TTS -> 第一帧
	BargeInCancel  *Histogram // 取消发起 -> 子链停止 + 排空完成

	// 帧卫生总量：会话被回收时累加进来；活动会话的实时值在抓取期通过
	// gauge func 加上。
	AccIngressDropped atomic.Uint64
	AccEgressDropped  atomic.Uint64
	AccDupFrames      atomic.Uint64
	AccStaleFrames    atomic.Uint64
	AccSubtitleDrops  atomic.Uint64
	AccIllegalEvents  atomic.Uint64
}

// New 构建进程级的仪表集合。
func New() *M {
	r := NewRegistry()
	registerRuntimeGauges(r)
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

// TurnRecord 是仪表盘的逐轮瀑布数据点。所有 *Ms 字段都是相对语句结束（t0）
// 的毫秒数；-1 标记一个缺失的边界（例如某轮在 TTS 产出任何东西之前就被取消）。
type TurnRecord struct {
	Session   string  `json:"session"`
	Time      string  `json:"time"` // 墙钟，HH:MM:SS
	Prompt    string  `json:"prompt"`
	Reply     string  `json:"reply"`
	Cancelled bool    `json:"cancelled"`
	WallMs    float64 `json:"wallMs"` // t0 -> 轮结束

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
	// SerialMs 是同样三段跨度若改为朴素串行 ASR->LLM->TTS 跑会花的时间——
	// 也就是仪表盘的「重叠对照」条。
	SerialMs float64 `json:"serialMs"`
}

// RecordTurn 把一个完成的轮喂进直方图与仪表盘 hub。它跑在流水线编排器
// goroutine 上：下面的一切都是原子操作、算术、和一次非阻塞 publish。
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
	// 朴素串行 = 每个阶段的完整跨度，首尾相接。
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
