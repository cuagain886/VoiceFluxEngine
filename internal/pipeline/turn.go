package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"voicestream/internal/adapter"
)

// maxHistory 限定传给 LLM 的对话记忆长度。
const maxHistory = 16

// TurnStats 是每轮的延迟分解（任务 5.6）。边界时间戳在各阶段交接处采集，
// 于是总延迟可拆成「待在模型调用里的时间」vs「待在内核管道里的时间」两本账。
type TurnStats struct {
	Prompt string
	Reply  string

	UtteranceEndAt  time.Time // t0：用户停止说话
	ASRFinalAt      time.Time // 最终转写可用
	LLMStartAt      time.Time // LLM 流打开
	LLMFirstTokenAt time.Time // LLM 吐出第一个 token
	LLMLastTokenAt  time.Time // LLM 吐出最后一个 token（完整跨度的右端）
	TTSStartAt      time.Time // 第一个 token 递给 TTS
	TTSFirstFrameAt time.Time // 第一帧合成音频进入出口环
	TTSLastFrameAt  time.Time // 最后一帧合成音频（完整跨度的右端）
	EndedAt         time.Time

	Cancelled      bool
	FlushedFrames  int           // 被打断 flush 排空的出口帧数
	BargeInLatency time.Duration // 取消发起 -> 子链停止 + 排空完成
}

// FirstResponse 是用户感知到的延迟：从说完话到第一帧下行音频可被传输取走。
func (s TurnStats) FirstResponse() time.Duration {
	if s.TTSFirstFrameAt.IsZero() {
		return 0
	}
	return s.TTSFirstFrameAt.Sub(s.UtteranceEndAt)
}

// ModelLatency 累加「等模型自己」的时间：ASR 定稿、LLM 首 token、TTS 首帧。
func (s TurnStats) ModelLatency() time.Duration {
	if s.TTSFirstFrameAt.IsZero() {
		return 0
	}
	return s.ASRFinalAt.Sub(s.UtteranceEndAt) +
		s.LLMFirstTokenAt.Sub(s.LLMStartAt) +
		s.TTSFirstFrameAt.Sub(s.TTSStartAt)
}

// KernelOverhead 是本项目该负责的那本账：首响延迟减去模型固有跨度——也就是
// 传输/编排层里的排队与交接开销。
func (s TurnStats) KernelOverhead() time.Duration {
	if s.TTSFirstFrameAt.IsZero() {
		return 0
	}
	return s.FirstResponse() - s.ModelLatency()
}

// turnHandle 拥有一条 LLM->TTS 响应子链。它的几个 goroutine 共享轮 context；
// 取消这个 context 就是打断机制。stats 字段由子链 goroutine 写入，只在 done
// 关闭之后才被读取（wg.Wait 提供了 happens-before 屏障）。
type turnHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	stats  TurnStats
	err    error // 第一个非取消类的阶段错误
}

// startTurn 为一条已定稿的语句启动响应子链：
//
//	LLM ──tokens──▶ 转发器 ──ttsIn──▶ TTS ──ttsOut──▶ 出口泵 ─▶ 环
//
// 四个 goroutine，全在轮 context 之下。转发器把 token 同时分流给 OnToken
// 观察者和回复累加器；它向 ttsIn 的阻塞发送就是 LLM 侧的背压。泵向环的
// push 从不阻塞——出口是实时的，宁可卸载也不阻塞。
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
		cancel() // 一个阶段死了就把整条子链拖下去
	}

	// LLM 阶段。
	wg.Add(1)
	h.stats.LLMStartAt = time.Now()
	go func() {
		defer wg.Done()
		fail(p.set.LLM.Stream(tctx, turn, tokens))
		close(tokens)
	}()

	// Token 转发器：观察、累加回复、喂给 TTS。
	wg.Add(1)
	var reply strings.Builder
	go func() {
		defer wg.Done()
		defer close(ttsIn)
		for tok := range tokens {
			now := time.Now()
			if h.stats.LLMFirstTokenAt.IsZero() {
				h.stats.LLMFirstTokenAt = now
			}
			h.stats.LLMLastTokenAt = now
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

	// TTS 阶段。
	wg.Add(1)
	go func() {
		defer wg.Done()
		fail(p.set.TTS.Stream(tctx, ttsIn, ttsOut))
		close(ttsOut)
	}()

	// 出口泵：本轮期间唯一向出口环写入的 goroutine。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for f := range ttsOut {
			p.egress.push(f)
			now := time.Now()
			if h.stats.TTSFirstFrameAt.IsZero() {
				h.stats.TTSFirstFrameAt = now
			}
			h.stats.TTSLastFrameAt = now
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

// finishTurn 记录一个自然完成的轮，并把它追加进对话历史。
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

// cancelTurn 是打断路径（任务 5.4）：取消子链、等每个阶段退出（适配器的取消
// 契约让这件事很快）、再清空在途下行音频，使出口不残留任何过时数据。ASR
// 循环完全不受影响——它全程继续监听。
func (p *Pipeline) cancelTurn(h *turnHandle) {
	begin := time.Now()
	h.cancel()
	<-h.done
	h.stats.Cancelled = true
	h.stats.FlushedFrames = p.egress.drain()
	h.stats.BargeInLatency = time.Since(begin)
	// Agent 已经说出口的那部分确实发生了；留在历史里，让下一轮的上下文反映
	// 真实情况。
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
