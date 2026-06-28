package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/ringbuf"
)

// StageFunc 是编排器组合的基本单元：消费 in、产出 out、尊重 ctx。
// adapter.ASR.Stream 和 adapter.TTS.Stream 在「形状」上就是 StageFunc；任何
// 符合此形状的函数都能替换某个阶段，而不必动编排器。
type StageFunc[In, Out any] func(ctx context.Context, in <-chan In, out chan<- Out) error

// Pipeline 编排一次对话：音频从入口环进来，ASR -> LLM -> TTS 跨 goroutine
// 流转，音频从出口环出去。
//
// 按数据语义分流的双背压（D3）：
//
//	音频边缘  环形缓冲，drop-oldest，从不阻塞生产者
//	文本跳点  有界 channel，channel 满则阻塞上游阶段
//
// 两者在入口汇合：当一轮卡住（模型慢），finals 排起队，ASR runner 在交接时
// 阻塞，停止抽干入口环，过载就表现为「被计数的丢帧」——处处内存有界，是
// 构造出来的性质。
//
// 轮策略：finals 严格逐个、按序处理；新的 final 绝不抢占正在跑的轮。抢占
// 这件事专门由 BargeIn() 负责，它由 M6 的 VAD 在 speech_start 时触发。
type Pipeline struct {
	cfg    config.Config
	set    adapter.Set
	logger *slog.Logger

	ingress *edge // 传输 -> ASR
	egress  *edge // TTS -> 传输

	// ingressPool 是入口读侧的缓冲池（可空）。runASRLoop 消费完每帧后把缓冲
	// 归还它，使上行热路径稳态零分配；空则退化为每帧落 GC（行为同旧版）。
	// 池的 free-ring 是 SPSC：唯一 Putter 是 runASRLoop goroutine，唯一 Getter
	// 是会话读 goroutine（会话层保证读循环交接串行化）。
	ingressPool *ringbuf.BufferPool

	endUtt  chan struct{} // 语句边界，latched（cap 1）
	bargeIn chan struct{} // 响应取消请求，latched（cap 1）

	// OnTranscript 和 OnToken 若在 Run 前设置，则观察 ASR 转写与 LLM token
	//（用于下行 TEXT 帧 / 字幕）。它们跑在阶段 goroutine 上：必须快、不可阻塞。
	OnTranscript func(adapter.Transcript)
	OnToken      func(adapter.Token)

	// OnTurnStart 和 OnTurnEnd 若在 Run 前设置，则观察响应子链的生命周期
	//（M6 的状态机在此监听）。它们跑在编排器 goroutine 上：快、不阻塞。
	OnTurnStart func()
	OnTurnEnd   func(cancelled bool)

	// OnTurnStats 若在 Run 前设置，则接收每个已发布轮的测量数据（M9 指标
	// 导出）。编排器 goroutine；快、不阻塞。
	OnTurnStats func(TurnStats)

	history []adapter.Message // 仅编排器 goroutine 访问

	statsMu sync.Mutex
	last    TurnStats
	hasLast bool
}

// New 从校验过的配置为一次会话构建流水线。
func New(cfg config.Config, set adapter.Set, logger *slog.Logger) (*Pipeline, error) {
	inPol, err := ringbuf.ParsePolicy(cfg.RingBuf.IngressPolicy)
	if err != nil {
		return nil, err
	}
	outPol, err := ringbuf.ParsePolicy(cfg.RingBuf.EgressPolicy)
	if err != nil {
		return nil, err
	}
	ingress, err := newEdge(cfg.RingBuf.IngressCapacity, inPol)
	if err != nil {
		return nil, fmt.Errorf("pipeline: ingress: %w", err)
	}
	egress, err := newEdge(cfg.RingBuf.EgressCapacity, outPol)
	if err != nil {
		return nil, fmt.Errorf("pipeline: egress: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Pipeline{
		cfg:     cfg,
		set:     set,
		logger:  logger,
		ingress: ingress,
		egress:  egress,
		endUtt:  make(chan struct{}, 1),
		bargeIn: make(chan struct{}, 1),
	}, nil
}

// SetIngressPool 注入入口读侧缓冲池：runASRLoop 把消费完的帧缓冲归还它，使
// 上行热路径稳态零分配。会话层在 New 之后调用；不设则退化为每帧落 GC（旧行为）。
func (p *Pipeline) SetIngressPool(pool *ringbuf.BufferPool) { p.ingressPool = pool }

// PushAudio 把一个上行帧投给入口环。它从不阻塞；drop-oldest 下永远成功
// （可能驱逐一个过时帧）。单生产者：传输读 goroutine。
func (p *Pipeline) PushAudio(f adapter.AudioFrame) bool { return p.ingress.push(f) }

// AwaitDownlink 阻塞直到有合成帧可用或 ctx 结束。单消费者：传输写 goroutine。
func (p *Pipeline) AwaitDownlink(ctx context.Context) (adapter.AudioFrame, error) {
	return p.egress.await(ctx)
}

// EndUtterance 标记用户当前语句结束（speech_end）。M6 的 VAD 调用它；在那
// 之前由传输控制面或测试调用。从不阻塞；重复信号会合并。
func (p *Pipeline) EndUtterance() {
	select {
	case p.endUtt <- struct{}{}:
	default:
	}
}

// BargeIn 请求取消正在进行的响应子链（RESPONDING 期间的 speech_start）。
// 从不阻塞；空闲时是 no-op。
func (p *Pipeline) BargeIn() {
	select {
	case p.bargeIn <- struct{}{}:
	default:
	}
}

// IngressDropped 和 EgressDropped 报告各音频边缘卸载掉的帧数。
func (p *Pipeline) IngressDropped() uint64 { return p.ingress.dropped() }
func (p *Pipeline) EgressDropped() uint64  { return p.egress.dropped() }

// LastTurn 返回最近一个完成轮的测量数据。
func (p *Pipeline) LastTurn() (TurnStats, bool) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.last, p.hasLast
}

func (p *Pipeline) publish(s TurnStats) {
	p.statsMu.Lock()
	p.last, p.hasLast = s, true
	p.statsMu.Unlock()
	if p.OnTurnStats != nil {
		p.OnTurnStats(s)
	}
	p.logger.Debug("turn complete",
		"prompt", s.Prompt,
		"first_response", s.FirstResponse(),
		"model_latency", s.ModelLatency(),
		"kernel_overhead", s.KernelOverhead(),
		"cancelled", s.Cancelled,
		"flushed_frames", s.FlushedFrames,
	)
}

// Run 驱动对话，直到 ctx 被取消或某个阶段失败。
func (p *Pipeline) Run(ctx context.Context) error {
	finals := make(chan utterance, p.cfg.Pipeline.TranscriptChanCap)
	asrErr := make(chan error, 1)
	go func() { asrErr <- p.runASRLoop(ctx, finals) }()

	var cur *turnHandle
	for {
		// 只有在没有轮运行时才接受新的 final：finals 在 channel 里排队，
		// 一旦排满，ASR runner 就阻塞——这是通往入口环的那条背压链的链头。
		var finalsC <-chan utterance
		var curDone <-chan struct{}
		if cur == nil {
			finalsC = finals
		} else {
			curDone = cur.done
		}

		select {
		case <-ctx.Done():
			if cur != nil {
				p.cancelTurn(cur)
			}
			<-asrErr
			return ctx.Err()

		case err := <-asrErr:
			if cur != nil {
				p.cancelTurn(cur)
			}
			return err

		case <-p.bargeIn:
			if cur != nil {
				p.cancelTurn(cur)
				cur = nil
			}

		case u := <-finalsC:
			if u.text == "" {
				continue // 空语句（如虚假边界）：不开轮
			}
			cur = p.startTurn(ctx, u)

		case <-curDone:
			p.finishTurn(cur)
			cur = nil
		}
	}
}

// utterance 是一条已定稿的用户输入加上它的计时锚点。finalAt 在 ASR 产出
// final 时就采集——早于任何排队——于是「等编排器」的时间计入内核开销，
// 而不是模型延迟。
type utterance struct {
	text    string
	endAt   time.Time // EndUtterance 触发的时刻：首响延迟的 t0
	finalAt time.Time // 最终转写变得可用的时刻
}

// runASRLoop 一句接一句地识别：把入口环抽进一个全新的 ASR 流，在语句边界
// 关闭它，把最终转写交给编排器，再循环。它拥有入口环的消费端，所以当它
// 阻塞（编排器忙、finals 满）时，入口就开始卸载——这是设计使然。
func (p *Pipeline) runASRLoop(ctx context.Context, finals chan<- utterance) error {
	for ctx.Err() == nil {
		// in 无缓冲：`in <- f` 返回即证明 ASR 已收到这帧、也就写完了上一帧，
		// 于是 pump 可逐帧把上一帧的缓冲安全归还池（在飞行中的缓冲恒为 2 块）。
		in := make(chan adapter.AudioFrame)
		out := make(chan adapter.Transcript, p.cfg.Pipeline.AudioChanCap)

		streamErr := make(chan error, 1)
		go func() {
			err := p.set.ASR.Stream(ctx, in, out)
			close(out)
			streamErr <- err
		}()

		// 并发收集转写，这样 partial 的产出在我们抽音频时永不阻塞识别器。
		var final adapter.Transcript
		collected := make(chan struct{})
		go func() {
			defer close(collected)
			for tr := range out {
				if p.OnTranscript != nil {
					p.OnTranscript(tr)
				}
				if tr.Final {
					final = tr
				}
			}
		}()

		// 抽帧直到语句边界。每帧的缓冲在 ASR 用完后归还入口池：无缓冲 in 让
		// `in <- f(N+1)` 的返回成为「ASR 已用完 f(N)」的信号，于是逐帧回收上一帧；
		// 末帧在 ASR 流返回后回收。被 drop-oldest 在环里驱逐的帧不经过这里、落 GC。
		var endAt time.Time
		var prev adapter.AudioFrame
		var havePrev bool
		recycle := func(f adapter.AudioFrame) {
			if p.ingressPool != nil {
				p.ingressPool.Put(f.PCM)
			}
		}
	pump:
		for {
			f, ok := p.ingress.pop()
			if !ok {
				select {
				case <-p.ingress.bell:
				case <-p.endUtt:
					endAt = time.Now()
					break pump
				case <-ctx.Done():
				}
				if ctx.Err() != nil {
					break pump
				}
				continue
			}
			select {
			case in <- f:
				if havePrev {
					recycle(prev) // ASR 收到 f ⇒ 已写完 prev ⇒ prev 可归还
				}
				prev, havePrev = f, true
			case <-p.endUtt:
				// 边界在交接某帧时到达；这一帧属于 hangover 区音频，
				// 刻意丢弃——它没交给 ASR，缓冲立即归还。
				endAt = time.Now()
				recycle(f)
				break pump
			case <-ctx.Done():
				recycle(f)
				break pump
			}
		}

		close(in)
		err := <-streamErr
		<-collected
		if havePrev {
			recycle(prev) // ASR 流已返回 ⇒ 最后交接的那帧确定用完
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("pipeline: ASR stream: %w", err)
		}

		// 把 final 交出去。队列满时在此阻塞正是设计中的停顿：入口靠
		// drop-oldest 继续吸收。
		select {
		case finals <- utterance{text: final.Text, endAt: endAt, finalAt: time.Now()}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}
