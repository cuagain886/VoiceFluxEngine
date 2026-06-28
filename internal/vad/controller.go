package vad

import (
	"log/slog"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
)

// Sink 是控制器所驱动的「流水线切片」。*pipeline.Pipeline 满足它；用接口
// 隔开，让 vad 不必依赖 pipeline。
type Sink interface {
	PushAudio(adapter.AudioFrame) bool
	EndUtterance()
	BargeIn()
}

// Controller 是内联粘合层：传输读 goroutine 对每个上行帧调用 Ingest，它
// 在*同一个 goroutine 里*跑检测器（事件走控制面——即流水线的 latched 信号
// ——而不是第二个环消费者，于是音频入口保持严格 SPSC），喂给状态机，执行
// 由此得到的动作，再把帧转发下去。
//
// 流水线的轮生命周期钩子必须接到 ResponseStarted 和 ResponseDone 上，状态机
// 才能看到对话的两半。
type Controller struct {
	det     Detector
	machine *Machine
	sink    Sink
	logger  *slog.Logger
}

// NewController 从配置装配检测器。det 可为 nil，此时用 cfg.VAD 构建 v1 的
// Energy 检测器；传入自定义 Detector 即可插入另一种 VAD，其它什么都不用动。
func NewController(cfg config.Config, det Detector, sink Sink, logger *slog.Logger) *Controller {
	if det == nil {
		det = &Energy{
			Enter:           cfg.VAD.EnergyThreshold,
			Exit:            cfg.VAD.ExitThreshold,
			MinSpeechFrames: durationFrames(cfg.VAD.MinSpeech, cfg.Audio.FrameDuration),
			HangoverFrames:  durationFrames(cfg.VAD.Hangover, cfg.Audio.FrameDuration),
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	m := NewMachine()
	m.OnIllegal = func(s State, ev Event) {
		logger.Warn("illegal state transition rejected", "state", s, "event", ev)
	}
	return &Controller{det: det, machine: m, sink: sink, logger: logger}
}

// durationFrames 把一段时长换算成帧数（向上取整，至少 1 帧）。
func durationFrames(d, frame time.Duration) int {
	if frame <= 0 {
		return 1
	}
	n := int((d + frame - 1) / frame)
	if n < 1 {
		n = 1
	}
	return n
}

// Ingest 处理一个上行帧：检测 → 迁移 → 执行动作 → 转发。它从不阻塞（sink
// 的信号是 latched 的、它的环推送是 drop-oldest），所以套接字读取者的节拍
// 不受影响。
func (c *Controller) Ingest(f adapter.AudioFrame) {
	if ev := c.det.Process(f.PCM); ev != None {
		c.apply(ev)
	}
	c.sink.PushAudio(f)
}

// ResponseStarted 接到流水线的「轮开始」钩子上。
func (c *Controller) ResponseStarted() { c.apply(ResponseStarted) }

// ResponseDone 接到流水线的「轮结束」钩子上（无论何种结局）。
func (c *Controller) ResponseDone() { c.apply(ResponseDone) }

// State 暴露对话状态（供指标和演示客户端使用）。
func (c *Controller) State() State { return c.machine.State() }

// IllegalEvents 暴露有多少未定义迁移被拒绝。
func (c *Controller) IllegalEvents() uint64 { return c.machine.Illegal() }

func (c *Controller) apply(ev Event) {
	act, err := c.machine.Apply(ev)
	if err != nil {
		return // 状态机已经计数并记日志了
	}
	switch act {
	case ActEndUtterance:
		c.sink.EndUtterance()
	case ActCancelTurn:
		c.sink.BargeIn()
	}
}
