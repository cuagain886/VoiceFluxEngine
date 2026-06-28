package loadgen

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"voicestream/internal/transport"
	"voicestream/internal/transport/transportpb"
)

// worker 是一个虚拟来电者：它拨号、做 M8 握手，然后以近实时节拍循环对话轮
// ——上行流保持连续（说话帧与静音帧一视同仁），与浏览器客户端完全一致，
// 所以每一帧都穿过真实热路径：传输解码、去重水位、内联 VAD、入口环、流水线。
type worker struct {
	id  int
	cfg *Config
	col *collector
	rng *rand.Rand

	conn *websocket.Conn
	dl   *downlink
	clk  *clock

	seq  uint64
	tsUs int64
}

// downlink 是读 goroutine 对连接的视角，由轮循环消费。事件时间戳在帧*到达*
// 时采集，于是测量不依赖轮循环何时来抽干这些 channel。
type downlink struct {
	lastAudioNs atomic.Int64
	audioC      chan time.Time // AUDIO 帧的到达时刻；有损（cap 8）
	bargeC      chan time.Time // BARGE_IN 控制的到达时刻
	errC        chan error     // 终止性读错误（cap 1）
}

func newDownlink() *downlink {
	return &downlink{
		audioC: make(chan time.Time, 8),
		bargeC: make(chan time.Time, 4),
		errC:   make(chan error, 1),
	}
}

func (d *downlink) fail(err error) {
	select {
	case d.errC <- err:
	default:
	}
}

// pcmFrame 以恒定幅度构建一个等价于 20ms 的 PCM 帧；内联能量 VAD 看到的
// RMS == 幅度。
func pcmFrame(samples int, amplitude float64) []byte {
	buf := make([]byte, samples*2)
	v := uint16(int16(amplitude * 32767))
	for i := 0; i < samples; i++ {
		binary.LittleEndian.PutUint16(buf[2*i:], v)
	}
	return buf
}

func (w *worker) run(ctx context.Context) {
	defer w.col.workerExit()
	// 随机相位偏移，让一步里的 worker 不会齐步说话。
	if !sleepUntil(ctx, time.Now().Add(time.Duration(w.rng.Int64N(int64(w.cfg.rampStagger()))))) {
		return
	}
	if err := w.connect(ctx); err != nil {
		if ctx.Err() == nil {
			w.col.fail("connect", err)
		}
		return
	}
	defer func() { _ = w.conn.Close(websocket.StatusNormalClosure, "loadgen done") }()

	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	go w.readLoop(rctx)

	w.clk = newClock(w.cfg.FrameInterval, w.netemFor())
	turnNo := 0
	for ctx.Err() == nil {
		turnNo++
		barge := w.cfg.BargeEvery > 0 && turnNo%w.cfg.BargeEvery == 0
		if err := w.turn(ctx, barge); err != nil {
			if ctx.Err() != nil {
				return // 是关停，不是失败
			}
			var to *timeoutError
			if errors.As(err, &to) {
				w.col.fail(to.phase, err)
				// 开下一轮前先重对齐：吞掉最终蹒跚而来的迟到响应，否则它的
				// 音频会冒充下一轮的首响、产出一个虚假的快样本。尽力而为
				// ——这里再来一次超时就放过。
				_ = w.silenceUntilQuiet(ctx, w.cfg.silencePCM)
				drainTimes(w.dl.bargeC)
				continue // 保持连接；继续给服务器加压
			}
			w.col.fail("conn", err)
			return
		}
		w.col.turnDone()
	}
}

// netemFor 把配置的扰动施加到这个 worker 上。种子按 worker 派生，使抖动表
// 各不相同但仍可复现。
func (w *worker) netemFor() Netem {
	n := w.cfg.Netem
	n.Seed = n.Seed*31 + uint64(w.id) + 1
	return n
}

func (w *worker) connect(ctx context.Context) error {
	c, _, err := websocket.Dial(ctx, w.cfg.URL, nil)
	if err != nil {
		return err
	}
	c.SetReadLimit(1 << 20)
	w.conn = c
	w.dl = newDownlink()

	// 握手：用全新会话发 START，期待 START ack。
	payload, err := proto.Marshal(&transportpb.ControlPayload{
		Kind: transportpb.ControlKind_CONTROL_KIND_START, Epoch: 1,
	})
	if err != nil {
		return err
	}
	if err := w.writeFrame(ctx, transport.Frame{
		Type: transportpb.FrameType_FRAME_TYPE_CONTROL, Payload: payload,
	}); err != nil {
		return err
	}
	hctx, hcancel := context.WithTimeout(ctx, w.cfg.TurnTimeout)
	defer hcancel()
	for {
		typ, data, err := c.Read(hctx)
		if err != nil {
			return fmt.Errorf("handshake read: %w", err)
		}
		if typ != websocket.MessageBinary {
			continue
		}
		f, err := transport.Decode(data)
		if err != nil {
			return fmt.Errorf("handshake decode: %w", err)
		}
		if f.Type != transportpb.FrameType_FRAME_TYPE_CONTROL {
			continue
		}
		var cp transportpb.ControlPayload
		if err := proto.Unmarshal(f.Payload, &cp); err != nil {
			return err
		}
		if cp.Kind != transportpb.ControlKind_CONTROL_KIND_START {
			return fmt.Errorf("handshake rejected: %v %q", cp.Kind, cp.Detail)
		}
		return nil
	}
}

func (w *worker) writeFrame(ctx context.Context, f transport.Frame) error {
	buf, err := f.MarshalBinary()
	if err != nil {
		return err
	}
	return w.conn.Write(ctx, websocket.MessageBinary, buf)
}

// sendAudio 发送一个定速的上行帧。TsUs 按名义帧时长（音频采样时钟）推进、
// 独立于墙钟节拍，于是即便 harness 跑得比实时快（测试）或帧被 netem 延迟，
// 内核看到的也是一条一致的 PTS 流。
func (w *worker) sendAudio(ctx context.Context, pcm []byte) error {
	if !w.clk.wait(ctx) {
		return ctx.Err()
	}
	w.seq++
	f := transport.Frame{
		Type: transportpb.FrameType_FRAME_TYPE_AUDIO,
		Seq:  w.seq, TsUs: w.tsUs, Payload: pcm,
	}
	w.tsUs += w.cfg.frameNominalUs()
	if err := w.writeFrame(ctx, f); err != nil {
		return err
	}
	w.col.framesSent.Add(1)
	return nil
}

// readLoop 在连接的整个生命周期里抽干下行：给音频到达盖戳，浮现 BARGE_IN
// 控制与终止性错误。
func (w *worker) readLoop(ctx context.Context) {
	for {
		typ, data, err := w.conn.Read(ctx)
		if err != nil {
			w.dl.fail(err)
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		f, err := transport.Decode(data)
		if err != nil {
			w.dl.fail(err)
			return
		}
		now := time.Now()
		switch f.Type {
		case transportpb.FrameType_FRAME_TYPE_AUDIO:
			w.dl.lastAudioNs.Store(now.UnixNano())
			select {
			case w.dl.audioC <- now:
			default: // 轮循环只需要第一帧的到达；其余合并
			}
		case transportpb.FrameType_FRAME_TYPE_CONTROL:
			var cp transportpb.ControlPayload
			if err := proto.Unmarshal(f.Payload, &cp); err != nil {
				continue
			}
			switch cp.Kind {
			case transportpb.ControlKind_CONTROL_KIND_BARGE_IN:
				select {
				case w.dl.bargeC <- now:
				default:
				}
			case transportpb.ControlKind_CONTROL_KIND_ERROR:
				w.dl.fail(fmt.Errorf("server error: %s", cp.Detail))
				return
			default:
			}
		default:
		}
	}
}

// timeoutError 标记一个超过 TurnTimeout 的阶段——这是一个*测量值*（系统已
// 降级到超出截止），不是 harness 缺陷；worker 记录它并继续加压。
type timeoutError struct{ phase string }

func (e *timeoutError) Error() string { return "loadgen: timeout waiting for " + e.phase }

// turn 驱动一个对话轮：
//
//	说话 ... 静音 ──▶ 第一帧下行音频        （客户端端到端首响）
//	[打断轮] 骑着响应、对着它说话 ──▶ BARGE_IN 控制（客户端端到端打断）
//	静音直到下行安静 ──▶ 下一轮
func (w *worker) turn(ctx context.Context, barge bool) error {
	speech := w.cfg.speechPCM
	silence := w.cfg.silencePCM

	// 阶段 1：这句话。首响 t0 取最后一帧说话的*理想*时隙——即模拟用户停止
	// 说话的时刻——于是 netem 延迟与发送积压都计入端到端测量。
	for i := 0; i < w.cfg.SpeechFrames; i++ {
		if err := w.sendAudio(ctx, speech); err != nil {
			return err
		}
	}
	spokeAt := w.clk.lastIdeal

	// 阶段 2：静音（VAD hangover 在服务端跑）直到响应到来。
	firstAt, err := w.silenceUntilAudioAfter(ctx, spokeAt, silence)
	if err != nil {
		return err
	}
	w.col.sample(sampleFirst, firstAt.Sub(spokeAt))

	if barge {
		// 先骑着响应一小会儿，让打断落在响应进行中途。
		if err := w.silenceFor(ctx, silence, w.cfg.BargeDelay); err != nil {
			return err
		}
		drainTimes(w.dl.bargeC)
		bargeStart := w.clk.peekIdeal() // 用户张嘴的时刻
		// 盖过 Agent 说话；是完整一句，所以它也成为下一轮的 prompt。
		for i := 0; i < w.cfg.SpeechFrames; i++ {
			if err := w.sendAudio(ctx, speech); err != nil {
				return err
			}
		}
		spokeAt = w.clk.lastIdeal
		bargeAt, err := w.awaitBarge(ctx, silence, bargeStart)
		if err != nil {
			return err
		}
		w.col.sample(sampleBarge, bargeAt.Sub(bargeStart))
		// 这句打断的话拿到自己的响应。被取消那一轮的陈旧音频污染不了这次
		// 等待：内核把 BARGE_IN 控制排在被 flush 的那轮帧*之后*，所以 bargeAt
		// 之后到达的任何东西都属于新一轮。
		firstAt, err := w.silenceUntilAudioAfter(ctx, bargeAt, silence)
		if err != nil {
			return err
		}
		w.col.sample(sampleFirst, firstAt.Sub(spokeAt))
	}

	// 阶段 3：让响应播完——下行安静 QuietGap。
	return w.silenceUntilQuiet(ctx, silence)
}

// silenceUntilAudioAfter 定速发静音帧，直到出现一个在 `after` 标记之后到达
// 的 AUDIO 帧。返回它的到达时刻。
func (w *worker) silenceUntilAudioAfter(ctx context.Context, after time.Time, silence []byte) (time.Time, error) {
	deadline := time.Now().Add(w.cfg.TurnTimeout)
	for {
		if err := w.sendAudio(ctx, silence); err != nil {
			return time.Time{}, err
		}
		select {
		case at := <-w.dl.audioC:
			if !at.Before(after) {
				return at, nil
			}
		case err := <-w.dl.errC:
			return time.Time{}, err
		default:
		}
		if time.Now().After(deadline) {
			return time.Time{}, &timeoutError{phase: "first-response"}
		}
	}
}

// silenceFor 定速发静音帧约 d 这么长的墙钟时间。
func (w *worker) silenceFor(ctx context.Context, silence []byte, d time.Duration) error {
	until := time.Now().Add(d)
	for time.Now().Before(until) {
		if err := w.sendAudio(ctx, silence); err != nil {
			return err
		}
	}
	return nil
}

// awaitBarge 定速发静音直到 BARGE_IN 控制（内核取消完那一轮时发出）到达。
// 接受 `after` 起的时间戳——该控制可能在我们还在说话时就已经落地了。
func (w *worker) awaitBarge(ctx context.Context, silence []byte, after time.Time) (time.Time, error) {
	deadline := time.Now().Add(w.cfg.TurnTimeout)
	for {
		select {
		case at := <-w.dl.bargeC:
			if !at.Before(after) {
				return at, nil
			}
		case err := <-w.dl.errC:
			return time.Time{}, err
		default:
		}
		if time.Now().After(deadline) {
			return time.Time{}, &timeoutError{phase: "barge-in"}
		}
		if err := w.sendAudio(ctx, silence); err != nil {
			return time.Time{}, err
		}
	}
}

// silenceUntilQuiet 定速发静音，直到已有 QuietGap 这么久没有下行音频到达
// ——即这一轮的响应已经完全播完。
func (w *worker) silenceUntilQuiet(ctx context.Context, silence []byte) error {
	deadline := time.Now().Add(w.cfg.TurnTimeout)
	for {
		if err := w.sendAudio(ctx, silence); err != nil {
			return err
		}
		last := time.Unix(0, w.dl.lastAudioNs.Load())
		if time.Since(last) >= w.cfg.QuietGap {
			drainTimes(w.dl.audioC)
			return nil
		}
		if time.Now().After(deadline) {
			return &timeoutError{phase: "quiesce"}
		}
	}
}

func drainTimes(c chan time.Time) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}
