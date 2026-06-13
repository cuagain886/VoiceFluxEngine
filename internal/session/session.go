// Package session 在传输之上管理对话生命周期（M7 接线、M8 生命周期）：
// 一次对话一个 Session——唯一 id、单调 epoch、空闲超时回收——并在 WebSocket
// 重连之间存活，上行带重放去重。一条连接内的帧由 TCP 保证有序；刻意不做
// 会话内重排窗口（D13）。
package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"voicestream/internal/adapter"
	"voicestream/internal/pipeline"
	"voicestream/internal/transport"
	"voicestream/internal/transport/transportpb"
	"voicestream/internal/vad"
)

// wireMsg 是一个下行帧——在写者给它分配序列号之前的样子。
type wireMsg struct {
	typ     transportpb.FrameType
	tsUs    int64
	payload []byte
}

// Session 是一次对话。它拥有流水线（因而也拥有对话历史）、VAD 控制器和下行
// 队列；这一切都在断连后存活。连接以「附着（attachment）」的形式来来去去：
// 同一时刻至多一个，各自绑定它在握手时出示的 epoch。
type Session struct {
	id  string
	mgr *Manager

	pipe      *pipeline.Pipeline
	ctrl      *vad.Controller
	runCancel context.CancelFunc // 会话生命周期：流水线 + 音频泵
	runCtx    context.Context

	wire chan wireMsg // 会话生命周期的下行队列

	clockUs     atomic.Int64  // 下行采样时钟，已入队音频的微秒数
	downlinkSeq atomic.Uint64 // 跨重连持续递增
	uplinkFloor atomic.Uint64 // 已投递的最高上行 seq（去重水位）
	lastActive  atomic.Int64  // 任一方向最后一帧的 unix 纳秒

	dupFrames     atomic.Uint64
	staleFrames   atomic.Uint64
	subtitleDrops atomic.Uint64

	mu     sync.Mutex
	epoch  uint64
	att    *attachment
	closed bool
}

// attachment 把一个连接在某个特定 epoch 上绑定到会话。一次接管会把上一个
// 附着标记为 stale：任何仍从它到达的东西都被计数并丢弃，绝不投递进流水线。
type attachment struct {
	epoch  uint64
	conn   transport.Conn
	cancel context.CancelFunc
	stale  atomic.Bool
	done   chan struct{} // 读者与写者都退出后关闭

	loops sync.WaitGroup
}

func newSession(m *Manager, id string) (*Session, error) {
	s := &Session{
		id:   id,
		mgr:  m,
		wire: make(chan wireMsg, 256),
	}
	s.touch()

	p, err := pipeline.New(m.cfg, m.adapterSetFor(), m.log)
	if err != nil {
		return nil, fmt.Errorf("session: pipeline: %w", err)
	}
	s.pipe = p
	s.ctrl = vad.NewController(m.cfg, nil, p, m.log)

	p.OnTranscript = func(tr adapter.Transcript) {
		s.enqueueText(tr.Text, tr.Final, transportpb.TextSource_TEXT_SOURCE_TRANSCRIPT, tr.TsUs)
	}
	p.OnToken = func(tok adapter.Token) {
		s.enqueueText(tok.Text, false, transportpb.TextSource_TEXT_SOURCE_TOKEN, s.clockUs.Load())
	}
	p.OnTurnStart = s.ctrl.ResponseStarted
	p.OnTurnEnd = func(cancelled bool) {
		s.ctrl.ResponseDone()
		if cancelled {
			s.enqueueControl(transportpb.ControlKind_CONTROL_KIND_BARGE_IN)
		}
	}
	if m.m != nil {
		p.OnTurnStats = func(ts pipeline.TurnStats) { m.m.RecordTurn(s.id, ts) }
	}

	s.runCtx, s.runCancel = context.WithCancel(context.Background())
	go func() {
		if err := p.Run(s.runCtx); err != nil && !errors.Is(err, context.Canceled) {
			m.log.Error("pipeline died", "session", s.id, "err", err)
			m.reclaim(s, "pipeline error")
		}
	}()
	go s.audioPump()
	return s, nil
}

// attach 在 claimedEpoch 上把 conn 绑定到会话。该认领必须严格大于当前 epoch
// ——相等或更低的认领是陈旧重连（例如一个僵尸客户端在和活跃客户端竞速），
// 予以拒绝。
func (s *Session) attach(conn transport.Conn, claimedEpoch uint64, ackDetail string) (*attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("session closed")
	}
	if claimedEpoch <= s.epoch {
		return nil, fmt.Errorf("stale epoch claim %d (current %d)", claimedEpoch, s.epoch)
	}
	if old := s.att; old != nil {
		old.stale.Store(true)
		old.cancel()
		_ = old.conn.Close(transport.StatusGoingAway, "superseded by reconnect")
	}
	s.epoch = claimedEpoch

	ctx, cancel := context.WithCancel(s.runCtx)
	att := &attachment{epoch: claimedEpoch, conn: conn, cancel: cancel, done: make(chan struct{})}
	s.att = att

	att.loops.Add(2)
	go s.readLoop(ctx, att)
	go s.writeLoop(ctx, att, ackDetail)
	go func() {
		att.loops.Wait()
		s.detach(att)
		close(att.done)
	}()
	s.touch()
	return att, nil
}

func (s *Session) detach(att *attachment) {
	s.mu.Lock()
	if s.att == att {
		s.att = nil
	}
	s.mu.Unlock()
}

// close 释放会话拥有的一切。幂等。
func (s *Session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	att := s.att
	s.mu.Unlock()

	s.runCancel() // 停掉流水线、音频泵，以及附着的读写循环（子 ctx）
	if att != nil {
		_ = att.conn.Close(transport.StatusNormalClosure, "session closed")
	}
}

func (s *Session) touch() { s.lastActive.Store(time.Now().UnixNano()) }

func (s *Session) lastActiveTime() time.Time { return time.Unix(0, s.lastActive.Load()) }

// advanceFloor 用 seq 与去重水位做比较并认领。重连后的重放会按序到达且低于
// 水位，于是被拒绝；这条水位让重排窗口变得不必要（TCP 保证每条连接有序）。
func (s *Session) advanceFloor(seq uint64) bool {
	for {
		cur := s.uplinkFloor.Load()
		if seq <= cur {
			return false
		}
		if s.uplinkFloor.CompareAndSwap(cur, seq) {
			return true
		}
	}
}

// readLoop 是每附着的入口边缘：去重 + 内联 VAD + 流水线。
func (s *Session) readLoop(ctx context.Context, att *attachment) {
	defer att.loops.Done()
	defer att.cancel() // 读者死亡也把写者一并停掉
	for {
		f, err := att.conn.ReadFrame(ctx)
		if err != nil {
			return
		}
		if att.stale.Load() {
			s.staleFrames.Add(1)
			continue
		}
		s.touch()
		switch f.Type {
		case transportpb.FrameType_FRAME_TYPE_AUDIO:
			if !s.advanceFloor(f.Seq) {
				s.dupFrames.Add(1)
				continue
			}
			s.ctrl.Ingest(adapter.AudioFrame{PCM: f.Payload, TsUs: f.TsUs})
		case transportpb.FrameType_FRAME_TYPE_CONTROL:
			var c transportpb.ControlPayload
			if perr := proto.Unmarshal(f.Payload, &c); perr != nil {
				continue
			}
			if c.Kind == transportpb.ControlKind_CONTROL_KIND_STOP {
				// 客户端主动结束整场对话。
				s.mgr.reclaim(s, "client stop")
				return
			}
		default:
		}
	}
}

// writeLoop 是这条连接的唯一写者：先发握手 ack，然后是下行帧与心跳 ping。
// 一个在连接死亡时被取出的帧会被丢弃、不重新入队——对实时音频而言，一次
// 写失败意味着那个时刻已经过去了。
func (s *Session) writeLoop(ctx context.Context, att *attachment, ackDetail string) {
	defer att.loops.Done()
	defer att.cancel()

	ack, err := proto.Marshal(&transportpb.ControlPayload{
		Kind:      transportpb.ControlKind_CONTROL_KIND_START,
		Detail:    ackDetail,
		SessionId: s.id,
		Epoch:     att.epoch,
		LastSeq:   s.uplinkFloor.Load(),
	})
	if err != nil {
		return
	}
	if err := s.writeFrame(ctx, att, transportpb.FrameType_FRAME_TYPE_CONTROL, 0, ack); err != nil {
		return
	}

	heartbeat := s.mgr.cfg.Server.HeartbeatPeriod
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	t := time.NewTicker(heartbeat)
	defer t.Stop()

	for {
		select {
		case m := <-s.wire:
			if err := s.writeFrame(ctx, att, m.typ, m.tsUs, m.payload); err != nil {
				return
			}
			s.touch()
		case <-t.C:
			pctx, pcancel := context.WithTimeout(ctx, heartbeat)
			err := att.conn.Ping(pctx)
			pcancel()
			if err != nil {
				return // 心跳超时：这个附着已经死了
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Session) writeFrame(ctx context.Context, att *attachment, typ transportpb.FrameType, tsUs int64, payload []byte) error {
	f := transport.Frame{Type: typ, Seq: s.downlinkSeq.Add(1), TsUs: tsUs, Payload: payload}
	return att.conn.WriteFrame(ctx, f)
}

// audioPump 在会话的整个生命周期里，把合成音频从出口环搬到会话的 wire 队列
// 上，并推进下行时钟。在队列满时阻塞是安全的：上游的出口环按「最旧优先」
// 卸载，所以内存有界、最新音频胜出。脱离附着期间队列只是填满、对话暂停。
func (s *Session) audioPump() {
	rate := int64(s.mgr.cfg.Audio.SampleRate)
	for {
		f, err := s.pipe.AwaitDownlink(s.runCtx)
		if err != nil {
			return
		}
		tsOut := s.clockUs.Load()
		samples := int64(len(f.PCM) / 2)
		s.clockUs.Store(tsOut + samples*1_000_000/rate)
		select {
		case s.wire <- wireMsg{typ: transportpb.FrameType_FRAME_TYPE_AUDIO, tsUs: tsOut, payload: f.PCM}:
		case <-s.runCtx.Done():
			return
		}
	}
}

// enqueueText 把一个 TEXT 帧入队，绝不阻塞产出它的阶段 goroutine：慢客户端
// 下字幕会被卸载（并计数）。
func (s *Session) enqueueText(text string, final bool, src transportpb.TextSource, tsUs int64) {
	payload, err := proto.Marshal(&transportpb.TextPayload{Text: text, Final: final, Source: src})
	if err != nil {
		return
	}
	select {
	case s.wire <- wireMsg{typ: transportpb.FrameType_FRAME_TYPE_TEXT, tsUs: tsUs, payload: payload}:
	default:
		s.subtitleDrops.Add(1)
	}
}

func (s *Session) enqueueControl(kind transportpb.ControlKind) {
	payload, err := proto.Marshal(&transportpb.ControlPayload{Kind: kind})
	if err != nil {
		return
	}
	select {
	case s.wire <- wireMsg{typ: transportpb.FrameType_FRAME_TYPE_CONTROL, tsUs: s.clockUs.Load(), payload: payload}:
	default:
		s.subtitleDrops.Add(1)
	}
}
