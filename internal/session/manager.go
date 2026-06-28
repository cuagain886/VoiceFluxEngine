package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/metrics"
	"voicestream/internal/transport"
	"voicestream/internal/transport/transportpb"
)

// Manager 拥有每一个活动会话：首次接触时创建、重连时重新附着、空闲超时时
// 回收。会话状态按设计是进程内的（可选的 Redis 外置化是一个独立、默认关闭
// 的外设——任务 12.1）。
type Manager struct {
	cfg config.Config
	set adapter.Set
	log *slog.Logger
	m   *metrics.M // nil = 关闭埋点

	mu       sync.Mutex
	sessions map[string]*Session

	staleEpochClaims  atomic.Uint64
	sessionsCreated   atomic.Uint64
	sessionsReclaimed atomic.Uint64
}

// NewManager 构建一个 manager；调用 Run 启动空闲回收器。m 可为 nil 以关闭
// 指标（测试用）。
func NewManager(cfg config.Config, set adapter.Set, logger *slog.Logger, m *metrics.M) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	mgr := &Manager{cfg: cfg, set: set, log: logger, m: m, sessions: map[string]*Session{}}
	if m != nil {
		mgr.registerGauges()
	}
	return mgr
}

// registerGauges 暴露抓取期才采样的值：活动会话数，以及各项帧卫生总量
//（活动会话当场求和 + 已回收会话累计下来的总量，于是计数器在会话翻滚下
// 仍保持单调）。
func (mgr *Manager) registerGauges() {
	r := mgr.m.Registry
	r.NewGaugeFunc("voicestream_sessions_active", "Live sessions.", func() float64 {
		return float64(mgr.Count())
	})
	r.NewGaugeFunc("voicestream_sessions_created_total", "Sessions ever created.", func() float64 {
		return float64(mgr.sessionsCreated.Load())
	})
	r.NewGaugeFunc("voicestream_sessions_reclaimed_total", "Sessions reclaimed.", func() float64 {
		return float64(mgr.sessionsReclaimed.Load())
	})
	r.NewGaugeFunc("voicestream_stale_epoch_claims_total", "Rejected stale reconnect claims.", func() float64 {
		return float64(mgr.staleEpochClaims.Load())
	})
	// sum 返回一个抓取期函数：累计值（来自已回收会话）+ 当前所有活动会话
	// 的实时求和——这就是「翻滚下保持单调」的实现。
	sum := func(acc *atomic.Uint64, live func(*Session) uint64) func() float64 {
		return func() float64 {
			total := acc.Load()
			for _, s := range mgr.snapshot() {
				total += live(s)
			}
			return float64(total)
		}
	}
	r.NewGaugeFunc("voicestream_ingress_dropped_frames_total", "Uplink frames shed by the ingress ring.",
		sum(&mgr.m.AccIngressDropped, func(s *Session) uint64 { return s.pipe.IngressDropped() }))
	r.NewGaugeFunc("voicestream_egress_dropped_frames_total", "Downlink frames shed by the egress ring.",
		sum(&mgr.m.AccEgressDropped, func(s *Session) uint64 { return s.pipe.EgressDropped() }))
	r.NewGaugeFunc("voicestream_dup_frames_total", "Replayed uplink frames dropped by dedup.",
		sum(&mgr.m.AccDupFrames, func(s *Session) uint64 { return s.dupFrames.Load() }))
	r.NewGaugeFunc("voicestream_stale_frames_total", "Frames from superseded attachments dropped.",
		sum(&mgr.m.AccStaleFrames, func(s *Session) uint64 { return s.staleFrames.Load() }))
	r.NewGaugeFunc("voicestream_subtitle_dropped_total", "TEXT/CONTROL frames shed under slow clients.",
		sum(&mgr.m.AccSubtitleDrops, func(s *Session) uint64 { return s.subtitleDrops.Load() }))
	r.NewGaugeFunc("voicestream_illegal_transitions_total", "State-machine events rejected as illegal.",
		sum(&mgr.m.AccIllegalEvents, func(s *Session) uint64 { return s.ctrl.IllegalEvents() }))
}

// Run 驱动空闲回收器直到 ctx 结束，然后回收一切。
func (m *Manager) Run(ctx context.Context) {
	// 回收检查频率取空闲超时的 1/4，并钳制在 [10ms, 1s]。
	tick := m.cfg.Session.IdleTimeout / 4
	if tick > time.Second {
		tick = time.Second
	}
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.reapIdle(time.Now())
		case <-ctx.Done():
			for _, s := range m.snapshot() {
				m.reclaim(s, "server shutdown")
			}
			return
		}
	}
}

// Count 报告活动会话数（8.4 泄漏门禁）。
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// StaleEpochClaims 统计被拒绝的「epoch 未递增」重连尝试（即「旧连接试图污染
// 新连接」这一指标）。
func (m *Manager) StaleEpochClaims() uint64 { return m.staleEpochClaims.Load() }

// Stats 暴露一个会话的帧卫生计数器（测试、M9 指标）。
type Stats struct {
	Epoch         uint64
	DupFrames     uint64
	StaleFrames   uint64
	SubtitleDrops uint64
}

// SessionStats 返回一个活动会话的计数器。
func (m *Manager) SessionStats(id string) (Stats, bool) {
	m.mu.Lock()
	s, ok := m.sessions[id]
	m.mu.Unlock()
	if !ok {
		return Stats{}, false
	}
	s.mu.Lock()
	epoch := s.epoch
	s.mu.Unlock()
	return Stats{
		Epoch:         epoch,
		DupFrames:     s.dupFrames.Load(),
		StaleFrames:   s.staleFrames.Load(),
		SubtitleDrops: s.subtitleDrops.Load(),
	}, true
}

// Handler 返回每连接的传输 handler：先握手（一个 CONTROL START 帧），再把
// 连接附着到它的会话上，serve 直到附着结束。
func (m *Manager) Handler() transport.Handler {
	return func(ctx context.Context, conn transport.Conn) error {
		hctx, hcancel := context.WithTimeout(ctx, 5*time.Second)
		f, err := conn.ReadFrame(hctx)
		hcancel()
		if err != nil {
			return err
		}
		var start transportpb.ControlPayload
		if f.Type != transportpb.FrameType_FRAME_TYPE_CONTROL ||
			proto.Unmarshal(f.Payload, &start) != nil ||
			start.Kind != transportpb.ControlKind_CONTROL_KIND_START {
			return writeError(ctx, conn, "expected START handshake")
		}

		s, detail, err := m.resolve(&start)
		if err != nil {
			m.staleEpochClaims.Add(1)
			return writeError(ctx, conn, err.Error())
		}
		att, err := s.attach(conn, start.Epoch, detail)
		if err != nil {
			// 与回收或并发认领竞争失败；报错，让客户端用新一轮握手重试。
			m.staleEpochClaims.Add(1)
			return writeError(ctx, conn, err.Error())
		}
		<-att.done
		return nil
	}
}

// resolve 把一个 START 映射到它的会话：空的/未知的 id 给一个新 id（一个
// 过期会话以「全新对话」恢复，而不是报错），否则返回已有会话。
func (m *Manager) resolve(start *transportpb.ControlPayload) (*Session, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if start.SessionId != "" {
		if s, ok := m.sessions[start.SessionId]; ok {
			return s, "resumed", nil
		}
	}
	id := newSessionID()
	s, err := newSession(m, id)
	if err != nil {
		return nil, "", err
	}
	m.sessions[id] = s
	m.sessionsCreated.Add(1)
	detail := "created"
	if start.SessionId != "" {
		detail = "expired; new session created"
		// 一个全新会话不应继承陈旧认领的 epoch 下限：把认领归一化，让
		// attach 把它当作首次连接来处理。
		start.Epoch = 1
	} else if start.Epoch == 0 {
		start.Epoch = 1
	}
	return s, detail, nil
}

func (m *Manager) reapIdle(now time.Time) {
	idle := m.cfg.Session.IdleTimeout
	for _, s := range m.snapshot() {
		if now.Sub(s.lastActiveTime()) > idle {
			m.reclaim(s, "idle timeout")
		}
	}
}

func (m *Manager) snapshot() []*Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s)
	}
	return out
}

// reclaim 移除会话并释放它拥有的一切：流水线 goroutine、环、附着的连接。
func (m *Manager) reclaim(s *Session, reason string) {
	m.mu.Lock()
	if _, ok := m.sessions[s.id]; !ok {
		m.mu.Unlock()
		return // 已被回收
	}
	delete(m.sessions, s.id)
	m.mu.Unlock()
	s.close()
	m.sessionsReclaimed.Add(1)
	if m.m != nil {
		// 把这个已死会话的计数器折叠进进程级总量，使导出的计数器在翻滚下
		// 保持单调。
		m.m.AccIngressDropped.Add(s.pipe.IngressDropped())
		m.m.AccEgressDropped.Add(s.pipe.EgressDropped())
		m.m.AccDupFrames.Add(s.dupFrames.Load())
		m.m.AccStaleFrames.Add(s.staleFrames.Load())
		m.m.AccSubtitleDrops.Add(s.subtitleDrops.Load())
		m.m.AccIllegalEvents.Add(s.ctrl.IllegalEvents())
	}
	m.log.Info("session reclaimed", "id", s.id, "reason", reason)
}

func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败的严重程度足以让 panic 强过「悄悄发出可预测的
		// 会话 id」。
		panic("session: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func writeError(ctx context.Context, conn transport.Conn, detail string) error {
	payload, err := proto.Marshal(&transportpb.ControlPayload{
		Kind:   transportpb.ControlKind_CONTROL_KIND_ERROR,
		Detail: detail,
	})
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = conn.WriteFrame(wctx, transport.Frame{
		Type:    transportpb.FrameType_FRAME_TYPE_CONTROL,
		Payload: payload,
	})
	return nil
}

// adapterSetFor 构建每会话的适配器集合。mock 在每次 stream 调用内是有状态的，
// 但共享是安全的；尽管如此，按会话构建仍让真实适配器（自带 HTTP 客户端、
// 未来的本地模型）彼此隔离。
func (m *Manager) adapterSetFor() adapter.Set { return m.set }
