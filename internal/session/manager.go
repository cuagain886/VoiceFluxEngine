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

// Manager owns every live session: creation on first contact, reattachment
// on reconnect, idle-timeout reclamation. Session state is in-process by
// design (the optional Redis externalization is a separate, off-by-default
// peripheral — task 12.1).
type Manager struct {
	cfg config.Config
	set adapter.Set
	log *slog.Logger
	m   *metrics.M // nil = instrumentation disabled

	mu       sync.Mutex
	sessions map[string]*Session

	staleEpochClaims  atomic.Uint64
	sessionsCreated   atomic.Uint64
	sessionsReclaimed atomic.Uint64
}

// NewManager builds a manager; call Run to start the idle reaper. m may be
// nil to disable metrics (tests).
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

// registerGauges exposes scrape-time values: live session count plus the
// frame-hygiene totals (live sessions summed on the fly + totals accumulated
// from reclaimed ones, so counters stay monotonic across session churn).
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

// Run drives the idle reaper until ctx ends, then reclaims everything.
func (m *Manager) Run(ctx context.Context) {
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

// Count reports live sessions (the 8.4 leak gate).
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// StaleEpochClaims counts rejected reconnect attempts with a non-increasing
// epoch (the "old connection trying to pollute the new one" metric).
func (m *Manager) StaleEpochClaims() uint64 { return m.staleEpochClaims.Load() }

// Stats exposes a session's frame-hygiene counters (tests, M9 metrics).
type Stats struct {
	Epoch         uint64
	DupFrames     uint64
	StaleFrames   uint64
	SubtitleDrops uint64
}

// SessionStats returns counters for a live session.
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

// Handler returns the per-connection transport handler: handshake first
// (a CONTROL START frame), then attach the connection to its session and
// serve until the attachment ends.
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
			// Lost a race with reclamation or a concurrent claim; report and
			// let the client retry with a fresh handshake.
			m.staleEpochClaims.Add(1)
			return writeError(ctx, conn, err.Error())
		}
		<-att.done
		return nil
	}
}

// resolve maps a START to its session: new id for an empty/unknown one
// (an expired session resumes as a fresh conversation rather than an error),
// the existing session otherwise.
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
		// A fresh session must not inherit the stale claim's epoch floor:
		// normalize the claim so attach treats it as a first connection.
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

// reclaim removes the session and releases everything it owns: pipeline
// goroutines, rings, the attached connection.
func (m *Manager) reclaim(s *Session, reason string) {
	m.mu.Lock()
	if _, ok := m.sessions[s.id]; !ok {
		m.mu.Unlock()
		return // already reclaimed
	}
	delete(m.sessions, s.id)
	m.mu.Unlock()
	s.close()
	m.sessionsReclaimed.Add(1)
	if m.m != nil {
		// Fold the dead session's counters into the process totals so the
		// exported counters stay monotonic across churn.
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
		// crypto/rand failing is unrecoverable enough that a panic beats
		// silently issuing predictable session ids.
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

// adapterSetFor builds the per-session adapter set. Mocks are stateful per
// stream call but safe to share; still, building per session keeps real
// adapters (own HTTP clients, future local models) isolated.
func (m *Manager) adapterSetFor() adapter.Set { return m.set }
