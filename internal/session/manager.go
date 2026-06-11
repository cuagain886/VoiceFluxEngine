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

	mu       sync.Mutex
	sessions map[string]*Session

	staleEpochClaims atomic.Uint64
}

// NewManager builds a manager; call Run to start the idle reaper.
func NewManager(cfg config.Config, set adapter.Set, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{cfg: cfg, set: set, log: logger, sessions: map[string]*Session{}}
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
