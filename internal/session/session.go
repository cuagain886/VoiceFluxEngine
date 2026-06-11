// Package session manages the conversation lifecycle on top of the
// transport (M7 wiring, M8 lifecycle): one Session per conversation — unique
// id, monotonic epoch, idle-timeout reclamation — surviving across WebSocket
// reconnects, with replay dedup on the uplink. Frames inside one connection
// are TCP-ordered; there is deliberately no in-session reorder window (D13).
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

// wireMsg is a downlink frame before the writer assigns its sequence number.
type wireMsg struct {
	typ     transportpb.FrameType
	tsUs    int64
	payload []byte
}

// Session is one conversation. It owns the pipeline (and therefore the
// conversation history), the VAD controller and the downlink queue; all of
// it survives a disconnect. Connections come and go as attachments: at most
// one at a time, each bound to the epoch it presented at handshake.
type Session struct {
	id  string
	mgr *Manager

	pipe      *pipeline.Pipeline
	ctrl      *vad.Controller
	runCancel context.CancelFunc // session-lifetime: pipeline + audio pump
	runCtx    context.Context

	wire chan wireMsg // session-lifetime downlink queue

	clockUs     atomic.Int64  // downlink sample clock, µs of audio queued
	downlinkSeq atomic.Uint64 // continues across reconnects
	uplinkFloor atomic.Uint64 // highest delivered uplink seq (dedup watermark)
	lastActive  atomic.Int64  // unix nanos of the last frame in either direction

	dupFrames     atomic.Uint64
	staleFrames   atomic.Uint64
	subtitleDrops atomic.Uint64

	mu     sync.Mutex
	epoch  uint64
	att    *attachment
	closed bool
}

// attachment binds one connection to the session at a specific epoch. A
// takeover marks the previous attachment stale: anything still arriving on
// it is counted and dropped, never delivered into the pipeline.
type attachment struct {
	epoch  uint64
	conn   transport.Conn
	cancel context.CancelFunc
	stale  atomic.Bool
	done   chan struct{} // closed when reader and writer have both exited

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

// attach binds conn to the session under claimedEpoch. The claim must be
// strictly greater than the current epoch — equal or lower claims are stale
// reconnects (e.g. a zombie client racing the live one) and are rejected.
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

// close releases everything the session owns. Idempotent.
func (s *Session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	att := s.att
	s.mu.Unlock()

	s.runCancel() // stops pipeline, audio pump, and attachment loops (child ctx)
	if att != nil {
		_ = att.conn.Close(transport.StatusNormalClosure, "session closed")
	}
}

func (s *Session) touch() { s.lastActive.Store(time.Now().UnixNano()) }

func (s *Session) lastActiveTime() time.Time { return time.Unix(0, s.lastActive.Load()) }

// advanceFloor claims seq against the dedup watermark. Replays after a
// reconnect arrive in order below the floor and are rejected; the watermark
// makes a reorder window unnecessary (TCP keeps each connection ordered).
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

// readLoop is the per-attachment ingress edge: dedup + inline VAD + pipeline.
func (s *Session) readLoop(ctx context.Context, att *attachment) {
	defer att.loops.Done()
	defer att.cancel() // reader death stops the writer too
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
				// Client-initiated end of the whole conversation.
				s.mgr.reclaim(s, "client stop")
				return
			}
		default:
		}
	}
}

// writeLoop is the sole writer for this connection: the handshake ack first,
// then downlink frames and heartbeat pings. A frame dequeued when the
// connection dies is dropped, not re-queued — for real-time audio a write
// failure means the moment has passed.
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
				return // heartbeat timeout: the attachment is dead
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

// audioPump moves synthesized audio from the egress ring onto the session
// wire queue for the lifetime of the session, advancing the downlink clock.
// Blocking on a full queue is safe: the egress ring upstream sheds
// oldest-first, so memory stays bounded and the freshest audio wins. While
// detached the queue simply fills and the conversation pauses.
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

// enqueueText queues a TEXT frame without ever blocking the stage goroutine
// that produced it: subtitles shed (counted) under a slow client.
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
