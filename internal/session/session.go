package session

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/pipeline"
	"voicestream/internal/transport"
	"voicestream/internal/transport/transportpb"
	"voicestream/internal/vad"
)

// VoiceHandler returns the per-connection handler that replaces M2's echo:
// one full voice session — pipeline, inline VAD, uplink reader, downlink
// writer — per WebSocket connection.
//
// Goroutine model per connection:
//
//	reader      ReadFrame -> ctrl.Ingest (VAD inline, D4) -> ingress ring
//	pipeline    ASR -> LLM -> TTS (M5, own internal goroutines)
//	audio pump  egress ring -> wire queue, stamping the session downlink clock
//	writer      sole WS writer: audio + text + control frames, heartbeat pings
//
// Downlink timing (7.5): the wire ts_us for AUDIO is a session-continuous
// sample clock (total downlink audio sent), not the TTS-internal per-turn
// PTS. A token TEXT frame is stamped with the clock value at forwarding
// time — the token is forwarded just before its audio is synthesized, so
// that value ≈ where its speech begins; the client reveals each token when
// playback reaches its ts (subtitle alignment).
func VoiceHandler(cfg config.Config, set adapter.Set, logger *slog.Logger) transport.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, conn transport.Conn) error {
		s := &voiceSession{
			cfg:  cfg,
			log:  logger,
			wire: make(chan wireMsg, 256),
		}
		return s.run(ctx, conn, set)
	}
}

// wireMsg is a downlink frame before the writer assigns its sequence number
// (single writer = single point of seq allocation).
type wireMsg struct {
	typ     transportpb.FrameType
	tsUs    int64
	payload []byte
}

type voiceSession struct {
	cfg  config.Config
	log  *slog.Logger
	wire chan wireMsg

	clockUs       atomic.Int64  // session downlink sample clock, µs of audio queued
	subtitleDrops atomic.Uint64 // TEXT/CONTROL frames shed because wire was full
}

func (s *voiceSession) run(ctx context.Context, conn transport.Conn, set adapter.Set) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p, err := pipeline.New(s.cfg, set, s.log)
	if err != nil {
		return fmt.Errorf("session: pipeline: %w", err)
	}
	ctrl := vad.NewController(s.cfg, nil, p, s.log)

	p.OnTranscript = func(tr adapter.Transcript) {
		s.enqueueText(tr.Text, tr.Final, transportpb.TextSource_TEXT_SOURCE_TRANSCRIPT, tr.TsUs)
	}
	p.OnToken = func(tok adapter.Token) {
		s.enqueueText(tok.Text, false, transportpb.TextSource_TEXT_SOURCE_TOKEN, s.clockUs.Load())
	}
	p.OnTurnStart = ctrl.ResponseStarted
	p.OnTurnEnd = func(cancelled bool) {
		ctrl.ResponseDone()
		if cancelled {
			// Tell the client to stop local playback now (7.4); the kernel
			// side has already flushed its in-flight audio.
			s.enqueueControl(transportpb.ControlKind_CONTROL_KIND_BARGE_IN)
		}
	}

	// errc is buffered for every goroutine so none can block on exit after
	// run returns.
	errc := make(chan error, 4)
	go func() { errc <- p.Run(ctx) }()
	go s.readLoop(ctx, conn, ctrl, errc)
	go s.audioPump(ctx, p, errc)
	go s.writeLoop(ctx, conn, errc)

	err = <-errc
	cancel()
	if drops := s.subtitleDrops.Load(); drops > 0 {
		s.log.Warn("subtitle frames shed (slow client)", "count", drops)
	}
	if err == context.Canceled {
		return nil
	}
	return err
}

// readLoop is the ingress edge: every uplink AUDIO frame passes through the
// inline VAD (same goroutine — the ingress ring keeps exactly one producer)
// and into the pipeline.
func (s *voiceSession) readLoop(ctx context.Context, conn transport.Conn, ctrl *vad.Controller, errc chan<- error) {
	for {
		f, err := conn.ReadFrame(ctx)
		if err != nil {
			errc <- err
			return
		}
		switch f.Type {
		case transportpb.FrameType_FRAME_TYPE_AUDIO:
			ctrl.Ingest(adapter.AudioFrame{PCM: f.Payload, TsUs: f.TsUs})
		case transportpb.FrameType_FRAME_TYPE_CONTROL:
			var c transportpb.ControlPayload
			if perr := proto.Unmarshal(f.Payload, &c); perr != nil {
				s.log.Debug("bad control payload", "err", perr)
				continue
			}
			if c.Kind == transportpb.ControlKind_CONTROL_KIND_STOP {
				errc <- nil // clean client-initiated end of session
				return
			}
		default:
			// TEXT uplink is not part of the v1 protocol; ignore.
		}
	}
}

// audioPump moves synthesized audio from the egress ring onto the wire
// queue, advancing the session downlink clock. Blocking on a full wire queue
// is safe: the egress ring upstream sheds oldest-first, so memory stays
// bounded while the freshest audio wins.
func (s *voiceSession) audioPump(ctx context.Context, p *pipeline.Pipeline, errc chan<- error) {
	rate := int64(s.cfg.Audio.SampleRate)
	for {
		f, err := p.AwaitDownlink(ctx)
		if err != nil {
			errc <- err
			return
		}
		tsOut := s.clockUs.Load()
		samples := int64(len(f.PCM) / 2)
		s.clockUs.Store(tsOut + samples*1_000_000/rate)
		select {
		case s.wire <- wireMsg{typ: transportpb.FrameType_FRAME_TYPE_AUDIO, tsUs: tsOut, payload: f.PCM}:
		case <-ctx.Done():
			errc <- ctx.Err()
			return
		}
	}
}

// writeLoop is the sole WS writer: wire frames plus heartbeat pings.
func (s *voiceSession) writeLoop(ctx context.Context, conn transport.Conn, errc chan<- error) {
	heartbeat := s.cfg.Server.HeartbeatPeriod
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	t := time.NewTicker(heartbeat)
	defer t.Stop()

	var seq uint64
	for {
		select {
		case m := <-s.wire:
			seq++
			f := transport.Frame{Type: m.typ, Seq: seq, TsUs: m.tsUs, Payload: m.payload}
			if err := conn.WriteFrame(ctx, f); err != nil {
				errc <- err
				return
			}
		case <-t.C:
			pctx, pcancel := context.WithTimeout(ctx, heartbeat)
			err := conn.Ping(pctx)
			pcancel()
			if err != nil {
				errc <- err // heartbeat timeout: treat as disconnect
				return
			}
		case <-ctx.Done():
			errc <- ctx.Err()
			return
		}
	}
}

// enqueueText queues a TEXT frame without ever blocking the stage goroutine
// that produced it. Subtitles are best-effort UX: under a slow client they
// shed (counted) while the audio product keeps flowing.
func (s *voiceSession) enqueueText(text string, final bool, src transportpb.TextSource, tsUs int64) {
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

func (s *voiceSession) enqueueControl(kind transportpb.ControlKind) {
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
