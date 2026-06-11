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

// worker is one virtual caller: it dials the server, performs the M8
// handshake, then loops conversation turns at real(istic)-time pacing — the
// uplink stream stays continuous (speech and silence frames alike), exactly
// like the browser client, so every frame crosses the real hot path:
// transport decode, dedup watermark, inline VAD, ingress ring, pipeline.
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

// downlink is the reader goroutine's view of the connection, consumed by the
// turn loop. Event stamps are taken at frame *arrival* so measurements do not
// depend on when the turn loop gets around to draining the channels.
type downlink struct {
	lastAudioNs atomic.Int64
	audioC      chan time.Time // arrival times of AUDIO frames; lossy (cap 8)
	bargeC      chan time.Time // arrival times of BARGE_IN controls
	errC        chan error     // terminal read error (cap 1)
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

// pcmFrame builds one 20ms-equivalent PCM frame at a constant amplitude; the
// inline energy VAD sees RMS == amplitude.
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
	// Random phase offset so a step's workers do not speak in lockstep.
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
				return // shutdown, not a failure
			}
			var to *timeoutError
			if errors.As(err, &to) {
				w.col.fail(to.phase, err)
				continue // stay connected; keep loading the server
			}
			w.col.fail("conn", err)
			return
		}
		w.col.turnDone()
	}
}

// netemFor applies the configured perturbation to this worker. Seed is
// derived per worker so jitter schedules differ but stay reproducible.
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

	// Handshake: START with a fresh session, expect the START ack.
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

// sendAudio sends one paced uplink frame. TsUs advances on the nominal frame
// duration (the audio sample clock), independent of wall pacing, so the
// kernel sees a consistent PTS stream even when the harness runs faster than
// real time (tests) or frames are netem-delayed.
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

// readLoop drains the downlink for the lifetime of the connection: stamps
// audio arrivals, surfaces BARGE_IN controls and terminal errors.
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
			default: // turn loop only needs the first arrival; rest coalesce
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

// timeoutError marks a phase that exceeded TurnTimeout — a *measurement*
// (the system degraded past the deadline), not a harness defect; the worker
// records it and keeps loading.
type timeoutError struct{ phase string }

func (e *timeoutError) Error() string { return "loadgen: timeout waiting for " + e.phase }

// turn drives one conversation round:
//
//	speech ... silence ──▶ first downlink audio   (client e2e first response)
//	[barge turns] ride the response, speak over it ──▶ BARGE_IN control
//	                                              (client e2e barge-in)
//	silence until the downlink goes quiet ──▶ next turn
func (w *worker) turn(ctx context.Context, barge bool) error {
	speech := w.cfg.speechPCM
	silence := w.cfg.silencePCM

	// Phase 1: the utterance.
	for i := 0; i < w.cfg.SpeechFrames; i++ {
		if err := w.sendAudio(ctx, speech); err != nil {
			return err
		}
	}
	spokeAt := time.Now()

	// Phase 2: silence (VAD hangover runs server-side) until the response.
	firstAt, err := w.silenceUntilAudioAfter(ctx, spokeAt, silence)
	if err != nil {
		return err
	}
	w.col.sample(sampleFirst, firstAt.Sub(spokeAt))

	if barge {
		// Ride the response briefly so the barge lands mid-flight.
		if err := w.silenceFor(ctx, silence, w.cfg.BargeDelay); err != nil {
			return err
		}
		drainTimes(w.dl.bargeC)
		bargeStart := time.Now()
		// Speak over the agent; a full utterance, so it also becomes the
		// next prompt.
		for i := 0; i < w.cfg.SpeechFrames; i++ {
			if err := w.sendAudio(ctx, speech); err != nil {
				return err
			}
		}
		spokeAt = time.Now()
		bargeAt, err := w.awaitBarge(ctx, silence, bargeStart)
		if err != nil {
			return err
		}
		w.col.sample(sampleBarge, bargeAt.Sub(bargeStart))
		// The interrupting utterance gets its own response. Stale audio of
		// the cancelled turn cannot pollute this wait: the kernel queues the
		// BARGE_IN control *after* the flushed turn's frames, so anything
		// arriving after bargeAt belongs to the new turn.
		firstAt, err := w.silenceUntilAudioAfter(ctx, bargeAt, silence)
		if err != nil {
			return err
		}
		w.col.sample(sampleFirst, firstAt.Sub(spokeAt))
	}

	// Phase 3: let the response finish — downlink quiet for QuietGap.
	return w.silenceUntilQuiet(ctx, silence)
}

// silenceUntilAudioAfter paces silence frames until an AUDIO frame arriving
// at or after the `after` mark shows up. Returns its arrival time.
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

// silenceFor paces silence frames for roughly d of wall time.
func (w *worker) silenceFor(ctx context.Context, silence []byte, d time.Duration) error {
	until := time.Now().Add(d)
	for time.Now().Before(until) {
		if err := w.sendAudio(ctx, silence); err != nil {
			return err
		}
	}
	return nil
}

// awaitBarge paces silence until the BARGE_IN control (sent when the kernel
// finished cancelling the turn) arrives. Accepts stamps from `after` on —
// the control may already have landed while we were still speaking.
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

// silenceUntilQuiet paces silence until no downlink audio has arrived for
// QuietGap — the turn's response has fully played out.
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
