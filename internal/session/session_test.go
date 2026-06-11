package session

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/transport"
	"voicestream/internal/transport/transportpb"
)

func testConfig() config.Config {
	cfg := config.Default()
	cfg.Server.StaticDir = "" // no web/ in the test working dir
	cfg.VAD.MinSpeech = 60 * time.Millisecond
	cfg.VAD.Hangover = 100 * time.Millisecond
	cfg.Session.IdleTimeout = 400 * time.Millisecond
	return cfg
}

func defaultSet() adapter.Set {
	return adapter.Set{
		ASR: &adapter.MockASR{Script: "hello voicestream"},
		LLM: &adapter.MockLLM{Latency: adapter.Latency{Delay: 50 * time.Millisecond}},
		TTS: &adapter.MockTTS{SampleRate: 16000, FrameDuration: 20 * time.Millisecond},
	}
}

// rig runs a manager + HTTP server; clients dial it like browsers would.
type rig struct {
	t   *testing.T
	mgr *Manager
	url string
}

func newRig(t *testing.T, ctx context.Context, cfg config.Config, set adapter.Set) *rig {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(cfg, set, logger)
	go mgr.Run(ctx)
	srv := transport.NewServerWithHandler(cfg.Server, logger, mgr.Handler())
	ts := httptest.NewServer(srv.HTTPHandler())
	t.Cleanup(ts.Close)
	return &rig{t: t, mgr: mgr, url: "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"}
}

// client is a synthetic browser speaking the M8 handshake protocol.
type client struct {
	t   *testing.T
	ctx context.Context
	c   *websocket.Conn

	seq         uint64 // uplink seq, persists across reconnects via reuse
	tsUs        int64
	lastDownSeq uint64
}

func (r *rig) dial(ctx context.Context) *client {
	r.t.Helper()
	c, _, err := websocket.Dial(ctx, r.url, nil)
	if err != nil {
		r.t.Fatalf("dial: %v", err)
	}
	c.SetReadLimit(1 << 20)
	return &client{t: r.t, ctx: ctx, c: c}
}

func (cl *client) close() { _ = cl.c.Close(websocket.StatusNormalClosure, "bye") }

func (cl *client) writeFrame(f transport.Frame) {
	cl.t.Helper()
	buf, err := f.MarshalBinary()
	if err != nil {
		cl.t.Fatalf("marshal: %v", err)
	}
	if err := cl.c.Write(cl.ctx, websocket.MessageBinary, buf); err != nil {
		cl.t.Fatalf("write: %v", err)
	}
}

func (cl *client) sendControl(kind transportpb.ControlKind, sessionID string, epoch, lastSeq uint64) {
	cl.t.Helper()
	payload, err := proto.Marshal(&transportpb.ControlPayload{
		Kind: kind, SessionId: sessionID, Epoch: epoch, LastSeq: lastSeq,
	})
	if err != nil {
		cl.t.Fatal(err)
	}
	cl.writeFrame(transport.Frame{Type: transportpb.FrameType_FRAME_TYPE_CONTROL, Payload: payload})
}

// handshake sends START and returns the server's ack (or ERROR) payload.
func (cl *client) handshake(sessionID string, epoch uint64) *transportpb.ControlPayload {
	cl.t.Helper()
	cl.sendControl(transportpb.ControlKind_CONTROL_KIND_START, sessionID, epoch, cl.lastDownSeq)
	for {
		f := cl.read()
		if f.Type != transportpb.FrameType_FRAME_TYPE_CONTROL {
			continue
		}
		var cp transportpb.ControlPayload
		if err := proto.Unmarshal(f.Payload, &cp); err != nil {
			cl.t.Fatalf("control payload: %v", err)
		}
		return &cp
	}
}

func (cl *client) read() transport.Frame {
	cl.t.Helper()
	for {
		typ, data, err := cl.c.Read(cl.ctx)
		if err != nil {
			cl.t.Fatalf("read: %v", err)
		}
		if typ != websocket.MessageBinary {
			continue
		}
		f, err := transport.Decode(data)
		if err != nil {
			cl.t.Fatalf("decode: %v", err)
		}
		if f.Seq > 0 {
			cl.lastDownSeq = f.Seq
		}
		return f
	}
}

func pcmFrame(amplitude float64) []byte {
	buf := make([]byte, 640)
	v := uint16(int16(amplitude * 32767))
	for i := 0; i < 320; i++ {
		binary.LittleEndian.PutUint16(buf[2*i:], v)
	}
	return buf
}

// sendAudio sends frames with explicit control over the seq counter so tests
// can replay ranges.
func (cl *client) sendAudio(amplitude float64, frames int) {
	cl.t.Helper()
	pcm := pcmFrame(amplitude)
	for i := 0; i < frames; i++ {
		cl.seq++
		cl.writeFrame(transport.Frame{
			Type: transportpb.FrameType_FRAME_TYPE_AUDIO,
			Seq:  cl.seq, TsUs: cl.tsUs, Payload: pcm,
		})
		cl.tsUs += 20_000
	}
}

func (cl *client) replayAudio(fromSeq, toSeq uint64) {
	cl.t.Helper()
	pcm := pcmFrame(0.0)
	for seq := fromSeq; seq <= toSeq; seq++ {
		cl.writeFrame(transport.Frame{
			Type: transportpb.FrameType_FRAME_TYPE_AUDIO,
			Seq:  seq, TsUs: int64(seq) * 20_000, Payload: pcm,
		})
	}
}

func waitFor(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestHandshakeCreatesSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := newRig(t, ctx, testConfig(), defaultSet())

	cl := r.dial(ctx)
	defer cl.close()
	ack := cl.handshake("", 1)
	if ack.Kind != transportpb.ControlKind_CONTROL_KIND_START {
		t.Fatalf("ack kind = %v, detail %q", ack.Kind, ack.Detail)
	}
	if ack.SessionId == "" || ack.Epoch != 1 {
		t.Fatalf("ack = %+v, want fresh id with epoch 1", ack)
	}
	if r.mgr.Count() != 1 {
		t.Fatalf("sessions = %d, want 1", r.mgr.Count())
	}
}

// TestConversationAndBargeIn re-runs the M7 acceptance on top of the
// handshake protocol: speech -> transcript/tokens/audio, interrupt ->
// BARGE_IN control frame.
func TestConversationAndBargeIn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r := newRig(t, ctx, testConfig(), defaultSet())

	cl := r.dial(ctx)
	defer cl.close()
	cl.handshake("", 1)

	cl.sendAudio(0.5, 5)
	cl.sendAudio(0.0, 6)

	var gotFinal, gotToken, bargeSent, gotBargeIn bool
	audioFrames := 0
	var lastSeq uint64
	for !gotBargeIn {
		f := cl.read()
		if f.Seq <= lastSeq {
			t.Fatalf("downlink seq not monotonic: %d after %d", f.Seq, lastSeq)
		}
		lastSeq = f.Seq
		switch f.Type {
		case transportpb.FrameType_FRAME_TYPE_TEXT:
			var tp transportpb.TextPayload
			if err := proto.Unmarshal(f.Payload, &tp); err != nil {
				t.Fatal(err)
			}
			if tp.Source == transportpb.TextSource_TEXT_SOURCE_TRANSCRIPT && tp.Final {
				gotFinal = true
			}
			if tp.Source == transportpb.TextSource_TEXT_SOURCE_TOKEN {
				gotToken = true
			}
		case transportpb.FrameType_FRAME_TYPE_AUDIO:
			audioFrames++
			if audioFrames == 3 && !bargeSent {
				cl.sendAudio(0.5, 4)
				bargeSent = true
			}
		case transportpb.FrameType_FRAME_TYPE_CONTROL:
			var cp transportpb.ControlPayload
			if err := proto.Unmarshal(f.Payload, &cp); err != nil {
				t.Fatal(err)
			}
			if cp.Kind == transportpb.ControlKind_CONTROL_KIND_BARGE_IN {
				gotBargeIn = true
			}
		}
	}
	if !gotFinal || !gotToken || audioFrames < 3 {
		t.Fatalf("final=%v token=%v audio=%d", gotFinal, gotToken, audioFrames)
	}
}

// TestReconnectResume covers 8.3: same session id, epoch+1, downlink seq
// continues, and the ack carries the server's uplink watermark.
func TestReconnectResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r := newRig(t, ctx, testConfig(), defaultSet())

	cl := r.dial(ctx)
	ack := cl.handshake("", 1)
	id := ack.SessionId
	cl.sendAudio(0.5, 5) // floor advances to 5
	waitFor(t, 2*time.Second, func() bool {
		st, ok := r.mgr.SessionStats(id)
		return ok && st.Epoch == 1 && cl.seq == 5 && st.DupFrames == 0
	}, "session not settled")
	seqBefore := cl.lastDownSeq
	cl.close() // abrupt disconnect — no STOP

	waitFor(t, 2*time.Second, func() bool { return r.mgr.Count() == 1 }, "session vanished early")

	cl2 := r.dial(ctx)
	defer cl2.close()
	cl2.seq, cl2.tsUs = cl.seq, cl.tsUs // same logical client state
	ack2 := cl2.handshake(id, 2)
	if ack2.SessionId != id || ack2.Epoch != 2 || ack2.Detail != "resumed" {
		t.Fatalf("resume ack = %+v", ack2)
	}
	if ack2.LastSeq != 5 {
		t.Fatalf("ack last_seq = %d, want uplink watermark 5", ack2.LastSeq)
	}
	if ack2.GetEpoch() != 2 {
		t.Fatalf("epoch = %d, want 2", ack2.Epoch)
	}
	// Drive a turn on the resumed attachment; downlink seq must continue
	// past the pre-disconnect counter, never restart.
	cl2.sendAudio(0.5, 5)
	cl2.sendAudio(0.0, 6)
	f := cl2.read()
	if f.Seq <= seqBefore {
		t.Fatalf("downlink seq restarted: %d after %d", f.Seq, seqBefore)
	}
	if r.mgr.Count() != 1 {
		t.Fatalf("sessions = %d, want 1 (resumed, not duplicated)", r.mgr.Count())
	}
}

// TestReplayDedup covers 8.2: replayed seqs at or below the watermark are
// dropped exactly as duplicates, fresh seqs pass.
func TestReplayDedup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := newRig(t, ctx, testConfig(), defaultSet())

	cl := r.dial(ctx)
	ack := cl.handshake("", 1)
	id := ack.SessionId
	cl.sendAudio(0.0, 8) // silence: no turn machinery, pure dedup path
	waitFor(t, 2*time.Second, func() bool {
		st, _ := r.mgr.SessionStats(id)
		return st.DupFrames == 0 && cl.seq == 8
	}, "frames not delivered")
	cl.close()

	cl2 := r.dial(ctx)
	defer cl2.close()
	cl2.seq = cl.seq
	cl2.handshake(id, 2)
	cl2.replayAudio(4, 8)  // 5 duplicates (<= watermark 8)
	cl2.replayAudio(9, 11) // 3 fresh
	waitFor(t, 2*time.Second, func() bool {
		st, _ := r.mgr.SessionStats(id)
		return st.DupFrames == 5
	}, "dup counter never reached 5")
	st, _ := r.mgr.SessionStats(id)
	if st.DupFrames != 5 {
		t.Fatalf("dupFrames = %d, want 5", st.DupFrames)
	}
}

// TestStaleEpochClaimRejected covers 8.3's pollution guard: a claim that
// does not increase the epoch gets an ERROR and the live attachment is
// untouched.
func TestStaleEpochClaimRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := newRig(t, ctx, testConfig(), defaultSet())

	cl := r.dial(ctx)
	defer cl.close()
	ack := cl.handshake("", 1)

	zombie := r.dial(ctx)
	defer zombie.close()
	ack2 := zombie.handshake(ack.SessionId, 1) // not an increase: stale
	if ack2.Kind != transportpb.ControlKind_CONTROL_KIND_ERROR {
		t.Fatalf("stale claim got %v (%q), want ERROR", ack2.Kind, ack2.Detail)
	}
	if r.mgr.StaleEpochClaims() == 0 {
		t.Fatal("stale claim not counted")
	}
	// The live client is unaffected: frames still flow.
	cl.sendAudio(0.0, 3)
	waitFor(t, 2*time.Second, func() bool {
		st, _ := r.mgr.SessionStats(ack.SessionId)
		return st.Epoch == 1 && st.DupFrames == 0
	}, "live attachment disturbed by stale claim")
}

// TestExpiredSessionResumesFresh: resuming an unknown (reclaimed) id yields
// a brand-new session rather than an error — the conversation restarts.
func TestExpiredSessionResumesFresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := newRig(t, ctx, testConfig(), defaultSet())

	cl := r.dial(ctx)
	defer cl.close()
	ack := cl.handshake("ffffffffffffffffffffffffffffffff", 7)
	if ack.Kind != transportpb.ControlKind_CONTROL_KIND_START {
		t.Fatalf("ack = %+v", ack)
	}
	if ack.SessionId == "ffffffffffffffffffffffffffffffff" {
		t.Fatal("expired id was resurrected instead of replaced")
	}
	if ack.Epoch != 1 {
		t.Fatalf("fresh session epoch = %d, want 1", ack.Epoch)
	}
}

// TestStopReclaims: CONTROL STOP ends the whole session, not just the
// connection.
func TestStopReclaims(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := newRig(t, ctx, testConfig(), defaultSet())

	cl := r.dial(ctx)
	defer cl.close()
	cl.handshake("", 1)
	if r.mgr.Count() != 1 {
		t.Fatal("session not created")
	}
	cl.sendControl(transportpb.ControlKind_CONTROL_KIND_STOP, "", 0, 0)
	waitFor(t, 3*time.Second, func() bool { return r.mgr.Count() == 0 }, "STOP did not reclaim")
}

// TestIdleReclaimAndNoLeak is the 8.4 soak gate: churn a batch of sessions,
// let the reaper collect them, and require session count zero and goroutines
// back to baseline.
func TestIdleReclaimAndNoLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := testConfig()
	cfg.Session.IdleTimeout = 200 * time.Millisecond
	r := newRig(t, ctx, cfg, defaultSet())

	baseline := runtime.NumGoroutine()
	const churn = 25
	for i := 0; i < churn; i++ {
		cl := r.dial(ctx)
		cl.handshake("", 1)
		cl.sendAudio(0.5, 5)
		cl.sendAudio(0.0, 6) // a real turn spins up the full machinery
		cl.close()           // abrupt: reclamation must come from the reaper
	}

	waitFor(t, 10*time.Second, func() bool { return r.mgr.Count() == 0 },
		"sessions not reclaimed after idle timeout")
	waitFor(t, 10*time.Second, func() bool { return runtime.NumGoroutine() <= baseline+5 },
		"goroutines did not return to baseline")
}
