package session

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http/httptest"
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
	return cfg
}

// pcmFrame builds one 20ms 16kHz mono frame of constant amplitude.
func pcmFrame(amplitude float64) []byte {
	buf := make([]byte, 640)
	v := uint16(int16(amplitude * 32767))
	for i := 0; i < 320; i++ {
		binary.LittleEndian.PutUint16(buf[2*i:], v)
	}
	return buf
}

// client is a minimal browser-equivalent over the real WS stack.
type client struct {
	t    *testing.T
	c    *websocket.Conn
	ctx  context.Context
	seq  uint64
	tsUs int64
}

func dial(t *testing.T, ctx context.Context, set adapter.Set, cfg config.Config) (*client, func()) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := transport.NewServerWithHandler(cfg.Server, logger, VoiceHandler(cfg, set, logger))
	ts := httptest.NewServer(srv.HTTPHandler())

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		ts.Close()
		t.Fatalf("dial: %v", err)
	}
	c.SetReadLimit(1 << 20)
	cl := &client{t: t, c: c, ctx: ctx}
	return cl, func() {
		_ = c.Close(websocket.StatusNormalClosure, "done")
		ts.Close()
	}
}

func (cl *client) sendAudio(amplitude float64, frames int) {
	cl.t.Helper()
	pcm := pcmFrame(amplitude)
	for i := 0; i < frames; i++ {
		cl.seq++
		f := transport.Frame{
			Type: transportpb.FrameType_FRAME_TYPE_AUDIO,
			Seq:  cl.seq, TsUs: cl.tsUs, Payload: pcm,
		}
		cl.tsUs += 20_000
		buf, err := f.MarshalBinary()
		if err != nil {
			cl.t.Fatalf("marshal: %v", err)
		}
		if err := cl.c.Write(cl.ctx, websocket.MessageBinary, buf); err != nil {
			cl.t.Fatalf("write: %v", err)
		}
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
		return f
	}
}

// TestVoiceSessionEndToEnd is the M7 server-side acceptance: a synthetic
// browser speaks, gets transcript + aligned tokens + audio back, interrupts
// mid-response, and receives the BARGE_IN control frame.
func TestVoiceSessionEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	set := adapter.Set{
		ASR: &adapter.MockASR{Script: "hello voicestream"},
		// Slow-ish tokens leave a window to interrupt mid-response.
		LLM: &adapter.MockLLM{Latency: adapter.Latency{Delay: 50 * time.Millisecond}},
		TTS: &adapter.MockTTS{SampleRate: 16000, FrameDuration: 20 * time.Millisecond},
	}
	cl, closeAll := dial(t, ctx, set, testConfig())
	defer closeAll()

	// One utterance: speech past min-speech, silence past hangover.
	cl.sendAudio(0.5, 5)
	cl.sendAudio(0.0, 6)

	var (
		gotTranscriptFinal bool
		gotToken           bool
		audioFrames        int
		lastSeq            uint64
		lastAudioTs        int64 = -1
		bargeSent          bool
		gotBargeIn         bool
	)
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
				t.Fatalf("text payload: %v", err)
			}
			switch tp.Source {
			case transportpb.TextSource_TEXT_SOURCE_TRANSCRIPT:
				if tp.Final {
					if tp.Text != "hello voicestream" {
						t.Fatalf("final transcript = %q", tp.Text)
					}
					gotTranscriptFinal = true
				}
			case transportpb.TextSource_TEXT_SOURCE_TOKEN:
				gotToken = true
			}
		case transportpb.FrameType_FRAME_TYPE_AUDIO:
			if f.TsUs <= lastAudioTs {
				t.Fatalf("audio ts_us not monotonic: %d after %d", f.TsUs, lastAudioTs)
			}
			lastAudioTs = f.TsUs
			audioFrames++
			if audioFrames == 3 && !bargeSent {
				// Agent is audibly mid-response: interrupt.
				cl.sendAudio(0.5, 4)
				bargeSent = true
			}
		case transportpb.FrameType_FRAME_TYPE_CONTROL:
			var cp transportpb.ControlPayload
			if err := proto.Unmarshal(f.Payload, &cp); err != nil {
				t.Fatalf("control payload: %v", err)
			}
			if cp.Kind == transportpb.ControlKind_CONTROL_KIND_BARGE_IN {
				if !bargeSent {
					t.Fatal("BARGE_IN before the client interrupted")
				}
				gotBargeIn = true
			}
		}
	}

	if !gotTranscriptFinal {
		t.Error("never received the final transcript")
	}
	if !gotToken {
		t.Error("never received an agent token")
	}
	if audioFrames < 3 {
		t.Errorf("audio frames = %d, want >= 3", audioFrames)
	}
}

// TestVoiceSessionCleanStop: a CONTROL STOP from the client ends the session
// without an error-level teardown.
func TestVoiceSessionCleanStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	set := adapter.Set{
		ASR: &adapter.MockASR{Script: "bye"},
		LLM: &adapter.MockLLM{},
		TTS: &adapter.MockTTS{SampleRate: 16000, FrameDuration: 20 * time.Millisecond},
	}
	cl, closeAll := dial(t, ctx, set, testConfig())
	defer closeAll()

	payload, err := proto.Marshal(&transportpb.ControlPayload{
		Kind: transportpb.ControlKind_CONTROL_KIND_STOP,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := transport.Frame{Type: transportpb.FrameType_FRAME_TYPE_CONTROL, Seq: 1, Payload: payload}
	buf, err := f.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.c.Write(ctx, websocket.MessageBinary, buf); err != nil {
		t.Fatal(err)
	}

	// The server should close the connection cleanly: the next read ends
	// with a close frame rather than hanging.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			if _, _, err := cl.c.Read(ctx); err != nil {
				return
			}
		}
	}()
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not close the session after STOP")
	}
}
