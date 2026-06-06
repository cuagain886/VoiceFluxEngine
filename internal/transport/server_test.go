package transport

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"voicestream/internal/config"
	"voicestream/internal/transport/transportpb"

	"github.com/coder/websocket"
)

// TestEchoRoundTrip is the M2 acceptance test: a frame sent over the wire comes
// back byte-for-byte identical through the echo handler.
func TestEchoRoundTrip(t *testing.T) {
	s := NewServer(
		config.ServerConfig{HeartbeatPeriod: time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ts := httptest.NewServer(http.HandlerFunc(s.handleWS))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.CloseNow()

	want, err := Frame{
		Type:    transportpb.FrameType_FRAME_TYPE_TEXT,
		Seq:     42,
		TsUs:    987654,
		Payload: []byte("the quick brown fox"),
	}.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := c.Write(ctx, websocket.MessageBinary, want); err != nil {
		t.Fatalf("write: %v", err)
	}

	typ, got, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("expected binary message, got %v", typ)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo mismatch:\n got=%x\nwant=%x", got, want)
	}

	c.Close(websocket.StatusNormalClosure, "done")
}
