package transport

import (
	"context"
	"fmt"

	"github.com/coder/websocket"
)

// wsConn adapts a *websocket.Conn to the transport-agnostic Conn interface.
type wsConn struct {
	c *websocket.Conn
}

// newWSConn wraps an accepted/dialed WebSocket connection. It raises the read
// limit so a full MaxPayload frame fits in one message (the library default is
// 32 KiB).
func newWSConn(c *websocket.Conn) *wsConn {
	c.SetReadLimit(int64(HeaderSize + MaxPayload))
	return &wsConn{c: c}
}

func (w *wsConn) ReadFrame(ctx context.Context) (Frame, error) {
	typ, data, err := w.c.Read(ctx)
	if err != nil {
		return Frame{}, err
	}
	if typ != websocket.MessageBinary {
		return Frame{}, fmt.Errorf("transport: expected binary message, got %v", typ)
	}
	return Decode(data)
}

func (w *wsConn) WriteFrame(ctx context.Context, f Frame) error {
	b, err := f.MarshalBinary()
	if err != nil {
		return err
	}
	return w.c.Write(ctx, websocket.MessageBinary, b)
}

func (w *wsConn) Ping(ctx context.Context) error {
	return w.c.Ping(ctx)
}

func (w *wsConn) Close(status StatusCode, reason string) error {
	return w.c.Close(websocket.StatusCode(status), reason)
}
