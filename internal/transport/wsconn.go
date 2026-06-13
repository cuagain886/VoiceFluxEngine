package transport

import (
	"context"
	"fmt"

	"github.com/coder/websocket"
)

// wsConn 把一个 *websocket.Conn 适配成与传输无关的 Conn 接口。scratch 是
// 写侧的编码缓冲：Conn 的契约保证同一时刻至多一个写者，所以跨 WriteFrame
// 调用复用它是安全的，并让稳态下行路径零分配。
type wsConn struct {
	c       *websocket.Conn
	scratch []byte
}

// newWSConn 包装一个已 accept/dial 的 WebSocket 连接。它把读上限调高，使一个
// 满 MaxPayload 的帧能装进一条消息（库默认上限是 32 KiB）。
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
	b, err := f.AppendBinary(w.scratch[:0])
	if err != nil {
		return err
	}
	w.scratch = b // 留下已增长的缓冲给下一帧用
	return w.c.Write(ctx, websocket.MessageBinary, b)
}

func (w *wsConn) Ping(ctx context.Context) error {
	return w.c.Ping(ctx)
}

func (w *wsConn) Close(status StatusCode, reason string) error {
	return w.c.Close(websocket.StatusCode(status), reason)
}
