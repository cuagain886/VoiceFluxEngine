package transport

import (
	"context"
	"fmt"
	"io"

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

// ReadFrameInto 读取下一帧，把 payload 读进调用方提供的 dst（按需扩容并返回扩容后的
// 切片），从而复用缓冲、绕开 c.Read 逐消息 io.ReadAll 的每帧分配——上行热路径的零分配
// 由此成立。它用 c.Reader 先读 24 字节头（栈数组）再把 payload 读进 dst[:0]（偏移 0），
// 所以返回的 Frame.Payload 即 dst[:n]、整块容量，调用方用完后可整块放回缓冲池。
//
// 这是 Conn 的可选扩展（FramePooledReader）：只有真实 WebSocket 连接实现它；测试桩等
// 其它实现走 ReadFrame 回退路径，行为不变（只是不池化）。
func (w *wsConn) ReadFrameInto(ctx context.Context, dst []byte) (Frame, []byte, error) {
	typ, r, err := w.c.Reader(ctx)
	if err != nil {
		return Frame{}, dst, err
	}
	if typ != websocket.MessageBinary {
		return Frame{}, dst, fmt.Errorf("transport: expected binary message, got %v", typ)
	}
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, dst, err
	}
	f, n, err := decodeHeader(hdr[:])
	if err != nil {
		return Frame{}, dst, err
	}
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	if _, err := io.ReadFull(r, dst); err != nil {
		return Frame{}, dst, err
	}
	// payload 读完后这条消息应当恰好到尾：多出字节意味着 length 与消息不符。
	var probe [1]byte
	if _, perr := io.ReadFull(r, probe[:]); perr == nil {
		return Frame{}, dst, fmt.Errorf("%w: trailing bytes after payload", ErrFrameLength)
	} else if perr != io.EOF {
		return Frame{}, dst, perr
	}
	f.Payload = dst
	return f, dst, nil
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
