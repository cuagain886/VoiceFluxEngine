package transport

import "context"

// StatusCode 是与具体传输无关的关闭码。取值镜像 RFC 6455 的 WebSocket 关闭
// 码；将来的 WebTransport 实现把它们映射到自己的关闭语义上。
type StatusCode uint16

const (
	StatusNormalClosure StatusCode = 1000
	StatusGoingAway     StatusCode = 1001
	StatusProtocolError StatusCode = 1002
	StatusMessageTooBig StatusCode = 1009
	StatusInternalError StatusCode = 1011
)

// Conn 是流水线层与会话层看到的、与具体传输无关的连接抽象。它们只依赖这个
// 接口、绝不依赖某个具体传输，所以将来把 WebSocket 换成 WebTransport 是一次
// drop-in 替换（设计 D12）。
//
// Read 和 Write 各自可由一个 goroutine 并发驱动，但同一时刻至多一个读者、
// 一个写者（每连接「一读一写」goroutine 模型）。
type Conn interface {
	// ReadFrame 阻塞直到有帧到达、ctx 被取消、或连接失败。
	ReadFrame(ctx context.Context) (Frame, error)
	// WriteFrame 发送一个帧。
	WriteFrame(ctx context.Context, f Frame) error
	// Ping 发送一个保活 ping 并等待匹配的 pong（或 ctx）。
	Ping(ctx context.Context) error
	// Close 以一个状态码和原因关闭连接。
	Close(status StatusCode, reason string) error
}
