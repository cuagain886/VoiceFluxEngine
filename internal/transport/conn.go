package transport

import "context"

// StatusCode is a transport-agnostic close code. The values mirror RFC 6455
// WebSocket close codes; a future WebTransport implementation maps them onto
// its own close semantics.
type StatusCode uint16

const (
	StatusNormalClosure StatusCode = 1000
	StatusProtocolError StatusCode = 1002
	StatusMessageTooBig StatusCode = 1009
	StatusInternalError StatusCode = 1011
)

// Conn is the transport-agnostic connection seen by the pipeline and session
// layers. They depend only on this interface, never on a concrete transport, so
// swapping WebSocket for WebTransport later is a drop-in (design D12).
//
// Read and Write may each be driven by one goroutine concurrently, but there
// must be at most one reader and one writer at a time (the per-connection
// read/write goroutine model).
type Conn interface {
	// ReadFrame blocks until a frame arrives, ctx is cancelled, or the
	// connection fails.
	ReadFrame(ctx context.Context) (Frame, error)
	// WriteFrame sends a frame.
	WriteFrame(ctx context.Context, f Frame) error
	// Ping sends a liveness ping and waits for the matching pong (or ctx).
	Ping(ctx context.Context) error
	// Close closes the connection with a status code and reason.
	Close(status StatusCode, reason string) error
}
