// Package transport implements the WebSocket streaming transport and the
// transport-agnostic binary frame protocol (M2). The frame schema is kept
// independent of the wire so a future WebTransport/gRPC stack can reuse it.
package transport
