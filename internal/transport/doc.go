// Package transport 实现 WebSocket 流式传输，以及与具体传输无关的二进制
// 帧协议（M2）。帧的格式刻意与「线路」解耦，使将来的 WebTransport/gRPC 栈
// 能复用它。
package transport
