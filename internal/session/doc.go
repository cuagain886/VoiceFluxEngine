// Package session manages session lifecycle (id + monotonic epoch, idle
// timeout, resource release), reconnect-resume, and replay dedup (M8). It does
// not maintain an in-session reorder window: a single WS/TCP connection
// delivers frames in order.
package session
