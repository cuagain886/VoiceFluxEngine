// Package ringbuf provides a single-producer/single-consumer lock-free ring
// buffer used at the audio ingress and egress edges (M3). Text stages between
// pipeline stages use channels instead.
package ringbuf
