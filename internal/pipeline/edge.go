package pipeline

import (
	"context"

	"voicestream/internal/adapter"
	"voicestream/internal/ringbuf"
)

// edge is an audio boundary of the pipeline: a lock-free ring plus a
// capacity-1 doorbell. The ring keeps the producer non-blocking (drop-oldest
// under load); the doorbell lets the consumer park instead of polling — a
// non-blocking send after each push wakes at most one waiter, and the
// level-triggered token means a push between "ring empty" and "park" is never
// missed.
type edge struct {
	ring *ringbuf.Ring[adapter.AudioFrame]
	bell chan struct{}
}

func newEdge(capacity int, policy ringbuf.Policy) (*edge, error) {
	ring, err := ringbuf.New[adapter.AudioFrame](capacity, policy)
	if err != nil {
		return nil, err
	}
	return &edge{ring: ring, bell: make(chan struct{}, 1)}, nil
}

// push offers a frame and rings the doorbell. Under drop-oldest it never
// blocks — this is what keeps the real-time producer (socket reader, TTS
// pump) decoupled from a slow consumer.
func (e *edge) push(f adapter.AudioFrame) bool {
	if !e.ring.Push(f) {
		return false
	}
	select {
	case e.bell <- struct{}{}:
	default: // a wakeup is already pending
	}
	return true
}

// pop takes the oldest frame without blocking.
func (e *edge) pop() (adapter.AudioFrame, bool) {
	return e.ring.Pop()
}

// await blocks until a frame is available or ctx is cancelled. The recheck
// loop handles spurious doorbell wakeups (e.g. after a drain).
func (e *edge) await(ctx context.Context) (adapter.AudioFrame, error) {
	for {
		if f, ok := e.ring.Pop(); ok {
			return f, nil
		}
		select {
		case <-e.bell:
		case <-ctx.Done():
			return adapter.AudioFrame{}, ctx.Err()
		}
	}
}

// drain discards everything buffered and reports how many frames went. It is
// safe to call concurrently with the regular consumer: pops are CAS-claimed
// per slot (the same property that makes producer-side eviction safe).
func (e *edge) drain() int {
	n := 0
	for {
		if _, ok := e.ring.Pop(); !ok {
			return n
		}
		n++
	}
}

func (e *edge) dropped() uint64 { return e.ring.Dropped() }
