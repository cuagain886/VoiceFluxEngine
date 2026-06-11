package ringbuf

import "sync/atomic"

// BufferPool recycles fixed-capacity byte buffers between the two ends of an
// audio edge so the steady-state hot path performs zero heap allocation: the
// producer Gets a buffer, fills it and pushes it through the data ring; the
// consumer pops, processes and Puts the buffer back.
//
// The free list is itself a Ring[[]byte] flowing in the opposite direction of
// the data ring, so the same SPSC discipline applies with the roles swapped:
// the data producer is the pool's only Getter and the data consumer the only
// Putter. sync.Pool is deliberately not used here — Put(&b) would heap-
// allocate the slice header on every cycle, defeating the point.
//
// The pool degrades instead of failing: Get falls back to a fresh allocation
// when every buffer is in flight (counted in Misses), and Put lets excess or
// foreign buffers fall to the GC.
type BufferPool struct {
	free   *Ring[[]byte]
	size   int
	misses atomic.Uint64
}

// NewBufferPool pre-allocates count buffers (count must be a positive power
// of two) of the given byte capacity.
func NewBufferPool(count, size int) (*BufferPool, error) {
	free, err := New[[]byte](count, Reject)
	if err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		free.Push(make([]byte, 0, size))
	}
	return &BufferPool{free: free, size: size}, nil
}

// Get returns an empty buffer with at least the pool's configured capacity.
// If the free list is exhausted it allocates a fresh buffer and counts a miss.
func (p *BufferPool) Get() []byte {
	if b, ok := p.free.Pop(); ok {
		return b
	}
	p.misses.Add(1)
	return make([]byte, 0, p.size)
}

// Put returns a buffer to the free list. Undersized buffers and buffers
// beyond the pool's capacity are dropped for the GC to reclaim.
func (p *BufferPool) Put(b []byte) {
	if cap(b) < p.size {
		return
	}
	p.free.Push(b[:0])
}

// Misses returns how many Gets were served by fresh allocation because every
// pooled buffer was in flight.
func (p *BufferPool) Misses() uint64 { return p.misses.Load() }
