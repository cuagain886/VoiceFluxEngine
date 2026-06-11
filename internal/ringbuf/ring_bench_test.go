package ringbuf

import (
	"sync/atomic"
	"testing"
)

// benchSPSC drives one producer (the benchmark goroutine) against one
// consumer goroutine, measuring the full cross-core handoff cost per element.
func benchSPSC(b *testing.B, push func(int) bool, pop func() (int, bool)) {
	b.ReportAllocs()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; n < b.N; n++ {
			for {
				if _, ok := pop(); ok {
					break
				}
			}
		}
	}()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for !push(n) {
			// spin: ring full
		}
	}
	<-done
}

func BenchmarkSPSCPadded(b *testing.B) {
	r, _ := New[int](1024, Reject)
	benchSPSC(b, r.Push, r.Pop)
}

func BenchmarkSPSCUnpadded(b *testing.B) {
	r := newNopadRing[int](1024)
	benchSPSC(b, r.push, r.pop)
}

// BenchmarkSPSCAudioFrame moves realistic elements: 640-byte PCM frames
// (20ms @ 16kHz/16-bit/mono) recycled through a BufferPool, i.e. the exact
// shape of the audio ingress hot path. The allocs/op column is the
// zero-allocation gate.
func BenchmarkSPSCAudioFrame(b *testing.B) {
	const frameBytes = 640
	r, _ := New[[]byte](1024, Reject)
	pool, _ := NewBufferPool(2048, frameBytes)
	pcm := make([]byte, frameBytes)

	b.ReportAllocs()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; n < b.N; n++ {
			for {
				if buf, ok := r.Pop(); ok {
					pool.Put(buf)
					break
				}
			}
		}
	}()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf := pool.Get()
		buf = append(buf, pcm...)
		for !r.Push(buf) {
			// spin: ring full
		}
	}
	<-done
}

func BenchmarkDropOldestSaturated(b *testing.B) {
	// No consumer: every push past capacity evicts. Measures the worst-case
	// producer cost when the downstream has stalled completely.
	r, _ := New[int](64, DropOldest)
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		r.Push(n)
	}
}

// nopadRing duplicates Ring's algorithm with the cache-line padding stripped
// (cursors packed next to each other), so the padded-vs-unpadded benchmark
// pair quantifies what the padding buys. Test-only; Reject policy only.
type nopadRing[T any] struct {
	mask  uint64
	slots []slot[T]
	tail  atomic.Uint64
	head  atomic.Uint64
}

func newNopadRing[T any](capacity int) *nopadRing[T] {
	r := &nopadRing[T]{mask: uint64(capacity - 1), slots: make([]slot[T], capacity)}
	for i := range r.slots {
		r.slots[i].seq.Store(uint64(i))
	}
	return r
}

func (r *nopadRing[T]) push(v T) bool {
	t := r.tail.Load()
	s := &r.slots[t&r.mask]
	if s.seq.Load() != t {
		return false
	}
	s.val = v
	s.seq.Store(t + 1)
	r.tail.Store(t + 1)
	return true
}

func (r *nopadRing[T]) pop() (T, bool) {
	var zero T
	for {
		h := r.head.Load()
		s := &r.slots[h&r.mask]
		diff := int64(s.seq.Load() - (h + 1))
		if diff < 0 {
			return zero, false
		}
		if diff > 0 {
			continue
		}
		if r.head.CompareAndSwap(h, h+1) {
			v := s.val
			s.val = zero
			s.seq.Store(h + uint64(len(r.slots)))
			return v, true
		}
	}
}
