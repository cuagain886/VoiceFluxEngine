package ringbuf

// The channels-vs-ring comparison (task 11.1). Each primitive is used the
// way you actually would: channels block, the ring spins. Scenarios mirror
// the ring benchmarks element-for-element so the only variable is the hop
// primitive itself:
//
//	SPSC int          raw cross-core handoff throughput
//	SPSC audio frame  640-byte PCM recycled through the same BufferPool
//	saturated drop    full buffer, no consumer: evict-oldest semantics
//	ping-pong         round-trip handoff latency, both directions
//
// The saturated-drop pair is the semantic heart of it: the ring does
// drop-oldest natively (one CAS), a channel has to emulate it with a
// select dance that is not atomic — under concurrency the evict and the
// retry can interleave with the consumer, which is exactly the race the
// ring's per-slot sequences close by construction (see M3 design doc).

import "testing"

func BenchmarkChanSPSC(b *testing.B) {
	ch := make(chan int, 1024)
	benchSPSC(b,
		func(v int) bool { ch <- v; return true },
		func() (int, bool) { return <-ch, true },
	)
}

// BenchmarkChanSPSCAudioFrame mirrors BenchmarkSPSCAudioFrame with the hop
// swapped: same 640-byte frames, same BufferPool free list, so the delta
// isolates the hop primitive.
func BenchmarkChanSPSCAudioFrame(b *testing.B) {
	const frameBytes = 640
	ch := make(chan []byte, 1024)
	pool, _ := NewBufferPool(2048, frameBytes)
	pcm := make([]byte, frameBytes)

	b.ReportAllocs()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; n < b.N; n++ {
			pool.Put(<-ch)
		}
	}()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf := pool.Get()
		buf = append(buf, pcm...)
		ch <- buf
	}
	<-done
}

// BenchmarkChanDropOldestSaturated emulates drop-oldest on a full channel
// with no consumer: non-blocking send, on failure evict one and retry. The
// outer loop is required because evict-then-send is not atomic — with a
// live consumer both selects can lose. That non-atomicity is the point of
// comparison, not an implementation slip.
func BenchmarkChanDropOldestSaturated(b *testing.B) {
	ch := make(chan int, 64)
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for {
			select {
			case ch <- n:
			default:
				select {
				case <-ch: // evict oldest
				default:
				}
				continue
			}
			break
		}
	}
}

// Ping-pong pairs: one element bounces between two goroutines, so ns/op is
// one full round trip (two handoffs) — the latency view, where the SPSC
// pairs above are the throughput view.

func BenchmarkPingPongChan(b *testing.B) {
	ping := make(chan int, 1)
	pong := make(chan int, 1)
	go func() {
		for range b.N {
			pong <- <-ping
		}
	}()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ping <- n
		<-pong
	}
}

func BenchmarkPingPongRing(b *testing.B) {
	ping, _ := New[int](2, Reject)
	pong, _ := New[int](2, Reject)
	go func() {
		for range b.N {
			var v int
			for {
				if got, ok := ping.Pop(); ok {
					v = got
					break
				}
			}
			for !pong.Push(v) {
			}
		}
	}()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for !ping.Push(n) {
		}
		for {
			if _, ok := pong.Pop(); ok {
				break
			}
		}
	}
}
