package ringbuf

import (
	"testing"
)

func TestNewRejectsBadCapacity(t *testing.T) {
	for _, c := range []int{0, -8, 3, 6, 100} {
		if _, err := New[int](c, Reject); err == nil {
			t.Errorf("capacity %d: expected error, got nil", c)
		}
	}
	if _, err := New[int](64, Reject); err != nil {
		t.Fatalf("capacity 64: unexpected error: %v", err)
	}
}

func TestPopEmpty(t *testing.T) {
	r, _ := New[int](8, Reject)
	if v, ok := r.Pop(); ok {
		t.Fatalf("Pop on empty ring returned ok=true, v=%d", v)
	}
}

func TestRejectWhenFull(t *testing.T) {
	r, _ := New[int](4, Reject)
	for i := 0; i < 4; i++ {
		if !r.Push(i) {
			t.Fatalf("push %d into non-full ring rejected", i)
		}
	}
	if r.Push(99) {
		t.Fatal("push into full ring accepted under Reject policy")
	}
	if d := r.Dropped(); d != 0 {
		t.Fatalf("Reject policy must not count drops, got %d", d)
	}
	// Backpressure released: one Pop frees one slot.
	if _, ok := r.Pop(); !ok {
		t.Fatal("pop from full ring failed")
	}
	if !r.Push(99) {
		t.Fatal("push after pop rejected")
	}
}

func TestDropOldestDeterministic(t *testing.T) {
	r, _ := New[int](8, DropOldest)
	for i := 0; i < 12; i++ {
		if !r.Push(i) {
			t.Fatalf("DropOldest push %d returned false", i)
		}
	}
	if d := r.Dropped(); d != 4 {
		t.Fatalf("dropped = %d, want 4", d)
	}
	// Oldest four (0..3) evicted; the freshest survive in order.
	for want := 4; want < 12; want++ {
		v, ok := r.Pop()
		if !ok {
			t.Fatalf("pop %d: ring empty early", want)
		}
		if v != want {
			t.Fatalf("pop = %d, want %d", v, want)
		}
	}
	if _, ok := r.Pop(); ok {
		t.Fatal("ring should be empty after draining")
	}
}

// TestConcurrentFIFO runs a real producer/consumer pair under -race: with
// Reject + retry the stream is lossless, so the consumer must observe the
// exact input sequence.
func TestConcurrentFIFO(t *testing.T) {
	const n = 100_000
	r, _ := New[int](64, Reject)

	go func() {
		for i := 0; i < n; i++ {
			for !r.Push(i) {
				// spin: backpressure
			}
		}
	}()

	for want := 0; want < n; want++ {
		for {
			v, ok := r.Pop()
			if !ok {
				continue
			}
			if v != want {
				t.Fatalf("out of order: got %d, want %d", v, want)
			}
			break
		}
	}
}

// TestConcurrentDropOldest exercises the producer-side eviction path (the
// head-CAS race) under -race. Order must stay strictly increasing and every
// pushed element must be accounted for as either received or dropped.
func TestConcurrentDropOldest(t *testing.T) {
	const n = 100_000
	r, _ := New[int](8, DropOldest)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			r.Push(i)
		}
	}()

	received := 0
	prev := -1
	drained := false
	for !drained {
		v, ok := r.Pop()
		if ok {
			if v <= prev {
				t.Fatalf("order violated: got %d after %d", v, prev)
			}
			prev = v
			received++
			continue
		}
		select {
		case <-done:
			// Producer finished; one final drain pass below.
			for {
				v, ok := r.Pop()
				if !ok {
					drained = true
					break
				}
				if v <= prev {
					t.Fatalf("order violated in drain: got %d after %d", v, prev)
				}
				prev = v
				received++
			}
		default:
		}
	}

	if got := received + int(r.Dropped()); got != n {
		t.Fatalf("accounting: received(%d) + dropped(%d) = %d, want %d",
			received, r.Dropped(), got, n)
	}
}

// TestSteadyStateZeroAlloc is the allocs/op gate from the spec: a full
// pool→ring→pool cycle must not touch the heap once warmed up.
func TestSteadyStateZeroAlloc(t *testing.T) {
	const frameBytes = 640 // 20ms PCM @ 16kHz/16-bit/mono
	r, err := New[[]byte](8, DropOldest)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewBufferPool(8, frameBytes)
	if err != nil {
		t.Fatal(err)
	}
	pcm := make([]byte, frameBytes)

	allocs := testing.AllocsPerRun(1000, func() {
		buf := pool.Get()
		buf = append(buf, pcm...)
		r.Push(buf)
		out, ok := r.Pop()
		if !ok {
			t.Fatal("pop failed")
		}
		pool.Put(out)
	})
	if allocs != 0 {
		t.Fatalf("steady-state allocs/op = %v, want 0", allocs)
	}
	if m := pool.Misses(); m != 0 {
		t.Fatalf("pool misses = %d, want 0", m)
	}
}

func TestLenAndCap(t *testing.T) {
	r, _ := New[int](8, Reject)
	if r.Cap() != 8 {
		t.Fatalf("Cap = %d, want 8", r.Cap())
	}
	if r.Len() != 0 {
		t.Fatalf("Len on empty = %d, want 0", r.Len())
	}
	r.Push(1)
	r.Push(2)
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}
}

func TestParsePolicy(t *testing.T) {
	if p, err := ParsePolicy("reject"); err != nil || p != Reject {
		t.Fatalf("ParsePolicy(reject) = %v, %v", p, err)
	}
	if p, err := ParsePolicy("drop_oldest"); err != nil || p != DropOldest {
		t.Fatalf("ParsePolicy(drop_oldest) = %v, %v", p, err)
	}
	if _, err := ParsePolicy("nonsense"); err == nil {
		t.Fatal("ParsePolicy(nonsense) should fail")
	}
}
