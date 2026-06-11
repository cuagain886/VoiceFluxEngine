package ringbuf

import "testing"

func TestBufferPoolRecycles(t *testing.T) {
	pool, err := NewBufferPool(4, 640)
	if err != nil {
		t.Fatal(err)
	}
	b := pool.Get()
	if cap(b) < 640 {
		t.Fatalf("Get cap = %d, want >= 640", cap(b))
	}
	if len(b) != 0 {
		t.Fatalf("Get len = %d, want 0", len(b))
	}
	b = append(b, 1, 2, 3)
	pool.Put(b)
	if pool.Misses() != 0 {
		t.Fatalf("misses = %d, want 0", pool.Misses())
	}
}

func TestBufferPoolMissWhenExhausted(t *testing.T) {
	pool, _ := NewBufferPool(2, 64)
	a, b := pool.Get(), pool.Get()
	c := pool.Get() // free list empty -> fresh allocation
	if pool.Misses() != 1 {
		t.Fatalf("misses = %d, want 1", pool.Misses())
	}
	if cap(c) < 64 {
		t.Fatalf("miss Get cap = %d, want >= 64", cap(c))
	}
	pool.Put(a)
	pool.Put(b)
	pool.Put(c) // free list is full again -> silently dropped for GC
}

func TestBufferPoolDropsForeignBuffers(t *testing.T) {
	pool, _ := NewBufferPool(2, 640)
	pool.Get()                       // one in flight
	pool.Put(make([]byte, 0, 16))    // undersized: must not enter the pool
	b := pool.Get()                  // should still be a pooled 640-cap buffer
	if cap(b) < 640 {
		t.Fatalf("pool handed back an undersized buffer, cap = %d", cap(b))
	}
}

func TestBufferPoolRejectsBadCount(t *testing.T) {
	if _, err := NewBufferPool(3, 64); err == nil {
		t.Fatal("expected error for non-power-of-two count")
	}
}
