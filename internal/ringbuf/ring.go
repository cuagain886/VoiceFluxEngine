package ringbuf

import (
	"fmt"
	"sync/atomic"
)

// Policy selects what Push does when the ring is full.
type Policy uint8

const (
	// Reject refuses the new element: Push returns false, feeding
	// backpressure to the producer. Used where data must not be lost.
	Reject Policy = iota
	// DropOldest evicts the oldest element to make room and increments the
	// drop counter. Used on real-time audio edges where freshness beats
	// completeness: a stale frame is worth less than the latest one.
	DropOldest
)

// String implements fmt.Stringer.
func (p Policy) String() string {
	switch p {
	case Reject:
		return "reject"
	case DropOldest:
		return "drop_oldest"
	default:
		return fmt.Sprintf("policy(%d)", uint8(p))
	}
}

// ParsePolicy maps a config string ("reject" / "drop_oldest") to a Policy.
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "reject":
		return Reject, nil
	case "drop_oldest":
		return DropOldest, nil
	default:
		return 0, fmt.Errorf("ringbuf: unknown policy %q (want \"reject\" or \"drop_oldest\")", s)
	}
}

// slot pairs an element with its sequence gate. seq encodes the slot's state
// relative to the cursors (capacity = cap, position = i within a lap):
//
//	seq == writeCursor   -> free, producer may fill it
//	seq == writeCursor+1 -> published, consumer may take it
//	seq == readCursor+cap -> consumed, free again next lap
//
// All data accesses to val are ordered through seq (and the head CAS), which
// is what makes the buffer race-free even when the producer evicts.
type slot[T any] struct {
	seq atomic.Uint64
	val T
}

// Ring is a bounded lock-free queue for one producer goroutine and one
// consumer goroutine (SPSC). Capacity is a power of two so positions reduce
// to an AND with a mask. Slots are pre-allocated; Push/Pop copy values in and
// out and perform no heap allocation in steady state.
//
// Why per-slot sequences instead of the classic two-counter SPSC ring: the
// DropOldest policy lets the *producer* evict the oldest element, so the read
// cursor has two writers (consumer, and producer-on-drop). With plain
// cursors the consumer could be mid-copy of the very slot the producer is
// overwriting — a torn read and a data race. Gating every slot access behind
// its own atomic sequence (Vyukov's bounded-queue scheme) and CAS-advancing
// head makes eviction safe: whoever wins the CAS owns the slot exclusively
// until it releases it by bumping seq.
type Ring[T any] struct {
	mask   uint64 // capacity - 1; read-only after New
	policy Policy
	slots  []slot[T]

	// Cursors grow monotonically and are reduced mod capacity via mask.
	// Each sits on its own cache line: tail is written ~every Push and head
	// ~every Pop, by different cores; sharing a line would ping-pong it
	// (false sharing). The leading pad also keeps tail off the read-only
	// header fields above, which both sides load constantly.
	_       [64]byte
	tail    atomic.Uint64 // next write position; advanced only by the producer
	_       [64]byte
	head    atomic.Uint64 // next read position; CAS-advanced by consumer and by producer drops
	_       [64]byte
	dropped atomic.Uint64 // elements evicted by DropOldest
	_       [64]byte
}

// New returns a ring with the given capacity (a positive power of two) and
// full-buffer policy. All slots are allocated up front.
func New[T any](capacity int, policy Policy) (*Ring[T], error) {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("ringbuf: capacity must be a positive power of two, got %d", capacity)
	}
	r := &Ring[T]{
		mask:   uint64(capacity - 1),
		policy: policy,
		slots:  make([]slot[T], capacity),
	}
	for i := range r.slots {
		r.slots[i].seq.Store(uint64(i))
	}
	return r, nil
}

// Push offers v to the ring. It must be called only from the producer
// goroutine. Under Reject it returns false when the ring is full; under
// DropOldest it always returns true, evicting the oldest element (and
// incrementing Dropped) when necessary.
func (r *Ring[T]) Push(v T) bool {
	if r.tryPush(v) {
		return true
	}
	if r.policy == Reject {
		return false
	}
	for {
		if _, ok := r.pop(); ok {
			r.dropped.Add(1)
		}
		// If pop failed the consumer freed the slot concurrently; either
		// way there is room now or in a few instructions.
		if r.tryPush(v) {
			return true
		}
	}
}

func (r *Ring[T]) tryPush(v T) bool {
	t := r.tail.Load()
	s := &r.slots[t&r.mask]
	if s.seq.Load() != t {
		return false // slot from last lap not consumed yet: full
	}
	s.val = v
	s.seq.Store(t + 1) // publish: consumer may now take it
	r.tail.Store(t + 1)
	return true
}

// Pop removes and returns the oldest element. It must be called only from
// the consumer goroutine. It returns ok=false when the ring is empty — it
// never blocks and never returns torn data.
func (r *Ring[T]) Pop() (T, bool) {
	return r.pop()
}

// pop is shared by the consumer (Pop) and the producer (DropOldest eviction),
// hence the CAS on head.
func (r *Ring[T]) pop() (T, bool) {
	var zero T
	for {
		h := r.head.Load()
		s := &r.slots[h&r.mask]
		diff := int64(s.seq.Load() - (h + 1))
		if diff < 0 {
			return zero, false // not yet published: empty
		}
		if diff > 0 {
			continue // head moved under us (eviction race): reload
		}
		if r.head.CompareAndSwap(h, h+1) {
			v := s.val
			s.val = zero // drop references so the GC isn't held hostage
			s.seq.Store(h + uint64(len(r.slots)))
			return v, true
		}
	}
}

// Len reports how many elements are buffered. It is a racy snapshot — exact
// only when producer and consumer are quiescent — intended for metrics.
func (r *Ring[T]) Len() int {
	t, h := r.tail.Load(), r.head.Load()
	if t < h {
		return 0
	}
	if n := int(t - h); n <= len(r.slots) {
		return n
	}
	return len(r.slots)
}

// Cap returns the fixed capacity.
func (r *Ring[T]) Cap() int { return len(r.slots) }

// Dropped returns how many elements DropOldest has evicted since New.
func (r *Ring[T]) Dropped() uint64 { return r.dropped.Load() }
