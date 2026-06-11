package loadgen

import (
	"context"
	"testing"
	"time"
)

var t0 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func TestShaperBaseDelay(t *testing.T) {
	s := newShaper(Netem{Delay: 10 * time.Millisecond})
	for i := 0; i < 5; i++ {
		ideal := t0.Add(time.Duration(i) * 20 * time.Millisecond)
		got := s.release(ideal)
		want := ideal.Add(10 * time.Millisecond)
		if !got.Equal(want) {
			t.Fatalf("frame %d: release = %v, want %v", i, got, want)
		}
	}
}

func TestShaperJitterBoundedAndOrderPreserving(t *testing.T) {
	s := newShaper(Netem{Delay: 5 * time.Millisecond, Jitter: 30 * time.Millisecond, Seed: 7})
	var prev time.Time
	for i := 0; i < 1000; i++ {
		ideal := t0.Add(time.Duration(i) * 20 * time.Millisecond)
		got := s.release(ideal)
		// Never earlier than base delay; order strictly preserved even when a
		// later frame draws less jitter (TCP HOL: it queues behind).
		if got.Before(ideal.Add(5 * time.Millisecond)) {
			t.Fatalf("frame %d released before base delay: %v", i, got)
		}
		if got.After(ideal.Add(5*time.Millisecond + 30*time.Millisecond)) {
			// A frame can release later than its own jitter only by queuing
			// behind a predecessor — which is bounded by the same maximum.
			if got.After(prev) {
				t.Fatalf("frame %d exceeds jitter bound without HOL cause: %v", i, got)
			}
		}
		if got.Before(prev) {
			t.Fatalf("frame %d reordered: %v before %v", i, got, prev)
		}
		prev = got
	}
}

func TestShaperBurstHoldsThenReleasesTogether(t *testing.T) {
	s := newShaper(Netem{BurstEvery: 100 * time.Millisecond, BurstHold: 40 * time.Millisecond})
	// Frames ideally due inside the hold span all release at hold end.
	holdEnd := t0.Add(40 * time.Millisecond)
	for _, off := range []time.Duration{0, 20 * time.Millisecond, 39 * time.Millisecond} {
		if got := s.release(t0.Add(off)); !got.Equal(holdEnd) {
			t.Fatalf("frame at +%v: release = %v, want %v (burst hold)", off, got, holdEnd)
		}
	}
	// Frames after the hold pass through untouched.
	if got := s.release(t0.Add(60 * time.Millisecond)); !got.Equal(t0.Add(60 * time.Millisecond)) {
		t.Fatalf("post-hold frame perturbed: %v", got)
	}
	// The next window holds again.
	if got := s.release(t0.Add(110 * time.Millisecond)); !got.Equal(t0.Add(140 * time.Millisecond)) {
		t.Fatalf("second window frame: release = %v, want %v", got, t0.Add(140*time.Millisecond))
	}
}

func TestClockPacesAndHonorsCancel(t *testing.T) {
	c := newClock(time.Millisecond, Netem{})
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 5; i++ {
		if !c.wait(ctx) {
			t.Fatal("wait returned false with live ctx")
		}
	}
	if elapsed := time.Since(start); elapsed < 3*time.Millisecond {
		t.Fatalf("5 slots at 1ms took only %v — not pacing", elapsed)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	c2 := newClock(time.Hour, Netem{})
	if c2.wait(cancelled) {
		// First slot fires immediately by design (next initialized to now).
		if c2.wait(cancelled) {
			t.Fatal("wait did not observe cancelled ctx on a future slot")
		}
	}
}
