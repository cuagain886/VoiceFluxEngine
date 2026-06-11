package loadgen

import (
	"math/rand/v2"
	"time"
)

// Netem configures arrival-timing perturbation for uplink frames, the
// portable equivalent of Linux `tc netem` for this harness (which must also
// run on Windows). It models what the spec demands be modeled — and nothing
// more: over WS/TCP, link impairments reach the application as *delayed,
// bursty arrival*, never as reorder or loss. Accordingly the shaper only
// moves departure times, and strictly preserves order (head-of-line
// semantics: a delayed frame holds back every frame behind it, which is
// exactly how a TCP retransmission stall surfaces).
//
// On Linux the same effect can be injected below the socket instead:
//
//	tc qdisc add dev lo root netem delay 40ms 20ms loss 1%
type Netem struct {
	Delay  time.Duration // base extra delay per frame
	Jitter time.Duration // extra uniform random delay in [0, Jitter)
	// BurstEvery/BurstHold emulate periodic stalls (e.g. retransmission or
	// wifi contention): within every BurstEvery window, frames ideally due in
	// the first BurstHold are held to the end of that hold, then released
	// together — the receiver sees a silence followed by a burst.
	BurstEvery time.Duration
	BurstHold  time.Duration
	Seed       uint64
}

func (n Netem) enabled() bool {
	return n.Delay > 0 || n.Jitter > 0 || (n.BurstEvery > 0 && n.BurstHold > 0)
}

// shaper turns ideal departure times into perturbed, order-preserving release
// times. It is not safe for concurrent use; each virtual session owns one.
type shaper struct {
	n           Netem
	rng         *rand.Rand
	epoch       time.Time // burst windows are phased off the first frame
	lastRelease time.Time
}

func newShaper(n Netem) *shaper {
	return &shaper{n: n, rng: rand.New(rand.NewPCG(n.Seed, n.Seed^0x9e3779b97f4a7c15))}
}

// release maps an ideal departure time to the shaped one. Monotonic by
// construction: a spike on frame i delays every later frame until the
// schedule catches up (burst delivery), mirroring TCP HOL blocking.
func (s *shaper) release(ideal time.Time) time.Time {
	if s.epoch.IsZero() {
		s.epoch = ideal
	}
	d := s.n.Delay
	if s.n.Jitter > 0 {
		d += time.Duration(s.rng.Int64N(int64(s.n.Jitter)))
	}
	if s.n.BurstEvery > 0 && s.n.BurstHold > 0 {
		phase := ideal.Sub(s.epoch) % s.n.BurstEvery
		if phase < s.n.BurstHold {
			d += s.n.BurstHold - phase
		}
	}
	t := ideal.Add(d)
	if t.Before(s.lastRelease) {
		t = s.lastRelease
	}
	s.lastRelease = t
	return t
}

// clock paces one session's uplink on a fixed ideal grid (one frame per
// interval), optionally shaped by a Netem. Send slots never drift: if the
// caller falls behind (or a burst hold releases late), subsequent frames go
// out back-to-back until the grid is caught up — like a real socket flushing
// a backlog.
type clock struct {
	interval time.Duration
	next     time.Time // ideal time of the next frame
	sh       *shaper   // nil when no perturbation
}

func newClock(interval time.Duration, n Netem) *clock {
	c := &clock{interval: interval}
	if n.enabled() {
		c.sh = newShaper(n)
	}
	return c
}

// wait blocks until the next frame's (shaped) departure slot, then advances
// the grid. Returns false if ctx ended first.
func (c *clock) wait(ctx ctxDone) bool {
	if c.next.IsZero() {
		c.next = time.Now()
	}
	at := c.next
	if c.sh != nil {
		at = c.sh.release(c.next)
	}
	c.next = c.next.Add(c.interval)
	return sleepUntil(ctx, at)
}

// ctxDone is the slice of context.Context the pacing helpers need; tests can
// fake it without building real contexts.
type ctxDone interface{ Done() <-chan struct{} }

func sleepUntil(ctx ctxDone, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
