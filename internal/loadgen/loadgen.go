// Package loadgen is the M10 load harness: it ramps concurrent virtual
// sessions against a running voicestream server, drives the real hot path
// (real frames through transport, dedup, VAD, rings, backpressure — models
// mocked, per design D9), perturbs uplink arrival timing (netem-equivalent),
// and collects the load/latency curve whose knee and degradation behaviour
// are the L2 deliverable.
package loadgen

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Config parametrizes one ramp run. Zero values get sensible defaults from
// normalize(); URL is mandatory.
type Config struct {
	URL        string // ws://host:port/ws
	MetricsURL string // http://host:port/metrics; "" skips server-side scraping

	Steps        []int         // concurrency ladder, e.g. 2,5,10,20,40,80
	StepDuration time.Duration // measurement window per step
	Warmup       time.Duration // settle time between scaling up and measuring

	FrameInterval time.Duration // wall-clock uplink pacing (20ms = real time)
	FrameDuration time.Duration // nominal PTS step per frame (20ms)
	SampleRate    int           // PCM sample rate (16000)
	SpeechFrames  int           // frames per utterance (30 = 600ms nominal)

	BargeEvery  int           // every Nth turn interrupts the response; 0 = never
	BargeDelay  time.Duration // how long to ride the response before barging
	QuietGap    time.Duration // downlink silence that ends a turn
	TurnTimeout time.Duration // per-phase deadline; exceeding it is a recorded error

	Netem Netem // uplink arrival-timing perturbation

	Logger *slog.Logger

	speechPCM, silencePCM []byte
}

func (c *Config) normalize() error {
	if c.URL == "" {
		return fmt.Errorf("loadgen: URL is required")
	}
	if len(c.Steps) == 0 {
		c.Steps = []int{1, 5, 10, 20, 40, 80}
	}
	for i, n := range c.Steps {
		if n <= 0 || (i > 0 && n < c.Steps[i-1]) {
			return fmt.Errorf("loadgen: steps must be positive and non-decreasing, got %v", c.Steps)
		}
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.StepDuration, 10*time.Second)
	def(&c.Warmup, 2*time.Second)
	def(&c.FrameInterval, 20*time.Millisecond)
	def(&c.FrameDuration, 20*time.Millisecond)
	def(&c.BargeDelay, 150*time.Millisecond)
	def(&c.QuietGap, 350*time.Millisecond)
	def(&c.TurnTimeout, 10*time.Second)
	if c.SampleRate <= 0 {
		c.SampleRate = 16000
	}
	if c.SpeechFrames <= 0 {
		c.SpeechFrames = 30
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	samples := int(int64(c.SampleRate) * int64(c.FrameDuration) / int64(time.Second))
	c.speechPCM = pcmFrame(samples, 0.5)
	c.silencePCM = pcmFrame(samples, 0)
	return nil
}

func (c *Config) frameNominalUs() int64 { return c.FrameDuration.Microseconds() }

// rampStagger spreads a step's new connections (and so their turn phases)
// across the warmup window. Two birds: the server never sees a thundering
// herd a real arrival process would not have, and — because the mock-driven
// turns all take near-identical wall time — workers spawned in lockstep
// would otherwise stay phase-locked forever, hammering the server in
// synchronized waves whose peak demand says nothing about steady load.
func (c *Config) rampStagger() time.Duration {
	if c.Warmup < 100*time.Millisecond {
		return c.Warmup/2 + time.Millisecond
	}
	return c.Warmup
}

type sampleKind int

const (
	sampleFirst sampleKind = iota // last speech frame sent -> first downlink audio
	sampleBarge                   // first interrupting frame sent -> BARGE_IN received
)

// collector aggregates client-side observations for the step currently being
// measured. Workers feed it from many goroutines.
type collector struct {
	framesSent atomic.Uint64
	live       atomic.Int64

	mu        sync.Mutex
	measuring bool
	first     []float64 // seconds
	barge     []float64
	turns     int
	errs      int
	log       *slog.Logger
}

func (c *collector) workerSpawn() { c.live.Add(1) }
func (c *collector) workerExit() { c.live.Add(-1) }

func (c *collector) sample(k sampleKind, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.measuring {
		return
	}
	switch k {
	case sampleFirst:
		c.first = append(c.first, d.Seconds())
	case sampleBarge:
		c.barge = append(c.barge, d.Seconds())
	}
}

func (c *collector) turnDone() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.measuring {
		c.turns++
	}
}

func (c *collector) fail(phase string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log.Warn("loadgen worker error", "phase", phase, "err", err)
	if c.measuring {
		c.errs++
	}
}

// window bounds one measurement interval.
func (c *collector) beginWindow() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.first, c.barge = nil, nil
	c.turns, c.errs = 0, 0
	c.measuring = true
	return c.framesSent.Load()
}

type windowAgg struct {
	first, barge []float64
	turns, errs  int
	frames       uint64
}

func (c *collector) endWindow(framesAtStart uint64) windowAgg {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.measuring = false
	return windowAgg{
		first: c.first, barge: c.barge,
		turns: c.turns, errs: c.errs,
		frames: c.framesSent.Load() - framesAtStart,
	}
}

// Run executes the ramp: for each step, scale the worker pool up, warm up,
// measure for StepDuration (client samples + server /metrics deltas), and
// emit one StepRecord. Workers persist across steps; the pool only grows.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	col := &collector{log: cfg.Logger}

	wctx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	var wg sync.WaitGroup
	spawned := 0
	spawn := func(n int) {
		for ; spawned < n; spawned++ {
			w := &worker{
				id:  spawned,
				cfg: &cfg,
				col: col,
				rng: rand.New(rand.NewPCG(uint64(spawned), 0xda3e39cb94b95bdb)),
			}
			col.workerSpawn()
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.run(wctx)
			}()
		}
	}

	rep := &Report{Config: describe(cfg), StartedAt: time.Now()}
	for _, n := range cfg.Steps {
		if ctx.Err() != nil {
			break
		}
		spawn(n)
		if !sleepUntil(ctx, time.Now().Add(cfg.Warmup)) {
			break
		}

		var s0 *snapshot
		if cfg.MetricsURL != "" {
			var err error
			if s0, err = scrape(ctx, cfg.MetricsURL); err != nil {
				return nil, fmt.Errorf("loadgen: pre-step scrape: %w", err)
			}
		}
		mark := col.beginWindow()
		t0 := time.Now()
		if !sleepUntil(ctx, t0.Add(cfg.StepDuration)) {
			col.endWindow(mark)
			break
		}
		agg := col.endWindow(mark)
		elapsed := time.Since(t0)

		var s1 *snapshot
		if cfg.MetricsURL != "" {
			var err error
			if s1, err = scrape(ctx, cfg.MetricsURL); err != nil {
				return nil, fmt.Errorf("loadgen: post-step scrape: %w", err)
			}
		}

		rec := buildRecord(n, elapsed, agg, s0, s1, int(col.live.Load()))
		rep.Records = append(rep.Records, rec)
		cfg.Logger.Info("step done",
			"concurrency", n,
			"turns", rec.Turns,
			"first_p99_ms", rec.E2EFirstP99,
			"barge_p99_ms", rec.E2EBargeP99,
			"drop_rate", rec.IngressDropRate,
			"cpu", rec.CPUUtil,
			"errors", rec.Errors,
		)
	}

	stopWorkers()
	wg.Wait()
	rep.FinishedAt = time.Now()
	rep.Analysis = Analyze(rep.Records)
	return rep, ctx.Err()
}

func describe(cfg Config) string {
	d := fmt.Sprintf("steps=%v step=%s frame=%s speech=%d barge_every=%d",
		cfg.Steps, cfg.StepDuration, cfg.FrameInterval, cfg.SpeechFrames, cfg.BargeEvery)
	if cfg.Netem.enabled() {
		d += fmt.Sprintf(" netem{delay=%s jitter=%s burst=%s/%s}",
			cfg.Netem.Delay, cfg.Netem.Jitter, cfg.Netem.BurstHold, cfg.Netem.BurstEvery)
	}
	return d
}
