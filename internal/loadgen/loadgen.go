// Package loadgen 是 M10 的负载 harness：它对一个运行中的 voicestream 服务器
// ramp 并发的虚拟会话，驱动真实热路径（真帧穿过传输、去重、VAD、环、背压
// ——模型走 mock，遵循设计 D9），扰动上行到达时序（netem 等价物），并采集
// 那条负载/延迟曲线——其拐点与降级行为正是 L2 的交付物。
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

// Config 参数化一次 ramp 运行。零值会由 normalize() 取得合理默认；URL 必填。
type Config struct {
	URL        string // ws://host:port/ws
	MetricsURL string // http://host:port/metrics；"" 则跳过服务端抓取

	Steps        []int         // 并发阶梯，如 2,5,10,20,40,80
	StepDuration time.Duration // 每步的测量窗口
	Warmup       time.Duration // 扩容到测量之间的沉降时间

	FrameInterval time.Duration // 墙钟上行定速（20ms = 实时）
	FrameDuration time.Duration // 每帧名义 PTS 步长（20ms）
	SampleRate    int           // PCM 采样率（16000）
	SpeechFrames  int           // 每句话的帧数（30 = 名义 600ms）

	BargeEvery  int           // 每第 N 轮打断响应；0 = 从不
	BargeDelay  time.Duration // 打断前先骑着响应多久
	QuietGap    time.Duration // 结束一轮的下行静默时长
	TurnTimeout time.Duration // 每阶段截止时间；超过即记一次错误

	Netem Netem // 上行到达时序扰动

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

// rampStagger 把一步里新建的连接（以及它们的轮相位）摊开在整个 warmup 窗口
// 上。一举两得：服务器永远不会遭遇一个真实到达过程不会产生的「惊群」；并且
// ——因为 mock 驱动的轮墙钟时间几乎相同——同时入场的 worker 否则会永久
// 相位锁定，以同步的波峰反复砸服务器，而那种波峰需求对稳态负载毫无意义。
func (c *Config) rampStagger() time.Duration {
	if c.Warmup < 100*time.Millisecond {
		return c.Warmup/2 + time.Millisecond
	}
	return c.Warmup
}

type sampleKind int

const (
	sampleFirst sampleKind = iota // 最后一帧说话发出 -> 第一帧下行音频
	sampleBarge                   // 第一帧打断发出 -> 收到 BARGE_IN
)

// collector 聚合「当前正在测量的那一步」的客户端侧观测。多个 worker
// goroutine 一起向它喂数据。
type collector struct {
	framesSent atomic.Uint64
	live       atomic.Int64

	mu        sync.Mutex
	measuring bool
	first     []float64 // 秒
	barge     []float64
	turns     int
	errs      int
	log       *slog.Logger
}

func (c *collector) workerSpawn() { c.live.Add(1) }
func (c *collector) workerExit()  { c.live.Add(-1) }

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

// beginWindow 划定一个测量区间的开始。
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

// Run 执行 ramp：每一步把 worker 池扩容、预热、测量 StepDuration（客户端
// 样本 + 服务端 /metrics 差分），产出一条 StepRecord。worker 跨步存活；
// 池只增不减。
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
