package loadgen

import (
	"math/rand/v2"
	"time"
)

// Netem 配置上行帧的到达时序扰动，是本 harness（也得在 Windows 上跑）对
// Linux `tc netem` 的跨平台等价物。它只建模规格要求建模的东西、绝不多做：
// WS/TCP 之上，链路损伤到达应用层时表现为*被延迟、突发的到达*，绝非乱序或
// 丢失。因此整形器只移动发出时刻，且严格保序（队头阻塞语义：一个被延迟的
// 帧把它后面每一帧都顶住，这正是 TCP 重传停顿的表现形式）。
//
// 在 Linux 上同样的效果可以改在套接字之下注入：
//
//	tc qdisc add dev lo root netem delay 40ms 20ms loss 1%
type Netem struct {
	Delay  time.Duration // 每帧的基础额外延迟
	Jitter time.Duration // [0, Jitter) 内均匀分布的额外随机延迟
	// BurstEvery/BurstHold 模拟周期性停顿（如重传或 wifi 争用）：在每个
	// BurstEvery 窗口内，本应在前 BurstHold 内发出的帧被顶到该 hold 末尾，
	// 然后一起放出——接收侧看到的是「先静默后突发」。
	BurstEvery time.Duration
	BurstHold  time.Duration
	Seed       uint64
}

func (n Netem) enabled() bool {
	return n.Delay > 0 || n.Jitter > 0 || (n.BurstEvery > 0 && n.BurstHold > 0)
}

// shaper 把理想发出时刻变成扰动后的、保序的放行时刻。它不是并发安全的；
// 每个虚拟会话各自拥有一个。
type shaper struct {
	n           Netem
	rng         *rand.Rand
	epoch       time.Time // 突发窗口以第一帧为相位起点
	lastRelease time.Time
}

func newShaper(n Netem) *shaper {
	return &shaper{n: n, rng: rand.New(rand.NewPCG(n.Seed, n.Seed^0x9e3779b97f4a7c15))}
}

// release 把一个理想发出时刻映射成整形后的时刻。单调是构造出来的：第 i 帧
// 上的一个尖峰会延迟其后每一帧，直到时间表追上（突发交付），镜像 TCP 的
// 队头阻塞。
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

// clock 把一个会话的上行按固定理想网格定速（每 interval 一帧），可选地经
// Netem 整形。发送时隙绝不漂移：若调用方落后了（或一次突发 hold 释放得晚），
// 后续的帧会背靠背地发出，直到追上网格——就像真实套接字在冲刷积压。
type clock struct {
	interval  time.Duration
	next      time.Time // 下一帧的理想时刻
	lastIdeal time.Time // 最近发出那一帧的理想时刻
	sh        *shaper   // 无扰动时为 nil
}

func newClock(interval time.Duration, n Netem) *clock {
	c := &clock{interval: interval}
	if n.enabled() {
		c.sh = newShaper(n)
	}
	return c
}

// wait 阻塞到下一帧的（整形后）发出时隙，然后推进网格。若 ctx 先结束则返回
// false。
//
// 调用后 lastIdeal 持有该时隙的*理想*时刻——即模拟用户产出这一帧的时刻，
// 在任何 netem 整形或发送积压之前。因此从它起算的延迟会包含上行扰动与
// 客户端排队，正如真实用户会感受到的那样。
func (c *clock) wait(ctx ctxDone) bool {
	if c.next.IsZero() {
		c.next = time.Now()
	}
	at := c.next
	if c.sh != nil {
		at = c.sh.release(c.next)
	}
	c.lastIdeal = c.next
	c.next = c.next.Add(c.interval)
	return sleepUntil(ctx, at)
}

// peekIdeal 返回下一次 wait() 将发出那一帧的理想时刻。
func (c *clock) peekIdeal() time.Time {
	if c.next.IsZero() {
		return time.Now()
	}
	return c.next
}

// ctxDone 是定速辅助函数所需的 context.Context 的最小切面；测试可以伪造它，
// 不必构造真实的 context。
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
