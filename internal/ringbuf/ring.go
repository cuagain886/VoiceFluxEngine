package ringbuf

import (
	"fmt"
	"sync/atomic"
)

// Policy 决定环满时 Push 的行为。
type Policy uint8

const (
	// Reject 拒收新元素：Push 返回 false，把背压回传给生产者。
	// 用于「数据不可丢」的场景。
	Reject Policy = iota
	// DropOldest 驱逐最旧元素腾出空间，并把丢帧计数器加一。用于实时音频
	// 边缘——这里「新鲜」比「完整」重要：一个过时的旧帧远不如最新帧值钱。
	DropOldest
)

// String 实现 fmt.Stringer。
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

// ParsePolicy 把配置字符串（"reject" / "drop_oldest"）映射为 Policy。
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

// slot 把一个元素和它的「序列号闸门」绑在一起。seq 编码了该槽相对游标的
// 状态（容量 = cap，位置 = 本圈内的下标 i）：
//
//	seq == 写游标      -> 空闲，生产者可填入
//	seq == 写游标+1    -> 已发布，消费者可取走
//	seq == 读游标+cap  -> 已消费，下一圈再次空闲
//
// 对 val 的所有数据访问都通过 seq（以及对 head 的 CAS）来排序——正是这一点
// 保证了即便生产者执行驱逐，缓冲区也不会发生数据竞争。
type slot[T any] struct {
	seq atomic.Uint64
	val T
}

// Ring 是一个有界无锁队列，服务于「单生产者 goroutine + 单消费者
// goroutine」（SPSC）。容量取 2 的幂，于是「位置取模」退化为与掩码做一次
// 按位与。槽位预分配；Push/Pop 把值拷进拷出，稳态下零堆分配。
//
// 为什么用「每槽序列号」而不是经典的双计数器 SPSC 环：DropOldest 策略让
// *生产者* 也能驱逐最旧元素，于是读游标有了两个写者（消费者，以及驱逐时的
// 生产者）。若用裸游标，消费者可能正拷贝某个槽时、生产者恰好在覆盖同一个槽
// ——撕裂读 + 数据竞争。把每次槽访问都用各自的原子序列号守起来（Vyukov 的
// 有界队列方案）、并用 CAS 推进 head，就让驱逐变安全了：谁赢得 CAS 谁就
// 独占该槽，直到它通过抬升 seq 释放。
type Ring[T any] struct {
	mask   uint64 // capacity - 1；New 之后只读
	policy Policy
	slots  []slot[T]

	// 游标单调增长，通过 mask 取模到容量范围内。每个游标独占一条 cache
	// line：tail 几乎每次 Push 都写、head 几乎每次 Pop 都写，且由不同的核
	// 执行；若共享一条 line 会让它在核间反复弹跳（伪共享）。前导的填充也
	// 把 tail 与上方那些只读的头部字段隔开——而那些字段两侧都在频繁读取。
	_       [64]byte
	tail    atomic.Uint64 // 下一个写位置；只由生产者推进
	_       [64]byte
	head    atomic.Uint64 // 下一个读位置；由消费者、以及驱逐时的生产者 CAS 推进
	_       [64]byte
	dropped atomic.Uint64 // 被 DropOldest 驱逐的元素数
	_       [64]byte
}

// New 返回一个具有给定容量（正的 2 的幂）与满缓冲策略的环。所有槽位预先分配。
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

// Push 把 v 投入环中。只能从生产者 goroutine 调用。Reject 策略下环满时返回
// false；DropOldest 策略下永远返回 true，必要时驱逐最旧元素（并递增 Dropped）。
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
		// 若 pop 失败，说明消费者并发地把槽腾空了；无论哪种情况，现在或
		// 几条指令之后就会有空位。
		if r.tryPush(v) {
			return true
		}
	}
}

func (r *Ring[T]) tryPush(v T) bool {
	t := r.tail.Load()
	s := &r.slots[t&r.mask]
	if s.seq.Load() != t {
		return false // 上一圈的槽还没被消费：满了
	}
	s.val = v
	s.seq.Store(t + 1) // 发布：消费者现在可以取走它
	r.tail.Store(t + 1)
	return true
}

// Pop 取出并返回最旧的元素。只能从消费者 goroutine 调用。环空时返回
// ok=false——它从不阻塞，也从不返回撕裂的数据。
func (r *Ring[T]) Pop() (T, bool) {
	return r.pop()
}

// pop 被消费者（Pop）和生产者（DropOldest 驱逐）共用，所以 head 上要用 CAS。
func (r *Ring[T]) pop() (T, bool) {
	var zero T
	for {
		h := r.head.Load()
		s := &r.slots[h&r.mask]
		diff := int64(s.seq.Load() - (h + 1))
		if diff < 0 {
			return zero, false // 尚未发布：空
		}
		if diff > 0 {
			continue // head 在我们脚下被移动了（驱逐竞争）：重新加载
		}
		if r.head.CompareAndSwap(h, h+1) {
			v := s.val
			s.val = zero // 抹掉引用，免得拖住 GC
			s.seq.Store(h + uint64(len(r.slots)))
			return v, true
		}
	}
}

// Len 报告当前缓冲了多少元素。这是一个有竞争的快照——只有生产者和消费者都
// 静止时才精确——仅供指标用途。
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

// Cap 返回固定容量。
func (r *Ring[T]) Cap() int { return len(r.slots) }

// Dropped 返回自 New 以来 DropOldest 驱逐了多少元素。
func (r *Ring[T]) Dropped() uint64 { return r.dropped.Load() }
