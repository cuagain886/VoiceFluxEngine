package ringbuf

import "sync/atomic"

// BufferPool 在一条音频边缘的两端之间循环复用定容字节缓冲，使稳态热路径
// 零堆分配：生产者 Get 一个缓冲、填充它、推入数据环；消费者 pop 出来、
// 处理完、把缓冲 Put 回来。
//
// 这条空闲链表本身就是一个 Ring[[]byte]，流向与数据环相反——于是同一套
// SPSC 纪律在角色互换后照样成立：数据生产者是池的唯一 Getter，数据消费者
// 是唯一 Putter。这里刻意不用 sync.Pool——它的 Put(&b) 每个周期都会在堆上
// 分配一个 slice header，正好抵消了零分配的初衷。
//
// 池是「降级」而非「失败」：当所有缓冲都在飞行中时 Get 退化为一次新分配
// （计入 Misses），Put 则把多余的或外来的缓冲交给 GC 回收。
type BufferPool struct {
	free   *Ring[[]byte]
	size   int
	misses atomic.Uint64
}

// NewBufferPool 预分配 count 个（count 必须是正的 2 的幂）给定字节容量的缓冲。
func NewBufferPool(count, size int) (*BufferPool, error) {
	free, err := New[[]byte](count, Reject)
	if err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		free.Push(make([]byte, 0, size))
	}
	return &BufferPool{free: free, size: size}, nil
}

// Get 返回一个空缓冲，容量至少为池配置的大小。若空闲链表已耗尽，则分配一个
// 新缓冲并计一次 miss。
func (p *BufferPool) Get() []byte {
	if b, ok := p.free.Pop(); ok {
		return b
	}
	p.misses.Add(1)
	return make([]byte, 0, p.size)
}

// Put 把一个缓冲归还空闲链表。容量不足的、或超出池容量的缓冲被丢弃，交给
// GC 回收。
func (p *BufferPool) Put(b []byte) {
	if cap(b) < p.size {
		return
	}
	p.free.Push(b[:0])
}

// Misses 返回有多少次 Get 因所有池化缓冲都在飞行中而由新分配兜底。
func (p *BufferPool) Misses() uint64 { return p.misses.Load() }
