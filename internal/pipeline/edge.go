package pipeline

import (
	"context"

	"voicestream/internal/adapter"
	"voicestream/internal/ringbuf"
)

// edge 是流水线的一条音频边界：一个无锁环 + 一个容量为 1 的「门铃」。环让
// 生产者不阻塞（负载下 drop-oldest）；门铃让消费者可以停车休眠而不是轮询
// ——每次 push 后做一次非阻塞发送，最多唤醒一个等待者，而「电平触发」的
// 令牌语义保证了「环空」到「停车」之间发生的 push 永不漏过。
type edge struct {
	ring *ringbuf.Ring[adapter.AudioFrame]
	bell chan struct{}
}

func newEdge(capacity int, policy ringbuf.Policy) (*edge, error) {
	ring, err := ringbuf.New[adapter.AudioFrame](capacity, policy)
	if err != nil {
		return nil, err
	}
	return &edge{ring: ring, bell: make(chan struct{}, 1)}, nil
}

// push 投入一帧并按响门铃。drop-oldest 策略下它从不阻塞——正是这一点让实时
// 生产者（套接字读取者、TTS 泵）与慢消费者解耦。
func (e *edge) push(f adapter.AudioFrame) bool {
	if !e.ring.Push(f) {
		return false
	}
	select {
	case e.bell <- struct{}{}:
	default: // 已经有一个唤醒在等着了
	}
	return true
}

// pop 取出最旧的帧，不阻塞。
func (e *edge) pop() (adapter.AudioFrame, bool) {
	return e.ring.Pop()
}

// await 阻塞直到有帧可用或 ctx 被取消。重检循环用来处理门铃的虚假唤醒
// （例如一次 drain 之后）。
func (e *edge) await(ctx context.Context) (adapter.AudioFrame, error) {
	for {
		if f, ok := e.ring.Pop(); ok {
			return f, nil
		}
		select {
		case <-e.bell:
		case <-ctx.Done():
			return adapter.AudioFrame{}, ctx.Err()
		}
	}
}

// drain 丢弃所有已缓冲的帧并报告丢了多少。它可以与常规消费者并发调用是安全
// 的：pop 是逐槽 CAS 认领的（正是这条性质让生产者侧的驱逐也安全）。
func (e *edge) drain() int {
	n := 0
	for {
		if _, ok := e.ring.Pop(); !ok {
			return n
		}
		n++
	}
}

func (e *edge) dropped() uint64 { return e.ring.Dropped() }
