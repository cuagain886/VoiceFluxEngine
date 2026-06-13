package vad

import (
	"fmt"
	"sync"
)

// State 是会话的对话状态。
type State uint8

const (
	Listening State = iota
	SpeakingUser
	Thinking
	RespondingAgent
)

// String 实现 fmt.Stringer。
func (s State) String() string {
	switch s {
	case Listening:
		return "LISTENING"
	case SpeakingUser:
		return "SPEAKING_USER"
	case Thinking:
		return "THINKING"
	case RespondingAgent:
		return "RESPONDING_AGENT"
	default:
		return "STATE(?)"
	}
}

// Action 是一次迁移要求流水线执行的动作。
type Action uint8

const (
	ActNone Action = iota
	// ActEndUtterance 把用户这句话提交到 ASR -> 轮 的通路。
	ActEndUtterance
	// ActCancelTurn 取消正在进行的（或刚启动就过时的）响应子链，并清空在途
	// 下行音频：这就是打断（barge-in）。
	ActCancelTurn
)

// Machine 是显式的对话状态机。事件来自两个 goroutine——语音事件来自入口读
// goroutine（内联 VAD），响应生命周期来自编排器——所以 Apply 用互斥量守护；
// 频率不过是「每音频帧一次 + 每轮几次」，锁竞争可忽略。
//
// 迁移表（— = 作为非法迁移拒绝，状态不变）：
//
//	状态 \ 事件        SpeechStart            SpeechEnd        ResponseStarted       ResponseDone
//	LISTENING         →SPEAKING_USER         —                →RESPONDING           no-op
//	SPEAKING_USER     —                      →THINKING (结束) 留在原地(取消:过时)  no-op
//	THINKING          →SPEAKING_USER (取消)  —                →RESPONDING           →LISTENING
//	RESPONDING        →SPEAKING_USER (取消)  —                —                     →LISTENING
//
// 几个「刻意设计」的格子说明：
//   - RESPONDING + SpeechStart 就是打断本体：取消，转去听用户。
//   - THINKING + SpeechStart：答案还没开口用户就又说话了；取消一切待发的
//     东西，免得一个过时的回复盖着用户说话。
//   - SPEAKING_USER + ResponseStarted：一个排队中、属于*旧*语句的轮在用户
//     已经在说话时触发了——按定义就是过时的，立即取消，而不是让 Agent 打断
//     用户。
//   - LISTENING/SPEAKING_USER 下的 ResponseDone 是 no-op 而非错误：打断取消
//     之后，轮的 done 事件会在状态机已经往前走了之后才到达。
type Machine struct {
	mu        sync.Mutex
	state     State
	illegal   uint64
	OnIllegal func(state State, ev Event) // 可选观察者；在热决策之外调用，不可阻塞
}

// NewMachine 从 LISTENING 起步。
func NewMachine() *Machine { return &Machine{state: Listening} }

// State 返回当前状态。
func (m *Machine) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Illegal 返回有多少事件作为「未定义迁移」被拒绝。
func (m *Machine) Illegal() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.illegal
}

// Apply 让一个事件走一遍迁移表，返回调用方必须执行的动作。未定义的迁移让
// 状态保持不变、计为非法、并返回错误。
func (m *Machine) Apply(ev Event) (Action, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	type cell struct {
		next State
		act  Action
	}
	var c cell
	legal := true
	switch m.state {
	case Listening:
		switch ev {
		case SpeechStart:
			c = cell{SpeakingUser, ActNone}
		case ResponseStarted:
			c = cell{RespondingAgent, ActNone}
		case ResponseDone:
			c = cell{Listening, ActNone}
		default:
			legal = false
		}
	case SpeakingUser:
		switch ev {
		case SpeechEnd:
			c = cell{Thinking, ActEndUtterance}
		case ResponseStarted:
			c = cell{SpeakingUser, ActCancelTurn}
		case ResponseDone:
			c = cell{SpeakingUser, ActNone}
		default:
			legal = false
		}
	case Thinking:
		switch ev {
		case SpeechStart:
			c = cell{SpeakingUser, ActCancelTurn}
		case ResponseStarted:
			c = cell{RespondingAgent, ActNone}
		case ResponseDone:
			c = cell{Listening, ActNone}
		default:
			legal = false
		}
	case RespondingAgent:
		switch ev {
		case SpeechStart:
			c = cell{SpeakingUser, ActCancelTurn} // 打断
		case ResponseDone:
			c = cell{Listening, ActNone}
		default:
			legal = false
		}
	}

	if !legal {
		m.illegal++
		state := m.state
		if m.OnIllegal != nil {
			m.OnIllegal(state, ev)
		}
		return ActNone, fmt.Errorf("vad: illegal event %s in state %s", ev, state)
	}
	m.state = c.next
	return c.act, nil
}
