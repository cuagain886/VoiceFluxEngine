package vad

import (
	"fmt"
	"sync"
)

// State is the session conversation state.
type State uint8

const (
	Listening State = iota
	SpeakingUser
	Thinking
	RespondingAgent
)

// String implements fmt.Stringer.
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

// Action is what a transition asks the pipeline to do.
type Action uint8

const (
	ActNone Action = iota
	// ActEndUtterance commits the user's utterance to the ASR -> turn path.
	ActEndUtterance
	// ActCancelTurn cancels the in-flight (or stale just-started) response
	// sub-chain and flushes in-flight downlink audio: barge-in.
	ActCancelTurn
)

// Machine is the explicit conversation state machine. Events arrive from two
// goroutines — speech events from the ingress reader (inline VAD), response
// lifecycle from the orchestrator — so Apply is mutex-guarded; at one call
// per audio frame plus a few per turn, contention is irrelevant.
//
// Transition table (— = rejected as illegal, state unchanged):
//
//	state \ event     SpeechStart            SpeechEnd        ResponseStarted       ResponseDone
//	LISTENING         →SPEAKING_USER         —                →RESPONDING           no-op
//	SPEAKING_USER     —                      →THINKING (end)  stay (cancel: stale)  no-op
//	THINKING          →SPEAKING_USER (cancel) —               →RESPONDING           →LISTENING
//	RESPONDING        →SPEAKING_USER (cancel) —               —                     →LISTENING
//
// Notes on the deliberate cells:
//   - RESPONDING + SpeechStart is THE barge-in: cancel and listen to the user.
//   - THINKING + SpeechStart: the user resumed before the answer started;
//     cancel whatever is pending so a stale reply doesn't talk over them.
//   - SPEAKING_USER + ResponseStarted: a queued turn for an *old* utterance
//     fired while the user is already talking — by definition stale, cancel
//     it immediately rather than letting the agent interrupt the user.
//   - ResponseDone in LISTENING/SPEAKING_USER is a no-op, not an error: after
//     a barge-in cancel, the turn's done event arrives once the machine has
//     already moved on.
type Machine struct {
	mu       sync.Mutex
	state    State
	illegal  uint64
	OnIllegal func(state State, ev Event) // optional observer; called outside hot decisions, must not block
}

// NewMachine starts in LISTENING.
func NewMachine() *Machine { return &Machine{state: Listening} }

// State returns the current state.
func (m *Machine) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Illegal returns how many events were rejected as undefined transitions.
func (m *Machine) Illegal() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.illegal
}

// Apply runs one event through the table, returning the action the caller
// must execute. Undefined transitions leave the state unchanged, count as
// illegal, and return an error.
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
			c = cell{SpeakingUser, ActCancelTurn} // barge-in
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
