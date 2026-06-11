package vad

import "testing"

func apply(t *testing.T, m *Machine, ev Event, wantState State, wantAct Action) {
	t.Helper()
	act, err := m.Apply(ev)
	if err != nil {
		t.Fatalf("Apply(%v): %v", ev, err)
	}
	if act != wantAct {
		t.Fatalf("Apply(%v): action = %v, want %v", ev, act, wantAct)
	}
	if got := m.State(); got != wantState {
		t.Fatalf("Apply(%v): state = %v, want %v", ev, got, wantState)
	}
}

func TestNormalConversationCycle(t *testing.T) {
	m := NewMachine()
	apply(t, m, SpeechStart, SpeakingUser, ActNone)
	apply(t, m, SpeechEnd, Thinking, ActEndUtterance)
	apply(t, m, ResponseStarted, RespondingAgent, ActNone)
	apply(t, m, ResponseDone, Listening, ActNone)
}

func TestBargeInTransition(t *testing.T) {
	m := NewMachine()
	apply(t, m, SpeechStart, SpeakingUser, ActNone)
	apply(t, m, SpeechEnd, Thinking, ActEndUtterance)
	apply(t, m, ResponseStarted, RespondingAgent, ActNone)
	// The barge-in cell: user speaks while the agent responds.
	apply(t, m, SpeechStart, SpeakingUser, ActCancelTurn)
	// The cancelled turn's done event trails in: benign no-op.
	apply(t, m, ResponseDone, SpeakingUser, ActNone)
	// Conversation continues normally.
	apply(t, m, SpeechEnd, Thinking, ActEndUtterance)
}

func TestUserResumesWhileThinking(t *testing.T) {
	m := NewMachine()
	apply(t, m, SpeechStart, SpeakingUser, ActNone)
	apply(t, m, SpeechEnd, Thinking, ActEndUtterance)
	// User keeps talking before the answer starts: cancel pending output.
	apply(t, m, SpeechStart, SpeakingUser, ActCancelTurn)
}

func TestStaleTurnWhileUserSpeaking(t *testing.T) {
	m := NewMachine()
	apply(t, m, SpeechStart, SpeakingUser, ActNone)
	// A queued turn for an old utterance fires mid-speech: cancelled, state holds.
	apply(t, m, ResponseStarted, SpeakingUser, ActCancelTurn)
}

func TestIllegalTransitionsRejected(t *testing.T) {
	cases := []struct {
		setup []Event
		ev    Event
	}{
		{nil, SpeechEnd},                                        // LISTENING + speech_end
		{[]Event{SpeechStart}, SpeechStart},                     // SPEAKING_USER + speech_start
		{[]Event{SpeechStart, SpeechEnd}, SpeechEnd},            // THINKING + speech_end
		{[]Event{SpeechStart, SpeechEnd, ResponseStarted}, SpeechEnd},       // RESPONDING + speech_end
		{[]Event{SpeechStart, SpeechEnd, ResponseStarted}, ResponseStarted}, // double start
	}
	for i, tc := range cases {
		m := NewMachine()
		var observed bool
		m.OnIllegal = func(State, Event) { observed = true }
		for _, ev := range tc.setup {
			if _, err := m.Apply(ev); err != nil {
				t.Fatalf("case %d: setup event %v rejected: %v", i, ev, err)
			}
		}
		before := m.State()
		if _, err := m.Apply(tc.ev); err == nil {
			t.Fatalf("case %d: %v in %v should be rejected", i, tc.ev, before)
		}
		if m.State() != before {
			t.Fatalf("case %d: illegal event moved state %v -> %v", i, before, m.State())
		}
		if m.Illegal() != 1 || !observed {
			t.Fatalf("case %d: illegal event not recorded", i)
		}
	}
}
