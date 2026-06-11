package vad

import (
	"encoding/binary"
	"testing"
)

// pcmFrame synthesizes a 20ms 16kHz mono frame of constant amplitude
// (0..1), whose RMS equals the amplitude — deterministic detector input.
func pcmFrame(amplitude float64) []byte {
	const samples = 320
	buf := make([]byte, samples*2)
	v := int16(amplitude * 32767)
	for i := 0; i < samples; i++ {
		binary.LittleEndian.PutUint16(buf[2*i:], uint16(v))
	}
	return buf
}

func newTestEnergy() *Energy {
	return &Energy{Enter: 0.01, Exit: 0.005, MinSpeechFrames: 3, HangoverFrames: 5}
}

func feed(t *testing.T, det *Energy, amplitude float64, n int, want Event) {
	t.Helper()
	f := pcmFrame(amplitude)
	for i := 0; i < n; i++ {
		got := det.Process(f)
		wanted := None
		if i == n-1 {
			wanted = want
		}
		if got != wanted {
			t.Fatalf("frame %d @%g: got %v, want %v", i, amplitude, got, wanted)
		}
	}
}

func TestSpeechStartAfterMinDuration(t *testing.T) {
	det := newTestEnergy()
	feed(t, det, 0.5, 3, SpeechStart) // exactly MinSpeechFrames loud frames
}

func TestShortBurstDoesNotTrigger(t *testing.T) {
	det := newTestEnergy()
	feed(t, det, 0.5, 2, None) // one frame short of min speech
	feed(t, det, 0.0, 1, None) // silence resets the run
	feed(t, det, 0.5, 2, None) // another short burst: still nothing
}

func TestHangoverBridgesShortPause(t *testing.T) {
	det := newTestEnergy()
	feed(t, det, 0.5, 3, SpeechStart)
	feed(t, det, 0.0, 4, None) // pause shorter than hangover: no end
	feed(t, det, 0.5, 1, None) // speech resumes, hangover run resets
	feed(t, det, 0.0, 4, None) // again short: still speaking
	feed(t, det, 0.0, 1, SpeechEnd) // fifth consecutive quiet frame: end
}

func TestDualThresholdHysteresis(t *testing.T) {
	det := newTestEnergy()
	// 0.007 sits between exit (0.005) and enter (0.01).
	feed(t, det, 0.007, 10, None) // not loud enough to *start* speech
	feed(t, det, 0.5, 3, SpeechStart)
	feed(t, det, 0.007, 20, None) // but loud enough to *sustain* it
	feed(t, det, 0.0, 5, SpeechEnd)
}

func TestNoiseFloorSilent(t *testing.T) {
	det := newTestEnergy()
	feed(t, det, 0.002, 50, None) // sub-threshold noise: never an event
}
