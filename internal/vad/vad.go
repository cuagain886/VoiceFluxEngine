package vad

import (
	"encoding/binary"
	"math"
)

// Event is what the detector and the session state machine speak. SpeechStart
// and SpeechEnd come from the detector; ResponseStarted and ResponseDone come
// from the pipeline's turn lifecycle.
type Event uint8

const (
	None Event = iota
	SpeechStart
	SpeechEnd
	ResponseStarted
	ResponseDone
)

// String implements fmt.Stringer.
func (e Event) String() string {
	switch e {
	case None:
		return "none"
	case SpeechStart:
		return "speech_start"
	case SpeechEnd:
		return "speech_end"
	case ResponseStarted:
		return "response_started"
	case ResponseDone:
		return "response_done"
	default:
		return "event(?)"
	}
}

// Detector is the pluggable per-frame voice activity interface. Process is
// called inline from the audio ingress goroutine — implementations must be
// allocation-free and fast (a WebRTC or ML VAD is a drop-in replacement
// later). It returns SpeechStart/SpeechEnd at utterance boundaries and None
// otherwise.
type Detector interface {
	Process(pcm []byte) Event
}

// Energy is the v1 detector: RMS energy with three false-trigger filters.
//
//	dual threshold   enter (higher) to start speech, exit (lower) to stay in
//	                 it — hysteresis so levels hovering near one line don't
//	                 flutter start/end events
//	min speech       energy must hold above enter for N consecutive frames
//	                 before speech_start fires — clicks and pops don't count
//	hangover         energy must hold below exit for M consecutive frames
//	                 before speech_end — natural mid-sentence pauses don't
//	                 split the utterance
//
// (The fourth suppression layer, client-side AEC, lives in the browser per
// the spec — it removes the agent's own playback before it ever reaches us.)
type Energy struct {
	Enter           float64 // RMS to qualify as speech (normalized 0..1)
	Exit            float64 // RMS below which speech decays; Exit <= Enter
	MinSpeechFrames int     // consecutive loud frames before speech_start
	HangoverFrames  int     // consecutive quiet frames before speech_end

	inSpeech bool
	run      int // consecutive frames of the opposing condition
}

// Process classifies one PCM frame (16-bit little-endian mono).
func (e *Energy) Process(pcm []byte) Event {
	level := rms(pcm)
	if !e.inSpeech {
		if level >= e.Enter {
			e.run++
			if e.run >= e.MinSpeechFrames {
				e.inSpeech = true
				e.run = 0
				return SpeechStart
			}
		} else {
			e.run = 0
		}
		return None
	}
	if level < e.Exit {
		e.run++
		if e.run >= e.HangoverFrames {
			e.inSpeech = false
			e.run = 0
			return SpeechEnd
		}
	} else {
		e.run = 0
	}
	return None
}

// rms computes the normalized root-mean-square level of a frame of 16-bit
// little-endian PCM. ~320 samples per 20ms frame: sub-microsecond, fine for
// the inline hot path.
func rms(pcm []byte) float64 {
	n := len(pcm) / 2
	if n == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < n; i++ {
		s := int16(binary.LittleEndian.Uint16(pcm[2*i:]))
		v := float64(s) / 32768
		sum += v * v
	}
	return math.Sqrt(sum / float64(n))
}
