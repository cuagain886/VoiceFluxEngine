package vad

import (
	"log/slog"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
)

// Sink is the slice of the pipeline the controller drives. *pipeline.Pipeline
// satisfies it; the interface keeps vad free of a pipeline dependency.
type Sink interface {
	PushAudio(adapter.AudioFrame) bool
	EndUtterance()
	BargeIn()
}

// Controller is the inline glue: the transport read goroutine calls Ingest
// for every uplink frame, which runs the detector *in that same goroutine*
// (events ride the control plane — the pipeline's latched signals — never a
// second ring consumer, so the audio ingress stays strictly SPSC), feeds the
// state machine, executes the resulting action, and forwards the frame.
//
// The pipeline's turn lifecycle hooks must be wired to ResponseStarted and
// ResponseDone so the machine sees both halves of the conversation.
type Controller struct {
	det     Detector
	machine *Machine
	sink    Sink
	logger  *slog.Logger
}

// NewController assembles the detector from config. det may be nil, in which
// case the v1 Energy detector is built from cfg.VAD; pass a custom Detector
// to plug in a different VAD without touching anything else.
func NewController(cfg config.Config, det Detector, sink Sink, logger *slog.Logger) *Controller {
	if det == nil {
		det = &Energy{
			Enter:           cfg.VAD.EnergyThreshold,
			Exit:            cfg.VAD.ExitThreshold,
			MinSpeechFrames: durationFrames(cfg.VAD.MinSpeech, cfg.Audio.FrameDuration),
			HangoverFrames:  durationFrames(cfg.VAD.Hangover, cfg.Audio.FrameDuration),
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	m := NewMachine()
	m.OnIllegal = func(s State, ev Event) {
		logger.Warn("illegal state transition rejected", "state", s, "event", ev)
	}
	return &Controller{det: det, machine: m, sink: sink, logger: logger}
}

func durationFrames(d, frame time.Duration) int {
	if frame <= 0 {
		return 1
	}
	n := int((d + frame - 1) / frame)
	if n < 1 {
		n = 1
	}
	return n
}

// Ingest processes one uplink frame: detect, transition, act, forward.
// It never blocks (the sink's signals are latched and its ring push is
// drop-oldest), so the socket reader's cadence is untouched.
func (c *Controller) Ingest(f adapter.AudioFrame) {
	if ev := c.det.Process(f.PCM); ev != None {
		c.apply(ev)
	}
	c.sink.PushAudio(f)
}

// ResponseStarted is wired to the pipeline's turn-start hook.
func (c *Controller) ResponseStarted() { c.apply(ResponseStarted) }

// ResponseDone is wired to the pipeline's turn-end hook (any outcome).
func (c *Controller) ResponseDone() { c.apply(ResponseDone) }

// State exposes the conversation state (for metrics and the demo client).
func (c *Controller) State() State { return c.machine.State() }

func (c *Controller) apply(ev Event) {
	act, err := c.machine.Apply(ev)
	if err != nil {
		return // already counted and logged by the machine
	}
	switch act {
	case ActEndUtterance:
		c.sink.EndUtterance()
	case ActCancelTurn:
		c.sink.BargeIn()
	}
}
