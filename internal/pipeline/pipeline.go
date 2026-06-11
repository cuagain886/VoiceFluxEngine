package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/ringbuf"
)

// StageFunc is the unit the orchestrator composes: consume in, produce out,
// honor ctx. adapter.ASR.Stream and adapter.TTS.Stream are StageFuncs by
// shape; any conforming function can replace a stage without touching the
// orchestrator.
type StageFunc[In, Out any] func(ctx context.Context, in <-chan In, out chan<- Out) error

// Pipeline orchestrates one conversation: audio in at the ingress ring,
// ASR -> LLM -> TTS across goroutines, audio out at the egress ring.
//
// Dual backpressure by data semantics (D3):
//
//	audio edges  ring buffer, drop-oldest, never blocks the producer
//	text hops    bounded channels, a full channel blocks the upstream stage
//
// The two meet at the ingress: when a turn stalls (slow model), finals queue
// up, the ASR runner blocks handing one over, stops draining the ingress
// ring, and the overload surfaces as counted frame drops — bounded memory
// everywhere, by construction.
//
// Turn policy: finals are processed strictly one at a time, in order; a new
// final never preempts a running turn. Preemption is exactly BargeIn(),
// which M6's VAD fires on speech_start.
type Pipeline struct {
	cfg    config.Config
	set    adapter.Set
	logger *slog.Logger

	ingress *edge // transport -> ASR
	egress  *edge // TTS -> transport

	endUtt  chan struct{} // utterance boundary, latched (cap 1)
	bargeIn chan struct{} // response cancel request, latched (cap 1)

	// OnTranscript and OnToken, when set before Run, observe ASR transcripts
	// and LLM tokens (for downlink TEXT frames / subtitles). They run on
	// stage goroutines: they must be fast and must not block.
	OnTranscript func(adapter.Transcript)
	OnToken      func(adapter.Token)

	history []adapter.Message // orchestrator-goroutine only

	statsMu sync.Mutex
	last    TurnStats
	hasLast bool
}

// New builds a pipeline for one session from the validated config.
func New(cfg config.Config, set adapter.Set, logger *slog.Logger) (*Pipeline, error) {
	inPol, err := ringbuf.ParsePolicy(cfg.RingBuf.IngressPolicy)
	if err != nil {
		return nil, err
	}
	outPol, err := ringbuf.ParsePolicy(cfg.RingBuf.EgressPolicy)
	if err != nil {
		return nil, err
	}
	ingress, err := newEdge(cfg.RingBuf.IngressCapacity, inPol)
	if err != nil {
		return nil, fmt.Errorf("pipeline: ingress: %w", err)
	}
	egress, err := newEdge(cfg.RingBuf.EgressCapacity, outPol)
	if err != nil {
		return nil, fmt.Errorf("pipeline: egress: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Pipeline{
		cfg:     cfg,
		set:     set,
		logger:  logger,
		ingress: ingress,
		egress:  egress,
		endUtt:  make(chan struct{}, 1),
		bargeIn: make(chan struct{}, 1),
	}, nil
}

// PushAudio offers an uplink frame to the ingress ring. It never blocks;
// under drop-oldest it always succeeds (possibly evicting a stale frame).
// Single producer: the transport read goroutine.
func (p *Pipeline) PushAudio(f adapter.AudioFrame) bool { return p.ingress.push(f) }

// AwaitDownlink blocks until a synthesized frame is available or ctx ends.
// Single consumer: the transport write goroutine.
func (p *Pipeline) AwaitDownlink(ctx context.Context) (adapter.AudioFrame, error) {
	return p.egress.await(ctx)
}

// EndUtterance marks the end of the user's current utterance (speech_end).
// M6's VAD calls this; until then the transport control plane or tests do.
// Never blocks; repeated signals coalesce.
func (p *Pipeline) EndUtterance() {
	select {
	case p.endUtt <- struct{}{}:
	default:
	}
}

// BargeIn requests cancellation of the in-flight response sub-chain
// (speech_start during RESPONDING). Never blocks; a no-op if idle.
func (p *Pipeline) BargeIn() {
	select {
	case p.bargeIn <- struct{}{}:
	default:
	}
}

// IngressDropped and EgressDropped report frames shed at each audio edge.
func (p *Pipeline) IngressDropped() uint64 { return p.ingress.dropped() }
func (p *Pipeline) EgressDropped() uint64  { return p.egress.dropped() }

// LastTurn returns the most recently completed turn's measurements.
func (p *Pipeline) LastTurn() (TurnStats, bool) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.last, p.hasLast
}

func (p *Pipeline) publish(s TurnStats) {
	p.statsMu.Lock()
	p.last, p.hasLast = s, true
	p.statsMu.Unlock()
	p.logger.Debug("turn complete",
		"prompt", s.Prompt,
		"first_response", s.FirstResponse(),
		"model_latency", s.ModelLatency(),
		"kernel_overhead", s.KernelOverhead(),
		"cancelled", s.Cancelled,
		"flushed_frames", s.FlushedFrames,
	)
}

// Run drives the conversation until ctx is cancelled or a stage fails.
func (p *Pipeline) Run(ctx context.Context) error {
	finals := make(chan utterance, p.cfg.Pipeline.TranscriptChanCap)
	asrErr := make(chan error, 1)
	go func() { asrErr <- p.runASRLoop(ctx, finals) }()

	var cur *turnHandle
	for {
		// A new final is only accepted when no turn is running: finals
		// queue in the channel, and once it is full the ASR runner blocks —
		// the head of the backpressure chain back to the ingress ring.
		var finalsC <-chan utterance
		var curDone <-chan struct{}
		if cur == nil {
			finalsC = finals
		} else {
			curDone = cur.done
		}

		select {
		case <-ctx.Done():
			if cur != nil {
				p.cancelTurn(cur)
			}
			<-asrErr
			return ctx.Err()

		case err := <-asrErr:
			if cur != nil {
				p.cancelTurn(cur)
			}
			return err

		case <-p.bargeIn:
			if cur != nil {
				p.cancelTurn(cur)
				cur = nil
			}

		case u := <-finalsC:
			if u.text == "" {
				continue // empty utterance (e.g. spurious boundary): no turn
			}
			cur = p.startTurn(ctx, u)

		case <-curDone:
			p.finishTurn(cur)
			cur = nil
		}
	}
}

// utterance is one finalized user input plus its timing anchors. finalAt is
// captured when ASR produced the final — before any queueing — so time spent
// waiting for the orchestrator counts as kernel overhead, not model latency.
type utterance struct {
	text    string
	endAt   time.Time // when EndUtterance fired: t0 of first-response latency
	finalAt time.Time // when the final transcript became available
}

// runASRLoop recognizes utterance after utterance: pump the ingress ring into
// a fresh ASR stream, close it on the utterance boundary, hand the final
// transcript to the orchestrator, repeat. It owns the consumer side of the
// ingress ring, so when it blocks (orchestrator busy, finals full) the
// ingress starts shedding — by design.
func (p *Pipeline) runASRLoop(ctx context.Context, finals chan<- utterance) error {
	for ctx.Err() == nil {
		in := make(chan adapter.AudioFrame, p.cfg.Pipeline.AudioChanCap)
		out := make(chan adapter.Transcript, p.cfg.Pipeline.AudioChanCap)

		streamErr := make(chan error, 1)
		go func() {
			err := p.set.ASR.Stream(ctx, in, out)
			close(out)
			streamErr <- err
		}()

		// Collect transcripts concurrently so partial emissions never block
		// the recognizer while we are pumping audio.
		var final adapter.Transcript
		collected := make(chan struct{})
		go func() {
			defer close(collected)
			for tr := range out {
				if p.OnTranscript != nil {
					p.OnTranscript(tr)
				}
				if tr.Final {
					final = tr
				}
			}
		}()

		// Pump frames until the utterance boundary.
		var endAt time.Time
	pump:
		for {
			f, ok := p.ingress.pop()
			if !ok {
				select {
				case <-p.ingress.bell:
				case <-p.endUtt:
					endAt = time.Now()
					break pump
				case <-ctx.Done():
				}
				if ctx.Err() != nil {
					break pump
				}
				continue
			}
			select {
			case in <- f:
			case <-p.endUtt:
				// Boundary arrived while handing over a frame; the frame is
				// hangover-region audio and is deliberately dropped.
				endAt = time.Now()
				break pump
			case <-ctx.Done():
				break pump
			}
		}

		close(in)
		err := <-streamErr
		<-collected
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("pipeline: ASR stream: %w", err)
		}

		// Hand the final over. Blocking here when the queue is full is the
		// designed stall: ingress keeps absorbing via drop-oldest.
		select {
		case finals <- utterance{text: final.Text, endAt: endAt, finalAt: time.Now()}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}
