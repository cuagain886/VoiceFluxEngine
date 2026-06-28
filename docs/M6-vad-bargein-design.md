# M6 — Inline VAD & Barge-in State Machine (Design)

Status: **implemented**.

Maps to spec `vad-barge-in` and design decisions **D4** (inline VAD preserves
SPSC), **D5** (explicit state machine), **D6** (client AEC). Tasks: 6.1–6.6.

## 1. Purpose

M6 closes the interruption pillar (打断): detect when the user speaks, decide
what that means for the conversation, and when the agent is mid-answer, cut it
off inside the 200ms budget. Three pieces:

| piece | file | job |
|---|---|---|
| `Energy` detector | `vad.go` | frame → `speech_start`/`speech_end`, with false-trigger filters |
| `Machine` | `machine.go` | conversation state, rejects illegal transitions |
| `Controller` | `controller.go` | the inline glue the transport reader calls per frame |

## 2. Inline placement — why VAD is not a consumer (6.1, D4)

VAD and ASR both need every ingress frame. Making VAD a second ring consumer
would force the ingress into MPMC (slower, more complex) or a fan-out stage
(another goroutine + queue). Instead `Controller.Ingest` runs the detector
**in the transport read goroutine itself**, then forwards the frame:

```go
func (c *Controller) Ingest(f adapter.AudioFrame) {
    if ev := c.det.Process(f.PCM); ev != None { c.apply(ev) }  // control plane
    c.sink.PushAudio(f)                                        // data plane
}
```

The ring keeps exactly one producer and one consumer; VAD events travel the
control plane — the pipeline's latched, non-blocking `EndUtterance`/`BargeIn`
signals — never the audio path. `Ingest` cannot block, so the socket reader's
cadence is untouched. Cost: one RMS pass over 320 samples (~sub-µs) per frame.

`Detector` is a one-method interface; passing a custom implementation to
`NewController` swaps in a WebRTC/ML VAD with zero other changes (the v1
`Energy` detector is built from config when none is given).

## 3. False-trigger suppression (6.2 / 6.5)

Four layers, outermost first:

1. **Client AEC** (browser `getUserMedia({echoCancellation:true})`, lands in
   M7): removes the agent's own playback from the mic signal *before* it
   reaches the kernel — the only practical defense against self-barge-in.
2. **Dual threshold (hysteresis)**: enter at `energy_threshold` (0.01),
   sustain at `exit_threshold` (0.005). Levels hovering between the two can
   keep speech alive but can never start it, so flutter around a single
   line is structurally impossible.
3. **Min speech duration**: `min_speech` (100ms ≈ 5 frames) of consecutive
   loud frames before `speech_start` — door slams and clicks don't qualify.
4. **Hangover**: `hangover` (300ms) of consecutive quiet frames before
   `speech_end` — natural mid-sentence pauses don't split utterances.

The detector is a tiny per-frame state machine over (inSpeech, run-length);
all thresholds and windows are config-driven, durations converted to frame
counts at construction. Deterministic by construction — the tests feed
synthesized constant-amplitude PCM and assert exact event positions.

## 4. The conversation state machine (6.3)

Events: VAD's `speech_start`/`speech_end` plus the pipeline's turn lifecycle
(`response_started`/`response_done`, via the M5 `OnTurnStart`/`OnTurnEnd`
hooks). The table ( — = rejected, state held, counted, observable):

| state \ event | speech_start | speech_end | response_started | response_done |
|---|---|---|---|---|
| LISTENING | →SPEAKING_USER | — | →RESPONDING | no-op |
| SPEAKING_USER | — | →THINKING **+EndUtterance** | stay **+CancelTurn** | no-op |
| THINKING | →SPEAKING_USER **+CancelTurn** | — | →RESPONDING | →LISTENING |
| RESPONDING | →SPEAKING_USER **+CancelTurn** | — | — | →LISTENING |

The non-obvious cells are deliberate policy:

- **RESPONDING + speech_start** is *the* barge-in (6.4).
- **THINKING + speech_start**: the user resumed before the answer began —
  cancel whatever is pending so a stale reply doesn't talk over them.
- **SPEAKING_USER + response_started**: a queued turn for an *old* utterance
  fired while the user is already talking; it is stale by definition, cancel
  it immediately rather than letting the agent interrupt the user.
- **response_done in LISTENING / SPEAKING_USER is a no-op, not an error**:
  after a barge-in the cancelled turn's done event always trails in after the
  machine has moved on. Treating it as illegal would make the normal barge-in
  sequence "erroneous".

Illegal events leave the state untouched, increment a counter, and notify an
observer hook — the spec's "记录该非法事件" without letting a bad event
corrupt the conversation.

The machine is mutex-guarded because its two event sources live on different
goroutines (ingress reader, orchestrator). One lock per audio frame is ~20ns
uncontended; not worth lock-free heroics, and noted honestly.

## 5. Barge-in end to end (6.4)

```
user's 1st loud frame ─▶ +min_speech ─▶ speech_start ─▶ machine: RESPONDING→SPEAKING_USER
  ─▶ BargeIn() latch ─▶ orchestrator: cancel turn ctx ─▶ all 4 stage goroutines exit
  ─▶ drain egress ring ─▶ agent is silent
```

The latency budget decomposes as: min-speech filter (config, 100ms default —
a *tuning choice*, not kernel cost) + machine transition (~µs) + adapter
cancel (prompt by M4 contract) + flush (µs). `TestBargeInLatencyUnder200ms`
measures the whole thing on the mock chain — first interrupting frame to
cancelled-and-flushed — under 200ms with a 60ms min-speech window.

## 6. Acceptance / testing (6.6)

All `-race`, repeated runs clean:

- Detector: exact-position `speech_start` after min duration; short bursts
  and noise floor never fire; hangover bridges sub-threshold pauses;
  hysteresis sustains-but-never-starts in the inter-threshold band.
- Machine: the normal cycle; the barge-in path including the trailing
  `response_done` no-op; user-resumes-while-THINKING; stale-turn-while-
  speaking; five illegal-transition cases (state held, counted, observed).
- Controller + real pipeline (the M7 wiring shape, minus the socket):
  a fully voice-driven turn with no explicit signal calls; barge-in latency
  < 200ms with egress verifiably empty after the flush; noise and sub-min-
  speech thuds during RESPONDING leave the turn uncancelled.

## References

- Spec: `openspec/changes/streaming-multimodal-agent-engine/specs/vad-barge-in/spec.md`
- Design decisions: D4 (inline VAD / SPSC), D5 (state machine), D6 (client
  AEC owns echo suppression).
- Consumes: M5 pipeline (`Sink` = PushAudio/EndUtterance/BargeIn, turn hooks).
- Consumed by: M7 (transport reader calls `Ingest`; browser enables AEC).
