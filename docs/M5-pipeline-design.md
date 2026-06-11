# M5 — Pipeline Orchestration & Dual Backpressure (Design)

Status: **implemented**.

Maps to spec `pipeline-orchestration` and design decisions **D2/D3** (ring at
audio edges, dual backpressure), **D5-adjacent** (turn lifecycle that M6's
state machine drives). Tasks: 5.1–5.7.

> Scope note: M5 is the engine; it is not yet wired to the WebSocket
> transport. Utterance boundaries (`EndUtterance`) and interruption
> (`BargeIn`) are explicit inputs here — M6's inline VAD becomes their caller,
> and M7 connects the rings to the socket. This isn't an accident: the kernel
> stays testable without a network or a microphone.

## 1. Topology

```
PushAudio ─▶ [ingress ring + doorbell] ─▶ ASR runner ──finals chan──▶ orchestrator
 (never blocks,  drop-oldest)              (one utterance                │ one turn
                                            per Stream call)            ▼ at a time
                                          LLM ──tokens──▶ forwarder ──ttsIn──▶ TTS
                                                            │ (tee: OnToken,    │
                                                            │  reply builder)   ▼
AwaitDownlink ◀─ [egress ring + doorbell] ◀──────────── egress pump ◀──ttsOut──┘
```

- **Audio edges are rings** (M3, drop-oldest): `PushAudio` and the egress pump
  never block — real-time producers shed stale frames instead of stalling.
- **Text hops are bounded channels**: `finals`, `tokens`, `ttsIn` block when
  full. Text is precious; blocking *is* the backpressure.
- Each edge pairs the ring with a **capacity-1 doorbell** so consumers park
  instead of polling: a non-blocking send after each push, level-triggered, so
  a frame arriving between "ring empty" and "park" is never missed. Producer
  cost: one failed channel send in the common case.

## 2. The backpressure chain (5.3) — how slow TTS becomes ingress drops

The spec demands: slow TTS → token channel fills → LLM blocks → pressure
reaches the audio entrance as frame drops, memory bounded throughout. The
chain in this implementation, link by link:

1. TTS consumes slowly → `ttsIn` (bounded) fills → the forwarder's send
   blocks → `tokens` (bounded) fills → the LLM adapter's send blocks
   (mid-HTTP-read for a real provider: the socket itself stops draining).
2. The turn never finishes → the orchestrator accepts no new final →
   `finals` (bounded) fills → the **ASR runner blocks** handing over its next
   final.
3. A blocked ASR runner stops popping the ingress ring → the ring fills →
   **drop-oldest evicts, counter increments**. The producer (`PushAudio`)
   never blocked at any point.

Every buffer in that chain has a fixed configured capacity
(`pipeline.{token,transcript,audio}_chan_cap`, `ring_buffer.*_capacity`), so
memory is bounded *by construction*, not by monitoring.
`TestBackpressurePropagatesToIngressDrop` drives exactly this chain and
asserts the drops appear and goroutine count stays flat.

**The key enabling decision**: the orchestrator processes finals strictly one
turn at a time and does **not** consume the next final while a turn runs. The
alternative (new final preempts the running turn) looks more "responsive" but
silently destroys the backpressure story — the orchestrator would always
drain `finals` instantly and the stall could never propagate. Preemption is a
deliberate, separate act: `BargeIn()`, which M6's VAD fires on `speech_start`.

## 3. Stage model (5.1)

`StageFunc[In, Out] = func(ctx, in <-chan In, out chan<- Out) error` is the
composition unit; `adapter.ASR.Stream` and `adapter.TTS.Stream` satisfy it by
shape, so any conforming function replaces a stage without orchestrator
changes. Goroutine ownership lives entirely in the pipeline:

- **ASR runner** (long-lived): per utterance, opens a fresh ASR stream, pumps
  ring→`in`, closes `in` on the boundary, collects partials concurrently
  (so a partial emission can never deadlock the recognizer against the pump),
  hands the final over.
- **Turn sub-chain** (per response, 4 goroutines): LLM runner, token
  forwarder, TTS runner, egress pump — all under one turn `context`. A
  `sync.WaitGroup` closer publishes stats and closes `done`; `done` is the
  only synchronization the orchestrator needs.
- "Cancel(ctx)" from the task list is realized as the turn context itself —
  one `cancel()` reaches every stage, and the M4 adapter contract (every send
  ctx-guarded) guarantees no stage can be wedged on a full channel.

## 4. Barge-in: cancel, flush, restart (5.4 / 5.5)

```go
cancelTurn: h.cancel()        // all four stage goroutines unwind promptly
            <-h.done          // nothing can write egress anymore
            egress.drain()    // flush in-flight downlink audio, counted
```

Ordering matters: drain only after `done`, otherwise the pump could repopulate
the ring behind the flush. Draining while the transport consumer is active is
safe because ring pops are CAS-claimed per slot — the same mechanism that
makes producer-side eviction safe (M3) makes a second drainer safe for free.

The ASR loop lives outside the turn context, so it keeps listening through
the cancel — `TestBargeInCancelsFlushesAndRestarts` barges mid-response, then
runs a second utterance and asserts a complete, residue-free reply (fresh
channels and context per turn mean there is no shared mutable state to leak;
the test also holds the whole cancel→flush sequence under the 200ms budget).

Cancelled turns keep what the agent managed to say in the conversation
history — the user heard it; the next turn's context should reflect reality.

## 5. Latency decomposition (5.6)

Boundary timestamps per turn (`TurnStats`): utterance end (t0), ASR final,
LLM start, LLM first token, first token into TTS, first frame into egress.
From these:

```
FirstResponse  = first egress frame − t0          (user-perceived)
ModelLatency   = ASR-final span + LLM-first-token span + TTS-first-frame span
KernelOverhead = FirstResponse − ModelLatency     (what this project owns)
```

One subtlety implemented deliberately: the ASR-final timestamp is captured in
the ASR runner *when the final is produced*, not when the orchestrator picks
it up — time spent queued in `finals` is kernel overhead and must not be
laundered as model latency. With mocks injecting known delays,
`TestLatencyDecomposition` asserts ModelLatency reflects the injected ~40ms
while KernelOverhead stays bounded. Per-item queue-time histograms and export
land in M9; the decomposition contract is what M5 fixes.

## 6. Other decisions

- **Conversation history**: kept per pipeline (last 16 messages), passed to
  the LLM each turn — this is what makes the real SSE adapter a conversation
  rather than stateless one-shots. Orchestrator-goroutine-only; no locks.
- **Latched signals**: `EndUtterance`/`BargeIn` are capacity-1 non-blocking
  sends — callable from VAD/transport/control paths without ever stalling
  them; repeats coalesce.
- **Empty finals are skipped** (no turn) — spurious utterance boundaries
  (e.g. VAD blips before M6's filters) cost nothing.
- **Egress under `reject` policy**: the pump discards frames the ring refuses
  (it has no blocking option — it must not stall TTS). Default egress policy
  remains `drop_oldest`; `reject` there is for experiments.
- A frame already popped when the utterance boundary fires is dropped with
  the boundary (it is hangover-region audio by definition).

## 7. Acceptance / testing (5.7)

All under `-race`, repeated runs clean:

- `TestThreeStageStreaming`: first downlink frame arrives while the LLM is
  still mid-stream (assert via token count at first frame) — pipelining, not
  batch; reply integrity and OnToken tee verified.
- `TestBackpressurePropagatesToIngressDrop`: the full §2 chain; ingress
  `Dropped() > 0`, goroutine count flat.
- `TestBargeInCancelsFlushesAndRestarts`: cancel < 200ms, egress verifiably
  empty after flush, next turn complete and residue-free.
- `TestLatencyDecomposition`: model vs kernel split sums exactly to
  first-response; all boundary timestamps captured.
- `TestShutdownLeavesNoGoroutines`: mid-turn cancel of the whole pipeline
  returns to baseline goroutine count.

## References

- Spec: `openspec/changes/streaming-multimodal-agent-engine/specs/pipeline-orchestration/spec.md`
- Design decisions: D2 (rings at audio edges only), D3 (dual backpressure),
  D4 (inline VAD keeps ingress SPSC — arrives M6), D7 (PTS timing).
- Consumes: M3 `ringbuf` (edges), M4 `adapter` (stages).
- Consumed by: M6 (VAD drives EndUtterance/BargeIn), M7 (transport wiring),
  M9 (lifts TurnStats into exported metrics).
