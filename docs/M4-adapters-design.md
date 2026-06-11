# M4 — Model Adapters: Interfaces, Mocks, Registry, Real LLM (Design)

Status: **implemented**.

Maps to spec `model-adapters` and design decision **D8** (unified streaming
interfaces; cloud-first; early real LLM). Tasks: 4.1–4.5.

> Live acceptance note: the real-provider test (`TestLiveProvider`) is gated on
> `VOICESTREAM_LLM_API_KEY` and skips without it. CI exercises the adapter
> against a canned SSE server instead, so the contract is verified on every
> run; the live lane needs a key.

## 1. Purpose

Models are **tenants, not kernel**. M4 fixes the wall socket they plug into:
three streaming interfaces, deterministic mocks behind them (so M5/M6 can be
built and benchmarked with zero external dependencies), a name→factory
registry (so swapping models is a config edit), and one real cloud streaming
LLM wired early — the spec's hedge against designing an interface that
collapses on first contact with real token timing.

## 2. Interface shape (4.1)

```go
ASR.Stream(ctx, in <-chan AudioFrame, out chan<- Transcript) error
LLM.Stream(ctx, turn Turn,            out chan<- Token)      error
TTS.Stream(ctx, in <-chan Token,      out chan<- AudioFrame) error
```

The deliberate choices, in decreasing order of importance:

1. **Synchronous call, caller-supplied channels.** `Stream` runs in the
   caller's goroutine and returns when done. The pipeline (M5) gives each
   stage its own goroutine anyway; putting goroutine ownership in the
   *pipeline* rather than the adapters means adapters stay dumb pipes and the
   orchestration layer owns all concurrency — one place to reason about
   lifecycle, one place to cancel.
2. **Blocking sends are the backpressure.** A full `out` channel blocks the
   adapter, which stops consuming `in` (or stops reading the provider's HTTP
   stream). That is exactly D3's text backpressure — no credit protocol, the
   channel *is* the mechanism.
3. **Every send is `select { out <- v; <-ctx.Done() }`.** A cancelled adapter
   can never deadlock on a full channel; this is what makes barge-in's
   sub-chain cancel prompt regardless of downstream state.
4. **Close conventions**: caller closes `in` to end the input (ASR: end of
   utterance; TTS: end of reply); the adapter never closes `out` — the caller
   does after `Stream` returns. One owner per channel end, no double-close
   class of bugs.
5. **One ASR `Stream` = one utterance.** Continuous listening with VAD-gated
   utterance boundaries is the pipeline's job (M5/M6), which restarts ASR
   streams per utterance — matching the sub-chain cancel/restart model (5.5).

Types are minimal on purpose (`Token` is just text; `Transcript` carries
`Final` + a PTS anchor; `AudioFrame` carries PCM + sample-clock PTS per D7).
Wire-level `seq`/`ts_us` stamping belongs to the pipeline/transport, not the
model boundary; adding fields later is non-breaking.

## 3. Deterministic mocks (4.2)

All three mocks share a `Latency{Delay, Jitter, Seed}` knob: fixed delay plus
uniform jitter drawn from a **seeded PCG PRNG** — the same seed reproduces the
same emission schedule, so benchmarks can separate kernel overhead from
"model" latency (D9's load = real hot path + mock models) while still
exercising irregular timing.

| mock | deterministic behaviour |
|---|---|
| `MockASR` | growing word-prefix partials of `Script` every N frames; final on input close; PTS anchored to last consumed frame |
| `MockLLM` | tokenizes a fixed reply (or `echo: <prompt>`); whitespace words, else 3-rune chunks for CJK; concatenation always equals the reply |
| `MockTTS` | rune count × tempo → zeroed PCM cut into frame-sized pieces; PTS from the cumulative synthesis sample clock, gapless across tokens |

## 4. Registry (4.3)

`database/sql`-driver pattern: `Register{ASR,LLM,TTS}(name, factory)` into
package-level maps; mocks self-register in `init()`; external implementations
self-register from their own package and become selectable via a blank import
in `cmd/server`. `adapter.Build(cfg)` resolves `adapters.{asr,llm,tts}` names
to a `Set{ASR, LLM, TTS}` — unknown names fail listing the registered options.

`cmd/server` builds the set **at startup** so a bad name or missing API key
dies at boot, not mid-conversation. The factory signature takes the whole
`config.Config`; each implementation picks what it needs (mock TTS reads the
audio format; the cloud LLM reads `adapters.cloud_llm`).

## 5. Real cloud LLM: `openai-compat` over SSE (4.4)

One adapter covers many providers: DeepSeek, Qwen (compatible mode), Moonshot,
GLM, OpenAI all speak the OpenAI chat-completions dialect — POST
`{base_url}/chat/completions` with `stream: true`, SSE `data:` lines carrying
`choices[0].delta.content`, terminated by `data: [DONE]`. Configuration:

```yaml
adapters:
  llm: openai-compat
  cloud_llm:
    base_url: https://api.deepseek.com/v1   # default; any compatible endpoint
    model: deepseek-chat
    api_key_env: VOICESTREAM_LLM_API_KEY    # env var NAME — key never in file
```

Implementation decisions:

- **Hand-rolled on `net/http` + `bufio.Scanner`, no SDK.** A streaming
  protocol parser is literally this project's subject matter; zero new
  dependencies; the parser tolerates real-provider noise (`:` comments, blank
  keep-alives, `event:` fields).
- **Cancellation = aborting the HTTP request** (`NewRequestWithContext`), so
  barge-in actually stops the provider-side generation and the socket, not
  just our reads. The body-read error caused by mid-stream cancel is reported
  as `ctx.Err()`, keeping the contract uniform with the mocks.
- **No client-level timeout** — streams are long-lived; `ctx` owns lifetime.
  Dial/header phase is bounded separately (`ResponseHeaderTimeout`).
- Non-200 responses surface the provider's error body (truncated) — the
  difference between "bad key" and "bad model name" should not require
  packet captures.

## 6. Acceptance / testing (4.5)

- **Incremental semantics**: ASR partials are prefixes then exactly one final;
  LLM emits >1 token whose concatenation equals the reply (word and CJK
  chunking); TTS PTS follows the synthesis sample clock frame by frame.
- **取消即停**: cancel mid-stream with an *undrained, unbuffered* output
  channel — `Stream` must return `context.Canceled` promptly (mock and real
  adapter both; the real one against an SSE server that would stream forever).
- **全 mock 装配**: `Build(config.Default())` yields a working set; full
  audio→ASR→LLM→TTS→audio chain runs through interfaces alone with monotonic
  output PTS.
- **真实接口相容**: canned-SSE `httptest` server verifies parsing, history
  serialization, auth header, provider-error surfacing — on every CI run.
  `TestLiveProvider` (key-gated) verifies real token timing end to end.

## References

- Spec: `openspec/changes/streaming-multimodal-agent-engine/specs/model-adapters/spec.md`
- Design decisions: D3 (channel-block backpressure), D7 (sample-clock PTS),
  D8 (cloud-first adapters, early real LLM), D9 (load = hot path + mocks).
- Local stack (whisper.cpp / llama.cpp / Piper) lands later as more
  registrations — the registry is the seam that makes that a no-core-change
  event.
