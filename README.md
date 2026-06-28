<div align="center">

# VoiceFluxEngine

**A real-time, model-agnostic _streaming kernel_ for voice agents.**

It owns three things only — **Timing · Backpressure · Interruption** (时序 / 背压 / 打断).
ASR / LLM / TTS are pluggable tenants, *not* part of the kernel.

[![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Transport: WebSocket](https://img.shields.io/badge/transport-WebSocket-blue)
![Dependencies: near-zero](https://img.shields.io/badge/deps-near--zero-success)

**English** · [简体中文](README.zh-CN.md)

</div>

![architecture](docs/assets/architecture.svg)

> **North Star:** a browser conversation you can **interrupt naturally** mid-response, running on a connection pushed to its capacity knee — the deliverable is a load/latency curve with a *proven* knee and graceful degradation.

---

## Contents

- [Why it exists](#why-it-exists)
- [What it proves](#what-it-proves)
- [Features](#features)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Benchmarks](#benchmarks)
- [Design highlights](#design-highlights)
- [Documentation](#documentation)
- [Scope (v1)](#scope-v1)
- [License](#license)

## Why it exists

Most "voice agent" stacks bolt ASR → LLM → TTS together and hope the timing works out. The hard part isn't the models — it's the **kernel** that sits between mic/speaker and the models: *when* to cut the user off, *how* to shed load when a model stalls, and *how fast* it can cancel an in-flight response the instant the user speaks again.

VoiceFluxEngine is **only** that kernel. Models are tenants you plug in by config — swap a mock for sherpa-onnx ASR, or DeepSeek for a local Ollama, without touching the kernel.

> Go module name is `voicestream`; the repository is `VoiceFluxEngine`.

## What it proves

| Metric | Measured | Source |
|---|---|---|
| Barge-in (kernel cancel) p99 | **&lt;2 ms** on the plateau · **≤143 ms** @ 3× knee load (budget 200 ms) | M10 capacity curve |
| Kernel overhead p99 (first-response − model latency) | **5 ms**, flat across the plateau | M9 / M10 decomposition |
| Capacity knee | **~500** concurrent sessions (i7-13620H · power-limited · loadgen co-located → **lower bound**) | M10 |
| Past-the-knee behavior | ingress drop-oldest sheds 21→38%, **egress zero drops**, no crash / no OOM; recovers to 0 sessions / 7 goroutines after a 5000-session storm | M10 |
| Audio hot-path allocation | ring SPSC **0 allocs/op** · downlink encode **0 allocs/op** (test-gated) | M3 / M11 |

> **Honesty over hype.** Numbers are single-machine, power-limited, with the load generator co-located on the same box — treat them as **lower bounds**, not leaderboard entries.

## Features

- 🎙️ **Natural barge-in.** Inline energy VAD (inside the ingress read goroutine, keeping audio strictly SPSC) drives a 4-state machine; `RESPONDING + speech_start` cancels the response sub-chain and flushes in-flight audio. The cancel path never goes through a congested queue, so the 200 ms budget holds even under overload.
- ⚖️ **Dual backpressure, split by data semantics.** High-rate audio edges (50 fps/stream) use a lock-free **SPSC ring with drop-oldest** — dropping stale real-time audio is a *feature*. Low-rate text uses **bounded blocking channels** — text must not drop. They converge at ingress: a slow model → chained blocking → *counted* ingress drops, with memory bounded by construction.
- 🔌 **Model-agnostic adapters.** Swap ASR / LLM / TTS by config, no kernel changes. **LLM** = any OpenAI-compatible SSE endpoint; **TTS** = sherpa-onnx *or* any OpenAI-compatible `/audio/speech` endpoint; **ASR** = sherpa-onnx — open-source local sidecars, with built-in mocks throughout. See [Configuration](#configuration).
- 📊 **Observability as backdrop.** Hand-rolled Prometheus text format (~150 lines, zero deps) + an SSE latency-**waterfall** dashboard. First-response is decomposed into *model latency* + *kernel overhead* and measured separately — the kernel never takes credit for the model.
- 🧪 **Load harness + capacity curve.** Ramp concurrent virtual sessions through the *real* hot path; find the knee and prove graceful degradation.
- 🪶 **Zero-alloc audio hot path.** The SPSC ring and downlink encoder run at 0 allocs/op, enforced by test gates.
- 🌐 **WebSocket + binary frame protocol.** protobuf for TEXT / CONTROL payloads, raw PCM for audio; 16 kHz / 16-bit / mono assumed at the wire.

## Quick start

```bash
go build ./...   # Go 1.26+
```

### 1 · Fastest path: all-mock, zero external dependencies

```bash
go run ./cmd/server   # then open http://localhost:8080/
```

Talk into the mic → hear a mock echo reply → **speak again mid-reply to interrupt it**. No models or keys required — this exercises the kernel itself: timing / backpressure / interruption.

### 2 · Real ASR / TTS via sherpa-onnx (local sidecar)

The kernel embeds no models — it's just a WebSocket client to a sherpa-onnx sidecar (here [ruzhila/voiceapi](https://github.com/ruzhila/voiceapi)). Full steps (model download, China mirror acceleration) are in [docs/sherpa-adapter-zh.md](docs/sherpa-adapter-zh.md).

**① One-time setup** — create a venv, install deps, download models:

```powershell
# Windows (PowerShell)
git clone https://github.com/ruzhila/voiceapi
cd voiceapi
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```
```bash
# Linux / macOS (bash)
git clone https://github.com/ruzhila/voiceapi
cd voiceapi
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

> Then download the models into `voiceapi/models/` (in mainland China prefix GitHub URLs with `https://ghfast.top/`). See [docs/sherpa-adapter-zh.md](docs/sherpa-adapter-zh.md).

**② Every run — two terminals (both required):**

**Terminal 1 — the sidecar** (keep it running; listens on `:8000`):

```powershell
# Windows (PowerShell)
cd voiceapi
.venv\Scripts\python app.py --asr-model zipformer-bilingual --threads 8
```
```bash
# Linux / macOS (bash)
cd voiceapi
.venv/bin/python app.py --asr-model zipformer-bilingual --threads 8
```

**Terminal 2 — the kernel** (back in the project root; listens on `:8080`). Put `VOICESTREAM_CONFIG=configs/sherpa.yaml` in a `.env` at the project root (auto-loaded at startup) and both platforms just run `go run ./cmd/server`; or set the env var inline, per platform:

```powershell
# Windows (PowerShell) — note the `;`, not bash's inline prefix
$env:VOICESTREAM_CONFIG = "configs/sherpa.yaml"; go run ./cmd/server
```
```bash
# Linux / macOS (bash)
VOICESTREAM_CONFIG=configs/sherpa.yaml go run ./cmd/server
```

Open `http://localhost:8080/` for **real ASR transcription → LLM reply → real TTS synthesis**, still interruptible.

> **Dependency chain (remember this):** `browser ⇄ voicestream (:8080) ⇄ voiceapi (:8000) ⇄ sherpa-onnx models`. voiceapi must stay up the whole time — the sherpa adapter connects lazily *on first speech*, so the kernel starts fine, but if voiceapi isn't running you'll get `connection refused` the moment you talk. That error just means: go start Terminal 1.

### 3 · Real LLM via any OpenAI-compatible endpoint

The `openai-compat` adapter is ready (it's the default in `configs/sherpa.yaml`) — wiring it up is just supplying a key (**keys only via env var, never in files**):

1. Confirm `llm: openai-compat` in `configs/sherpa.yaml` (default).
2. Put the key in `.env` at the project root (auto-loaded): `VOICESTREAM_LLM_API_KEY=sk-...` (cloud key; for a local model use any non-empty value).
3. Run (same as Terminal 2 above).

The default `cloud_llm` points at DeepSeek (`deepseek-chat`); switch vendor/local by changing only `cloud_llm.base_url` + `model`:

- **Cloud:** DeepSeek `https://api.deepseek.com/v1` · Qwen `https://dashscope.aliyuncs.com/compatible-mode/v1` · Kimi `https://api.moonshot.cn/v1`
- **Local OSS:** Ollama `http://localhost:11434/v1`, vLLM, LM Studio (same adapter, no code change).

A built-in **voice-assistant system prompt** keeps replies plain and spoken — no Markdown / emoji that TTS would read out literally. Override it via `cloud_llm.system_prompt`.

Now you have the **real ASR → real LLM → real TTS** loop end to end, interruptible throughout.

### 4 · Test & load

```bash
go test -race ./...                                             # full suite (race detector)
go run ./cmd/loadgen -steps 50,100,200,400,800 -out docs/load   # reproduce the capacity curve
```

> **`.env` auto-load:** the server reads `.env` at the project root on startup (real env vars win). Template: [.env.example](.env.example). Putting `VOICESTREAM_CONFIG` and `VOICESTREAM_LLM_API_KEY` there avoids the cross-platform env-var syntax differences below.
>
> **Env-var syntax differs by shell:** PowerShell uses `$env:NAME = "value"` (separate from the command with `;`) — it does **not** accept bash's `NAME=value command` prefix. bash uses the `NAME=value command` prefix or `export`.
>
> **venv Python path:** Windows `.venv\Scripts\python`, Linux/macOS `.venv/bin/python`.

## Configuration

Config resolves in this order, later wins: **built-in defaults → YAML file (`VOICESTREAM_CONFIG`) → env-var overrides**. With nothing set you get the all-mock default on `:8080`. Secrets are **never** read from files — only from env vars (auto-loaded from `.env`). The fully-annotated reference config is [configs/sherpa.yaml](configs/sherpa.yaml).

**Pick your models by config — the kernel never changes.** Each stage names an adapter; each adapter reads its own block:

| Stage | `adapters.*` | Options | Config block | Secret (env var) |
|---|---|---|---|---|
| **ASR** | `asr` | `mock` · `sherpa` | `adapters.sherpa` | — (local sidecar) |
| **LLM** | `llm` | `mock` · `openai-compat` | `adapters.cloud_llm` | `VOICESTREAM_LLM_API_KEY` |
| **TTS** | `tts` | `mock` · `sherpa` · `openai-tts` | `adapters.sherpa` / `adapters.openai_tts` | `VOICESTREAM_TTS_API_KEY` (cloud only) |

Swapping a backend is a one-line edit. For example, cloud TTS via the generic `openai-tts` adapter (OpenAI, or any compatible `/audio/speech` proxy — kokoro, edge-tts, self-hosted):

```yaml
adapters:
  tts: openai-tts            # the only kernel-visible change
  openai_tts:
    base_url: "https://api.openai.com/v1"
    model: "tts-1"
    voice: "alloy"
    api_key_env: "VOICESTREAM_TTS_API_KEY"   # key lives in env, never in the file
```

> ⚠️ **16 kHz only (D11: no resampling in v1).** The endpoint must return raw 16-bit mono PCM at `audio.sample_rate`. Real OpenAI `pcm` is fixed at **24 kHz** — use a sample-rate-configurable compatible service, or set `audio.sample_rate: 24000`, otherwise pitch/speed will be wrong.

**Environment variables** (all optional; put them in `.env` — template at [.env.example](.env.example)):

| Var | Effect |
|---|---|
| `VOICESTREAM_CONFIG` | path to the YAML config; unset = built-in all-mock default |
| `VOICESTREAM_LLM_API_KEY` | API key for the `openai-compat` LLM (name set by `cloud_llm.api_key_env`) |
| `VOICESTREAM_TTS_API_KEY` | API key for the `openai-tts` TTS (name set by `openai_tts.api_key_env`) |
| `VOICESTREAM_ADDR` | override the listen address (e.g. `:9090`) |
| `VOICESTREAM_SAMPLE_RATE` | override the wire sample rate |

## Benchmarks

### Capacity curve (L2)

![capacity curve](docs/assets/capacity-curve.svg)

Flat across the plateau; at the knee the CPU wall hits first; past it the **dual backpressure sheds at ingress by design** — pressure propagates back along TTS → token → finals → ASR and shows up as *counted* ingress drops, not memory growth or a crash. Interactive version: `docs/load/capacity.html`; reproduce with `go run ./cmd/loadgen -h`.

### Latency waterfall (L1)

![waterfall](docs/assets/waterfall.svg)

LLM and TTS bars overlap heavily by nature — that's the pipeline; the grey serial bar is the same three spans laid end to end. Interrupted turns are boxed in red with the kernel-cancel cost. Live version at `http://localhost:8080/dash.html` (SSE, pushed per turn).

### channels vs lock-free ring (L3)

![channels vs ring](docs/assets/bench-chan-vs-ring.svg)

The ring isn't about raw throughput: drop-oldest is roughly on par — the real difference is the **atomicity of the eviction semantics** (a channel's `select` emulation doesn't hold under concurrency). An honest finding too: a ring *without* false-sharing padding is slower than a channel — a lock-free structure done wrong is worse than not doing it.

## Design highlights

- **Dual backpressure split by data semantics.** Audio edges (50 fps/stream) ride a lock-free SPSC ring with drop-oldest (dropping stale real-time audio is a feature); mid-stream text rides bounded blocking channels (text cannot drop). They meet at ingress: slow model → chained blocking → *counted* ingress drops, memory bounded by construction.
- **Interruption is the control plane.** An inline energy VAD (inside the ingress read goroutine, preserving strict SPSC) drives a 4-state machine; `RESPONDING + speech_start` → sub-chain ctx cancel + in-flight flush. The cancel path bypasses any congested queue, so the 200 ms budget survives overload.
- **Sessions decoupled from connections.** A monotonic epoch fences stale frames, a seq-watermark dedups replays (TCP ordering makes a reorder window unnecessary — honestly omitted), reconnect resumes mid-stream, idle reclaim is the backstop.
- **Observability as backdrop.** Hand-rolled Prometheus text format (~150 lines, zero deps) and an SSE waterfall dashboard; first-response = model latency + kernel overhead, measured separately so the kernel is never credited with the model's cost.

## Documentation

> The in-depth design docs live under `docs/` and are written in **Chinese** (中文), per project convention; code identifiers, commands and spec terms stay in English.

| Doc | Topic |
|---|---|
| `docs/M2-transport-design*.md` | Frame protocol & WS transport |
| `docs/M3-ringbuf-design*.md` | Vyukov slot-sequence SPSC ring, contention-free drop-oldest |
| `docs/M5-pipeline-design*.md` | Orchestration, dual backpressure, latency decomposition |
| `docs/M6-vad-bargein-design*.md` | Inline VAD & barge-in state machine |
| `docs/M8-session-design.md` | Session lifecycle, epoch, replay dedup |
| `docs/M9-metrics-design.md` | Metrics & waterfall dashboard |
| `docs/M10-loadgen-design.md` | Load harness, capacity curve, wall attribution |
| `docs/M11-bench-tuning-design.md` | channels vs ring benchmark, pprof zero-alloc iteration |
| [`docs/sherpa-adapter-zh.md`](docs/sherpa-adapter-zh.md) | Wiring open-source sherpa-onnx ASR/TTS (sidecar + WS client) |
| [`docs/protocol-spec-zh.md`](docs/protocol-spec-zh.md) | Wire protocol spec (WS frame layout + `.proto`) |
| [`docs/ops-manual-zh.md`](docs/ops-manual-zh.md) | Deploy / run / metrics / load-test manual |

Getting started from zero: `docs/concepts-zh.md` (glossary) → `docs/study-guide-zh.md` (code reading order) → `docs/project-deep-dive-zh.md` (design rationale & trade-offs). Full proposal/design/spec/tasks: `openspec/changes/streaming-multimodal-agent-engine/`.

## Scope (v1)

WebSocket + PCM 16 kHz / 16-bit / mono. Models are pluggable: **LLM** = any OpenAI-compatible SSE endpoint (cloud or local), **ASR** = open-source sherpa-onnx (local sidecar), **TTS** = sherpa-onnx or any OpenAI-compatible `/audio/speech` endpoint — switched entirely by config, no kernel changes. FFmpeg transcoding and gRPC/WebTransport transport are deferred to future, separate changes.

## License

[MIT](LICENSE) © 2026 cuagain886
