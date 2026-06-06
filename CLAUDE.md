# CLAUDE.md — voicestream

Real-time multimodal voice agent **streaming kernel** in Go. A model-agnostic
middleware that sits between mic/speaker and AI models; its only job is
**Timing / Backpressure / Interruption (时序 / 背压 / 打断)**. ASR/LLM/TTS are
pluggable tenants, not part of the kernel.

Full proposal / design / specs / tasks live in
`openspec/changes/streaming-multimodal-agent-engine/`. Per-module design docs
live in `docs/`.

## Module layout (`module voicestream`)

| Path | Responsibility | Milestone |
|------|----------------|-----------|
| `cmd/server` | entrypoint | M1 |
| `internal/config` | runtime config (load/validate) | M1 |
| `internal/transport` | WebSocket transport + binary frame protocol | M2 |
| `internal/ringbuf` | SPSC lock-free ring buffer (audio edges only) | M3 |
| `internal/adapter` | ASR/LLM/TTS streaming interfaces + mock | M4 |
| `internal/pipeline` | ASR→LLM→TTS orchestration + dual backpressure | M5 |
| `internal/vad` | inline energy VAD + barge-in state machine | M6 |
| `web/` | browser demo client (getUserMedia AEC, playout buffer) | M7 |
| `internal/session` | session lifecycle, reconnect, replay dedup | M8 |
| `internal/metrics` | latency / drop instrumentation | M9 |
| `internal/loadgen` | load harness + capacity curve | M10 |

## Commands

- Build: `go build ./...`
- Test (race): `go test -race ./...`
- Vet / lint: `go vet ./...` ; `golangci-lint run`
- Run server: `go run ./cmd/server` (use package mode, not single-file)
- Bench: `go test -run=^$ -bench=. -benchmem ./...`
- Regen protobuf: `protoc --go_out=. --go_opt=paths=source_relative <proto>`
  (toolchain: `protoc` + `protoc-gen-go`)

## Conventions (MUST follow)

1. **Git per update** — every change set is committed with a clear, focused
   message. `main` must build green (`go build ./...`, `go vet ./...`,
   `go test -race ./...` all pass) before committing.
2. **Docs per module** — when a module/milestone completes, write its
   design + rationale doc under `docs/` (e.g. `docs/M2-transport-design.md`).
3. **Decisions live in OpenSpec** — design decisions → `design.md`; new/changed
   requirements → `specs/`. Never silently diverge from the specs; update them.
4. **Honesty over hype** — do not claim transport behaviors that TCP/WS already
   provide (e.g. no in-session reorder window over a single WS connection).
   See design D12/D13.

## Scope guardrails

- v1 = **WebSocket only**, **PCM 16k/16-bit/mono** assumed at the wire, models
  **cloud-first** (one real streaming LLM early; ASR/TTS mock → real).
- **Deferred to future changes**: FFmpeg audio-codec, gRPC transport.
- **North Star L0**: browser conversation with natural barge-in (= project
  exists). Then L1 latency dashboard, L2 capacity curve, L3 channels-vs-ringbuf
  benchmark.
- Payload encoding decision: **protobuf** for TEXT/CONTROL payloads; audio
  payload is raw PCM bytes (no protobuf wrapping).
