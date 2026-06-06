# M2 — Frame Protocol & WebSocket Transport (Design)

Status: design (implementation pending) · Decision: **Option B** (real protobuf
payloads via protoc toolchain).

Maps to spec `streaming-transport` and design decisions **D1, D7, D12, D13**.
Tasks: 2.1–2.7.

## 1. Purpose & deliverable

M2 builds the **wire layer** — the bottom of the kernel — in three sub-layers:

```
③ Conn abstraction (transport-agnostic)   ReadFrame / WriteFrame / Close   ← upper layers use only this
② WebSocket endpoint                       coder/websocket, binary messages ← v1's only implementation
① Frame codec                              fixed binary header + payload    ← the hand-rolled protocol
```

**Deliverable = an echo loop**: server accepts a WS connection, reads a Frame,
and writes it back unchanged. This exercises every part of the transport
(upgrade, decode, encode, bidirectional flow). Once echo round-trips byte-for-byte,
the transport layer is proven; M3–M5 later replace "echo" with the real pipeline.
After M2 the process stays **resident, listening on :8080** (unlike the M1
scaffold which exits immediately).

## 2. Frame format — fixed binary header + typed payload (hybrid)

```
 offset  size  field     type        rationale
 ──────────────────────────────────────────────────────────────────────
 0       2     magic     0x56 0x53   sync / sanity; fail-fast on garbage
 2       1     version   uint8       reject incompatible protocol versions
 3       1     type      uint8       demux: AUDIO / TEXT / CONTROL
 4       8     seq       uint64 BE   monotonic; replay dedup + (future UDP) reorder detect
 12      8     ts_us     int64  BE   sample-clock PTS — audio/token alignment (D7)
 20      4     length    uint32 BE   payload size; bounds-checked (reject > maxPayload, e.g. 64KB)
 ──────────────────────────────────────────────────────────────────────
 24      ...   payload   bytes       AUDIO = raw PCM; TEXT/CONTROL = protobuf
```

24-byte fixed header, **big-endian** (network byte order).

**Why this shape:**

- **Fixed offsets → zero-allocation parse.** Decoding is `binary.BigEndian.Uint64(buf[4:12])`
  style index-and-read; no object allocation, no reflection. Serves the
  zero-alloc audio hot-path goal.
- **magic + version + length = "don't trust the wire."** Maps directly to the
  spec scenarios: garbage dies fast (magic), incompatible versions are rejected
  (version), partial/oversized headers don't cause out-of-bounds reads (length
  bound-check).
- **Payload split by type.** Audio is high-frequency (50 fps) opaque bytes —
  wrapping it in protobuf would burn CPU/allocations for no structural benefit,
  so audio payload is **raw PCM**. TEXT/CONTROL are low-frequency and evolve —
  they use **protobuf** (compact, schema evolution, codegen). Right tool per layer.

### Why our own framing when WebSocket already frames?

A WebSocket binary message already carries a length and a boundary, so over WS
our `length` field looks redundant. We keep our own framing anyway:

1. **Transport independence (D12/D13).** The header must work over WebTransport
   datagrams and raw TCP — byte-stream transports with **no message boundaries**.
   Leaning on WS framing would lock us to WS. Self-framing is portable.
2. **Multiplexing.** One WS connection carries three logical streams
   (audio / text / control); `type` + `seq` demux them. WS knows nothing of our
   logical streams.

The header is designed for the worst case (byte stream) and still works over the
easy case (WS messages). v1 uses **1 Frame = 1 WS binary message** (no batching).

## 3. Payload encoding — protobuf (Option B)

TEXT and CONTROL payloads are protobuf messages compiled with `protoc` +
`protoc-gen-go`. Audio payload is raw PCM bytes (the header's `length` delimits
it). A `.proto` defines the frame `type` enum and the TEXT/CONTROL message
bodies (e.g. partial/final transcript, token, start/stop/barge-in control).

> Toolchain prerequisite: `protoc` (compiler) + `protoc-gen-go` (Go plugin) must
> be installed; see Open items.

## 4. Conn abstraction (D12 seam)

```go
// Upper layers (pipeline/session) depend only on this; they never import websocket.
type Conn interface {
    ReadFrame(ctx context.Context) (Frame, error)
    WriteFrame(ctx context.Context, f Frame) error
    Close(code StatusCode, reason string) error
}
```

`wsConn` implements `Conn` over coder/websocket today; a future `wtConn`
implements the **same** interface over WebTransport — swapping transport touches
no upper-layer code. `ctx` on read/write enables cancellation (barge-in,
shutdown), which is why coder/websocket (context-based API) was chosen over
gorilla.

## 5. Connection goroutine model

coder/websocket permits one concurrent reader and one concurrent writer, so each
connection uses **1 read goroutine + 1 write goroutine**:

```
┌── read goroutine ──┐                 ┌── write goroutine ──┐
│ ReadFrame          │   ...pipeline   │ drain egress        │
│  └→ (M4+) inline VAD│   (echo in M2)  │  └→ WriteFrame      │
│  └→ ingress ring   │                 │                     │
└────────────────────┘                 └─────────────────────┘
```

M2 uses the simplest echo (read → write). The read/write split is the shape that
carries into M3–M5, where the middle becomes the real pipeline.

## 6. Heartbeat & low-latency knobs

- **Heartbeat (2.5):** use WebSocket built-in **ping/pong** for liveness;
  heartbeat timeout → emit a disconnect event to the session layer (M8). Our
  CONTROL frame channel is reserved for app semantics (start/stop/barge-in),
  not liveness.
- **Disable compression (2.6):** turn off permessage-deflate via coder/websocket
  `AcceptOptions` (compression adds latency/CPU on small real-time frames).
- **TCP_NODELAY (2.6):** Go's `net/http` already enables `TCP_NODELAY` on
  accepted connections by default — so this is **verify, not implement**. Don't
  re-do what the runtime already does.
- **Downlink pacing (2.6):** deferred — there is no real downlink in the echo;
  pacing lands with the real TTS egress (M5).

## 7. Acceptance / testing (2.7)

- **Unit:** build a Frame (type/seq/ts_us/payload) → Encode → Decode →
  fields equal + payload byte-equal. Negative tests: bad magic, incompatible
  version, truncated/oversized header all rejected without out-of-bounds reads.
- **Loopback:** start the server; `wscat`/browser connects, sends a frame,
  receives the identical bytes back. Process stays resident listening on :8080.

## 8. Open items

- Install `protoc` + `protoc-gen-go` before implementing task 2.1.
- Define the `.proto` message set for TEXT/CONTROL (kept minimal in M2;
  expanded as M4/M6 features land).
- `maxPayload` bound value (default 64KB) — confirm against expected frame sizes.

## References

- Spec: `openspec/changes/streaming-multimodal-agent-engine/specs/streaming-transport/spec.md`
- Design decisions: D1 (WS-first), D7 (sampling-clock PTS), D12 (positioning),
  D13 (low-latency transport & honest jitter).
