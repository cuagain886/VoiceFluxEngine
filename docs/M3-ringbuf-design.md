# M3 — SPSC Lock-Free Ring Buffer (Design)

Status: **implemented**.

Maps to spec `ring-buffer` and design decisions **D2** (ring at audio edges
only), **D3** (dual backpressure). Tasks: 3.1–3.5.

## 1. Purpose & placement

The ring buffer is the audio hot-path primitive — it exists at exactly **two
places** (D2):

```
transport ──ingress ring──▶ ASR          (one producer: WS read goroutine)
TTS       ──egress  ring──▶ transport    (one producer: TTS stage)
```

Everything between text stages (ASR→LLM, LLM→TTS) uses plain channels: text is
low-frequency and *blocking there is the backpressure mechanism* (D3). Audio is
high-frequency (50 fps) and must never block the socket reader, so the ring
absorbs bursts and sheds load by dropping the **stalest** frames — for
real-time speech, a fresh frame is worth more than a complete history.

The ingress stays strictly SPSC because VAD runs *inline* in the read goroutine
(D4 in `design.md`), not as a second consumer.

## 2. The core design problem: drop-oldest breaks classic SPSC

A textbook SPSC ring needs only two atomic cursors — `tail` written by the
producer, `head` written by the consumer — and no CAS at all. We deliberately
**did not** build that, because the spec requires the `drop-oldest` policy:
when the ring is full the producer must evict the oldest element and keep the
newest.

Eviction means **the producer advances `head`** — the read cursor suddenly has
two writers. With plain cursors this is unfixable:

- If the producer just overwrites the slot, the consumer may be mid-copy of
  that very slot → torn read, and a hard data race under `-race`.
- "Read, then validate-and-discard" variants still perform the racy read; the
  race detector (correctly) flags them even though the value is discarded.

### Solution: per-slot sequence gates (Vyukov bounded-queue scheme)

Every slot carries its own `atomic.Uint64` sequence that encodes the slot's
state relative to the monotonically growing cursors:

```
seq == t        slot is free; producer at tail position t may write it
seq == t + 1    slot is published; consumer at head position t may read it
seq == h + cap  slot consumed at position h; free again next lap
```

- **Push** (producer-only, no CAS): check `slots[t&mask].seq == t`, write the
  value, release with `seq = t+1`, advance `tail`.
- **Pop** (consumer *and* producer-on-evict ⇒ **CAS on `head`**): check
  `seq == h+1`, then `CAS(head, h, h+1)`. Whoever wins the CAS owns the slot
  exclusively until releasing it with `seq = h+cap`. The loser reloads and
  retries.
- **Drop-oldest** is then trivially safe: `Push` on full = `pop()` (discard,
  `dropped++`) + retry `tryPush`.

Every data access to `slot.val` is ordered through the slot's own atomic, so
the structure is race-free *by construction* — `-race` clean is a property of
the algorithm, not of lucky scheduling.

A subtlety this scheme also fixes: a consumer holding a stale `head` can tell
"empty" (`seq < h+1`) apart from "head moved under me by an eviction"
(`seq > h+1`) via signed sequence difference, and retries instead of falsely
reporting empty.

**Cost of the trade**: one CAS per pop (uncontended in steady state — the
producer only touches `head` when full), and 8 bytes of sequence per slot.
**What we keep**: push stays CAS-free, FIFO order, bounded memory, and a
provably race-free eviction path.

### Known limitation (accepted)

During an eviction the producer may briefly spin while the consumer is inside
its ~2-instruction claim window (CAS won, `seq` not yet released). The
structure is lock-free, not wait-free. The window is a few nanoseconds; if the
consumer goroutine is descheduled exactly there, the producer spins until it
returns. Acceptable for v1; noted for honesty (CLAUDE.md convention 4).

## 3. Memory layout — cursors on private cache lines

```go
type Ring[T any] struct {
    mask   uint64        // read-only after New
    policy Policy        //   "
    slots  []slot[T]     //   "
    _      [64]byte
    tail    atomic.Uint64 // written ~every Push (producer core)
    _      [64]byte
    head    atomic.Uint64 // written ~every Pop  (consumer core)
    _      [64]byte
    dropped atomic.Uint64
    _      [64]byte
}
```

`tail` and `head` are written by *different cores* at frame rate. If they
shared a cache line, every write on one core would invalidate the line on the
other (false sharing) and both sides would stall on cache-coherence traffic.
The leading pad also keeps the hot cursors off the read-only header fields
(`mask`, `slots`) that both sides load on every operation.

Measured on a 13th-gen i7 (16 threads), same algorithm with and without the
padding, `int` elements, capacity 1024:

| variant | ns/op (steady) | allocs/op |
|---|---|---|
| `BenchmarkSPSCPadded` | **~55** | 0 |
| `BenchmarkSPSCUnpadded` | ~320–340 | 0 |

**≈6× throughput from 192 bytes of padding.** This is the quantified answer to
"why pad" (task 3.5); the unpadded twin lives in `ring_bench_test.go` and is
test-only.

Adjacent *slots* still share cache lines by design — padding every slot would
multiply the memory footprint and wreck cache locality for the common
drain-in-order pattern. The hot contention is on the cursors, which is where
the padding goes.

## 4. Zero allocation in steady state

Two halves:

1. **The ring itself**: slots are pre-allocated in `New`; `Push`/`Pop` copy the
   element value in/out of the slot. No per-operation allocation, ever.
2. **The payload bytes** (`BufferPool`): a free list of fixed-capacity
   `[]byte`, implemented as *another* `Ring[[]byte]` flowing in the opposite
   direction of the data ring — the data producer is the pool's only `Get`ter,
   the data consumer its only `Put`ter, so the SPSC discipline is preserved
   with roles swapped. `sync.Pool` was rejected: `Put(&b)` heap-allocates the
   slice header on every cycle, which defeats the purpose.

The pool degrades instead of failing: `Get` on an exhausted free list falls
back to `make` (counted in `Misses()` — a metrics hook for M9), and `Put` of
excess or undersized buffers lets them fall to the GC.

Gates (both in CI via `go test` + bench):

- `TestSteadyStateZeroAlloc`: `testing.AllocsPerRun` over a full
  pool→ring→pool cycle with 640-byte PCM frames == **0**.
- `BenchmarkSPSCAudioFrame` (the realistic ingress shape, 640-byte frames
  through pool + ring): **~175 ns/op, 0 allocs/op**.

## 5. Full-buffer policies (D3 dual backpressure)

| policy | behaviour | used at |
|---|---|---|
| `DropOldest` | evict stalest element, `Dropped()++`, keep newest | audio ingress/egress (default) |
| `Reject` | `Push` returns `false` → caller feels backpressure | pool free list; anywhere lossless |

Configurable per edge via `ring_buffer.ingress_policy` / `egress_policy`
(validated through `ringbuf.ParsePolicy` at config load). The drop counter is
the raw input for M9's drop-rate metric and M10's "which wall did we hit
first" analysis.

Deterministic semantics check (`TestDropOldestDeterministic`): pushing 0..11
into a capacity-8 DropOldest ring yields `Dropped()==4` and pops 4..11 — oldest
evicted, newest kept, order preserved.

## 6. Other decisions

- **Cursors are `uint64` and never wrap-handled.** At 50 fps a session would
  need ~11.7 billion years to overflow. The signed-difference comparison
  (`int64(seq - (h+1))`) is wrap-correct anyway.
- **`Len()` is a racy snapshot** (two independent atomic loads), clamped to
  `[0, Cap]` — fine for metrics, documented as such, never used for
  correctness.
- **Generic `Ring[T]`**: the pipeline (M5) will instantiate it with its frame
  struct; the pool instantiates `Ring[[]byte]`. No interface boxing — the
  element is stored inline in the slot, which is also what makes the zero-alloc
  property hold.
- **Popped slots are zeroed** (`s.val = zero`) so a `[]byte`-bearing element
  doesn't pin its backing array until the slot's next lap.

## 7. Acceptance / testing (3.5)

- `-race` suite: lossless FIFO order under concurrency (`TestConcurrentFIFO`,
  100k elements, exact sequence), concurrent eviction with accounting
  `received + dropped == pushed` and strict monotonic order
  (`TestConcurrentDropOldest`, capacity 8 to force constant eviction races).
- Empty-pop returns `ok=false` without blocking; reject-when-full leaves the
  drop counter untouched; non-power-of-two capacities rejected at `New`.
- Benchmarks: see tables above; `BenchmarkDropOldestSaturated` (producer with
  fully stalled consumer) ≈ 100 ns/op, 0 allocs — the worst case still costs
  about two normal operations (one evict + one push), no cliff.

## References

- Spec: `openspec/changes/streaming-multimodal-agent-engine/specs/ring-buffer/spec.md`
- Design decisions: D2 (audio edges only), D3 (dual backpressure), D4 (inline
  VAD keeps ingress SPSC).
- Algorithm lineage: D. Vyukov, bounded MPMC queue (per-slot sequences),
  specialized here to single-producer push + dual-claimant pop.
- The channels-vs-ring comparison benchmark is deliberately deferred to L3
  (task 11.1), where it runs against the *real* pipeline workload.
