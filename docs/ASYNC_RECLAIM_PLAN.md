# Async Reclaim Plan

Plan for turning high-frequency discard / range-delete / dedup cleanup work
into an asynchronous, coalesced reclaim pipeline after the current soak round
finishes.

## Why

The 2026-04-26 soak metrics showed that small synchronous reclaim ops pay mostly
fixed coordination cost, not data-structure mutation cost:

- `range_delete` averaged about 2 ms/call, but actual scan + apply was only
  about 60 us. The rest was mostly `drop_gate` wait and WAL/fsync.
- `cleanup_dedup_for_dead_pbas` became much cheaper overall once the soak
  harness batched freed PBAs. This matches the real Onyx flusher path, where
  dead PBAs flow through an async cleanup thread.

The lesson is: foreground paths should make logical visibility durable quickly;
physical reclaim should be batched and coalesced in the background.

## Goals

- Keep discard/delete logical visibility crash-safe and synchronous: once the
  caller gets success, reads must not return the discarded LBA.
- Move expensive physical reclaim work off the foreground path:
  refcount decref, dedup reverse cleanup, allocator free, and any GC follow-up.
- Coalesce adjacent or overlapping small ranges before issuing physical
  `range_delete` work.
- Preserve the existing replay model: WAL recovery must produce the same
  logical visibility and eventually-reclaimable state.
- Make every step observable in metrics before optimizing it.

## Non-goals

- Do not weaken read-after-discard semantics.
- Do not rely on leaf pinning as a prerequisite.
- Do not make background cleanup required for recovery correctness; it may lag,
  pause, or replay idempotently.

## Core Invariant

Split reclaim into two states:

```text
logical delete durable  ->  physical cleanup pending  ->  physical cleanup done
```

Only the first transition is user-visible. The second and third transitions are
space-reclamation work and may be delayed.

Reads must consult logical delete state before returning an L2P mapping. The
allocator must not reuse old PBAs until refcount/dedup cleanup has committed.

## Design Options

### Option A: Foreground Range Tombstone

Foreground writes a compact WAL op such as:

```rust
WalOp::L2pRangeTombstone {
    vol_ord,
    start,
    end,
    epoch,
}
```

Apply records a tombstone in memory / persisted metadata. Reads treat a mapping
under a newer tombstone as absent. A background worker later expands tombstones
into physical cleanup:

```text
tombstone ranges -> coalesce -> scan captured mappings -> decref -> dedup cleanup
```

Pros:
- Lowest foreground work.
- Natural coalescing of many small discards.

Risks:
- Read path must handle tombstone overlay correctly.
- Need tombstone compaction so old tombstones do not grow forever.

### Option B: Foreground Queue Record

Foreground keeps current logical `range_delete`, but appends freed PBA / range
cleanup jobs into a durable queue. Background drains and batches cleanup.

Pros:
- Smaller change to read path.
- Good first step for dedup cleanup and allocator free.

Risks:
- Small `range_delete` still pays one WAL/fsync and gate acquisition per call.
- Does not solve high-frequency tiny discard fully.

### Preferred Path

Start with Option B for cleanup batching if Onyx needs a near-term bridge, then
move to Option A for real discard scalability. Option A is the long-term design.

## Implementation Sessions

### A1. Metrics Before Mechanics

- Add summary output for:
  - `range_delete_drop_gate_wait_us`
  - range delete WAL/chunk cost
  - pending cleanup queue depth
  - async cleanup batch size and latency
- Exit: `metadb_metrics_summary.py` explains whether range cost is scan,
  apply, WAL, gate wait, or cleanup.

### A2. Async Dedup Cleanup Queue

- Keep logical remap/range-delete behavior unchanged.
- Move freed PBA cleanup behind a durable or replay-derived queue.
- Batch by count and time:
  - `cleanup_batch_max_pbas`
  - `cleanup_batch_max_delay_ms`
- Preserve idempotency: repeated cleanup of the same dead PBA is a no-op.
- Exit tests:
  - crash before cleanup enqueue
  - crash after enqueue before cleanup
  - crash mid-cleanup
  - duplicate PBA in batch
  - hash re-registered to a new PBA before cleanup

### A3. Range Delete Coalescer

- Add a foreground pending-range queue for discard-like operations.
- Coalesce adjacent / overlapping ranges per volume.
- Background emits larger physical `L2pRangeDelete` operations.
- Exit tests:
  - random overlapping ranges vs reference model
  - crash/reopen with pending ranges
  - concurrent remap racing with pending range
  - snapshot pinning still suppresses invalid decref

### A4. Range Tombstone Read Overlay

- Add persisted range tombstones with monotonically increasing epochs / LSNs.
- Read path checks tombstone overlay before returning a live L2P value.
- Background cleanup removes tombstones only after all covered old mappings are
  physically reclaimed or proven absent.
- Exit tests:
  - read-after-discard returns `None` immediately
  - remap after discard is visible
  - old mapping cannot reappear after crash replay
  - tombstone compaction preserves visibility

### A5. Production Tuning

- Tune batch sizes with real Onyx workloads on NVMe:
  - discard throughput
  - p99 foreground write latency
  - cleanup lag
  - allocator delayed-free backlog
  - WAL batch size and fsync rate
- Exit: default config documents safe batch sizes and backpressure thresholds.

## Open Questions

- Should range tombstones live in a small side LSM, a per-volume interval tree
  persisted in metadb pages, or a WAL-replayed in-memory structure checkpointed
  through manifest?
- Should foreground discard return after tombstone WAL fsync, or can Onyx
  tolerate an even lighter acknowledged queue record?
- How much cleanup lag can Onyx tolerate before allocator pressure must
  backpressure foreground writes?
- Should drop-snapshot freed PBA cleanup use the same async queue, or stay
  synchronous until snapshot churn becomes a measured bottleneck?

## Soak Coverage

Keep two soak modes:

- `realistic`: async/batched cleanup and coalesced range work. This predicts
  normal Onyx behavior.
- `pathological`: `--cleanup-batch-size 1` plus high-frequency tiny
  `range_delete`. This keeps pressure on gates, WAL ordering, and mutex edges.

Both modes must keep `metadb-verify --strict` clean across restarts.
