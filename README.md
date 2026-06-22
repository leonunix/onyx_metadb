# onyx-metadb

Embedded metadata engine for [Onyx Storage](https://github.com/leonunix/onyx_storage).

Purpose-built to replace RocksDB for Onyx's metadata plane. Onyx's three
metadata workloads have very different access patterns and a single
general-purpose LSM cannot serve them all well at the same time, so each one
gets its own structure under a single shared WAL:

| Workload | Access pattern | Structure |
|----------|----------------|-----------|
| L2P (LBA → `L2pValue`) | per-volume, dense int keys, point + range, snapshot / clone, multi-writer | Per-volume paged COW radix tree |
| Refcount (PBA → u32) | uniform-random keys, point read / incref / decref, no snapshots | Per-shard paged-array + delta map (apply lane drains delta into pages at commit boundaries) |
| Dedup index (SHA-256 → 27 B entry) | global, uniform-random keys, point lookups dominate, append-heavy | On-disk cuckoo (4 slots / bucket) with a four-tier cache (L0 fingerprint set, L1 hot LRU, shared `PageCache`, on-disk pages) |
| Dedup reverse (PBA → set of hashes) | one access pattern: prefix-scan-by-PBA on refcount drop | Paged radix + overflow chains (no SST scan) |

All four share one WAL so cross-index updates commit atomically in one fsync.
L2P snapshots are per-volume, O(1) to take, and diffable — Onyx can compute
the exact set of keys that become reclaimable when a snapshot is dropped.

> Earlier phases used a sharded COW B+tree (refcount) and fixed-record LSMs
> (dedup_index, dedup_reverse). Both are retired (`src/btree` and `src/lsm`
> deleted in `0e1c69e`); current code in `src/refcount`, `src/dedup`, and
> `src/paged_reverse` replaces them with paged structures sized for Onyx's
> actual access patterns.

## Why not RocksDB / redb / fjall

- **RocksDB**: LSM read/write amplification kills dedup; single DB with both
  workloads fights itself; varlen/CF abstractions cost dearly for fixed-size
  records.
- **redb**: single-writer per DB. Onyx has N concurrent flush lanes — a
  serialized root-COW is a hard throughput ceiling.
- **fjall**: pure LSM. Bolting a B+tree partition onto its shared-WAL protocol
  is equivalent to a rewrite.

See [`docs/DESIGN.md`](docs/DESIGN.md) for the full rationale and architecture.

## Status

| Phase | Delivers | Status |
|-------|----------|--------|
| 0     | Scaffolding, docs, CI | landed |
| 1     | WAL + page store + recovery | landed |
| 2     | B+tree single-writer (later retired) | landed |
| 3     | COW + refcount + snapshots | landed |
| 4     | Sharded multi-writer B+tree (later retired) | landed |
| 5     | Fixed-record LSM + PBA refcount (later retired) | landed |
| 6     | Transactions + WAL replay + `dedup_reverse` | landed |
| 6.5   | Paged COW radix L2P + bounded page cache + index pin | landed |
| 7     | Integration with onyx-storage (in-tree path dep, RocksDB removed from onyx) | landed |
| 8a    | Standalone soak hardening (`metadb-soak`, fault injection, proptest scale-up) — continuous gate for deep changes | continuous |
| Restructure | Refcount → paged-array + delta; `dedup_reverse` → paged radix + overflow; `dedup_index` → on-disk cuckoo + L0/L1; `src/btree` and `src/lsm` retired | landed |
| 8b    | Production polish (metrics, dumps, real-hardware soak, dedup-shard tuning, NVMe perf) | ongoing |

Current commits land on `main`. See [`docs/ROADMAP.md`](docs/ROADMAP.md) for
entry / exit criteria per phase.

## Public API at a glance

All reads and writes go through one `Db`. Transactions batch a set of ops
into one WAL record + one fsync.

```rust
let db = Db::create(path)?;                  // or Db::open

// Volume lifecycle (L2P is per-volume; refcount and dedup are global)
let vol = db.create_volume()?;               // returns VolumeOrdinal
let cloned = db.clone_volume(snap_id)?;      // O(1) snapshot-based clone
let promoted = db.promote_volume(cloned)?;   // drive clone promotion to completion (lineage independence)
let report = db.drop_volume(vol)?;           // batched decref + dedup_reverse cleanup

// Atomic batch via Transaction: one WAL record + one fsync
let mut tx = db.begin();
tx.insert(vol, lba, l2p_value);
tx.l2p_remap(vol, lba, new_value);           // atomic replace + old-PBA decref
tx.incref_pba(pba, 1);
tx.decref_pba(pba, 1);
tx.put_dedup(hash, dedup_value);             // or put_dedup_guarded for liveness check
tx.register_dedup_reverse(pba, hash);
tx.unregister_dedup_reverse(pba, hash);
let lsn = tx.commit()?;

// Point reads
let val: Option<L2pValue>   = db.get(vol, lba)?;
let rc:  u32                = db.get_refcount(pba)?;
let d:   Option<DedupValue> = db.get_dedup(&hash)?;

// Batched reads (shard-aware bucketing)
let vals  = db.multi_get(vol, &lbas)?;
let rcs   = db.multi_get_refcount(&pbas)?;
let hits  = db.multi_get_dedup(&hashes)?;
let revs  = db.multi_scan_dedup_reverse_for_pba(&dead_pbas)?;
let live  = db.multi_dedup_entries_are_live(&candidates)?;

// Range scan (L2P only)
for item in db.range(vol, lba_lo..lba_hi)? { let (k, v) = item?; }
db.scan_range_unordered_chunked(vol, range, |chunk| { /* ... */ })?;

// Range delete (logical delete + refcount cascade)
let lsn = db.range_delete(vol, start, end)?;

// Snapshots (L2P only — refcount is cumulative, dedup is global)
let snap = db.take_snapshot(vol)?;
let view = db.snapshot_view(snap).unwrap();
let diff = db.diff_with_current(snap)?;
let drop_report = db.drop_snapshot(snap)?;

// Cleanup helpers
let lsn = db.cleanup_dedup_for_dead_pbas(&dead_pbas)?;

// Diagnostics
let cache   = db.cache_stats();
let metrics = db.metrics_snapshot();
let snaps   = db.snapshots_for(vol);
```

## Layout

```
src/
  db.rs / db/       Db facade: shard routing, apply gate, commit_ops, L2P /
                    refcount / dedup / snapshot / volume lifecycle entry points
  tx.rs             Transaction API (WAL op buffering + commit)
  wal/              Append-only WAL: segments, group commit, recovery
  page_store.rs     4 KiB page allocator + free list (O_DIRECT on Linux);
                    background writeback worker with in-flight pid protection
  manifest.rs / manifest/
                    Double-buffered manifest: volume entries (ordinal + flags),
                    L2P / refcount / dedup shard roots + heads, checkpoint LSN,
                    chained meta pages
  paged/            Paged COW radix tree used for L2P
  paged_meta.rs     Page-IO + COW reuse layer (refcount array, paged_reverse)
  paged_reverse/    dedup_reverse paged-array + overflow chains
  refcount/         PBA refcount: per-shard paged array + DeltaMap, apply lane
  dedup/            dedup_index: L0 fingerprint set, L1 hot LRU, on-disk cuckoo
                    (4 slots / bucket), per-shard apply lanes
  dedup_types.rs    Hash32, DedupValue, size constants
  apply_gate.rs     RwLock between commit-apply and flush / snapshot / drop
  epoch.rs          Per-shard epoch slots
  affinity.rs       CPU pinning for apply lanes / WAL writer
  cache.rs          16-shard page cache (LRU, scan-resistant, dirty pin)
  metrics.rs        Runtime counters and latency accumulators
  recovery.rs       WAL replay on open (apply must be idempotent)
  verify.rs         Structural verifier + offline audit (metadb-verify backing)
  testing/          Fault injection + shared test harness
  bin/              CLI binaries (verify, soak, bench, dump, replay)

scripts/
  metadb_metrics_summary.py  Summarize soak metrics.jsonl into rates / latency / hints

docs/
  ASYNC_RECLAIM_PLAN.md  Plan for async discard / reclaim coalescing
  DESIGN.md              Architecture, on-disk formats, recovery semantics
  ONYX_INTEGRATION_SPEC.md / ONYX_INTEGRATION_PLAN.md / ONYX_SOAK_GUIDE.md
                         Onyx integration specification, session breakdown,
                         soak runbook (Phase 7 landed; specs stay current)
  ROADMAP.md             Phased plan + per-phase exit criteria
  TESTING.md             Test strategy: proptest, invariants, fault injection

tests/              Integration tests + proptests (crash / cache / clone / volume / wal)
```

## Build & test

```bash
cargo build
cargo test
cargo build --release
```

The repository carries ~570 `#[test]` markers (unit + integration + proptest
harnesses). Fault-injection tests and longer proptests are behind `#[ignore]`;
run with `cargo test -- --ignored` when preparing a release.

## Soak & diagnostics

`dev.sh` wraps the standalone soak harness and keeps run artifacts under
`.dev/soak/<timestamp>/`.

```bash
./dev.sh start 24h concurrent --restart-interval 2h --pipeline-depth 128
./dev.sh metrics
./dev.sh metrics-summary
./dev.sh verify
```

Useful environment overrides:

```bash
METADB_SOAK_OPS_PER_CYCLE=1000000
METADB_SOAK_THREADS=16
METADB_SOAK_PIPELINE_DEPTH=128
METADB_SOAK_ONYX_MAX_PBA=100000000
METADB_SOAK_FAULT_DENSITY_PCT=0
METADB_SOAK_SNAPSHOTS=0
```

The small-transaction soak stresses crash safety, WAL group commit, lock
ordering, and restart verification. It is intentionally not a substitute for
the future Onyx flusher-style batch metadata workload; 30w-class frontend IOPS
must be evaluated with batched metadata transactions on the target NVMe class
hardware.

## License

Apache-2.0
