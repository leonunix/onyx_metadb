# onyx-metadb Roadmap

Phased plan to reach a v0.1 that onyx-storage can depend on in production.

Each phase has:
- **Scope**: what gets built.
- **Entry criteria**: what must be true before starting.
- **Exit criteria**: what must be true before calling the phase done. Every
  exit criterion is testable; no hand-waving.
- **Test work**: the testing deliverable that lands in the same PR set.
- **Risk / decisions**: the concrete choices resolved in this phase.

Time estimates are for single-engineer throughput, full-time focus.
Optimistic but not unreasonable — padding lives in the hardening tail.

---

## Phase 0 — Scaffolding (done in this commit)

Cargo workspace member, docs, CI skeleton. No engine code.

**Exit**:
- `cargo build` compiles the empty lib.
- `cargo fmt --check` and `cargo clippy -- -D warnings` pass in CI.
- `docs/DESIGN.md`, `docs/ROADMAP.md`, `docs/TESTING.md` committed.

---

## Phase 1 — WAL + page store + recovery skeleton  (3 weeks)

First vertical slice: durably write bytes, read them back, survive crashes.
No index yet; pages are opaque blobs.

### Scope

- 4 KiB page file with:
  - Header, CRC32C, refcount, generation (see DESIGN §5.1).
  - Allocate / free / read / write primitives.
  - In-memory free list rebuilt at open.
- Append-only WAL:
  - Segment files, CRC'd records, rotation on segment full.
  - Group commit queue (submit → batched fsync → ack LSN).
- Manifest v0:
  - Double-buffered header with CRC + sequence number.
  - Holds `L_ck`, free list head, WAL tail hint.
- Recovery:
  - Pick newest valid manifest.
  - Replay WAL from `L_ck + 1` to tail.
  - Truncate torn WAL tail.
- Fault-injection framework behind `#[cfg(feature = "fault-injection")]`:
  - Named injection points (`wal.fsync.before`, `page.write.after`, ...).
  - Hooks configurable via env var or test harness.

### Exit criteria

- Property test: random sequence of page-store ops (alloc/free/write/read)
  with reference model → all reads match reference.
- Property test: submit N txns of random ops, kill at random injection
  points, recover, compare state to an in-memory reference. 1000+ seeds
  pass.
- Throughput smoke: 200k commits/s sustained on a local NVMe with
  group-commit enabled. (Number is a sanity check on the WAL, not a product
  SLA — but a regression here is a red flag.)
- Docs: recovery protocol documented with sequence diagram.

### Decisions resolved

- Free list persisted vs. rebuilt: start rebuilt (scan page headers on
  open). Simpler. Revisit if open time is unacceptable.
- WAL segment size: default 64 MiB. Tunable.
- O_DIRECT: Linux yes, macOS F_NOCACHE.

---

## Phase 2 — B+tree, single-writer, single-partition  (3 weeks) — **landed**

Correctness-first B+tree. No COW. No snapshots. One partition. One writer
thread. In-place page edits.

### Delivered

- [`btree::format`](../src/btree/format.rs): leaf (112 × 36 B entries) and
  internal (251 keys + 252 children in fixed-offset regions) page formats.
- [`btree::cache::PageBuf`](../src/btree/cache.rs): per-BTree HashMap cache
  with dirty tracking, alloc / read / modify / flush. No eviction yet —
  working set is bounded by tree depth.
- [`btree::tree::BTree`](../src/btree/tree.rs): create / open / get /
  insert (with split) / delete (with borrow + merge + root collapse) /
  range (RangeBounds<u64>) / flush. Generation counter monotonic per
  mutation.
- [`btree::invariants`](../src/btree/invariants.rs): structural checker —
  keys ascending, separator brackets, leaf-depth uniformity, fill
  thresholds, root not zero-key.
- [`tests/btree_proptest.rs`](../tests/btree_proptest.rs): proptest vs
  `std::collections::BTreeMap` reference, invariants after every op +
  20 k-op deterministic ChaCha8 stress.

### Deferred to phase 2 hardening (combined with phase 1 hardening)

- Clock-pro page cache with pinning (phase 8).
- WAL replay of L2P ops (phase 6 — unified transaction layer).
- Crash-injection tests at btree.split.before_parent_update /
  btree.merge.mid_parent_update — fault points still present but not yet
  exercised via recovery tests.
- 1 M-op / 1000-seed proptest budget (currently 64 cases × 500 ops).

### Decisions resolved

- Latch protocol: none (single writer, `&mut self`). Phase 4 will wrap
  with sharding.
- Underflow threshold: 50% fill (MAX / 2). At exactly threshold, two
  merging siblings + pivot fit in one page exactly; any higher threshold
  would force borrows where merges would work.
- Key encoding: big-endian u64 (byte-wise sort matches numeric sort).
- Separator convention: `keys[i]` is the first key present in the subtree
  rooted at `children[i+1]`. `internal_search` returns the child index
  via `partition_point(|&k| k <= needle)`.
- No sibling pointers in leaves. Range scan walks the stack of ancestor
  internals; the COW path in phase 3 would otherwise need to propagate
  sibling-pointer updates across many pages per write.

---

## Phase 3 — COW + refcount + snapshot primitives  (3 weeks)

Add snapshot capability. All writes become COW.

### Scope

- Per-page refcount maintained through writes, splits, merges.
- `take_snapshot` / `drop_snapshot` / `snapshot_view`.
- Diff iterator with page-id Merkle skip.
- Manifest v1 gains `snapshots` table.

### Exit criteria

- Property test:
  - Arbitrary interleavings of puts/deletes/snapshots/drops/reads.
  - Snapshot reads return the state at snapshot LSN (checked against a
    `Vec<BTreeMap>` history reference).
  - After drop_snapshot, no page it uniquely owned remains allocated.
  - Diff(A, B) produces exactly the set of keys where `B.get(k) !=
    A.get(k)` (computed against reference).
- Stress test: 1000 snapshots, random drop order, run
  `metadb-verify`; no dangling pages, no negative refcounts.
- Crash injection in COW cascade (`cow.new_page_written.before_parent_link`)
  → recovery never observes a half-committed root pointer.

### Decisions resolved

- Snapshot taxonomy: only "application" snapshots exposed via API. Internal
  "checkpoint snapshots" (short-lived) not exposed to users in v0.1.
- Refcount width: 32 bits. A page shared across 2³² snapshots is not a
  scenario we plan for in v0.1; we will fail-loud on overflow.

---

## Phase 4 — Sharded multi-writer B+tree  (2 weeks)

Scale single-partition write throughput.

### Scope

- N independent B+tree shards per partition.
- Shard router (xxh3 → shard id).
- Writers issue concurrent WAL submissions; group commit coalesces.
- Range scan fans out across shards and merges.

### Exit criteria

- Concurrency stress: 16 writers × 4 shards × mixed workload for 30
  minutes, no assertion failures, no deadlocks (lock-order audit script
  added to CI).
- Throughput: N writers give ≥ 0.8× N single-writer throughput up to CPU
  or disk saturation.
- Snapshot take / drop still correct under concurrent writers (property
  test extended).
- Fan-out range scan order-preserving (verified against reference).

### Decisions resolved

- Shard count default: 16 per partition. Configurable.
- Lock ordering: always acquire by ascending shard id to prevent deadlock
  on cross-shard operations (in practice, only `drop_partition` needs
  this).

---

## Phase 5 — Fixed-record LSM for dedup (+ PBA refcount)  (3 weeks)

Second index type. Integrates with the shared WAL and manifest.

### Scope

- Memtable with immutable handoff.
- SST format (DESIGN §5.4): bloom header + sorted fixed records.
- L0..Ln leveled layout, compaction thread.
- Dedup ops: `put_dedup`, `delete_dedup`, `get_dedup`.
- PBA refcount: implemented as a second LSM or as a dedicated paged
  counter depending on phase-5 spike.

### Exit criteria

- Property test vs. `BTreeMap<[u8; 32], Entry>` reference for
  put/get/delete/compact. 1M-ops runs × 1000 seeds.
- Compaction stress: 10 GiB of inserts, verify all records queryable at
  every level of the compaction tree.
- Cross-index transaction test: `put(partition, lba, v) &&
  put_dedup(hash, e) && incref(pba, 1)` all visible after commit, all
  absent after crash-before-commit.
- PBA refcount cleanup: when refcount hits 0 during a commit, the commit
  record includes the dedup-reverse cleanup ops atomically (see
  DESIGN §2.2).

### Decisions resolved (spike before coding)

- PBA refcount as LSM vs. paged B+tree vs. paged counter array:
  - LSM has write-amp tail; paged array has sparse-file waste; B+tree
    adds a second index type managing refs. Spike each for a week, pick.
  - Default bet: second B+tree partition (reuses the phase 2-4 code),
    unless spike reveals a problem.

---

## Phase 6 — Transaction layer + MVCC reads  (2 weeks)

Tie the two indexes together under one transaction API.

### Scope

- `Transaction` type buffering ops for both indexes + refcount.
- `commit()` packs all ops into one WAL record.
- Apply phase updates all index memtables / page caches under one
  visibility fence.
- `SnapshotId`-bound reads for dedup and refcount (not just B+tree).

### Exit criteria

- All previous property tests still pass under the unified API.
- Read-your-writes: a txn that commits successfully is visible on the
  next read from any thread.
- Snapshot isolation: a snapshot taken at LSN `L` sees exactly the state
  as of `L`, including dedup entries.
- Benchmarks: mixed workload (80% B+tree put, 10% dedup put, 10%
  incref), target ≥ 150 k txns/s on local NVMe with 16 writers.

---

## Phase 7 — Integration with onyx-storage  (2 weeks)

Replace RocksDB usage in onyx-storage.

### Scope

- `onyx-storage/src/meta` becomes a thin adapter over `onyx-metadb`.
- Migration tool `metadb-import-rocks`: RocksDB CFs → metadb.
- End-to-end: create volume → write → snapshot → overwrite → diff →
  drop snapshot → reclaim.
- Soak harness integration (onyx-storage's stability_harness points at
  metadb).

### Exit criteria

- Full onyx-storage test suite passes with metadb backend.
- Soak test 24h at target load, no corruption, no unbounded growth.
- Perf comparison report committed: metadb vs. RocksDB on the same
  workload, measuring put latency p50/p99, get latency p50/p99,
  metadata commit fsync latency, space overhead, CPU, write amplification.

---

## Phase 8 — Hardening  (ongoing; 4+ weeks before production)

Continuous work that doesn't gate earlier phases but gates production.

### Scope

- Fuzz campaigns: WAL parser, page decoder, manifest decoder, bloom.
- Week-long soak with fault injection on real hardware.
- Perf tuning: page cache policy, compaction throttle, group-commit
  timing.
- Operational tooling: metrics (Prometheus-exporter-compatible), tracing
  integration, `metadb-dump` / `metadb-verify` / `metadb-replay`.
- Documentation: recovery playbook, tuning guide.

---

## Summary table

| Phase | Weeks | Cum. weeks | Delivers                               |
|-------|-------|-----------:|----------------------------------------|
| 0     | ~1    |   1        | Scaffolding, docs, CI                  |
| 1     | 3     |   4        | WAL + page store + recovery            |
| 2     | 3     |   7        | B+tree single-writer                   |
| 3     | 3     |  10        | COW + refcount + snapshots             |
| 4     | 2     |  12        | Sharded multi-writer B+tree            |
| 5     | 3     |  15        | Fixed-record LSM + PBA refcount        |
| 6     | 2     |  17        | Unified transactions                   |
| 7     | 2     |  19        | Onyx integration + migration           |
| 8     | 4+    |  23+       | Hardening                              |

Total to a production-usable v0.1: ~5 months.
