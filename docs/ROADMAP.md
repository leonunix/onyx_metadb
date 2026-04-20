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

## Phase 3 — COW + refcount + snapshot primitives  (3 weeks) — **landed**

Add snapshot capability. All writes become COW.

### Delivered

- [`btree::cache::PageBuf`](../src/btree/cache.rs): incref / decref
  (cascading on zero) / cow_for_write (O(fanout) child refcount bumps
  for internal nodes; fast-path no-op when refcount is 1).
- [`btree::tree::BTree`](../src/btree/tree.rs): top-down CoW descent
  in both insert and delete. Rebalance paths CoW siblings only when
  they need to be modified (borrow); merges just decref the absorbed
  sibling. Root CoW updates `self.root` so the manifest edge follows.
- [`btree::tree::BTree::diff_subtrees`](../src/btree/tree.rs):
  recursive parallel walker exploiting the CoW invariant that
  identical page ids imply identical subtree contents. Internals
  with matching separators recurse pair-wise, preserving Merkle skip
  at every level; diverging structure falls back to flatten+merge.
- [`manifest::Manifest` v2](../src/manifest.rs): adds
  `partition_root`, `next_snapshot_id`, and a fixed-offset snapshot
  table. Up to `MAX_SNAPSHOTS_PER_MANIFEST` (≈ 166) per slot;
  chained manifests remain future work.
- [`db::Db`](../src/db.rs): the glue. `create` / `open` / `flush` /
  `get` / `insert` / `delete` / `range` / `take_snapshot` /
  `snapshot_view` / `drop_snapshot` / `diff` / `diff_with_current`.
- [`tests/db_snapshot_proptest.rs`](../tests/db_snapshot_proptest.rs):
  proptest that mirrors Db against a `BTreeMap`-per-live-snapshot
  reference, asserting read-path equivalence after every op and
  cross-checking `diff(a, b)` against a naive merge-diff of the
  reference snapshots; plus a 5000-op deterministic ChaCha8 stress
  covering insert/delete/snapshot/drop/diff interleavings.

### Deferred to phase 3 hardening

- 1000-snapshot stress run (current proptest caps live snapshots at
  ~100 to stay under MAX_SNAPSHOTS_PER_MANIFEST until chained
  manifests land).
- `metadb-verify` binary (no dangling pages, no negative refcounts):
  the `invariants` module already covers structural checks for the
  btree itself, but a full page-reachability audit across snapshots
  hasn't been written.
- Crash injection at `cow.new_page_written.before_parent_link`:
  needs WAL integration (phase 6) to assert anything meaningful
  about recovery.
- CoW perf optimisation: internal-page CoW bumps every child's
  refcount (O(fanout) page writes). Common case is a no-op because
  refcount is 1 post-descent, but deep trees under heavy snapshot
  load will want batching. Phase 8.

### Decisions resolved

- Refcount width: 32 bits. A page shared across 2³² snapshots is not
  a scenario we plan for in v0.1; incref is overflow-checked and
  fails loud.
- CoW propagation: top-down during descent, not post-hoc during
  walk-up. Siblings are CoW'd only when the rebalance path actually
  mutates them; pages only read and then decref'd (e.g., the right
  sibling in a right-merge) are not cloned.
- Snapshot durability: `take_snapshot` bumps the root refcount AND
  flushes the manifest before returning. Partial-state window would
  leave the refcount bumped but the manifest entry absent — next
  flush drops the stray refcount, cost-free.
- Snapshot taxonomy: only application-visible snapshots; no separate
  "checkpoint snapshot" kind in v0.1.

---

## Phase 4 — Sharded multi-writer B+tree  (2 weeks) — **partially landed**

The sharded index layer is in-tree and tested. WAL-backed concurrent
commit / group-commit integration is still pending, so this phase is not
fully closed yet.

### Delivered

- [`manifest::Manifest` v3](../src/manifest.rs): current per-shard roots
  are stored inline in the manifest body; each snapshot stores its shard
  roots in a dedicated `SnapshotRoots` page. Legacy v2 single-root
  manifests still decode and are upgraded on open.
- [`page::PageType`](../src/page.rs): adds `SnapshotRoots` as an explicit
  page type so snapshot metadata is covered by the same CRC / verifier
  machinery as the rest of the page store.
- [`db::Db`](../src/db.rs): one `Mutex<BTree>` per shard, xxh3 router,
  thread-safe `&self` point ops, and fan-out `range` / `diff` /
  `snapshot_view` / `take_snapshot` / `drop_snapshot`.
- [`db::SnapshotView`](../src/db.rs): snapshot reads hold a shared guard so
  `drop_snapshot` cannot free snapshot-owned pages while a live view is
  still reading them.
- [`tests/db_concurrency.rs`](../tests/db_concurrency.rs): multi-writer
  stress (16 writers / 4 shards) plus a round-based snapshot/reference
  test that checks snapshot correctness under concurrent writers.
- [`tests/db_snapshot_proptest.rs`](../tests/db_snapshot_proptest.rs):
  existing snapshot property test now runs against the sharded `Db`
  implementation.

### Remaining to call phase 4 done

- WAL-backed concurrent commit path: writers still mutate shard trees
  directly. "submit to WAL, group-commit, then publish shard roots" stays
  coupled to phase 6's transaction layer.
- Throughput target not yet measured: we have correctness coverage, but no
  benchmark proving ≥ 0.8× N single-writer throughput up to saturation.
- Long soak / CI hardening still missing: no 30-minute randomized soak and
  no lock-order audit script in CI yet.

### Decisions resolved

- Shard count default: 16 per partition. Configurable via
  [`Config::shards_per_partition`](../src/config.rs).
- Lock ordering: cross-shard operations acquire shard locks in ascending
  shard id (the current implementation uses shard-vector order), so
  `flush` / `take_snapshot` / `drop_snapshot` share one deadlock-free order.

---

## Phase 5 — Fixed-record LSM for dedup (+ PBA refcount)  (3 weeks) — **landed**

Second index type. Phase 5 landed 5a–5e; the three items deferred at the
time (`dedup_reverse`, WAL-backed replay, and the 5f proptest) all landed
together under phase 6's transaction layer — see that section below.

### Decisions locked before coding

- **PBA refcount** is a **second B+tree shard group** inside `Db`, not a
  separate LSM. Reuses all of phase 2–4 code. Refcount value reuses the
  existing 28-byte `L2pValue` (u32 refcount in the first 4 bytes, 24 bytes
  reserved). No generics on BTree; the space overhead is acceptable for
  v0.1 and can be reclaimed in phase 8 if it shows up in the budget.
- **Dedup LSM records** are fixed 64 bytes: `[hash: 32][kind: 1][value:
  28][padding: 3]`. 63 records per 4 KiB payload, exact fit. Value is
  28 bytes (`DedupValue`) so it mirrors `L2pValue` layout and the writer's
  output buffer can be shared with L2P paths where helpful.

### Delivered

- **5a** [`src/lsm/{format,memtable}.rs`](../src/lsm): fixed 64-byte record
  codec; memtable = `BTreeMap<Hash32, DedupOp>` + `RwLock` with a
  one-active + one-frozen slot handoff. `LookupResult` distinguishes
  `Hit(v) | Tombstone | Miss` so newer tombstones correctly shadow older
  puts during a multi-level walk.
- **5b** [`src/lsm/{bloom,sst}.rs`](../src/lsm): double-hashing bloom
  (10 bits/entry default → 7 hashes; ≈ 1 % FP rate), contiguous
  `[header | bloom pages | body pages]` SST runs, `SstWriter` from a
  frozen memtable or a sorted slice, `SstReader` with bloom + range
  reject + binary search (intra-page + outer page narrowing), and
  `SstScan` for compaction input.
- **5c** [`src/lsm/{lsm,persist}.rs`](../src/lsm): `Lsm` facade with
  memtable → L0 flush, newest-first L0 fanout and disjoint L1+ scan,
  `LsmLevels` page chain for per-level SST-handle persistence, and
  `persist_levels` / `free_old_level_heads` for atomic manifest swap.
- **5d** [`src/lsm/compact.rs`](../src/lsm/compact.rs): `L0 → L1`
  (all L0 SSTs + overlapping L1) and leveled `Ln → Ln+1` (largest Ln
  victim + overlapping `Ln+1`). Peekable k-way merge with newest-source
  wins; tombstones drop at the deepest level. Compaction takes a
  modify-lock (serialises with flush) and a reader-drain RwLock barrier
  before freeing victim pages.
- **5e** [`src/manifest.rs`](../src/manifest.rs), [`src/db.rs`](../src/db.rs):
  manifest bumped to v4 (backwards-compatible read of v3, which upgrades
  in place on open). `Db` now owns both L2P and refcount shard groups
  plus a single dedup LSM. Refcount API: `get_refcount`, `incref_pba`,
  `decref_pba` (checked arithmetic; decref-to-zero removes the row).
  Dedup API: `put_dedup`, `delete_dedup`, `get_dedup`,
  `dedup_should_flush`, `flush_dedup_memtable`, `compact_dedup_once`.
  Snapshots capture both tree groups (two `SnapshotRoots` pages per
  snapshot entry); `drop_snapshot` releases both.

### Still deferred

- **1 M-op / 1000-seed LSM proptest budget**. Phase 6 landed the
  proptest skeleton (`tests/db_phase6_proptest.rs`) at a realistic case
  count (16 × 30-80 ops, single-thread); the original "LSM vs.
  `BTreeMap<[u8; 32], DedupOp>` at 1 M ops × 1000 seeds" budget is
  aspirational and belongs in phase 8 hardening.
- **Compaction stress bench** (10 GiB of inserts, queryable at every
  level). Rolled into phase 8 hardening.
- **Post-snapshot dedup reads from `SnapshotView`**. `SnapshotView`
  still only surfaces L2P reads today; exposing dedup / refcount reads
  at a `SnapshotId` is a phase-6 follow-up listed below.

---

## Phase 6 — Transaction layer + WAL replay  (2 weeks) — **landed**

Tied the three indexes together under one transaction API, wired every
mutation through the WAL, and picked up every phase-5 "deferred"
bullet. Scope, landing commits, and the two bugs the new proptest
uncovered are captured below.

### Delivered

- **6a** [`src/wal/op.rs`](../src/wal/op.rs): `WalOp` enum — fixed-size
  tagged variants for `L2P_PUT/DEL`, `DEDUP_PUT/DEL`,
  `DEDUP_REVERSE_PUT/DEL`, `INCREF`, `DECREF`. `encode_body` /
  `decode_body` concatenate ops into / from a WAL record body with no
  length prefix (tag implies payload size).
- **6b** [`src/tx.rs`](../src/tx.rs): `Transaction<'db>` buffers ops
  against a `Db` and commits them as a single WAL record. `commit()`
  returns the durable LSN; `commit_with_outcomes` additionally
  surfaces per-op pre-images, which the auto-commit wrappers
  (`Db::insert`, `Db::incref_pba`, …) use to preserve their existing
  return types.
- **6c** [`src/db.rs`](../src/db.rs): `Db` gains a `Wal`, a
  `commit_lock`, and a `last_applied_lsn`. Every mutating method is
  now a one-op auto-commit shim on top of `Transaction`. The commit
  path takes `commit_lock` around WAL submit + apply so LSN order ==
  apply order trivially; phase 8 can replace this with an
  LSN-ordered condvar apply path for group-commit throughput.
  `flush()` / `take_snapshot` / `drop_snapshot` also take
  `commit_lock` and stamp `manifest.checkpoint_lsn` from the WAL
  cursor (not from tree generations), so recovery replays cleanly
  from `checkpoint_lsn + 1`.
- **6d** [`src/recovery.rs`](../src/recovery.rs): `replay_into(dir,
  from_lsn, |op| ...)` walks the WAL and invokes the caller's apply
  function for each decoded op. `truncate_torn_tail` shears off a
  torn final-segment tail before the new Wal writer resumes.
  `Db::open` calls these in sequence: replay, truncate, then
  `Wal::create` starting at `last_applied + 1`.
- **6e** [`src/lsm/lsm.rs`](../src/lsm/lsm.rs),
  [`src/db.rs`](../src/db.rs): the phase-5-deferred
  **`dedup_reverse`** LSM. Key encoding is
  `[pba: 8 B BE][hash[..24]]`, value is `[hash[24..] | zero padding]`;
  the full 32-byte hash round-trips via `encode_reverse_entry` /
  `decode_reverse_hash`. `Lsm::scan_prefix(prefix)` walks memtable +
  frozen + every SST whose min/max range overlaps the prefix, newest
  first, with tombstone shadowing. Manifest bumped to v5 (with a v4
  compat read path) to carry `dedup_reverse_level_heads` alongside
  `dedup_level_heads`. Db API: `register_dedup_reverse`,
  `unregister_dedup_reverse`, `scan_dedup_reverse_for_pba`. Callers
  can bundle `put_dedup` + `register_dedup_reverse` + `incref_pba`
  (or the inverse) into one `tx.commit()` for atomic
  register/release.
- **6f** [`tests/db_phase6_proptest.rs`](../tests/db_phase6_proptest.rs):
  proptest mirrors a `Db` against a triple-`BTreeMap` reference
  model through random mixes of the phase-6 API, including
  mid-sequence reopens that force WAL-replay to rebuild in-memory
  state. Two further unit tests cover failed-commit atomicity (WAL
  fsync fault → no in-memory apply) and the end-to-end
  decref-to-zero cleanup via `dedup_reverse`.
- **6g** (incidental) [`src/lsm/bloom.rs`](../src/lsm/bloom.rs):
  dropped the xxh3 rehash of already-hashed LSM keys. `h1` / `h2`
  now come straight from `hash[8..16]` and `hash[16..24]`, which lie
  in the raw content-hash region of both LSM key layouts. Saves one
  xxh3 call per bloom query. Module docs call out that this is an
  Onyx-specific purpose-built choice.

### Bugs surfaced by the new proptest (both fixed)

1. `Db::flush` was advancing `manifest.checkpoint_lsn` past LSNs whose
   ops were still sitting in the LSM memtable (never spilled to an L0
   SST). On reopen, WAL replay started at `checkpoint_lsn + 1` and
   silently dropped those ops. Fix: flush both LSM memtables to L0
   SSTs before committing the new manifest, so `checkpoint_lsn` is
   only ever as high as what is durable on disk.
2. `WriterState::init` always called `SegmentFile::create` (create_new),
   which collides with a pre-existing empty segment after a
   create-with-no-writes → reopen sequence. Fix: list segments at
   startup; if the newest existing segment's `start_lsn` is ≤ the
   requested `start_lsn`, open it for append; otherwise create fresh.
   A segment whose `start_lsn` is strictly ahead of the requested
   `start_lsn` is a hard corruption error.

### Exit criteria (status)

- ☑ All previous property tests + db tests pass under the unified
  transaction API (295 tests green as of `0227f39`).
- ☑ Read-your-writes: every commit returns only after WAL fsync, and
  reads after that point see the committed state.
- ☑ `dedup_reverse` prefix-scan cleanup landed and is covered by
  `decref_to_zero_cleanup_via_dedup_reverse` in the phase-6 proptest
  suite.
- ☑ Atomic `put(lba) + put_dedup + register_dedup_reverse +
  incref(pba)` commit — covered by
  `tx_atomically_registers_dedup_index_and_reverse` and the phase-6
  end-to-end test.
- ☐ Benchmarks (≥ 150 k txns/s on local NVMe with 16 writers). Not
  run — the single `commit_lock` MVP design intentionally sacrifices
  group-commit throughput, so the current numbers will be far below
  target. Revisit together with the LSN-ordered condvar apply path
  under phase 8.
- ☐ `SnapshotView` for dedup / refcount at a given `SnapshotId`. Not
  wired: snapshots still only expose L2P reads. Small follow-up
  (would extend `SnapshotView` with `dedup_at` / `refcount_at` that
  take the snapshot's per-group shard root).

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

### Backlog rolled in from earlier phases

- **1 M-op / 1000-seed LSM + Db proptest budget** (phase 5f / 6f were
  landed at a lower case count for realistic wall-clock).
- **Compaction stress bench** (10 GiB inserts, queryable at every
  level).
- **LSN-ordered condvar apply path** to bring back group-commit WAL
  throughput (phase 6's MVP intentionally serialises submit + apply
  under one `commit_lock`).
- **`SnapshotView` for dedup / refcount** at a given `SnapshotId`.
  Snapshots currently expose L2P reads only.

---

## Summary table

| Phase | Weeks | Cum. weeks | Delivers                                        | Status              |
|-------|-------|-----------:|-------------------------------------------------|---------------------|
| 0     | ~1    |   1        | Scaffolding, docs, CI                           | landed              |
| 1     | 3     |   4        | WAL + page store + recovery                     | landed              |
| 2     | 3     |   7        | B+tree single-writer                            | landed              |
| 3     | 3     |  10        | COW + refcount + snapshots                      | landed              |
| 4     | 2     |  12        | Sharded multi-writer B+tree                     | partially landed    |
| 5     | 3     |  15        | Fixed-record LSM + PBA refcount                 | landed              |
| 6     | 2     |  17        | Transactions + WAL replay + `dedup_reverse`     | landed              |
| 7     | 2     |  19        | Onyx integration + migration                    |                     |
| 8     | 4+    |  23+       | Hardening                                       |                     |

Total to a production-usable v0.1: ~5 months.
