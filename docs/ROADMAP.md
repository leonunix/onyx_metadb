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
- `metadb-verify` binary: **landed in phase 8a** as the offline
  page-walker / reachability audit. It stayed deferred past phase 3
  because the full set of page owners (paged L2P, refcount B+tree,
  dedup level heads, snapshot roots) was not stable enough earlier.
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

## Phase 4 — Sharded multi-writer B+tree  (2 weeks) — **landed**

The sharded index layer is complete. The three items originally carved
out as "remaining" all found later-phase owners — reclassifying them as
4-scope didn't help anyone, since two of them require phase-8b
infrastructure and the third folds into phase-8a's soak plan. Phase 4
is done; those concerns are tracked where they actually get resolved.

### Delivered

- [`manifest::Manifest` v3](../src/manifest.rs): current per-shard roots
  are stored inline in the manifest body; each snapshot stores its shard
  roots in a dedicated `SnapshotRoots` page. Legacy v2 single-root
  manifests still decode and are upgraded on open. (Further evolved to
  v5 by phase 5e / 6e.)
- [`page::PageType`](../src/page.rs): adds `SnapshotRoots` as an explicit
  page type so snapshot metadata is covered by the same CRC / verifier
  machinery as the rest of the page store.
- [`db::Db`](../src/db.rs): one `Mutex<BTree>` per shard (refcount) and
  one `Mutex<PagedL2p>` per shard (L2P, post-6.5a), xxh3 router,
  thread-safe `&self` point ops, and fan-out `range` / `diff` /
  `snapshot_view` / `take_snapshot` / `drop_snapshot`.
- [`db::SnapshotView`](../src/db.rs): snapshot reads hold a shared guard so
  `drop_snapshot` cannot free snapshot-owned pages while a live view is
  still reading them.
- **WAL-backed commit** (landed in phase 6 via [`Db::commit_ops`](../src/db.rs)):
  every mutation submits to the WAL under `commit_lock`, waits for
  group-commit fsync at the WAL writer, applies to in-memory state, and
  bumps `last_applied_lsn`. Phase 4's "submit to WAL, group-commit,
  then publish shard roots" concern is covered — the per-op apply is
  serialised by `commit_lock`, which is an intentional MVP choice that
  phase 8b will relax.
- [`tests/db_concurrency.rs`](../tests/db_concurrency.rs): multi-writer
  stress (16 writers / 4 shards) plus a round-based snapshot/reference
  test that checks snapshot correctness under concurrent writers.
- [`tests/db_snapshot_proptest.rs`](../tests/db_snapshot_proptest.rs):
  snapshot property test runs against the sharded `Db` implementation.

### Cross-phase ownership (what moved and why)

- **Concurrent apply / group-commit throughput**: the WAL writer
  itself already batches fsyncs (see `group_commit_max_batch_bytes` /
  `group_commit_timeout_us` in `Config`), but `Db::commit_ops` holds
  one global `commit_lock` across submit + apply, so in-flight
  commits can't coalesce at the Db layer. That's a deliberate
  phase-6 trade-off (simple LSN ordering == apply ordering) and
  reversing it requires an LSN-ordered condvar apply path. Owned by
  **[Phase 8b](#phase-8b--production-polish-parallel-with-or-after-phase-7)**.
- **Throughput benchmark (≥ 0.8× N single-writer)**: architecturally
  blocked by the `commit_lock` above. Re-benched post-8b against
  the ≥ 150 k txns/s target. Owned by **Phase 8b**.
- **Randomised long soak + lock-order audit script**: superseded by
  phase 8a's week-long `metadb-soak` harness. Owned by **Phase 8a**.

### Decisions resolved

- Shard count default: 16 per partition. Configurable via
  [`Config::shards_per_partition`](../src/config.rs).
- Lock ordering: cross-shard operations acquire shard locks in ascending
  shard id (the current implementation uses shard-vector order), so
  `flush` / `take_snapshot` / `drop_snapshot` share one deadlock-free order.
  Explicit runtime audit is part of the 8a soak harness.

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

## Phase 6.5a — Paged L2P radix tree  (~1 week) — **landed**

The L2P index started life as a per-shard COW B+tree. That was the
right call while we were still figuring out the rest of the engine:
shared page-store + refcount semantics, manifest + WAL integration,
snapshot / drop / diff. Now that the surrounding machinery is stable,
the B+tree itself has become overkill for Onyx's L2P workload:

- Keys are always `u64` LBAs — dense, no lexicographic surprises.
- Values are fixed 28 B (`BlockmapValue` equivalent).
- Reads are point lookups; no range scan on the hot path.
- Most volumes are densely written (ops come from the buffer flusher
  which coalesces by LBA), so a radix tree's "no key storage,
  arithmetic addressing" fits without sparseness penalties.

### What changed

- New `src/paged/` module:
  - `format.rs`: on-disk leaf (128 × 28 B with a 128-bit presence
    bitmap) and index (256 × 8 B child pointers, level byte in the
    type-header) layouts. Leaf addressing: `lba & 0x7F` for bit,
    `lba >> 7` for leaf id. Index addressing: `(leaf_idx >> (8*L)) &
    0xFF` at level `L`.
  - `cache.rs`: `PageBuf` with cascade over paged-index children.
    Structurally parallel to `btree::cache::PageBuf` but purpose-built
    for paged pages, keeping the cascade logic clear.
  - `tree.rs`: `PagedL2p` with `create` / `open` / `get` / `insert` /
    `delete` / `flush` / `range` / `get_at` / `incref_root_for_snapshot`
    / `decref_root` / `drop_subtree` / `diff_subtrees`. Same API shape
    as `BTree` so `Db` swaps in place. Root grows on demand up to
    level 4 (covers 2 PiB of 4 KiB LBAs per shard); empty leaves and
    indexes are pruned upward on delete.
- `src/page.rs`: two new `PageType` variants (`PagedLeaf`, `PagedIndex`).
- `src/db.rs`: L2P shards now hold `Mutex<PagedL2p>` (refcount shards
  still `Mutex<BTree>` — that's phase 6.5b's target). `apply_op_bare`,
  `collect_range_for_roots`, `diff_roots`, `drop_snapshot`, the
  snapshot save/restore/compare paths, and the manifest refresh all
  thread through the new type without structural change.

### Why not touch refcount yet

- Refcount writes come from flusher / GC batches, not the user read
  path — B+tree traversal cost isn't visible to latency-sensitive
  ops.
- Refcount value shrinks to 4 B (vs 28 B for L2P); the B+tree gets 3×
  the branching factor once the value type is specialized. That's
  phase 6.5b.

### What this does not change

- Snapshot model: per-shard root pointers live in the manifest, exactly
  as before. Readers take a read lock on `snapshot_views`; mutators
  take the write lock only in `drop_snapshot`.
- Recovery: WAL replay goes through `Db::apply_op` just like a live
  commit; the swap is invisible to the replay machinery.
- Dedup LSMs: unchanged. Hash-keyed, sparse, prefix-scanned — paged
  would be wrong for them.

### Exit criteria

- 29 paged-specific unit tests (format + cache + tree) pass.
- All pre-6.5a Db / snapshot / WAL proptests still pass unmodified
  (319 lib tests + integration suites).
- Clippy clean on the new module.

### Deferred / follow-ups

- 6.5b: `BTree` value specialization from 28 B → 4 B for refcount.
- Manifest version bump from v5 → v6 was considered but deferred: the
  page-level check in `PagedL2p::open` fails cleanly on a pre-6.5a
  manifest (which would point at `L2pLeaf` / `L2pInternal` pages), so
  no silent corruption risk.
- No `drop_subtree`-style efficient shrink path; deletes just prune
  upward and leave root at whatever level it grew to. Root never
  shrinks back down. Fine for Onyx's workload; revisit if it becomes
  a problem.

---

## Phase 6.5b — BTree specialized to refcount  (~2 days) — **landed**

After 6.5a moved L2P to a paged radix tree, the B+tree only served
refcount. Snapshotting refcount doesn't make sense in Onyx's model
(refcount is a running tally across all volumes and snapshots, not a
point-in-time value), so most of the B+tree's machinery was dead
weight. 6.5b trimmed the tree down to what refcount actually needs and
shrunk the value type to match.

### What changed

- **L2pValue + DiffEntry moved to `crate::paged`**. BTree stops
  pretending to be a general-purpose index; `paged` owns the L2P
  concepts.
- **BTree value shrunk from 28 B → 4 B `u32`**. Leaf entry size drops
  from 36 B to 12 B, so each leaf page holds **336 entries instead of
  112** (3× the branching factor with zero on-disk waste).
- **Snapshot machinery removed**:
  - `cow_for_write`, `incref`, `decref`, `DecrefOutcome` gone from
    `btree::cache::PageBuf`.
  - `incref_root_for_snapshot`, `drop_subtree`, `diff_subtrees`,
    `get_at`, `range_at`, `collect_subtree`, `merge_diff_into` gone
    from `btree::tree::BTree`.
  - Writes are in-place; pages still carry a `refcount` field in the
    header (shared with other page types) but it stays at 1 for every
    live page.
- **Manifest**: snapshot entries now leave `refcount_shard_roots`
  empty and `refcount_roots_page = NULL_PAGE`. The encode-time
  validation is relaxed: a per-snapshot refcount roots vector of
  length 0 is accepted alongside the legacy full-length form, so
  pre-6.5b manifests still decode.
- **`Db`**: `take_snapshot` skips the per-tree refcount incref;
  `drop_snapshot` skips the refcount drop_subtree loop; the
  `refcount_from_value` / `refcount_to_value` helpers are gone — the
  refcount path reads and writes `u32` directly through BTree's new
  API.

### Why refcount snapshot isn't needed

Snapshot semantics in VDO-style deduplicating storage: taking a
snapshot means every PBA the L2P points to picks up one more live
reference. Dropping a snapshot releases those references. Refcount
reflects the current live graph, not "what the count was at some
earlier LSN." If an API ever wants "refcount value at snapshot time,"
the answer is either "current refcount" (it already counts the
snapshot) or "has to be reconstructed by walking the snapshot's L2P"
— which no Onyx caller asks for.

### Exit criteria

- 282 lib tests + all integration suites (btree_proptest,
  db_concurrency, db_phase6_proptest, db_snapshot_proptest, WAL crash,
  phase-1 end-to-end) pass.
- Clippy clean.
- `Db::diff` / `diff_with_current` / snapshot views keep working
  (they never touched refcount).

### Deferred / follow-ups

- BTree page types still named `L2pLeaf` / `L2pInternal` for historical
  reasons. The numeric values stay at 1 / 2, so on-disk format is
  unchanged; renaming is a pure code-churn optional cleanup.
- Manifest version stays at v5; the schema is wire-compatible with
  6.5b's empty-refcount-snapshot convention. A v6 bump that drops the
  `refcount_shard_roots` / `refcount_roots_page` fields from
  `SnapshotEntry` entirely is possible but not urgent.

---

## Phase 6.5 — Page cache + bounded memory  (3–5 days) — **landed**

Before 6.5 the phase-2–6 code bypassed any shared cache:

- `BTree::PageBuf` was an unbounded `HashMap<PageId, Page>`, one per
  tree. Soak / long-running workloads would grow it without bound.
- `SstReader` re-read header + bloom + body pages from disk on every
  `get`. Bloom pages got hit dozens of times per query with no
  cross-query reuse.

### What landed

- **`src/cache.rs`** — sharded `PageCache` with 16 per-shard LRUs
  backed by `PageStore::read_page`. Exposes `get` (populate+LRU),
  `get_for_modify` (pop, so dirty writes can't expose stale data
  via the shared cache), `get_bypass` (skip LRU — used by compaction
  scans), `insert` (write-through when a dirty page is flushed), and
  `invalidate` / `invalidate_run` (called by compaction / free). All
  counters (`hits`, `misses`, `evictions`, `current_pages`,
  `current_bytes`) reported via `PageCacheStats`.
- **`src/btree/cache.rs` + `src/paged/cache.rs`** — both `PageBuf`
  variants now back onto the shared `PageCache`. `modify` pops the
  page from the shared cache (implicit dirty-pin: while dirty, the
  page lives only in the private `PageBuf` and cannot evict from the
  shared cache); `flush` writes-through and reinserts as clean.
- **`src/lsm/sst.rs`** — `SstReader::get` fetches bloom + data pages
  through the cache. `SstScan` uses `get_bypass` so full-level
  compaction can't flush hot pages out of LRU.
- **`src/db.rs`** — `Db` owns one `Arc<PageCache>` sized from
  `cfg.page_cache_bytes`, hands clones to every L2P shard, refcount
  shard, dedup_index LSM, and dedup_reverse LSM. `Db::cache_stats()`
  is public for soak / dashboard instrumentation.
- **`src/config.rs::page_cache_bytes`** — default 512 MiB, wired
  through to every constructor.

### Decisions (all held)

- **Unified** cache, not two caches. Cross-subsystem reuse of hot
  pages under a single budget.
- **LRU** via the `lru` crate for MVP. Clock-Pro is deferred until
  we have measurements to justify it.
- **16 shards**, matching the B+tree shard fanout.
- **Scan-resistant** for LSM compaction via the `get_bypass` path.
- **Dirty pin** implemented as "invalidate on modify, re-insert on
  flush" rather than a refcounted pin. Same guarantee (dirty pages
  can't evict before they're durable); simpler invariant.

### Exit criteria

- **Correctness under tiny cache**: `db_vs_reference_with_reopens_tiny_cache`
  runs the full phase-6 op mix against `page_cache_bytes = 1 MiB`.
  The cache evicts aggressively under this cap — a regression guard
  for the dirty-pin / invalidate-on-modify path.
- **Observability**: `Db::cache_stats()` exercised by
  `cache_stats_show_hits_evictions_and_respect_budget` (hits grow,
  misses grow, evictions fire, current_bytes never exceeds the
  configured budget).
- **Cache effectiveness**: `tests/cache_bench.rs::warm_cache_serves_reads_without_re_reading_from_disk`
  — deterministic stats assertion that warm-pass misses are an order
  of magnitude below cold-pass misses and warm-pass hits strictly
  grow. `warm_vs_cold_wall_clock` (`#[ignore]`) reports the actual
  ratio; the 5× figure in the original exit criteria was realistic
  for disk-bound workloads but a 20 K-entry in-memory bench is
  dominated by hash / lock / allocator overhead, so the assertion
  landed stats-only, with timing kept as a diagnostic print.
- **No regression**: all 282 lib tests + 17 integration tests pass
  at default cache size.
- No B+tree or LSM functionality regression. All pre-6.5 tests
  still pass with cache enabled at default size.

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

## Phase 8a — Pre-integration hardening  (2 weeks) — gates Phase 7

Correctness hardening that must land **before** staking Onyx on
metadb. The guiding principle: once Phase 7 begins, any symptom under
Onyx load has an ambiguous origin ("is it Onyx? is it metadb?") and
debug time multiplies. metadb must prove it is rock-solid standalone
first.

### Scope

**Static + pre-soak correctness**

- **Scale up `db_phase6_proptest`** from 16 cases × 30–80 ops to
  500+ cases × 200+ ops. Budget a multi-hour run.
- **Mid-commit crash injection**: fault points between "WAL fsync
  succeeded" and "apply finished", plus "apply finished" and
  "`last_applied_lsn` bumped". Today we only test the WAL-fsync
  failure edge.
- **Manifest-swap crash injection**: fault between "new
  `dedup_level_heads` page chain written" and "manifest commit
  succeeded". Check that the post-recovery state is consistent with
  either the pre- or post-commit view.
- **10 GiB compaction stress**: insert until the LSM reaches L3+,
  verify every inserted key is readable at every point during the
  compaction storm (no bloom / range / binary-search regression
  under pressure).
- **Fuzz**: `cargo-fuzz` on the WAL record decoder, page header
  decoder, manifest body decoder, and bloom-filter bit decoder.
  Run for a few hours per target; file any findings.
- **`metadb-verify` CLI**: walks the page store, cross-checks
  manifest `shard_roots` / `refcount_shard_roots` /
  `dedup_level_heads` against on-disk pages, asserts free-list has
  no duplicates and no overlap with live pages, asserts each live
  page's refcount matches the count of pages that point to it.
  Also the primary debugging tool for Phase 7.
- **Read-side batch APIs for integration hot paths**:
  `Lsm::multi_get`, `Lsm::multi_scan_prefix`,
  `Db::multi_get`, `Db::multi_get_refcount`,
  `Db::multi_get_dedup`, and
  `Db::multi_scan_dedup_reverse_for_pba`. These share the single-key
  lookup implementation where possible, amortise shard / level
  snapshots across a batch, and land with dedicated tests for
  ordering, duplicates, mixed memtable+SST / cross-shard paths, and
  explicit tombstone / shadowing semantics. The point is to remove
  "did the new batch helper misread metadata?" from the Phase-7
  debugging surface before Onyx starts leaning on those paths.
- **Lock-order audit**: enable `parking_lot::deadlock::check_deadlock`
  in the soak harness's monitor thread; fail the run if a cycle is
  detected. Folded in here rather than given its own line-item
  because the soak is where a bad lock order would actually bite —
  audit without exercise would just rubber-stamp the current layout.

**Standalone soak (the load-bearing piece of 8a)**

- **`metadb-soak` harness**: standalone binary, NOT driven by Onyx.
  A self-contained workload generator that does:
  - Multi-threaded random mix of insert / delete / put_dedup /
    delete_dedup / incref / decref / snapshot / drop_snapshot /
    flush, against a reference model held in-memory.
  - Periodic process restart (simulated crash: `kill -9` + reopen)
    and reference replay.
  - Configurable fault-injection density via `FaultController`.
  - Continuous `metadb-verify` audit between restarts.
  - Structured log + summary report.
- **Week-long local soak** under that harness, configured with a
  realistic write:read mix, moderate fault-injection, and a target
  of billions of ops. This has to clear cleanly BEFORE phase 7
  begins. Real-hardware soak (if needed) comes in phase 8b with
  real workload data.

### Exit criteria

- All proptest / fuzz runs clean for one full cycle at the new
  budgets.
- **Week-long `metadb-soak` completes with zero corruption, zero
  assertion failures, zero reference-model divergence, and no
  unbounded memory or disk growth.** If the soak trips, the bug is
  fixed and the week restarts from zero — no amnesty.
- `metadb-verify` returns no issues at every checkpoint during and
  after the soak.
- Every fault-injection bug surfaced here is fixed or has an
  explicit ticketed waiver.

---

## Phase 8b — Production polish  (parallel with or after Phase 7)

Items that don't gate Phase 7 but do gate production.

### Scope

- ☑ **LSN-ordered condvar apply path** (landed). Phase 6's single
  `commit_lock` is gone; WAL submit now happens under no Db-level
  lock so concurrent submitters coalesce at the WAL writer, and
  apply order is restored via a `commit_cvar` queue keyed on
  `last_applied_lsn`. Commit takes `apply_gate.read()` across
  apply + bump; flush / take_snapshot / drop_snapshot take
  `apply_gate.write()` to sample a quiescent tree state. Regression
  guard: `tests/db_concurrency.rs::concurrent_commits_coalesce_into_wal_group_batches`
  asserts fsync count falls well below commit count under 8 writers
  (currently ≈ 12 % on the dev host). The ≥ 150 k txns/s headline
  target is a hardware-bound follow-up — bench at integration time
  against real NVMe.
- **Snapshot reads for dedup / refcount**: `SnapshotView::dedup_at`
  / `refcount_at(SnapshotId)` using each snapshot's per-group
  shard root.
- **Prometheus-compatible metrics exporter** for all subsystems.
- **Operational tooling**: `metadb-dump`, `metadb-replay`,
  companion to `metadb-verify` from 8a.
- **Real-hardware soak** under the Onyx `stability_harness` once
  integration is in place — reuses the phase-8a `metadb-soak`
  invariants where possible but drives real workload through the
  Onyx write path. Complements, doesn't replace, the standalone
  8a soak.
- **Documentation**: recovery playbook, tuning guide.
- **Bloom scan resistance sanity check** once the cache in phase
  6.5 lands — make sure compaction doesn't defeat it.

### Backlog from earlier phases

- **1 M-op / 1000-seed LSM + Db proptest budget**. Phase 5f / 6f
  landed a smaller case count for realistic wall-clock; Phase 8a
  scales it up, but the full 1M × 1000 aspirational target lives
  here.
- **SnapshotView dedup/refcount** — listed above; rolled forward
  from the phase-6 exit-criteria checklist.

### Exit criteria

- All phase-8b scope items landed.
- Real-hardware week-long soak passes with no corruption and no
  unbounded growth.
- Prometheus exporter demonstrates non-zero time-series under a
  reproducible benchmark.

---

## Summary table

| Phase | Weeks | Cum. weeks | Delivers                                              | Status              |
|-------|-------|-----------:|-------------------------------------------------------|---------------------|
| 0     | ~1    |   1        | Scaffolding, docs, CI                                 | landed              |
| 1     | 3     |   4        | WAL + page store + recovery                           | landed              |
| 2     | 3     |   7        | B+tree single-writer                                  | landed              |
| 3     | 3     |  10        | COW + refcount + snapshots                            | landed              |
| 4     | 2     |  12        | Sharded multi-writer B+tree                           | landed              |
| 5     | 3     |  15        | Fixed-record LSM + PBA refcount                       | landed              |
| 6     | 2     |  17        | Transactions + WAL replay + `dedup_reverse`           | landed              |
| 6.5a  | ~1    |  18        | Paged L2P radix tree (replaces B+tree for L2P)        | landed              |
| 6.5b  | ~0.5  |  18.5      | Refcount B+tree value specialization (28 B → 4 B)     | landed              |
| 6.5   | ~1    |  19.5      | Bounded page cache (paged + LSM)                      | landed              |
| 8a    | 2     |  21.5      | Pre-integration hardening + week soak (gates phase 7) | in progress         |
| 7     | 2     |  23.5      | Onyx integration + migration                          |                     |
| 8b    | 3+    |  26.5+     | Production polish (parallel with / after 7)           |                     |

Phase 8a runs before Phase 7 and owns the **standalone week-long
soak**. Integrating before metadb has cleared a self-contained soak
would conflate Onyx-side symptoms with metadb-side bugs and multiply
debug cost. 8b is the continuous-work tail and overlaps with Phase 7;
its real-hardware soak runs through the Onyx write path and
complements (not replaces) the 8a soak.

Total to a production-usable v0.1: ~5–6 months.
