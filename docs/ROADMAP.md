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

## Phase 5 — Fixed-record LSM for dedup (+ PBA refcount)  (3 weeks)

Second index type. Integrates with the shared WAL and manifest.

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
- **dedup_reverse** is a second LSM instance, not a BTree. Append-heavy
  (every write registers), range-prefix scan on PBA free — both are LSM's
  sweet spot.

### Sub-phase breakdown

- **5a — LSM scaffolding + memtable**  `src/lsm/{mod,format,memtable}.rs`
  - Record format constants, `Hash32`, `DedupValue`, encode/decode helpers.
  - `Memtable`: `BTreeMap<Hash32, DedupOp>` + `RwLock`, freeze / release,
    one active + at most one frozen slot.
  - `LookupResult` distinguishes `Hit(value) | Tombstone | Miss` so
    higher levels don't override an explicit delete.
- **5b — SST writer/reader + bloom**  `src/lsm/{sst,bloom}.rs`
  - Bloom: 10 bits/entry default, 4 hash functions derived via double
    hashing.
  - SST header page (type = `LsmData`): record count, min/max hash,
    bloom offset/length, body offset, body page count.
  - Body pages: 63 sorted records per page, binary search across pages
    via in-header min/max.
  - `SstWriter` streams a sorted input (frozen memtable or merged
    compaction cursor) into a page run.
- **5c — `Lsm` facade + L0 fanout**  `src/lsm/lsm.rs`
  - `Lsm` owns memtable + `Vec<Vec<SstHandle>>` (one Vec per level).
  - `put` / `delete` go to memtable; on overflow, freeze + flush to L0.
  - `get` walks memtable → L0 (reverse insertion order) → L1.. in order.
  - Manifest v4: add `lsm_levels: Vec<Vec<SstHandle>>` per LSM instance.
- **5d — Compaction**  `src/lsm/compact.rs`
  - L0 → L1 when L0 file count ≥ trigger.
  - Ln → Ln+1 when level byte size ≥ ratio × previous.
  - Tombstones drop at the last level only.
  - Single compaction thread per LSM instance.
- **5e — Second LSM + PBA refcount tree**  `src/db.rs`, `src/manifest.rs`
  - `Db` gains `dedup_index: Lsm`, `dedup_reverse: Lsm`, and
    `refcount_shards: Vec<Shard>`.
  - Surface API: `put_dedup`, `get_dedup`, `delete_dedup`,
    `incref_pba`, `decref_pba`, `get_refcount`,
    `scan_dedup_reverse_for_pba`.
  - Refcount value layout: first 4B big-endian u32 refcount in
    `L2pValue`; decref to zero returns `None` and the caller is expected
    to follow up with a dedup_reverse scan.
  - Manifest v4: shard_roots → `l2p_shard_roots` + `refcount_shard_roots`;
    snapshot entries extended with both vectors; `lsm_levels` table for
    each LSM instance.
- **5f — Property + crash tests**  `tests/lsm_proptest.rs`,
  `tests/db_dedup_proptest.rs`
  - LSM vs. `BTreeMap<[u8; 32], DedupOp>` reference: put/delete/get +
    flush + compact, millions of ops.
  - Crash injection at flush / compact / manifest swap; assert LSM
    matches memory reference after recovery.
  - End-to-end: `insert(lba, v) + put_dedup(hash, e) + incref(pba)` one
    at a time (phase 6 will make them atomic); after each op, the state
    is queryable and survives reopen.

### Exit criteria

- Property test vs. `BTreeMap<[u8; 32], DedupOp>` reference for
  put/get/delete/compact. 1M-ops runs × 1000 seeds.
- Compaction stress: 10 GiB of inserts, verify all records queryable at
  every level of the compaction tree.
- Cross-index consistency test: `put(lba, v)`, `put_dedup(hash, e)`,
  `incref(pba, 1)` individually, all visible after reopen.
- PBA refcount cleanup: when refcount hits 0, the caller can scan
  dedup_reverse by PBA prefix and delete the matching dedup_index entry.
  (Atomic cross-index commit lands in phase 6.)

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
