# onyx-metadb

Embedded metadata engine for [Onyx Storage](https://github.com/leonunix/onyx_storage).

Purpose-built to replace RocksDB for Onyx's metadata plane, which has two
distinct workloads that a general-purpose LSM cannot serve well at the same
time:

| Workload | Access pattern | Best fit |
|----------|---------------|----------|
| L2P (LBA → `L2pValue`) | per-volume-ish, dense int keys, point + range, snapshot-heavy, must support multi-writer | Paged COW radix tree |
| Refcount (PBA → u32) | uniform-random keys, point read / incref / decref, no snapshots | Sharded COW B+tree |
| Dedup (SHA-256 → entry) | global, uniform-random keys, append-heavy, point lookups only | Fixed-record LSM |

All three share one WAL so cross-index updates commit atomically in one
fsync. L2P snapshots are per-shard, O(1) to take, and diffable — Onyx can
compute the exact set of keys that become reclaimable when a snapshot is
dropped.

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
| 2     | B+tree single-writer | landed |
| 3     | COW + refcount + snapshots | landed |
| 4     | Sharded multi-writer B+tree | landed |
| 5     | Fixed-record LSM + PBA refcount | landed |
| 6     | Transactions + WAL replay + `dedup_reverse` | landed |
| 6.5a  | Paged L2P radix tree (replaces B+tree for L2P) | landed |
| 6.5b  | Refcount B+tree value specialization (28 B → 4 B) | landed |
| 6.5   | Bounded page cache (paged + LSM) | landed |
| 8a    | Pre-integration hardening (crash injection + proptest scale-up + week-long `metadb-soak`) — **gates Phase 7** | in progress |
| 7     | Integration with onyx-storage (adapter + migration tool) | blocked on 8a |
| 8b    | Production polish (metrics, dumps, real-hardware soak) | parallel with / after 7 |

Current commits land on `main`. See [`docs/ROADMAP.md`](docs/ROADMAP.md) for
entry / exit criteria per phase.

## Public API at a glance

All reads and writes go through one `Db`. Transactions batch a set of ops
into one WAL record + one fsync.

```rust
let db = Db::create(path)?;                  // or Db::open
let mut tx = db.begin();
tx.insert(lba, l2p_value);
tx.incref_pba(pba, 1);
tx.put_dedup(hash, dedup_value);
tx.register_dedup_reverse(pba, hash);
let lsn = tx.commit()?;                      // one WAL record, one fsync

// Point reads
let val: Option<L2pValue>   = db.get(lba)?;
let rc:  u32                = db.get_refcount(pba)?;
let d:   Option<DedupValue> = db.get_dedup(&hash)?;

// Batched reads (shard-aware / shared LSM drain + levels snapshot)
let vals  = db.multi_get(&lbas)?;
let rcs   = db.multi_get_refcount(&pbas)?;
let hits  = db.multi_get_dedup(&hashes)?;
let revs  = db.multi_scan_dedup_reverse_for_pba(&dead_pbas)?;

// Range scan (L2P only)
for item in db.range(lba_lo..lba_hi)? { let (k, v) = item?; }

// Snapshots (L2P only)
let snap = db.take_snapshot()?;
let view = db.snapshot_view(snap).unwrap();
let diff = db.diff_with_current(snap)?;
db.drop_snapshot(snap)?;
```

## Layout

```
src/
  db.rs             Db facade: shard routing, snapshots, commit_ops
  tx.rs             Transaction API (WAL op buffering + commit)
  wal/              Append-only WAL: segments, group commit, recovery
  page_store.rs     4 KiB page allocator + free list (O_DIRECT on Linux)
  manifest.rs       Double-buffered manifest: tree roots, checkpoint LSN
  paged/            Paged COW radix tree used for L2P
  btree/            COW B+tree specialized to refcount (u32 value)
  lsm/              Fixed-record LSM (dedup_index + dedup_reverse)
  cache.rs          Unified 16-shard page cache (LRU, scan-resistant)
  recovery.rs       WAL replay on open
  verify.rs         Structural verifier (groundwork for metadb-verify)
  testing/          Fault injection + shared test harness
  bin/              CLI binaries (metadb-verify scheduled for 8a)

docs/
  DESIGN.md         Architecture, on-disk formats, recovery semantics
  ROADMAP.md        Phased plan + per-phase exit criteria
  TESTING.md        Test strategy: proptest, invariants, fault injection

tests/              Integration tests + proptests (crash / compaction / cache)
```

## Build & test

```bash
cargo build
cargo test               # 290 unit + 25 integration, under a minute
cargo build --release
```

Fault-injection tests and longer proptests are behind `#[ignore]`; run with
`cargo test -- --ignored` when preparing a release.

## License

Apache-2.0
