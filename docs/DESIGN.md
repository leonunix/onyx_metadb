# onyx-metadb Design

Status: **draft v0.1 — phase 0**. All formats and APIs are subject to change
until v0.1 is tagged on main.

## 1. Goals and non-goals

### Goals

1. Embedded crate, single-process, single-host. Consumed by onyx-storage as a
   library.
2. Two workload-specialized index types in one engine:
   - Sharded COW B+tree for L2P (LBA → BlockmapValue).
   - Fixed-record LSM for global dedup (hash → DedupEntry).
3. Global atomic transactions: one WAL, one fsync per group commit.
4. Multi-writer concurrency. Throughput scales with flush lane count.
5. Native snapshots per partition:
   - O(1) to take.
   - Snapshot reads are latch-free (treat root as immutable).
   - Snapshot diff skips identical subtrees.
   - Snapshot drop returns precise per-PBA refcount deltas.
6. Crash safety verified by automated fault-injection, not by inspection.
7. Fixed-size keys and values end-to-end. No varlen framing, no codec dance.

### Non-goals

- General-purpose KV. This is not a RocksDB replacement for arbitrary users.
- Variable-length values. Dedup and L2P are both fixed-size; exploit it.
- Secondary indexes, SQL, query planner.
- Network layer, replication, consensus. Onyx handles that above us.
- Compression. Records are already dense; compression costs CPU for no win.

## 2. Workload model

Onyx has two metadata workloads coexisting in one RocksDB today:

### 2.1 L2P

- Per-volume column family: `blockmap:{volume_id}`.
- Key: 8-byte big-endian LBA.
- Value: 28-byte `BlockmapValue` (see `onyx_storage/src/meta/codec.rs`).
- Access:
  - Point `get` on worker read path.
  - Point `put`/`delete` on flusher batch commit (WriteBatch-atomic).
  - Range scan during GC / scanner.
  - Batch `multi_get` on flusher for cross-checking old mappings.
- Scale: a 1 TiB volume = 256 M LBAs. Hundreds of volumes plausible.
- Pattern: *spatial locality* (sequential LBA writes collapse into adjacent
  keys), *in-place overwrite* (same LBA written many times), *needs snapshot*.

LSM is a bad fit: high write amplification from compaction, and range scans
walk through dead versions. B+tree paged layout matches the workload
directly.

### 2.2 Dedup

- Global CF: `dedup_index`, plus `dedup_reverse` for PBA-triggered cleanup.
- Key: 32-byte SHA-256 content hash.
- Value: ~27 bytes (PBA + refcount + flags).
- Access:
  - Point lookup on every flusher unit (is this content already stored?).
  - Point `put` on dedup miss (register new hash).
  - Point `delete` during PBA-refcount-zero cleanup.
- Pattern: uniform-random keys, append-heavy, no ranges, no in-place update.
  This is the LSM sweet spot.

The two workloads must commit atomically (a dedup hit rewrites an LBA's
blockmap and increments a PBA refcount in one transaction), so they share a
WAL. But their indexes should be different data structures.

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Public API                                                  │
│   Db • Partition • Snapshot • Transaction • DiffIter        │
├─────────────────────────────────────────────────────────────┤
│ Transaction Manager: group commit, MVCC read, LSN issuing   │
├───────────────────────────┬─────────────────────────────────┤
│ L2P Index                 │ Dedup Index                     │
│   Sharded COW B+tree      │   Fixed-record LSM              │
│   - per-partition root    │   - memtable + L0..Ln           │
│   - N shards / partition  │   - bloom per SST               │
│   - 4 KiB pages           │   - no compression              │
├───────────────────────────┴─────────────────────────────────┤
│ Page cache: per-page refcount, clock-pro eviction           │
├─────────────────────────────────────────────────────────────┤
│ Page store: COW 4 KiB pages, free list, page checksum       │
├─────────────────────────────────────────────────────────────┤
│ WAL: append-only, group commit, CRC'd records, aligned fsync│
├─────────────────────────────────────────────────────────────┤
│ File IO: O_DIRECT where supported, 4 KiB aligned buffers    │
└─────────────────────────────────────────────────────────────┘
```

## 4. Public API (sketch)

```rust
pub struct Db { /* ... */ }
pub struct Partition<'db> { /* ... */ }
pub struct Snapshot<'db> { /* ... */ }
pub struct Transaction<'db> { /* ... */ }

pub type PartitionId = u32;
pub type SnapshotId = u64;
pub type Lsn = u64;
pub type Lba = u64;
pub type Pba = u64;

impl Db {
    pub fn open(cfg: Config) -> Result<Self>;
    pub fn close(self) -> Result<()>;

    pub fn create_partition(&self, name: &str) -> Result<PartitionId>;
    pub fn drop_partition(&self, id: PartitionId) -> Result<DropReport>;
    pub fn partition(&self, id: PartitionId) -> Option<Partition<'_>>;

    pub fn snapshot(&self, partition: PartitionId, name: &str) -> Result<SnapshotId>;
    pub fn list_snapshots(&self, partition: PartitionId) -> Vec<SnapshotInfo>;
    pub fn drop_snapshot(&self, snap: SnapshotId) -> Result<DropReport>;
    pub fn snapshot_view(&self, snap: SnapshotId) -> Option<Snapshot<'_>>;
    pub fn diff(&self, a: SnapshotId, b: SnapshotId) -> Result<DiffIter<'_>>;

    pub fn begin(&self) -> Transaction<'_>;
}

/// Returned by drop_partition / drop_snapshot. Onyx uses this to update its
/// allocator. The metadb has already updated its own dedup refcounts.
pub struct DropReport {
    pub freed_pbas: Vec<(Pba, u32)>, // (pba, extent_len_in_blocks) hit refcount 0
    pub dec_only:   Vec<(Pba, i32)>, // non-zero refcount changes (rare; for audits)
    pub lsn:        Lsn,
}

impl Partition<'_> {
    pub fn get(&self, lba: Lba) -> Result<Option<BlockmapValue>>;
    pub fn range(&self, start: Lba, end: Lba) -> RangeIter<'_>;
}

impl Snapshot<'_> {
    pub fn get(&self, lba: Lba) -> Result<Option<BlockmapValue>>;
    pub fn range(&self, start: Lba, end: Lba) -> RangeIter<'_>;
}

pub enum DiffEntry {
    AddedInB { lba: Lba, value: BlockmapValue },
    RemovedInB { lba: Lba, old: BlockmapValue },
    Changed { lba: Lba, a: BlockmapValue, b: BlockmapValue },
}

impl Transaction<'_> {
    pub fn put(&mut self, partition: PartitionId, lba: Lba, v: BlockmapValue);
    pub fn delete(&mut self, partition: PartitionId, lba: Lba);
    pub fn put_dedup(&mut self, hash: [u8; 32], entry: DedupEntry);
    pub fn delete_dedup(&mut self, hash: [u8; 32]);
    pub fn incref(&mut self, pba: Pba, delta: i32);
    pub fn commit(self) -> Result<Lsn>;
}
```

## 5. On-disk formats

### 5.1 Page (4 KiB, all indexes share this)

```
offset  size  field
------  ----  -----------------------------------------------
  0     4     magic = 0x4F4E5850 ("ONXP")
  4     1     page_type: 1=L2P-leaf 2=L2P-internal 3=LSM-SST-data 4=free-list ...
  5     1     version = 1
  6     2     key_count
  8     4     crc32c of bytes [16..4096]
 12     4     flags (reserved)
 16     8     generation (LSN at which page was written)
 24     4     refcount (number of parents pointing to this page)
 28     4     reserved
 32     32    page-type specific header
 64     4032  entries / payload
```

### 5.2 L2P leaf page

Entries after page header:
```
[8B LBA big-endian][28B BlockmapValue] × N
```
Capacity: (4096-64) / 36 = 112 entries. Keys are sorted ascending.
Binary search within a page is cheap (112 entries → 7 compares).

### 5.3 L2P internal page

```
[8B separator key][8B child page_id] × (N+1 style)
```
We store N+1 child pointers with N separator keys. Layout:
```
[header: 64B]
[child_0: 8B]
[key_0: 8B][child_1: 8B][key_1: 8B][child_2: 8B] ...
```
Capacity: ~253 children.

### 5.4 LSM SST

SSTs are sorted runs of fixed 59-byte records (32B hash + 27B value + 0 pad).
Layout (multiple 4 KiB pages):

```
page 0:  header (bloom params, record count, min/max key, crc)
page 1..b: bloom filter
page b+1..end: fixed records, sorted
```

Within an SST, a lookup is: bloom check → binary search on record array (no
per-block index needed since records are fixed size and pages are dense).

### 5.5 WAL record

```
offset  size  field
------  ----  -----------------------------------------------
  0     8     lsn
  8     4     length (of body)
 12     4     crc32c of body
 16     N     body
```

Body is a sequence of typed ops:
```
[op_tag: 1B][partition: 4B][...fixed payload...]
```

Op tags (illustrative):
- 0x01 L2P_PUT(partition, lba, value[28])
- 0x02 L2P_DEL(partition, lba)
- 0x10 DEDUP_PUT(hash[32], entry[27])
- 0x11 DEDUP_DEL(hash[32])
- 0x20 PBA_INCREF(pba, delta: i32)
- 0x30 SNAPSHOT_TAKE(partition, snap_id, name_len, name...)
- 0x31 SNAPSHOT_DROP(snap_id)
- 0x40 PARTITION_CREATE(partition, name_len, name...)
- 0x41 PARTITION_DROP(partition)
- 0x80 CHECKPOINT(manifest_lsn)

WAL records are written into 4 KiB-aligned segments. Segment boundary has a
zero-padded tail; recovery tolerates tail-of-segment.

### 5.6 Manifest

A separate file (or pair of files for atomic swap) containing:
- Last durable checkpoint LSN.
- Table: partition_id → current_root_page_id, name, generation.
- Table: snapshot_id → partition_id, root_page_id, name, created_lsn.
- LSM manifest: per-level list of SST file IDs, min/max key, record count.
- Free list head page_id.

Updated only at checkpoint. Pointed-to by a double-buffered header with CRC
and a sequence number — recovery picks the newest valid header.

## 6. Sharded COW B+tree

### 6.1 Why shard

A single B+tree has a hot path at the root under concurrent writers.
Latch-coupling reduces contention but doesn't eliminate it for the
immediate-parent-of-root. COW makes root update serializable per partition.

We shard per partition so that a single partition's writes scale with
hardware. Shard count is configurable (default 16). Onyx's flush lanes
already shard by hash; this aligns naturally.

### 6.2 Shard scheme

```
shard_id = xxh3(lba.to_be_bytes()) & (N - 1)
```

- Uniform random across shards. Sequential writes hit different shards.
- Range scans fan out over all N shards and merge (cost: N-way merge, small
  N, keys cheap).
- A write touches exactly one shard.

### 6.3 Per-shard structure

Each shard has:
- `root_page_id: AtomicU64`
- `writer: Mutex<ShardWriter>`
- Read path: lock-free; hold a page cache reference and follow pointers.

### 6.4 Write path (COW)

```
begin_shard_write(shard):
    writer = shard.writer.lock()
    root = load(shard.root_page_id)
    new_root = cow_insert(root, key, value)
    publish(shard.root_page_id, new_root)  // after WAL fsync
    writer.unlock()
```

`cow_insert` walks down. At each level:
- If the target page has refcount > 1 (shared with a snapshot): allocate a
  new page, copy entries, modify, decrement old refcount, install the new
  page id into parent. Continues CoW cascade up.
- If refcount == 1: in-place edit is safe (no snapshot holds this page).
  We still write it as a new page if the WAL replay model requires it; TBD
  in phase 3 implementation decision. Current default: always COW, simpler
  recovery.

### 6.5 Split / merge

Leaf full → split into two leaves, insert separator in parent. Parent may
cascade. Root split grows tree by 1.

Leaf underflow → merge with sibling or redistribute. Parent may cascade.
Root merge shrinks tree by 1. Underflow threshold tunable; default 30% fill.

## 7. Fixed-record LSM (dedup)

### 7.1 Memtable

`BTreeMap<[u8; 32], DedupOp>` protected by a single lock, plus an
immutable-handoff slot for flush. Write rate is bounded by flush throughput.

Alternative considered: skiplist. Not worth the complexity for our workload
(point lookups dominate; memtable small).

### 7.2 SST lookup

```
lookup(hash):
    for level in 0..num_levels:
        for sst in level:  // overlap-sorted in L0, key-ranged in L1+
            if bloom(sst).maybe_contains(hash):
                if let Some(v) = binary_search(sst, hash) { return Some(v); }
    None
```

Bloom default: 10 bits per entry (~1% false positive).

### 7.3 Compaction

- L0 → L1 triggered by L0 file count (default 4).
- L1 → Ln triggered by level size ratio (default 10x).
- Compaction merges records by key; later generation wins; tombstone drops
  at last level only.
- Single-thread compaction is fine for dedup (bounded by disk, not CPU).

## 8. Transactions and group commit

### 8.1 Commit path

```
tx.commit():
    record = serialize_ops(tx.ops)
    (lsn, ack) = wal.submit(record)
    ack.wait()     // returns after fsync
    return lsn
```

### 8.2 Group commit

A single `wal_committer` thread drains the submit queue:
```
loop:
    batch = queue.drain_up_to(MAX_BATCH_BYTES or TIMEOUT_200US)
    for r in batch: write(wal_fd, r)
    fsync(wal_fd)
    for r in batch:
        apply_to_indexes(r)  // in-memory; page cache dirty marking
        r.ack.signal(assigned_lsn)
```

- `apply_to_indexes` updates memtable (LSM) and page cache (B+tree).
- Dirty pages flush to disk lazily via checkpoint.

### 8.3 Read visibility

After `apply_to_indexes` runs, reads see the commit. We expose two read
modes:
- **Committed read** (default): reads the latest applied state. No stale.
- **Snapshot read**: bind to a `SnapshotId` taken at a prior LSN.

Readers don't acquire WAL locks. Per-page latches suffice.

## 9. Snapshots

### 9.1 Take (O(1))

```
snapshot(partition, name):
    tx = begin
    tx.snapshot_take(partition, name)
    tx.commit()

apply SNAPSHOT_TAKE:
    shard_roots = partition.per_shard_root()
    for each shard_root:
        refcount_inc(shard_root)
    manifest.add_snapshot(snap_id, shard_roots, created_lsn)
```

The snapshot just pins the current N shard roots. No data moves. Later
writes to shared pages trigger CoW (§6.4).

### 9.2 Read at snapshot

```
snapshot.get(lba):
    shard = shard_of(lba)
    root = manifest.snapshot_root(snap_id, shard)
    btree_lookup(root, lba)
```

No latches beyond per-page read latch. Pages are immutable once committed.

### 9.3 Diff (Merkle-skip)

```
diff(snap_a, snap_b):
    for shard in 0..N:
        root_a = manifest.snapshot_root(snap_a, shard)
        root_b = manifest.snapshot_root(snap_b, shard)
        yield from diff_subtree(root_a, root_b)

diff_subtree(a, b):
    if a == b: return  // identical page_id → identical subtree
    if both leaves:
        yield per-entry diff
    else:
        // Align children by separator key and recurse.
        // Mismatched child ranges: walk each side fully.
```

Key invariant: page_id equality implies subtree equality (pages are
immutable; COW allocates new page_ids for changes). This is the Merkle
shortcut that makes whole-tree diff cheap for mostly-unchanged regions.

**Onyx usage**: to GC PBAs that became free between two snapshots, diff them
and for each `Changed { lba, a, b }` or `RemovedInB { lba, old }`, the old
PBA's reference held by snapshot A is released when A is dropped. The
`drop_snapshot` walk does this accounting precisely.

### 9.4 Drop snapshot

```
drop_snapshot(snap_id):
    for shard in 0..N:
        root = manifest.snapshot_root(snap_id, shard)
        walk_and_release(root, report)
    manifest.remove_snapshot(snap_id)
    return report  // (pba, delta_refcount) list

walk_and_release(page, report):
    if refcount_dec(page) > 0: return  // still shared; children untouched
    // refcount hit 0 → this page and everything it uniquely owned is freed
    if is_leaf(page):
        for (lba, bmv) in page.entries():
            dec_pba_refcount(bmv.pba, by: 1)
            if pba_refcount(bmv.pba) == 0:
                report.freed_pbas.push(bmv.pba)
    else:
        for child in page.children():
            walk_and_release(child, report)
    free_page(page)
```

- Correctness: `refcount` is the count of parents. If parent-to-child
  invariant is maintained on COW and split/merge, dropping one root
  decrements exactly the pages uniquely owned by that snapshot.
- Cost: bounded by pages unique to this snapshot. Shared subtrees cost
  O(root refcount decrement).
- Output: `DropReport { freed_pbas, ... }`. Onyx's allocator reclaims these
  PBAs.

### 9.5 Merge of adjacent snapshots

"Merge S2 into S3 (drop S2 but keep S3)" = `drop_snapshot(S2)`. No special
code path. Refcounts handle it.

"Merge S1..S4 into one fat snapshot" = drop S2, S3 individually. No
transient state; each drop is atomic.

## 10. Recovery

### 10.1 Model

- Manifest describes state as of some checkpoint LSN `L_ck`.
- WAL contains all records `L_ck < lsn <= L_tail` that are durable.
- Page files may contain partially-written pages for LSNs > `L_ck`. CRC
  detects these; they are discarded.

### 10.2 Steps

1. Read double-buffered manifest header. Pick newest with valid CRC.
2. Load manifest: partition roots, snapshot roots, LSM manifest,
   `L_ck`.
3. Initialize indexes from manifest.
4. Replay WAL from `L_ck + 1` to tail:
   - Verify each record's CRC. Torn tail → truncate.
   - For each op, call the same `apply_to_indexes` path as normal commit.
5. Resume. All committed transactions visible.

### 10.3 Guarantees

- **Commit durability**: `tx.commit()` returns only after `fsync(wal_fd)`.
- **Page integrity**: every page verified by CRC on read; torn pages fail
  loud and fast.
- **Atomicity**: a WAL record either fully replays or not at all (CRC'd).
- **Ordering**: LSN order = commit order = replay order.

### 10.4 Checkpoint

```
checkpoint():
    freeze memtable → flush to L0 SST
    flush dirty page cache for all partitions
    write new manifest with current L_ck = max_applied_lsn
    fsync manifest
    truncate WAL up to L_ck
```

Checkpoint is heavy (flush everything) but not latency-critical. Run
periodically (default: every 15 minutes or 1 GiB of WAL).

## 11. Configuration

```rust
pub struct Config {
    pub path: PathBuf,
    pub page_size: u32,                 // default 4096; fixed for v1
    pub shards_per_partition: u32,      // default 16
    pub wal_segment_bytes: u64,         // default 64 MiB
    pub group_commit_max_batch: usize,  // default 256 txns
    pub group_commit_timeout_us: u64,   // default 200 µs
    pub page_cache_bytes: u64,          // default 512 MiB
    pub lsm_memtable_bytes: u64,        // default 64 MiB
    pub lsm_bloom_bits_per_entry: u32,  // default 10
    pub lsm_l0_sst_count_trigger: u32,  // default 4
    pub lsm_level_ratio: u32,           // default 10
    pub checkpoint_bytes: u64,          // default 1 GiB of WAL
    pub direct_io: bool,                // default true on Linux
}
```

## 12. Threading model

- 1 `wal_committer` thread: drains commit queue, writes, fsyncs.
- 1 `checkpointer` thread: periodic flush.
- 1 `lsm_compactor` thread: runs compactions.
- N `flush` threads for page cache dirty writeback (configurable).
- Reader threads use the public API from the caller's threads. No dedicated
  pool.

All IO to the WAL is serialized through `wal_committer`. All writes to a
given B+tree shard are serialized through that shard's writer lock.

## 13. Integration with Onyx

### 13.1 What moves out of RocksDB

- `blockmap:{volume_id}` CFs → metadb partitions.
- `dedup_index` + `dedup_reverse` CFs → metadb dedup LSM.
- `refcount` CF → metadb per-PBA refcount (fits naturally as a small
  fixed-record LSM or a separate paged B+tree; decision in phase 5).

### 13.2 What stays

- `volumes` CF stays in a minimal separate store (or a metadb system
  partition) — it's low-traffic config data.

### 13.3 Migration

Phase 7 milestone. Tooling plan:
- `metadb-import-rocks`: read existing RocksDB CFs, write to metadb.
- Verify round-trip equivalence with diff tool.
- Cutover: stop engine, swap metadata backend, restart, validate.

## 14. Open questions

Each gets resolved in its phase:

- **Refcount index**: separate small LSM or a second B+tree partition?
  (phase 5)
- **Free list**: in-memory-only rebuilt from page scan, or persisted
  B+tree? (phase 1)
- **Compression of L2P pages**: none for v1; revisit if page-cache pressure
  becomes measurable.
- **O_DIRECT on macOS**: emulate with F_NOCACHE or skip. (phase 1)
- **Endian**: big-endian for sort keys (same as RocksDB convention), native
  for page header fields.

## 15. Out-of-band tooling

See `docs/TESTING.md` for the full matrix. At minimum:

- `metadb-dump`: inspect a page, partition, or snapshot manifest.
- `metadb-verify`: full consistency check (page CRCs, refcount
  consistency, manifest vs. WAL).
- `metadb-bench`: microbench harness (put/get/range/snapshot/diff).
- `metadb-replay`: deterministic replay of a recorded workload under a
  seed for debugging.
- `metadb-faultctl`: manual fault injection for manual test scenarios.
