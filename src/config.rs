//! Configuration knobs exposed to the embedder.
//!
//! Defaults picked to match the values documented in `docs/DESIGN.md §11`.
//! Each field has a phase at which it first becomes meaningful; fields that
//! are not yet consumed by any code path are still listed here so the
//! surface is stable across phases.

use std::path::PathBuf;

/// Upper bound on [`Config::dedup_shards`]. Sized so the manifest's
/// per-shard level-head section never starves the snapshot/volume
/// region.
pub const MAX_DEDUP_SHARDS: u32 = 64;

/// Page size in bytes. Fixed for v1; not a runtime choice.
///
/// Changing this would require re-encoding every on-disk format constant,
/// so it lives here as a compile-time constant rather than a config field.
pub const PAGE_SIZE: usize = 4096;

/// Embedder-provided configuration for opening a database.
#[derive(Clone, Debug)]
pub struct Config {
    /// Directory that will hold the page file, WAL segments, and manifest.
    pub path: PathBuf,

    /// Number of shards per partition for the L2P B+tree. Consumed from
    /// phase 4 onward.
    ///
    /// Phase 7 renames the concept from "partition" to "volume". The field
    /// still exists for transitional tests, but new code should read
    /// [`Config::shards_per_volume`] — the two resolve to the same numeric
    /// value. After Phase 7's commit-path reshape lands, `shards_per_partition`
    /// becomes a deprecated alias.
    pub shards_per_partition: u32,

    /// Upper bound on the number of volumes a single `Db` can hold. Used to
    /// validate manifest capacity at create / open time; exceeding it is a
    /// configuration error, not a runtime drift. Defaults to 1024.
    pub max_volumes: u32,

    /// Size of a single WAL segment before rotation. Consumed from phase 1.
    pub wal_segment_bytes: u64,

    /// Number of WAL writer lanes used by `Db`. Each lane has its own
    /// writer thread, segment directory, and fsync stream. Global LSNs
    /// are still assigned from one short critical section so recovery
    /// can merge lanes into the existing total order. Default to one
    /// lane so synchronous Onyx flush writers actually coalesce into
    /// group commits; raise this only on storage where parallel fsync
    /// streams beat larger batches.
    pub wal_lanes: u32,

    /// Upper bound on a single group-commit batch, in bytes.
    pub group_commit_max_batch_bytes: usize,

    /// How long the group-commit thread waits for more work before flushing
    /// a partial batch, in microseconds.
    pub group_commit_timeout_us: u64,

    /// Maximum bytes held by the in-memory page cache.
    pub page_cache_bytes: u64,

    /// Byte size at which a memtable is frozen and handed to the flusher.
    pub lsm_memtable_bytes: u64,

    /// Bloom filter sizing for LSM SSTs.
    pub lsm_bloom_bits_per_entry: u32,

    /// L0 SST count that triggers L0→L1 compaction.
    pub lsm_l0_sst_count_trigger: u32,

    /// Size ratio between adjacent LSM levels (L_{n+1} = ratio × L_n).
    pub lsm_level_ratio: u32,

    /// Number of dedup LSM shards (`dedup_index` and `dedup_reverse` each
    /// fan out into this many independent LSMs). Must be a power of two
    /// in `[1, 64]`. Routing is by SHA-256 high bits, so shards see a
    /// uniform random partition of the workload.
    ///
    /// Set at [`Db::create`](crate::db::Db::create) and recorded in the
    /// manifest. Opening with `cfg.dedup_shards != manifest.dedup_shards`
    /// is rejected — the shard count is part of the on-disk layout, not
    /// a runtime tunable. Default is 1 (single-shard, identical to the
    /// pre-Phase-2 layout).
    pub dedup_shards: u32,

    /// Amount of WAL (bytes) accumulated before a checkpoint is forced.
    pub checkpoint_bytes: u64,

    /// Use `O_DIRECT` on Linux (or `F_NOCACHE` on macOS) for page and WAL
    /// IO. Disable when running on tmpfs in tests.
    pub direct_io: bool,

    /// How many 4 KiB pages to pre-extend the page file by on each
    /// `set_len` call. `PageStore::allocate` maintains `file_size >=
    /// high_water * PAGE_SIZE`; when `high_water` crosses a chunk
    /// boundary, one syscall extends the file by `page_grow_chunk_pages`
    /// pages instead of one page per allocation. Amortises the extend
    /// cost during bulk ingest (prefill dominated by syscalls before
    /// this knob landed). Must be `>= 1`. 512 pages = 2 MiB per extend.
    pub page_grow_chunk_pages: u64,

    /// Per-`Db` upper bound on bytes used to pin L2P index pages in the
    /// page cache, so random L2P gets never miss on inner nodes.
    /// Index pages are ~1/256 of leaf bytes (INDEX_FANOUT=256), so 512 MiB
    /// covers ~130 GiB of leaf data, i.e. on the order of billions of
    /// LBAs. Pinned pages live outside the LRU and do not compete with
    /// leaf capacity. Set to 0 to disable; on large-memory deployments
    /// (e.g. 512 GiB RAM) this can be raised to tens of GiB to cover
    /// trillion-key datasets.
    pub index_pin_bytes: u64,

    /// Rebuild the in-memory page free list by scanning the whole page file
    /// during open. This is safest for offline tests and small databases, but
    /// it makes startup time proportional to the historical high-water mark.
    /// Large embedded deployments can disable it and rely on normal runtime
    /// reclaim plus offline `metadb-verify` for deep repair.
    pub rebuild_free_list_on_open: bool,

    /// Run the full orphan-page reclamation walk during open. The walk is an
    /// online repair pass after crash/replay windows, but it also scans every
    /// page below high-water. Large services usually want fast open and an
    /// explicit maintenance/verify job instead.
    pub reclaim_orphans_on_open: bool,

    /// Number of buckets in the dedup_index cuckoo hash table. Each
    /// bucket holds 4 entries (`crate::dedup::cuckoo::ENTRIES_PER_BUCKET`),
    /// so the upper-bound capacity is `dedup_cuckoo_buckets * 4`. Pick
    /// `target_entries / (4 * load_factor)`; a load factor over ~0.85
    /// makes cuckoo eviction chains long and risks `Corruption("table
    /// full")`. Recorded in the manifest at `Db::create`.
    pub dedup_cuckoo_buckets: u64,

    /// Maximum number of entries kept in the dedup_index L1 hot
    /// cache. Each entry costs roughly 64 B (`(fp, hash, value, lru
    /// link)`), so 1 M entries ≈ 64 MiB. Larger caches reduce L3
    /// IOPS but cost RAM 1:1.
    pub dedup_l1_cache_entries: usize,
}

impl Config {
    /// Phase-7 accessor for the per-volume shard count. Reads from
    /// [`Config::shards_per_partition`]; kept as a dedicated method so call
    /// sites migrate to the new name without waiting for the field rename in
    /// Phase B.
    pub fn shards_per_volume(&self) -> u32 {
        self.shards_per_partition
    }

    /// Fresh config with every knob at its documented default and `path`
    /// pointing at the given directory.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            shards_per_partition: 16,
            max_volumes: 1024,
            wal_segment_bytes: 64 * 1024 * 1024,
            wal_lanes: 1,
            group_commit_max_batch_bytes: 4 * 1024 * 1024,
            group_commit_timeout_us: 1,
            page_cache_bytes: 512 * 1024 * 1024,
            lsm_memtable_bytes: 64 * 1024 * 1024,
            lsm_bloom_bits_per_entry: 10,
            lsm_l0_sst_count_trigger: 4,
            lsm_level_ratio: 10,
            dedup_shards: 1,
            checkpoint_bytes: 1024 * 1024 * 1024,
            direct_io: cfg!(target_os = "linux"),
            page_grow_chunk_pages: 512,
            index_pin_bytes: 512 * 1024 * 1024,
            rebuild_free_list_on_open: true,
            reclaim_orphans_on_open: true,
            // Single-meta-page cap is 503 pages × 16 buckets =
            // 8 048 buckets × 4 entries = ~32 K cuckoo capacity at
            // load factor 1.0 (target ~27 K at 0.85). Sized for
            // unit tests + small soaks; production overrides via
            // config until chained meta pages land in stage 3.x.
            dedup_cuckoo_buckets: 8_000,
            // 8 K L1 hot cache by default — matches the test-scale
            // capacity above. Production should bump in lock-step
            // with `dedup_cuckoo_buckets`.
            dedup_l1_cache_entries: 8_000,
        }
    }
}
