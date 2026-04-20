//! Configuration knobs exposed to the embedder.
//!
//! Defaults picked to match the values documented in `docs/DESIGN.md §11`.
//! Each field has a phase at which it first becomes meaningful; fields that
//! are not yet consumed by any code path are still listed here so the
//! surface is stable across phases.

use std::path::PathBuf;

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
    pub shards_per_partition: u32,

    /// Size of a single WAL segment before rotation. Consumed from phase 1.
    pub wal_segment_bytes: u64,

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

    /// Amount of WAL (bytes) accumulated before a checkpoint is forced.
    pub checkpoint_bytes: u64,

    /// Use `O_DIRECT` on Linux (or `F_NOCACHE` on macOS) for page and WAL
    /// IO. Disable when running on tmpfs in tests.
    pub direct_io: bool,
}

impl Config {
    /// Fresh config with every knob at its documented default and `path`
    /// pointing at the given directory.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            shards_per_partition: 16,
            wal_segment_bytes: 64 * 1024 * 1024,
            group_commit_max_batch_bytes: 4 * 1024 * 1024,
            group_commit_timeout_us: 200,
            page_cache_bytes: 512 * 1024 * 1024,
            lsm_memtable_bytes: 64 * 1024 * 1024,
            lsm_bloom_bits_per_entry: 10,
            lsm_l0_sst_count_trigger: 4,
            lsm_level_ratio: 10,
            checkpoint_bytes: 1024 * 1024 * 1024,
            direct_io: cfg!(target_os = "linux"),
        }
    }
}
