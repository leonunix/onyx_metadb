//! Fixed-record LSM for the global dedup index.
//!
//! Key workload: 32-byte SHA-256 content hashes mapped to a 28-byte
//! `DedupEntry`. Point lookups dominate; range scans are only used by
//! PBA-triggered reverse cleanup (a separate LSM instance keyed on
//! `(pba, hash)`).
//!
//! Modules landed in phase 5:
//! - [`format`]: fixed 64-byte record codec and associated sizes.
//! - [`memtable`]: sorted in-memory buffer of pending ops with a
//!   freeze/release handoff protocol for the flusher.
//! - [`bloom`]: double-hashing bloom filter sized per-SST.
//! - [`sst`]: on-disk run format, writer, reader, and scan iterator.
//! - [`persist`]: chained `LsmLevels` pages for serialising the level
//!   → SST-handle table into the manifest.
//! - [`lsm`]: memtable + multi-level facade with point `get`, memtable
//!   flush to L0, and reader-drain coordination.
//! - [`compact`]: L0 → L1 and leveled Ln → Ln+1 merges.
//!
//! # Phase-5 items deferred into phase 6
//!
//! - **`dedup_reverse` LSM instance** (`(pba, hash_first_24B) →
//!   hash_last_8B`). Used by PBA refcount → 0 to locate and tombstone
//!   the corresponding `dedup_index` entries. Intentionally not
//!   implemented here because correctness requires atomic commit
//!   across refcount tree + `dedup_index` + `dedup_reverse`, which is
//!   exactly what phase 6's transaction layer provides. See
//!   [`docs/ROADMAP.md`](../../docs/ROADMAP.md) Phase 5 → "Deferred".
//! - **WAL-backed mutation replay**. Today a crash between `Db::flush`
//!   calls can lose LSM + refcount writes. Phase 6 wires ops through
//!   the WAL.
//! - **Property + crash-injection test suite (phase 5f)**. Planned
//!   against the unified transaction API from phase 6 rather than the
//!   intermediate per-call API here.

pub mod bloom;
pub mod compact;
pub mod format;
#[allow(clippy::module_inception)]
pub mod lsm;
pub mod memtable;
pub mod persist;
pub mod sst;

pub use bloom::{BloomFilter, DEFAULT_BITS_PER_ENTRY};
pub use compact::CompactionReport;
pub use format::{
    DEDUP_VALUE_SIZE, DedupValue, HASH_SIZE, Hash32, KIND_DELETE, KIND_PUT, LSM_RECORD_SIZE,
    RECORDS_PER_PAGE, Record,
};
pub use lsm::{Lsm, LsmConfig, LsmStats};
pub use memtable::{DedupOp, LookupResult, Memtable, MemtableStats};
pub use persist::{HANDLES_PER_PAGE, LSM_LEVELS_LAYOUT_VERSION};
pub use sst::{SST_LAYOUT_VERSION, SstHandle, SstReader, SstScan, SstWriter};
