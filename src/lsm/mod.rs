//! Fixed-record LSM for the global dedup index.
//!
//! Key workload: 32-byte SHA-256 content hashes mapped to a 28-byte
//! `DedupEntry`. Point lookups dominate; range scans are only used by
//! PBA-triggered reverse cleanup (a separate LSM instance keyed on
//! `(pba, hash)`).
//!
//! This module lands in slices:
//! 1. [`format`]: fixed 64-byte record codec and associated sizes.
//! 2. [`memtable`]: sorted in-memory buffer of pending ops with a
//!    freeze/release handoff protocol for the flusher.
//! 3. (future) `sst`: on-disk run format, writer, reader, bloom filter.
//! 4. (future) `lsm`: the multi-level facade that glues memtable + SSTs.
//! 5. (future) `compact`: L0→L1 and Ln→Ln+1 merges.

pub mod bloom;
pub mod format;
pub mod memtable;
pub mod sst;

pub use bloom::{BloomFilter, DEFAULT_BITS_PER_ENTRY};
pub use format::{
    DEDUP_VALUE_SIZE, DedupValue, HASH_SIZE, Hash32, KIND_DELETE, KIND_PUT, LSM_RECORD_SIZE,
    RECORDS_PER_PAGE, Record,
};
pub use memtable::{DedupOp, LookupResult, Memtable, MemtableStats};
pub use sst::{SST_LAYOUT_VERSION, SstHandle, SstReader, SstScan, SstWriter};
