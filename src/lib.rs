//! onyx-metadb: embedded metadata engine for Onyx Storage.
//!
//! Two workload-specialized indexes in a single engine, sharing one WAL:
//! - Sharded COW B+tree for L2P (per-volume, fixed 8B key → 28B value).
//! - Fixed-record LSM for global dedup (32B hash → 27B entry).
//!
//! Public API, recovery semantics, and snapshot model are documented in
//! [`docs/DESIGN.md`](../docs/DESIGN.md). Implementation phases are in
//! [`docs/ROADMAP.md`](../docs/ROADMAP.md).
//!
//! # What's implemented today
//!
//! Phase 1 is landing piecewise. The first slice (this commit) provides
//! the shared foundation:
//! - [`types`]: integer aliases and sentinels.
//! - [`error`]: the flat `MetaDbError` enum and `Result` alias.
//! - [`config`]: `Config` with defaults matching DESIGN §11.
//! - [`page`]: 4 KiB page codec with CRC32C.
//! - [`page_store`]: file-backed alloc / read / write / free.
//!
//! WAL, manifest, recovery, and the index types land in subsequent
//! commits. See the roadmap.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod btree;
pub mod cache;
pub mod config;
pub mod db;
pub mod error;
pub mod lsm;
pub mod manifest;
pub mod page;
pub mod page_store;
pub mod recovery;
pub mod testing;
pub mod tx;
pub mod types;
pub mod wal;

pub use btree::{
    BTree, DiffEntry, L2pValue, MAX_INTERNAL_KEYS, MAX_LEAF_ENTRIES, PageBuf, RangeIter,
};
pub use cache::{PageCache, PageCacheStats};
pub use config::{Config, PAGE_SIZE};
pub use db::{Db, DbRangeIter, DropReport, SnapshotView};
pub use error::{MetaDbError, Result};
pub use lsm::{
    DEDUP_VALUE_SIZE, DedupOp, DedupValue, HASH_SIZE, Hash32, LSM_RECORD_SIZE, LookupResult,
    Memtable, MemtableStats, RECORDS_PER_PAGE, Record,
};
pub use manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, max_snapshots_for_shards,
};
pub use page::{PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
pub use page_store::PageStore;
pub use recovery::{ReplayOutcome, replay};
pub use tx::{ApplyOutcome, Transaction};
pub use types::{
    FIRST_DATA_PAGE, Lba, Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, PartitionId,
    Pba, SnapshotId,
};
