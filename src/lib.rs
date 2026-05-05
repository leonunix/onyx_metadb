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

pub mod affinity;
pub mod apply_gate;
pub mod cache;
pub mod config;
pub mod db;
pub mod dedup;
pub mod dedup_types;
pub mod epoch;
pub mod error;
pub mod fuzz;
pub mod manifest;
pub mod metrics;
pub mod page;
pub mod page_store;
pub mod paged;
pub mod paged_meta;
pub mod paged_reverse;
pub mod recovery;
pub mod refcount;
pub mod testing;
pub mod tx;
pub mod types;
pub mod verify;
pub mod wal;

pub use cache::{PageCache, PageCacheStats};
pub use config::{Config, MAX_DEDUP_SHARDS, PAGE_SIZE};
pub use db::{
    Db, DbDedupIter, DbRangeIter, DbRefcountIter, DropReport, DropVolumeReport, PendingState,
    SnapshotView,
};
pub use dedup_types::{DEDUP_VALUE_SIZE, DedupValue, HASH_SIZE, Hash8, LsmStats};
pub use error::{MetaDbError, Result};
pub use manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, VOLUME_ENTRY_FIXED_SIZE,
    VOLUME_FLAG_DROP_PENDING, VolumeEntry, decode_volume_entry_inline, encode_volume_entry_inline,
    max_snapshots_for_shards, volume_entry_inline_size,
};
pub use metrics::{MetaMetrics, MetaMetricsSnapshot};
pub use page::{PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
pub use page_store::PageStore;
pub use paged::{DiffEntry, L2pValue, PagedL2p};
pub use recovery::{ReplayOutcome, replay};
pub use tx::{ApplyOutcome, Transaction};
pub use types::{
    FIRST_DATA_PAGE, INVALID_VOLUME, Lba, Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId,
    PartitionId, Pba, SnapshotId, VolumeOrdinal,
};
pub use verify::{VerifyOptions, VerifyReport, verify_path};
