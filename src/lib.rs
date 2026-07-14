//! onyx-metadb: embedded metadata engine for Onyx Storage.
//!
//! Two workload-specialized indexes:
//! - Sharded COW radix tree for L2P (per-volume, fixed 8B key → 36B value).
//! - Cuckoo hash for global dedup (8B hash → 27B entry).
//!
//! Durability comes from two sources: the LV2 write buffer (owned by
//! onyx; replayed through the flusher on recovery) carries every
//! data-plane mutation, and an internal lifecycle journal carries the
//! rare non-data-plane ops (volume create/drop/clone, snapshot drop,
//! promotion cursor, discard). The legacy metadb-internal WAL has been
//! retired (lifecycle journal cutover).

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod affinity;
pub mod apply_gate;
pub mod bfg;
pub mod cache;
pub mod config;
pub mod db;
pub mod deadlist;
pub mod dedup;
pub mod dedup_types;
pub mod epoch;
pub mod error;
pub mod fuzz;
pub(crate) mod io_submitter;
pub mod lifecycle_log;
pub mod livelist;
pub mod manifest;
pub mod metrics;
pub mod op;
pub mod page;
pub mod page_store;
pub mod paged;
pub mod paged_meta;
pub mod refcount;
pub mod testing;
pub mod tx;
pub mod types;
pub(crate) mod u64_hash;
pub mod verify;

pub use cache::{PageCache, PageCacheStats};
pub use config::{Config, MAX_DEDUP_SHARDS, MetaDbJournalMode, PAGE_SIZE};
pub use db::{
    Db, DbDedupIter, DbRangeIter, DbRefcountIter, DedupScanBatch, DedupScanCursor,
    DeferredOutcomeHandle, DropReport, DropVolumeReport, FreedPbasSink, PendingState,
    RestoreReport, SnapshotView,
};
pub use dedup_types::{DEDUP_VALUE_SIZE, DedupValue, HASH_SIZE, Hash8, LsmStats};
pub use error::{MetaDbError, Result};
pub use lifecycle_log::{JournalDevice, RING_BLOCK_SIZE};
pub use manifest::{
    LoadedManifest, MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry,
    VOLUME_ENTRY_FIXED_SIZE, VOLUME_FLAG_CLONE_LINEAGE, VOLUME_FLAG_DROP_PENDING, VolumeEntry,
    decode_volume_entry_inline, encode_volume_entry_inline, max_snapshots_for_shards,
    volume_entry_inline_size,
};
pub use metrics::{FlushKind, MetaMetrics, MetaMetricsSnapshot};
pub use page::{PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
pub use page_store::{BlockPageDevice, PageBlockIo, PageDevice, PageStore, ReclaimOutcome};
pub use paged::{DiffEntry, L2pValue, PagedL2p};
pub use tx::{ApplyOutcome, Transaction};
pub use types::{
    FIRST_DATA_PAGE, INVALID_VOLUME, Lba, Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId,
    PartitionId, Pba, SnapshotId, VolumeOrdinal,
};
pub use verify::{VerifyOptions, VerifyReport, audit_clone_birth_shadow, verify_path};
