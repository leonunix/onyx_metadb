//! Sharded embedded metadata database: the glue between `PageStore`,
//! `ManifestStore`, and one `BTree` per shard.
//!
//! Phase 4 scope:
//! - N independent COW B+tree shards behind one `Db`
//! - xxh3-based shard router
//! - thread-safe point writes via one mutex per shard
//! - fan-out range / diff / snapshot operations

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use xxhash_rust::xxh3::xxh3_64;

use crate::btree::BTree;
use crate::btree::format::RcEntry;
use crate::cache::{PageCache, PageCacheStats};
use crate::config::Config;
use crate::error::{MetaDbError, Result};
use crate::lsm::{DedupValue, Hash32, Lsm, LsmConfig};
use crate::manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, VolumeEntry,
    write_snapshot_roots_page,
};
use crate::metrics::{MetaMetrics, MetaMetricsSnapshot};
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::paged::PagedL2p;
use crate::paged::{DiffEntry, L2pValue};
use crate::testing::faults::{FaultController, FaultPoint};
use crate::tx::{ApplyOutcome, Transaction};
use crate::types::{FIRST_DATA_PAGE, Lba, Lsn, PageId, Pba, SnapshotId, VolumeOrdinal};
use crate::verify;
use crate::wal::{Wal, WalOp, encode_body};

/// Ordinal of the always-present bootstrap volume. Phase B commit 5 keeps the
/// surface API single-volume, so every L2P routing decision lands here. Later
/// commits take per-volume arguments from callers and route through the map
/// directly.
const BOOTSTRAP_VOLUME_ORD: VolumeOrdinal = 0;

/// Embedded metadata database.
pub struct Db {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    metrics: Arc<MetaMetrics>,
    manifest_state: Mutex<ManifestState>,
    /// Per-volume L2P paged radix-tree shard groups. Phase B commit 5 always
    /// contains exactly one entry for [`BOOTSTRAP_VOLUME_ORD`]; commit 6/7
    /// introduce real `create_volume` / `drop_volume` / `clone_volume` traffic
    /// that mutates this map. The map lives behind an `RwLock` so the hot
    /// path (commit / get / range) takes `.read()` — contention happens only
    /// against the rare volume-lifecycle writer.
    ///
    /// Each volume owns its own `Vec<L2pShard>`; xxh3 routing divides by
    /// `volume.shards.len()`, so shard routing is identical to the pre-7
    /// flat-shard layout as long as every volume is created with the same
    /// shard count.
    volumes: RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>,
    /// PBA refcount B+tree shards (PBA → first 4 bytes = u32 big-endian
    /// refcount, remaining 24 bytes reserved). Refcount is a global running
    /// tally — not per-volume — and stays at the top level for that reason.
    refcount_shards: Vec<Shard>,
    /// Global dedup index: 32-byte SHA-256 content hash → 28-byte opaque
    /// `DedupValue`.
    dedup_index: Lsm,
    /// Reverse index: key = `[pba: 8B BE][hash_first_24B]`, value =
    /// `[hash_last_8B | zero padding]`. Used by PBA refcount → 0 to
    /// discover and clean up the `dedup_index` entries whose PBA is
    /// going away. Prefix-scan by 8-byte PBA locates every matching
    /// row.
    dedup_reverse: Lsm,
    /// Write-ahead log. All mutations route through here so they survive
    /// crash between checkpoints.
    wal: Wal,
    /// Excludes apply phases from flush / snapshot. Commit takes
    /// `.read()` across the apply + bump; flush / take_snapshot /
    /// drop_snapshot take `.write()` so they observe a quiescent tree
    /// state matching `last_applied_lsn`. Replaces the phase-6
    /// `commit_lock`: submission to the WAL now happens **outside** any
    /// lock, so concurrent submitters land in the same WAL group-commit
    /// batch. Apply order is restored by the LSN-ordered condvar queue
    /// below, not by serialising WAL submits.
    apply_gate: RwLock<()>,
    /// LSN of the most recent op applied to in-memory state. Initialised
    /// from `manifest.checkpoint_lsn` on open (the manifest promises that
    /// every LSN at or below this value is already reflected in the
    /// trees / SSTs) and bumped on every commit. Paired with
    /// [`commit_cvar`](Self::commit_cvar) to form the apply-order queue.
    last_applied_lsn: Mutex<Lsn>,
    /// Notified whenever `last_applied_lsn` advances. Commit threads
    /// wait on this after WAL submit returns with their assigned LSN,
    /// re-checking `*last_applied_lsn + 1 == lsn` on each wakeup. Every
    /// LSN is unique, so at most one thread waits for any given
    /// predecessor value.
    commit_cvar: Condvar,
    /// Snapshot readers hold a shared guard; `drop_snapshot` takes the
    /// exclusive side so it can't free pages still visible to a live view.
    snapshot_views: RwLock<()>,
    /// Serialises `drop_snapshot` against *all* other mutations.
    /// Every write path in [`commit_ops`](Self::commit_ops) (user writes,
    /// incref/decref, dedup puts) takes the read side; `drop_snapshot`
    /// takes the write side. This is necessary because the drop plan
    /// is rc-dependent — a concurrent `cow_for_write` landing between
    /// plan computation and WAL apply can change the rcs of pages
    /// shared with the snapshot, invalidating the cascade decisions
    /// baked into the plan. The normal `apply_gate` is insufficient
    /// because concurrent submitters queue WAL records *before*
    /// taking the apply gate, so their ops can sneak between our plan
    /// and our apply.
    ///
    /// Lock order: `drop_gate` → `apply_gate` → `volumes` →
    /// `manifest_state` → shard mutex → `snapshot_views`. Everyone
    /// entering `commit_ops` or `drop_snapshot` respects this prefix;
    /// internal reads that don't mutate skip `drop_gate` entirely. The
    /// `volumes` link is read-only in the hot path — `.read()` is taken
    /// just long enough to clone the `Arc<Volume>` out so shard mutexes
    /// can be acquired without keeping the map guard alive.
    drop_gate: RwLock<()>,
    /// Per-volume cache of live snapshot info — `created_lsn` plus the
    /// L2P shard roots needed to read the snapshot's value at any lba.
    /// `apply_l2p_remap` consults this to decide whether decref of a
    /// pba would orphan content a live snapshot still pins.
    ///
    /// Decision (per L2pRemap op overwriting `(V, lba, old_pba)`):
    /// 1. Fast filter: if `old_pba.birth_lsn > min(snap.created_lsn for
    ///    snap in cache[V])`, no snap can pin this content → decref.
    /// 2. Otherwise read each snap's L2P at `(V, lba)`; suppress decref
    ///    iff any snap has that lba mapping to `old_pba`.
    ///
    /// Populated from `manifest.snapshots` at open and refreshed on
    /// `take_snapshot` / `drop_snapshot` / `drop_volume`. Vec is empty
    /// (or absent) when the volume has no live snapshot — fast filter
    /// returns false in that case, hot path stays free of snap reads.
    snap_info_cache: Mutex<BTreeMap<VolumeOrdinal, Vec<SnapInfo>>>,
    /// Runtime cap on the volumes table size. Seeded from
    /// [`Config::max_volumes`] at create / open. `create_volume` refuses
    /// to mint a new ordinal once the live volume count hits this value.
    max_volumes: u32,
    #[allow(dead_code)]
    faults: Arc<FaultController>,
    #[allow(dead_code)]
    db_path: PathBuf,
}

struct ManifestState {
    store: ManifestStore,
    manifest: Manifest,
}

/// Snapshot view info cached per volume, used by `apply_l2p_remap` /
/// `apply_l2p_range_delete` to decide whether decref of a pba would
/// orphan content a live snapshot still pins. See [`Db::snap_info_cache`].
#[derive(Clone, Debug)]
struct SnapInfo {
    created_lsn: Lsn,
    /// Per-shard root page ids. Indexed by `shard_for_key_l2p(...)`,
    /// matching the volume's live shard layout (snapshot's roots are
    /// captured from the same shard group at take time, so the indices
    /// align).
    l2p_shard_roots: Box<[PageId]>,
}

struct Shard {
    tree: Mutex<BTree>,
}

struct L2pShard {
    tree: RwLock<PagedL2p>,
}

/// L2P home for one user-facing volume. Owns its own shard group; shard
/// routing inside a volume uses `xxh3_64(lba) % shards.len()`, identical to
/// the pre-7 flat layout.
///
/// Fields beyond `shards` are placeholders for commit 6/7 semantics:
/// `created_lsn` will be stamped by `CreateVolume` / `CloneVolume` so
/// recovery can skip L2P ops for volumes that hadn't been created yet at a
/// given LSN; `flags` is reserved for the drop-pending bit.
#[allow(dead_code)]
struct Volume {
    ord: VolumeOrdinal,
    shards: Vec<L2pShard>,
    created_lsn: Lsn,
    flags: AtomicU8,
}

impl Volume {
    fn new(ord: VolumeOrdinal, shards: Vec<L2pShard>, created_lsn: Lsn) -> Self {
        Self {
            ord,
            shards,
            created_lsn,
            flags: AtomicU8::new(0),
        }
    }
}

struct DedupManifestUpdate {
    old_dedup_heads: Vec<PageId>,
    old_dedup_reverse_heads: Vec<PageId>,
}

/// Iterator over a globally key-ordered range scan assembled from all
/// shards.
pub struct DbRangeIter {
    inner: std::vec::IntoIter<(u64, L2pValue)>,
}

impl DbRangeIter {
    fn new(items: Vec<(u64, L2pValue)>) -> Self {
        Self {
            inner: items.into_iter(),
        }
    }
}

impl Iterator for DbRangeIter {
    type Item = Result<(u64, L2pValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

/// Iterator over every `(Pba, refcount)` pair in the global refcount
/// table, in Pba order. Currently materialised upfront across all
/// refcount shards; the `impl Iterator` surface lets future commits
/// swap the body for a lazy walker without touching call sites.
pub struct DbRefcountIter {
    inner: std::vec::IntoIter<(Pba, u32)>,
}

impl Iterator for DbRefcountIter {
    type Item = Result<(Pba, u32)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

/// Iterator over every live `(Hash32, DedupValue)` entry in the
/// dedup forward index. Tombstoned rows are hidden. Output is sorted
/// by hash.
pub struct DbDedupIter {
    inner: std::vec::IntoIter<(Hash32, DedupValue)>,
}

impl Iterator for DbDedupIter {
    type Item = Result<(Hash32, DedupValue)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

#[derive(Clone, Debug)]
struct OwnedRange {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl OwnedRange {
    fn new<R: RangeBounds<u64>>(range: R) -> Self {
        Self {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        }
    }
}

impl RangeBounds<u64> for OwnedRange {
    fn start_bound(&self) -> Bound<&u64> {
        ref_bound(&self.start)
    }

    fn end_bound(&self) -> Bound<&u64> {
        ref_bound(&self.end)
    }
}

/// Paths used by a `Db` on disk.
fn page_file(root: &Path) -> PathBuf {
    root.join("pages.onyx_meta")
}

/// Directory that holds the WAL segments.
fn wal_dir(root: &Path) -> PathBuf {
    root.join("wal")
}

fn clone_bound(bound: Bound<&u64>) -> Bound<u64> {
    match bound {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn ref_bound(bound: &Bound<u64>) -> Bound<&u64> {
    match bound {
        Bound::Included(v) => Bound::Included(v),
        Bound::Excluded(v) => Bound::Excluded(v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

mod apply;
mod commit;
mod helpers;
mod indexes;
mod l2p;
mod lifecycle;
mod snapshot;
mod volume;

pub use snapshot::{DropReport, SnapshotView};

use apply::*;
use helpers::*;
pub use volume::DropVolumeReport;

#[cfg(test)]
mod tests;
