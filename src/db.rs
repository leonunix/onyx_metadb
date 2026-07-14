//! Sharded embedded metadata database: the glue between `PageStore`,
//! `ManifestStore`, and one `BTree` per shard.
//!
//! - N independent COW B+tree shards behind one `Db`
//! - xxh3-based shard router
//! - thread-safe point writes via one mutex per shard
//! - fan-out range / diff / snapshot operations

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize};
use std::thread::JoinHandle;
use std::time::Instant;

use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use xxhash_rust::xxh3::xxh3_64;

use crate::apply_gate::ApplyGate;
use crate::cache::{PageCache, PageCacheStats};
use crate::config::Config;
use crate::dedup_types::{DedupValue, Hash8};
use crate::error::{MetaDbError, Result};
use crate::manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, VolumeEntry,
    write_snapshot_roots_page,
};
use crate::metrics::{MetaMetrics, MetaMetricsSnapshot};
use crate::op::WalOp;
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::paged::PagedL2p;
use crate::paged::{DiffEntry, L2pValue};
use crate::testing::faults::{FaultController, FaultPoint};
use crate::tx::{ApplyOutcome, Transaction};
use crate::types::{FIRST_DATA_PAGE, Lba, Lsn, PageId, Pba, SnapshotId, VolumeOrdinal};
use crate::verify;

/// Ordinal of the always-present bootstrap volume. Legacy single-volume entry
/// points route here; volume-aware APIs route through the volume map directly.
const BOOTSTRAP_VOLUME_ORD: VolumeOrdinal = 0;

/// Embedded metadata database.
pub struct Db {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    metrics: Arc<MetaMetrics>,
    /// Wrapped in `Arc` so background workers (e.g. the Lineage GC pass
    /// inside [`async_reclaim`]) can clone the handle at start-up and
    /// perform their own short manifest commits without holding an
    /// `Arc<Db>` reference (which would create a circular shutdown).
    /// All access still goes through the same `Mutex<ManifestState>`,
    /// so lock semantics are unchanged.
    manifest_state: Arc<Mutex<ManifestState>>,
    /// Per-volume L2P paged radix-tree shard groups. The map lives behind an
    /// `RwLock` so the hot path (commit / get / range) takes `.read()`; contention
    /// happens only against rare volume-lifecycle writers.
    ///
    /// Each volume owns its own `Vec<L2pShard>`; xxh3 routing divides by
    /// `volume.shards.len()`.
    ///
    /// Wrapped in `Arc` so background workers (e.g. the L2P streaming
    /// writeback in [`streaming_flush`]) can hold a reference and iterate
    /// volumes from a non-`&self` context.
    volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
    /// PBA refcount B+tree shards (PBA → first 4 bytes = u32 big-endian
    /// refcount, remaining 24 bytes reserved). Refcount is a global running
    /// tally — not per-volume — and stays at the top level for that reason.
    refcount_shards: Vec<Shard>,
    /// Global dedup index: content hash -> opaque `DedupValue`. Backed by
    /// [`crate::dedup::DedupIndex`] so the apply path can fan writes across
    /// independent dedup shards.
    dedup_index: Arc<crate::dedup::DedupIndex>,
    /// One FIFO apply lane per dedup shard. Each shard's lane
    /// preserves WAL-order apply for ops within that shard; ops in
    /// disjoint shards run in parallel because they hold disjoint
    /// `DispatchLaneKey::Dedup(u32)` footprints. Length always equals
    /// `manifest.dedup_shards`.
    dedup_lanes: Box<[ApplyLane]>,
    /// One background lane per dedup shard for memtable flushes.
    /// Separate from `dedup_lanes` so foreground apply is never queued
    /// behind SST construction; per-shard so a slow flush in one shard
    /// can't stall the rest.
    dedup_maintenance_lanes: Box<[ApplyLane]>,
    /// Per-shard double-queue guards. Each shard keeps high-QPS
    /// writers from filling its maintenance lane with duplicate
    /// background flush jobs once the active LSM crosses its
    /// threshold.
    dedup_maintenance_queued: Box<[Arc<AtomicBool>]>,
    /// LSN allocator. Data-plane durability rides on onyx's LV2 buffer;
    /// lifecycle records ride on [`crate::lifecycle_log`]. The allocator bumps a
    /// monotonic counter under a mutex while the caller registers its dispatch
    /// footprint in the same critical section, preserving the "every lower
    /// LSN's footprint is known before a higher LSN can be assigned" invariant
    /// the dispatch scheduler relies on.
    lsn_alloc: LsnAllocator,
    /// Highest unlogged LSN applied in memory but not yet covered by a
    /// durable checkpoint. This is only for embedder fast paths whose
    /// recovery source is an upper-layer durable log.
    unlogged_pending_lsn: Mutex<Option<Lsn>>,
    /// WAL commits take the write side while they checkpoint any pending
    /// unlogged work and reserve their own WAL LSN. Unlogged commits take the
    /// read side, preventing a durable WAL record from landing behind an
    /// uncheckpointed no-WAL LSN.
    unlogged_commit_gate: RwLock<()>,
    unlogged_commits_enabled: bool,
    /// Buffer-as-sole-journal selector. Snapshotted from
    /// `Config::journal_mode` on open; immutable for the lifetime of
    /// the Db. Drives the WAL vs unlogged routing in
    /// `commit_ops_with_options`.
    journal_mode: crate::config::MetaDbJournalMode,
    /// When true, L2P-only commits skip the apply-lane channel hop and apply
    /// directly on the caller thread. See
    /// `Config::commit_direct_apply_enabled`.
    commit_direct_apply_enabled: bool,
    /// Excludes apply phases from flush / snapshot. Commit takes
    /// `.read()` across the apply + bump; flush / take_snapshot /
    /// drop_snapshot take `.write()` so they observe a quiescent tree
    /// state matching `last_applied_lsn`. WAL submission happens **outside**
    /// this lock, so concurrent submitters can land in the same WAL
    /// group-commit batch. Apply order is restored by the LSN-ordered condvar
    /// queue below, not by serialising WAL submits.
    /// Wrapped in `Arc` for the same reason as `manifest_state`: lets
    /// background workers clone the handle and acquire `read()` /
    /// `write()` without depending on `Arc<Db>`.
    apply_gate: Arc<ApplyGate>,
    /// LSN of the most recent op applied to in-memory state. Initialised
    /// from `manifest.checkpoint_lsn` on open (the manifest promises that
    /// every LSN at or below this value is already reflected in the
    /// trees / SSTs) and bumped on every commit. Paired with
    /// [`commit_cvar`](Self::commit_cvar) to form the apply-order queue.
    last_applied_lsn: Mutex<Lsn>,
    /// Non-blocking mirror of `last_applied_lsn` for status snapshots.
    last_applied_lsn_observed: AtomicU64,
    /// Notified whenever `last_applied_lsn` advances. Lifecycle ops
    /// and serial-apply commits wait on this in
    /// `wait_for_global_apply_turn`. Laned commits no longer require
    /// strict LSN-order apply (the lane dispatch already serializes
    /// per-lane), so `finish_global_apply` does NOT wait on this — it
    /// just inserts its LSN into `applied_set` and advances the
    /// watermark via contiguous-prefix pop. The cvar is only notified
    /// when the watermark actually advances, so out-of-order finishes
    /// don't produce a broadcast storm.
    commit_cvar: Condvar,
    /// Set of completed LSNs whose apply is done but that aren't yet
    /// covered by the contiguous-prefix watermark in
    /// `last_applied_lsn`. Used by `finish_global_apply` to bump the
    /// watermark out of order: a commit completes, inserts its LSN
    /// into the set, then pops contiguous prefixes off the head while
    /// `last_applied_lsn + 1` is present.
    ///
    /// Mutex is held only briefly (insert + a tight prefix-pop loop)
    /// so contention scales sub-linearly even at high concurrency.
    /// Pre-redesign, every commit parked on `commit_cvar` waiting for
    /// `last_applied_lsn + 1 == lsn` — at 128 in-flight commits this
    /// formed a 128-deep cvar chain with one wake per LSN advance.
    /// The set+watermark approach drops the chain entirely (laned
    /// commits never park) without losing the manifest checkpoint
    /// invariant (watermark is monotonic; manifest still stores it
    /// as "all LSNs ≤ X are durable").
    ///
    /// Per-lane LSN order is still enforced by the dispatch_state
    /// footprint protocol — out-of-order apply across LANES is
    /// allowed only when lanes are disjoint, so the apply itself
    /// preserves all required ordering.
    applied_set: Mutex<BTreeSet<Lsn>>,
    /// LSNs of commits that currently hold `apply_gate.read()`.
    ///
    /// This lets a lower-LSN predecessor bypass a pending checkpoint
    /// writer when a higher-LSN commit is already inside apply and
    /// waiting for global LSN order. Without that rescue path:
    /// higher commit holds read → checkpoint waits for write → lower
    /// commit parks behind writer → higher waits forever for lower.
    active_apply_lsns: Mutex<BTreeSet<Lsn>>,
    /// Sticky failure used to wake LSN-ordered waiters if a lower-LSN
    /// commit fails after a higher-LSN commit has already been durably
    /// acked by another WAL lane.
    commit_poison: Mutex<Option<String>>,
    /// Sticky failure set when a forced BFG sync fails non-recoverably, for
    /// example a faulted manifest fsync. Once set, forced-sync lifecycle ops
    /// (`take_snapshot`, `flush_with_gate`, `create_volume`, restore) fail fast
    /// instead of blocking on the slot the failed sync left stuck in Syncing. A
    /// failed inline sync may have left deferred RC apply state un-retryable, so
    /// recovery is a process restart. Paired with `BfgStateMachine::aborted`,
    /// which unblocks the BFG waiters.
    sync_poison: Mutex<Option<String>>,
    /// Scheduler for post-WAL dispatch into apply lanes. It lets a higher
    /// LSN bypass lower LSNs only when their declared lane footprints are
    /// disjoint; conflicting commits still dispatch in WAL order.
    dispatch_state: Mutex<DispatchState>,
    /// Notified whenever a dispatch reservation is registered, becomes
    /// durable, or completes.
    dispatch_cvar: Condvar,
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
    /// Background L2P streaming writeback worker (one thread that
    /// round-robins all volumes' L2P shards). Lifetime is bounded by
    /// `Db`: started after the rest of the engine is wired up,
    /// shut down at the top of `Drop` so it can never race against
    /// page_store / refcount teardown.
    l2p_writeback: Mutex<Option<streaming_flush::StreamingFlusher>>,
    /// Round-robin cursor for partial-sample flush selection. Each
    /// `flush_with_gate` invocation walks the global shard list
    /// starting from `flush_cursor` and accepts shards in order
    /// until the cumulative dirty work hits `flush_select_budget`;
    /// then bumps the cursor past the last accepted shard so the
    /// next round drains the remainder before revisiting hot
    /// shards. Combined with the dirty-count gating in
    /// `select_shards_for_flush`, this caps single-flush sample
    /// size and prevents cold shards from starving.
    flush_cursor: AtomicUsize,
    /// Cumulative `(dirty_l2p_pages + pending_rc_deltas)` cap per
    /// flush sample. Zero disables partial sampling (every flush is
    /// a full sample). See `Config::flush_select_budget`.
    flush_select_budget: usize,
    /// Background worker that drains `page_store.deferred_free`.
    /// `None` when async reclaim is disabled (config knob) — in
    /// that case `flush_with_gate` falls back to in-line reclaim.
    /// Wrapped in `Mutex` so `Drop` can take + stop the worker
    /// even from a `&self` context.
    async_reclaim: Mutex<Option<async_reclaim::AsyncReclaim>>,
    /// Background per-clone page-livelist condense worker. Bounds livelist
    /// chain growth under clone-overwrite churn.
    /// `None` when `cfg.livelist_condense_min_segments == 0`. Mirrors
    /// `async_reclaim`'s `Mutex<Option<…>>` so `Drop` can take + stop it
    /// from a `&self` context. Independent of `async_reclaim_enabled`.
    livelist_condense: Mutex<Option<livelist_condense::LivelistCondenser>>,
    /// Background Lineage GC driver — the sole production trigger for
    /// FreePbas-emitting PBA reclaim. Mirrors `async_reclaim` /
    /// `bfg_sync`: wrapped in `Mutex` so `Drop` can take + stop it from a
    /// `&self` context. `None` when `cfg.lineage_gc_enabled = false`
    /// (metadb-standalone default; onyx defaults it on).
    lineage_gc_worker: Mutex<Option<lineage_gc::LineageGcWorker>>,
    /// Whether the B2 buffered-commit path is active. Read by commit
    /// and read paths to decide whether to consult the L2P buffer or
    /// go straight to the tree. When `true`, the per-shard ring buffer
    /// in [`crate::db::l2p_buffer::L2pBuffer`] stamps commits by their
    /// `BfgGuard.bfg`; the [`crate::db::bfg_sync::BfgSyncThread`]
    /// drains each Syncing BFG's slot into the tree (or
    /// `flush_with_gate` does so inline when `bfg_threads_enabled =
    /// false`).
    l2p_buffer_enabled: bool,
    /// Cached copy of `Config::lineage_gc_emit_freepbas`. Create/open reject
    /// the old flag-off mode; this stays as a compatibility field for
    /// config/wire stability.
    lineage_gc_emit_freepbas: bool,
    /// Cached copy of `Config::lineage_gc_drop_dedup_shared`. When true the
    /// head-advance planner drops `rc > 0` (dedup-membership) dead-list records
    /// and surfaces only `rc == 0` ones instead of bailing the whole segment.
    /// Only safe in a DB that never creates snapshots/clones (see the config
    /// field doc); onyx sets it, metadb standalone defaults false.
    lineage_gc_drop_dedup_shared: bool,
    /// Optional sink invoked when a `commit_free_pbas` apply produces a
    /// non-empty `ApplyOutcome::FreePbas.freed_pbas`. Onyx registers a sink at
    /// startup so engine-side cleanup (`SpaceAllocator::retire_*` + dedup
    /// candidate cache invalidation) can drain reclamation signals out of
    /// metadb's internal lineage-GC commit path. Internal `commit_ops` calls
    /// invoke the sink synchronously after a successful commit; the sink must
    /// not call back into metadb's commit path. None when no consumer has
    /// subscribed.
    freed_pbas_sink: Mutex<Option<FreedPbasSink>>,
    /// Staged-outcome map shared between `commit_ops_deferred` and the L2P
    /// compactor's per-pass drain. Always constructed even when
    /// `Config::commit_deferred_outcomes_enabled = false`, so the sync path can
    /// route through the deferred entry point without an extra branch on the
    /// hot path. See [`crate::db::commit::outcomes`].
    deferred_outcomes: Arc<crate::db::commit::DeferredOutcomeAggregator>,
    /// Cached copy of `Config::commit_deferred_outcomes_enabled` so the
    /// commit path can decide whether to park the outcome vec
    /// (`deferred=true`) or send it through the channel immediately
    /// (`deferred=false`, synchronous delivery).
    commit_deferred_outcomes_enabled: bool,
    /// Cached copy of `Config::wal_async_commits_enabled`. Only consulted by
    /// `commit_ops_deferred` and only when `commit_deferred_outcomes_enabled`
    /// is also true. When both flags are on, the deferred-outcome commit path
    /// threads `SubmitOptions { synchronous: false }` into the WAL so the
    /// writer thread acks after `seg.append` and skips the per-batch fsync.
    /// Durability is restored at the next `flush_with_gate` via
    /// `WalSet::fsync_all_lanes` before the manifest commit.
    wal_async_commits_enabled: bool,
    /// Global BFG epoch state machine. Commit path acquires a
    /// [`crate::bfg::BfgGuard`] before applying ops so `BfgSyncThread` can drain
    /// a frozen slot. Initialised from `manifest.checkpoint_bfg` so a re-open
    /// resumes accounting where the previous run's last sync left off.
    pub(crate) bfg: Arc<crate::bfg::BfgStateMachine>,
    /// Cached copy of `Config::bfg_threads_enabled`. When `true`, the
    /// background BFG quiesce + sync workers are spawned at open time
    /// and `flush_with_gate` reaches durability via `force_roll +
    /// wait_until_synced`.
    pub(crate) bfg_threads_enabled: bool,
    /// Cached copy of `Config::rc_checkpoint_streaming_enabled`. Only affects
    /// the threads-on RC checkpoint implementation; false selects the legacy
    /// one-shot fold/write path for an exact A/B.
    pub(crate) rc_checkpoint_streaming_enabled: bool,
    /// Cached copy of `Config::parallel_l2p_drain_enabled`. Fans the
    /// per-BFG L2P syncing-slot drain out across shards when `true`.
    pub(crate) parallel_l2p_drain_enabled: bool,
    /// Cached copy of `Config::l2p_drain_chunk_entries`. Bounds buffered
    /// entries folded per `tree.write()` hold in the syncing-slot drain
    /// (0 = one-shot fold).
    pub(crate) l2p_drain_chunk_entries: usize,
    /// Enable one-generation look-ahead: after the current dirty checkpoint is
    /// frozen, a dedicated worker folds the Quiescing successor while current
    /// checkpoint IO runs.
    pub(crate) l2p_checkpoint_pipeline_enabled: bool,
    /// Maximum submitted L2P mutations in one Open BFG before the commit
    /// path asks the quiesce worker to roll it. This turns checkpoint cadence
    /// into a work bound; the timer remains the idle/low-rate fallback.
    pub(crate) bfg_l2p_work_limit: usize,
    /// Cached copy of `Config::rc_authoritative_reclaim`. When `true`, every
    /// L2P remap increfs its new head_pba so refcount is the authoritative
    /// live-reference count (reclaim = `rc==0`, no full-volume reverify scan).
    pub(crate) rc_authoritative_reclaim: bool,
    /// Notifier always allocated so `flush_with_gate` can hand a clone
    /// to the (optional) quiesce worker without taking a mutex. Cheap —
    /// just a `Mutex<bool> + Condvar`.
    pub(crate) bfg_quiesce_notifier: Arc<bfg_quiesce::QuiesceNotifier>,
    /// Sync-side notifier, always allocated; see above.
    pub(crate) bfg_sync_notifier: Arc<bfg_sync::SyncNotifier>,
    /// Quiesce worker handle. `Some` iff `bfg_threads_enabled`. `Mutex`
    /// so `Drop` can take + stop it from a `&self` context.
    pub(crate) bfg_quiesce: Mutex<Option<bfg_quiesce::BfgQuiesceThread>>,
    /// Sync worker handle. `Some` iff `bfg_threads_enabled`. Stop order
    /// in `Drop` is quiesce → sync so no new BFG enters Syncing after
    /// the quiesce side is gone, and the sync side drains whatever it
    /// has before exiting.
    pub(crate) bfg_sync: Mutex<Option<bfg_sync::BfgSyncThread>>,
    /// Serial successor-fold worker used by the checkpoint pipeline. It never
    /// changes BFG durability state; the sync worker consumes its ticket before
    /// returning and remains the sole `mark_synced` owner.
    l2p_prefold: Mutex<Option<l2p_prefold::L2pPrefoldWorker>>,
    /// Buffer-backed journal watermark hook. The onyx engine
    /// stamps this atomic to "the highest LV2 buffer entry seq whose
    /// flusher-derived mutations are now in metadb's in-memory state".
    /// The next checkpoint commit copies it into
    /// `manifest.last_processed_buffer_seq` so a future open can scope
    /// buffer replay to `seq > this`.
    ///
    /// Zero until a Shadow/Buffer-mode caller starts publishing — the
    /// legacy WAL path leaves it at 0 and the manifest field also
    /// stays 0 (recovery falls back to `checkpoint_lsn` semantics).
    pub(crate) buffer_applied_watermark: AtomicU64,
    /// Companion watermark for the lifecycle journal. Highest
    /// lifecycle-log seq whose effects are in memory. Persisted as
    /// `manifest.lifecycle_replay_seq`.
    pub(crate) lifecycle_applied_watermark: AtomicU64,
    /// Buffer-backed lifecycle journal. `Some` iff the embedder selected a
    /// non-WAL [`crate::config::MetaDbJournalMode`]: in that mode lifecycle ops
    /// (`CreateVolume`, `DropVolume`, `CloneVolume`, `DropSnapshot`, promotion
    /// records) bypass the WAL and append a single
    /// [`crate::lifecycle_log::op::LifecycleOp`] record per fsync. WAL mode
    /// leaves the field `None` and the existing `submit_wal_ops` path is
    /// unchanged.
    pub(crate) lifecycle_journal: Option<Mutex<crate::lifecycle_log::LifecycleWriter>>,
    /// Snapshot-lifecycle ops that ride the BFG sync cycle instead of blocking
    /// the write path. `take_snapshot` pushes a [`PendingSyncTask`] here under
    /// a `bfg.enter()` guard, forces the open group to sync, and blocks on
    /// `wait_until_synced`; [`Db::drain_pending_sync_tasks`] (called from
    /// [`Db::run_sync_cycle_body`]'s manifest window) does the real work in
    /// syncing context, durable atomically with that group's manifest commit,
    /// and fills each task's `result` slot. Writers join the next open group
    /// and never block on the snapshot.
    pub(crate) pending_sync_tasks: Mutex<Vec<PendingSyncTask>>,
}

/// A snapshot-lifecycle op queued to ride the BFG sync cycle (see
/// [`Db::pending_sync_tasks`]).
///
/// The take is **manifest-only**: the sync cycle folds the snapshot-root
/// incref into its OWN page-rc checkpoint, atomic with the manifest
/// commit that records the `SnapshotEntry` (no lifecycle-journal record).
/// The two `Option` fields below make the cycle's in-place abort-retry
/// idempotent across the cycle's phases.
pub(crate) struct PendingSyncTask {
    pub(crate) op: SyncTaskOp,
    /// Open BFG at enqueue time. The cycle processes the task only when
    /// it is syncing exactly this BFG, so the forced sync the caller
    /// triggered is the one that runs it.
    pub(crate) target_bfg: crate::types::Bfg,
    /// Outcome, filled by the cycle. The caller reads it after
    /// `wait_until_synced` (threads-on) or after driving the inline
    /// `flush_with_gate(Forced)` (threads-off) returns. No condvar: the
    /// BFG-sync completion the caller already waits on happens-after the
    /// cycle fills this.
    pub(crate) result: Arc<Mutex<Option<Result<SnapshotId>>>>,
    /// `Some` once the cycle has assigned the id + written the SnapshotRoots
    /// page + built the `SnapshotEntry` (done once, the first attempt). It
    /// carries the exact entry to insert into the committed manifest and the
    /// stable snapshot-root set the force-incref is recomputed from on every
    /// cycle attempt. The take is manifest-only and needs no reserved LSN.
    pub(crate) committed_entry: Option<SnapshotEntry>,
}

/// The operation a [`PendingSyncTask`] carries.
pub(crate) enum SyncTaskOp {
    /// Take a snapshot of `vol_ord`. The cycle assigns the id (returned
    /// via the task `result`), captures the volume's just-flushed roots,
    /// and rides the incref on a `LifecycleOp::TakeSnapshot` record.
    TakeSnapshot { vol_ord: VolumeOrdinal },
}

/// Synchronous callback invoked with the freed-PBA set produced by a
/// `commit_free_pbas` apply. Used by onyx to receive lineage-GC retire
/// signals; see [`Db::set_freed_pbas_sink`]. The vector is owned and
/// can be drained by the sink without copy.
pub type FreedPbasSink = Arc<dyn Fn(VolumeOrdinal, Vec<Pba>) + Send + Sync>;

struct ManifestState {
    store: ManifestStore,
    manifest: Manifest,
}

#[derive(Clone, Debug)]
pub(crate) struct DispatchFootprint {
    global: bool,
    lanes: BTreeSet<DispatchLaneKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum DispatchLaneKey {
    L2p(VolumeOrdinal, usize),
    Refcount(usize),
    /// One key per dedup shard. Ops carrying disjoint shard ids run
    /// in parallel; ops within the same shard serialize by WAL LSN.
    Dedup(u32),
}

#[derive(Default)]
struct DispatchState {
    pending: BTreeMap<Lsn, DispatchEntry>,
}

/// Monotonic LSN allocator for buffer-backed commits. Allocation runs under a
/// short mutex so the caller's `reserve` callback (which registers the commit's
/// dispatch footprint) completes before any higher LSN is handed out. The
/// dispatch scheduler relies on every lower-LSN footprint being visible before
/// a higher LSN can race ahead.
pub(crate) struct LsnAllocator {
    next_lsn: Mutex<Lsn>,
}

impl LsnAllocator {
    pub(crate) fn new(start_lsn: Lsn) -> Self {
        Self {
            next_lsn: Mutex::new(start_lsn),
        }
    }

    /// Reserve the next LSN. The `reserve` callback runs while the
    /// allocator mutex is still held, so a higher LSN cannot be assigned
    /// until the callback returns.
    pub(crate) fn reserve<F>(&self, reserve: F) -> Result<Lsn>
    where
        F: FnOnce(Lsn),
    {
        let mut g = self.next_lsn.lock();
        let lsn = *g;
        *g = g.checked_add(1).ok_or(MetaDbError::OutOfSpace)?;
        reserve(lsn);
        Ok(lsn)
    }
}

struct DispatchEntry {
    footprint: DispatchFootprint,
    durable: bool,
}

/// Snapshot view info cached per volume. Kept for snapshot-aware readers and
/// compatibility with older remap code; RangeDelete no longer uses it for PBA
/// refcount decisions.
#[derive(Clone, Debug)]
struct SnapInfo {
    created_lsn: Lsn,
    /// The snapshot's fold-watermark (`max(root.birth_lsn)` over captured
    /// roots). The birth COW-kill oracle (`youngest_snap_lsn`) feeds on
    /// `max(capture_watermark)`, NOT `created_lsn`; see
    /// `SnapshotEntry::capture_watermark`. Rebuilt from the durable
    /// `SnapshotEntry` on reopen so replay reproduces the same gate.
    capture_watermark: Lsn,
    /// Per-shard root page ids. Indexed by `shard_for_key_l2p(...)`,
    /// matching the volume's live shard layout (snapshot's roots are
    /// captured from the same shard group at take time, so the indices
    /// align).
    l2p_shard_roots: Box<[PageId]>,
}

type ApplyWork = Box<dyn FnOnce() + Send + 'static>;
const APPLY_LANE_READY_BURST_BEFORE_MAINTENANCE: usize = 64;

/// Diagnostic snapshot of in-memory bookkeeping that can grow
/// unbounded if a downstream drain stalls. See [`Db::pending_state`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PendingState {
    pub dispatch_pending: usize,
    pub deferred_free: usize,
    pub dedup_lane_queue: usize,
    pub l2p_apply_queue: usize,
    pub l2p_private_pages: usize,
    pub l2p_retired_pages: usize,
    pub l2p_pagebuf_total: usize,
    pub l2p_pagebuf_dirty: usize,
    pub rc_apply_queue: usize,
    pub rc_private_pages: usize,
    pub rc_retired_pages: usize,
    pub rc_pagebuf_total: usize,
    pub rc_pagebuf_dirty: usize,
    /// Sum of `RcShard::pending_delta_count` across all shards. This
    /// is the work the next flush sample phase will drain, so the
    /// watermark thread uses it (alongside `l2p_pagebuf_dirty`) to
    /// threshold-trigger a checkpoint before the queue grows large
    /// enough to dominate `flush_sample_us`.
    pub rc_pending_deltas: usize,
}

#[derive(Clone, Copy)]
enum ApplyLaneKind {
    L2p,
    Refcount,
    Dedup,
    DedupMaintenance,
}

struct ApplyLane {
    inner: Arc<ApplyLaneInner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct ApplyLaneInner {
    state: Mutex<ApplyLaneState>,
    cvar: Condvar,
    kind: ApplyLaneKind,
    /// Lane ordinal (== shard index for L2P / refcount lanes). Used by
    /// the H2 per-shard metrics to attribute each task to its shard.
    ordinal: usize,
    metrics: Arc<MetaMetrics>,
}

struct ApplyLaneState {
    maintenance: VecDeque<Arc<ApplyLaneTaskSlot>>,
    queue: VecDeque<ApplyLaneTask>,
    last_enqueued_lsn: Lsn,
    last_applied_lsn: Lsn,
    ready_since_maintenance: usize,
    shutdown: bool,
}

struct ApplyLaneTask {
    lsn: Option<Lsn>,
    slot: Arc<ApplyLaneTaskSlot>,
    /// `Instant::now()` at `enqueue_task` push time, used by the H2
    /// per-lane queue-wait metric in [`apply_lane_worker`]. `None` for
    /// maintenance tasks (no queue-wait reporting needed).
    enqueued_at: Option<Instant>,
}

struct ApplyLaneTaskSlot {
    work: Mutex<Option<ApplyWork>>,
    cvar: Condvar,
}

struct PendingApplyWork {
    slot: Option<Arc<ApplyLaneTaskSlot>>,
}

impl ApplyLane {
    fn new(
        last_applied_lsn: Lsn,
        kind: ApplyLaneKind,
        ordinal: usize,
        metrics: Arc<MetaMetrics>,
    ) -> Self {
        let inner = Arc::new(ApplyLaneInner {
            state: Mutex::new(ApplyLaneState {
                maintenance: VecDeque::new(),
                queue: VecDeque::new(),
                last_enqueued_lsn: last_applied_lsn,
                last_applied_lsn,
                ready_since_maintenance: 0,
                shutdown: false,
            }),
            cvar: Condvar::new(),
            kind,
            ordinal,
            metrics,
        });
        let worker_inner = inner.clone();
        let worker = std::thread::Builder::new()
            .name("onyx-metadb-apply-lane".to_string())
            .spawn(move || {
                let role = match kind {
                    ApplyLaneKind::L2p => crate::affinity::ThreadRole::L2pApply,
                    ApplyLaneKind::Refcount => crate::affinity::ThreadRole::RefcountApply,
                    ApplyLaneKind::Dedup | ApplyLaneKind::DedupMaintenance => {
                        crate::affinity::ThreadRole::DedupApply
                    }
                };
                crate::affinity::bind_current(role, ordinal);
                apply_lane_worker(worker_inner)
            })
            .expect("failed to spawn metadb apply lane worker");
        Self {
            inner,
            worker: Mutex::new(Some(worker)),
        }
    }

    fn enqueue_ready(&self, lsn: Lsn, work: ApplyWork) {
        self.enqueue_task(lsn, ApplyLaneTaskSlot::ready(work));
    }

    fn enqueue_pending(&self, lsn: Lsn) -> PendingApplyWork {
        let slot = ApplyLaneTaskSlot::pending();
        self.enqueue_task(lsn, slot.clone());
        PendingApplyWork { slot: Some(slot) }
    }

    #[allow(dead_code)]
    fn enqueue_maintenance(&self, work: ApplyWork) {
        self.handle().enqueue_maintenance(work);
    }

    fn handle(&self) -> ApplyLaneHandle {
        ApplyLaneHandle {
            inner: self.inner.clone(),
        }
    }

    fn enqueue_task(&self, lsn: Lsn, slot: Arc<ApplyLaneTaskSlot>) {
        let now = Instant::now();
        let mut state = self.inner.state.lock();
        debug_assert!(
            state.last_enqueued_lsn < lsn,
            "apply lane enqueue order violated: last={}, new={lsn}",
            state.last_enqueued_lsn
        );
        state.last_enqueued_lsn = lsn;
        state.queue.push_back(ApplyLaneTask {
            lsn: Some(lsn),
            slot,
            enqueued_at: Some(now),
        });
        // Sample queue depth (post-push) for the H2 backlog metric.
        let depth = state.queue.len() + state.maintenance.len();
        match self.inner.kind {
            ApplyLaneKind::L2p => self
                .inner
                .metrics
                .record_l2p_apply_lane_queue_depth(self.inner.ordinal, depth),
            ApplyLaneKind::Refcount => self
                .inner
                .metrics
                .record_rc_apply_lane_queue_depth(self.inner.ordinal, depth),
            ApplyLaneKind::Dedup | ApplyLaneKind::DedupMaintenance => {
                self.inner.metrics.record_dedup_lane_queue_depth(depth)
            }
        }
        self.inner.cvar.notify_one();
    }
}

#[derive(Clone)]
struct ApplyLaneHandle {
    inner: Arc<ApplyLaneInner>,
}

impl ApplyLaneHandle {
    fn enqueue_maintenance(&self, work: ApplyWork) {
        let mut state = self.inner.state.lock();
        state.maintenance.push_back(ApplyLaneTaskSlot::ready(work));
        // Maintenance pushes also count toward backlog depth.
        let depth = state.queue.len() + state.maintenance.len();
        match self.inner.kind {
            ApplyLaneKind::L2p => self
                .inner
                .metrics
                .record_l2p_apply_lane_queue_depth(self.inner.ordinal, depth),
            ApplyLaneKind::Refcount => self
                .inner
                .metrics
                .record_rc_apply_lane_queue_depth(self.inner.ordinal, depth),
            ApplyLaneKind::Dedup | ApplyLaneKind::DedupMaintenance => {
                self.inner.metrics.record_dedup_lane_queue_depth(depth)
            }
        }
        self.inner.cvar.notify_one();
    }
}

impl ApplyLane {
    #[allow(dead_code)]
    fn last_applied_lsn(&self) -> Lsn {
        self.inner.state.lock().last_applied_lsn
    }

    pub(crate) fn queue_len(&self) -> usize {
        let state = self.inner.state.lock();
        state.queue.len() + state.maintenance.len()
    }

    pub(crate) fn try_queue_len(&self) -> Option<usize> {
        self.inner
            .state
            .try_lock()
            .map(|state| state.queue.len() + state.maintenance.len())
    }

    /// Block until every WAL-tagged task currently enqueued on this
    /// lane has finished. Snapshots `last_enqueued_lsn` on entry and
    /// waits for `last_applied_lsn` to catch up to that snapshot;
    /// later enqueues are not waited for. Lanes that never received
    /// the target LSN (because their dedup bucket was empty) trivially
    /// satisfy the snapshot. Used by `Db::wait_apply_idle` to give
    /// callers a sync point after async dedup commits.
    fn wait_for_drain(&self) {
        let mut state = self.inner.state.lock();
        let target = state.last_enqueued_lsn;
        while state.last_applied_lsn < target && !state.shutdown {
            self.inner.cvar.wait(&mut state);
        }
    }
}

impl Drop for ApplyLane {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock();
            state.shutdown = true;
            self.inner.cvar.notify_all();
        }
        if let Some(worker) = self.worker.get_mut().take() {
            let _ = worker.join();
        }
    }
}

impl ApplyLaneTaskSlot {
    fn ready(work: ApplyWork) -> Arc<Self> {
        Arc::new(Self {
            work: Mutex::new(Some(work)),
            cvar: Condvar::new(),
        })
    }

    fn pending() -> Arc<Self> {
        Arc::new(Self {
            work: Mutex::new(None),
            cvar: Condvar::new(),
        })
    }

    fn set(&self, work: ApplyWork) {
        let mut guard = self.work.lock();
        debug_assert!(guard.is_none(), "apply lane task filled twice");
        *guard = Some(work);
        self.cvar.notify_one();
    }

    fn take(&self) -> ApplyWork {
        let mut guard = self.work.lock();
        while guard.is_none() {
            self.cvar.wait(&mut guard);
        }
        guard.take().expect("apply lane task disappeared")
    }

    fn is_ready(&self) -> bool {
        self.work.lock().is_some()
    }
}

impl PendingApplyWork {
    fn set(mut self, work: ApplyWork) {
        if let Some(slot) = self.slot.take() {
            slot.set(work);
        }
    }
}

impl Drop for PendingApplyWork {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            slot.set(Box::new(|| {}));
        }
    }
}

fn apply_lane_worker(inner: Arc<ApplyLaneInner>) {
    // H2 burst tracker: counts tasks processed since the previous time the
    // worker had to call `cvar.wait()` (i.e., since the queues went empty).
    // Flushed via `record_*_apply_lane_burst` right before the next wait
    // and again on shutdown. `tasks / wakeups` then gives the average
    // burst, with `burst_max` as the tail.
    let mut tasks_since_wait: u64 = 0;
    loop {
        // Time spent waiting on `inner.cvar` (queue empty) — accumulated
        // here so a single popped task's "wakeup" cost is the entire
        // idle window since the last task finished.
        let mut idle_total = std::time::Duration::ZERO;
        // Wakeup-shape counters for the upcoming task pop. Aggregated
        // separately from the burst tracker because a single pop can
        // span multiple spurious wakeups.
        let mut wakeups: u64 = 0;
        let mut empty_wakeups: u64 = 0;
        // If the inner loop ended a burst before going idle, the burst
        // length lands here and gets recorded after we process the task.
        let mut closed_burst: u64 = 0;

        let task = {
            let mut state = inner.state.lock();
            loop {
                if state.shutdown {
                    if tasks_since_wait > 0 {
                        record_lane_burst(&inner, tasks_since_wait);
                        tasks_since_wait = 0;
                    }
                    return;
                }
                match (state.maintenance.is_empty(), state.queue.is_empty()) {
                    (false, false) => {
                        let queue_front_ready = state
                            .queue
                            .front()
                            .map(|task| task.slot.is_ready())
                            .unwrap_or(false);
                        if !queue_front_ready
                            || state.ready_since_maintenance
                                >= APPLY_LANE_READY_BURST_BEFORE_MAINTENANCE
                        {
                            let slot = state
                                .maintenance
                                .pop_front()
                                .expect("maintenance checked non-empty");
                            state.ready_since_maintenance = 0;
                            break ApplyLaneTask {
                                lsn: None,
                                slot,
                                enqueued_at: None,
                            };
                        }
                        state.ready_since_maintenance += 1;
                        break state.queue.pop_front().expect("queue checked non-empty");
                    }
                    (false, true) => {
                        let slot = state
                            .maintenance
                            .pop_front()
                            .expect("maintenance checked non-empty");
                        state.ready_since_maintenance = 0;
                        break ApplyLaneTask {
                            lsn: None,
                            slot,
                            enqueued_at: None,
                        };
                    }
                    (true, false) => {
                        state.ready_since_maintenance =
                            state.ready_since_maintenance.saturating_add(1);
                        break state.queue.pop_front().expect("queue checked non-empty");
                    }
                    (true, true) => {}
                }
                // About to block. Close the open burst exactly once before
                // the first wait in this pop (subsequent spurious wakeups
                // don't restart it).
                if tasks_since_wait > 0 {
                    closed_burst = tasks_since_wait;
                    tasks_since_wait = 0;
                }
                let wait_start = Instant::now();
                inner.cvar.wait(&mut state);
                idle_total += wait_start.elapsed();
                wakeups += 1;
                // Spurious / racing wakeup: cvar returned but neither
                // queue has anything for us.
                if state.maintenance.is_empty() && state.queue.is_empty() && !state.shutdown {
                    empty_wakeups += 1;
                }
            }
        };

        let popped_at = Instant::now();
        let queue_wait = task
            .enqueued_at
            .map(|t| popped_at.duration_since(t))
            .unwrap_or_default();

        // For RC tasks enqueued via `enqueue_pending`, `slot.take()`
        // blocks on the slot's cvar until the commit thread calls
        // `set()` to fill in the deferred refcount delta. Time it
        // separately so we can tell apart "lane is starved on commit
        // handoff" vs "lane is starved by its own backlog".
        let take_start = Instant::now();
        let work = task.slot.take();
        let pending_set_wait = take_start.elapsed();

        let exec_start = Instant::now();
        let _ = catch_unwind(AssertUnwindSafe(work));
        let exec = exec_start.elapsed();
        tasks_since_wait = tasks_since_wait.saturating_add(1);

        let is_wal_task = task.lsn.is_some();
        match inner.kind {
            ApplyLaneKind::L2p => {
                if is_wal_task {
                    inner
                        .metrics
                        .record_l2p_apply_lane_task(inner.ordinal, queue_wait, exec);
                }
                if !idle_total.is_zero() {
                    inner
                        .metrics
                        .record_l2p_apply_lane_idle(inner.ordinal, idle_total);
                }
                inner
                    .metrics
                    .record_l2p_apply_lane_wakeups(inner.ordinal, wakeups, empty_wakeups);
                if closed_burst > 0 {
                    inner
                        .metrics
                        .record_l2p_apply_lane_burst(inner.ordinal, closed_burst);
                }
            }
            ApplyLaneKind::Refcount => {
                if is_wal_task {
                    inner.metrics.record_rc_apply_lane_task(
                        inner.ordinal,
                        queue_wait,
                        pending_set_wait,
                        exec,
                    );
                }
                if !idle_total.is_zero() {
                    inner
                        .metrics
                        .record_rc_apply_lane_idle(inner.ordinal, idle_total);
                }
                inner
                    .metrics
                    .record_rc_apply_lane_wakeups(inner.ordinal, wakeups, empty_wakeups);
                if closed_burst > 0 {
                    inner
                        .metrics
                        .record_rc_apply_lane_burst(inner.ordinal, closed_burst);
                }
            }
            ApplyLaneKind::Dedup | ApplyLaneKind::DedupMaintenance => {
                // Per-task timing is captured inside the dedup closure
                // via `record_dedup_lane_task`; only the worker-level
                // wakeup / idle / burst view belongs here.
                if !idle_total.is_zero() {
                    inner.metrics.record_dedup_lane_idle(idle_total);
                }
                inner
                    .metrics
                    .record_dedup_lane_wakeups(wakeups, empty_wakeups);
                if closed_burst > 0 {
                    inner.metrics.record_dedup_lane_burst(closed_burst);
                }
            }
        }

        let Some(lsn) = task.lsn else { continue };

        let mut state = inner.state.lock();
        debug_assert!(
            state.last_applied_lsn < lsn,
            "apply lane finished out of order: last={}, done={}",
            state.last_applied_lsn,
            lsn
        );
        state.last_applied_lsn = lsn;
        inner.cvar.notify_all();
    }
}

/// Dispatch a burst record to the kind-specific metric. Called on
/// shutdown when there's a pending burst we'd otherwise lose. Kept as a
/// free function so the main worker loop reads top-to-bottom.
fn record_lane_burst(inner: &ApplyLaneInner, burst: u64) {
    match inner.kind {
        ApplyLaneKind::L2p => inner
            .metrics
            .record_l2p_apply_lane_burst(inner.ordinal, burst),
        ApplyLaneKind::Refcount => inner
            .metrics
            .record_rc_apply_lane_burst(inner.ordinal, burst),
        ApplyLaneKind::Dedup | ApplyLaneKind::DedupMaintenance => {
            inner.metrics.record_dedup_lane_burst(burst);
        }
    }
}

struct Shard {
    rc: Arc<crate::refcount::RcShard>,
    apply_lane: ApplyLane,
    /// Highest LSN whose effects on this shard are durable on disk.
    /// Initialized at open from `manifest.checkpoint_lsn`. Bumped
    /// to `wal_checkpoint` after a flush selects this shard and
    /// the manifest commit succeeds. Used by
    /// `Db::compute_min_last_flushed_lsn` so partial flushes can
    /// keep `manifest.checkpoint_lsn` truthful for WAL prune /
    /// recovery start.
    last_flushed_lsn: AtomicU64,
}

struct L2pShard {
    /// Apply serialises on `tree` for COW correctness; readers don't
    /// touch it. See [`crate::paged::ReadView`] for the lock-free
    /// path.
    tree: RwLock<PagedL2p>,
    read_view: RwLock<Arc<crate::paged::ReadView>>,
    active_readers: AtomicUsize,
    apply_lane: ApplyLane,
    /// Highest LSN whose effects on this shard are durable on disk.
    /// See `Shard::last_flushed_lsn`.
    last_flushed_lsn: AtomicU64,
    /// In-memory write buffer (B2 buffered-commit path). Constructed
    /// for every shard but only populated when `use_buffer = true`.
    /// See [`crate::db::l2p_buffer`] for the concurrency model.
    l2p_buffer: Arc<crate::db::l2p_buffer::L2pBuffer>,
    /// When `true`, commit path inserts into `l2p_buffer` instead of
    /// mutating `tree`; read path consults `l2p_buffer` first. Set
    /// from `Config::l2p_buffer_enabled` at shard construction and
    /// never mutated after open.
    use_buffer: bool,
}

/// L2P home for one user-facing volume. Owns its own shard group; shard
/// routing inside a volume uses `xxh3_64(lba) % shards.len()`, identical to
/// the old flat layout when there is only one volume.
#[allow(dead_code)]
struct Volume {
    ord: VolumeOrdinal,
    shards: Vec<L2pShard>,
    created_lsn: Lsn,
    flags: AtomicU8,
    /// In-memory dead-list append buffer. Apply path pushes here under
    /// `apply_gate.read()`; checkpoint flush drains under `apply_gate.write()`.
    /// See [`crate::deadlist`].
    dead_list: Arc<crate::deadlist::DeadListState>,
    /// Persistent anchor of the volume's dead-list chain (oldest segment).
    /// Loaded from `VolumeEntry.dead_list_head_pid` on `Db::open`, advanced
    /// by Lineage GC. `NULL_PAGE` while the chain is empty.
    dead_list_head_pid: AtomicU64,
    /// Persistent anchor of the volume's dead-list chain (newest segment).
    /// Loaded from `VolumeEntry.dead_list_tail_pid` on `Db::open`, advanced
    /// on every checkpoint flush that writes a new segment.
    dead_list_tail_pid: AtomicU64,
    /// In-memory HEAD page-deadlist append buffer.
    /// Holds `(PageId, birth_lsn, death_lsn)` for L2P metadata pages that
    /// died off the head during COW while pinned by a snapshot. Separate
    /// from `dead_list` (PBAs) above; same [`crate::deadlist`] codec.
    /// Pushed by the apply-layer / compactor-fold emitter, drained at
    /// checkpoint, sealed into a snapshot on `take_snapshot`.
    ///
    /// **One accumulator per L2P shard** (`len == shards.len()`), not one per
    /// volume. The steady flush drains each
    /// shard under its OWN durable-root watermark `root_birth_lsn(shard)`
    /// (`flush.rs`), which is the exact crash-complete seal bound — a
    /// single per-volume bound is provably unsound (max over-seals a
    /// low-folded shard → premature free; min leaves the residual). The
    /// chain itself stays single per volume (the per-shard drains merge
    /// into one segment); only this RAM accumulator fans out by shard.
    /// Every push site routes to `page_dead_list[sid]` with the L2P shard
    /// index in scope; drop-time seal / peek / rollback fan out over all.
    page_dead_list: Vec<Arc<crate::deadlist::DeadListState>>,
    /// v18: oldest segment of the HEAD page-deadlist chain. Loaded from
    /// `VolumeEntry.page_dead_list_head_pid`; reset to `NULL_PAGE` when a
    /// snapshot seals the chain.
    page_dead_list_head_pid: AtomicU64,
    /// v18: newest segment of the HEAD page-deadlist chain (apply-time
    /// append anchor). Loaded from `VolumeEntry.page_dead_list_tail_pid`.
    page_dead_list_tail_pid: AtomicU64,
    /// In-memory per-clone page-livelist append buffer. Holds ALLOC/FREE
    /// `LiveRecord`s for this clone's clone-private L2P pages
    /// (`birth > branched_at_lsn`). Empty / unused for non-clone volumes.
    /// Drained at checkpoint into the livelist chain.
    page_live_list: Arc<crate::livelist::LiveListState>,
    /// v19: oldest segment of the per-clone page-livelist chain. Loaded from
    /// `VolumeEntry.page_live_list_head_pid`; advanced by condense.
    page_live_list_head_pid: AtomicU64,
    /// v19: newest segment of the page-livelist chain (apply-time append
    /// anchor). Loaded from `VolumeEntry.page_live_list_tail_pid`.
    page_live_list_tail_pid: AtomicU64,
    /// Oldest segment of this volume's promoted-PBA log chain. Loaded from
    /// `VolumeEntry.promoted_log_head_pid`.
    promoted_log_head_pid: AtomicU64,
    /// v20: newest segment of the promoted-PBA log (append anchor for the
    /// promotion walker). Loaded from `VolumeEntry.promoted_log_tail_pid`.
    promoted_log_tail_pid: AtomicU64,
    /// Clone-lineage tracking; mirrors [`VolumeEntry::parent_vol_ord`].
    /// `INVALID_VOLUME` encodes
    /// `Option::None`. Loaded on `Db::open`; mutated only by clone /
    /// promotion-complete paths under `apply_gate.write()` (single
    /// writer, so a relaxed atomic is sufficient).
    parent_vol_ord: parking_lot::RwLock<Option<VolumeOrdinal>>,
    /// Snapshot's `created_lsn` at the moment this volume was cloned.
    /// `0` if no parent. Lineage GC uses this to decide whether parent
    /// PBAs in `[birth, death)` are still observable from descendants.
    branched_at_lsn: Lsn,
    /// Background promotion walker progress. `Some(lba)` while the
    /// walker still has work to do; `None` when idle. Mutated only by
    /// the walker thread under `apply_gate.read()` and cleared by the
    /// `PromotionComplete` apply path; readers see relaxed snapshots.
    promotion_cursor: parking_lot::RwLock<Option<crate::types::Lba>>,
}

/// Build one fresh page-deadlist accumulator per L2P shard (H1: the HEAD
/// page-deadlist fans out by shard so the flush can seal each shard under
/// its own `root_birth_lsn`; see [`Volume::page_dead_list`]).
fn fresh_page_dead_lists(nshards: usize) -> Vec<Arc<crate::deadlist::DeadListState>> {
    (0..nshards)
        .map(|_| Arc::new(crate::deadlist::DeadListState::new()))
        .collect()
}

impl Volume {
    fn new(ord: VolumeOrdinal, shards: Vec<L2pShard>, created_lsn: Lsn) -> Self {
        let nshards = shards.len();
        Self {
            ord,
            shards,
            created_lsn,
            flags: AtomicU8::new(0),
            dead_list: Arc::new(crate::deadlist::DeadListState::new()),
            dead_list_head_pid: AtomicU64::new(crate::types::NULL_PAGE),
            dead_list_tail_pid: AtomicU64::new(crate::types::NULL_PAGE),
            page_dead_list: fresh_page_dead_lists(nshards),
            page_dead_list_head_pid: AtomicU64::new(crate::types::NULL_PAGE),
            page_dead_list_tail_pid: AtomicU64::new(crate::types::NULL_PAGE),
            page_live_list: Arc::new(crate::livelist::LiveListState::new()),
            page_live_list_head_pid: AtomicU64::new(crate::types::NULL_PAGE),
            page_live_list_tail_pid: AtomicU64::new(crate::types::NULL_PAGE),
            promoted_log_head_pid: AtomicU64::new(crate::types::NULL_PAGE),
            promoted_log_tail_pid: AtomicU64::new(crate::types::NULL_PAGE),
            parent_vol_ord: parking_lot::RwLock::new(None),
            branched_at_lsn: 0,
            promotion_cursor: parking_lot::RwLock::new(None),
        }
    }

    fn with_dead_list_anchor(
        ord: VolumeOrdinal,
        shards: Vec<L2pShard>,
        created_lsn: Lsn,
        head_pid: crate::types::PageId,
        tail_pid: crate::types::PageId,
    ) -> Self {
        let nshards = shards.len();
        Self {
            ord,
            shards,
            created_lsn,
            flags: AtomicU8::new(0),
            dead_list: Arc::new(crate::deadlist::DeadListState::new()),
            dead_list_head_pid: AtomicU64::new(head_pid),
            dead_list_tail_pid: AtomicU64::new(tail_pid),
            page_dead_list: fresh_page_dead_lists(nshards),
            page_dead_list_head_pid: AtomicU64::new(crate::types::NULL_PAGE),
            page_dead_list_tail_pid: AtomicU64::new(crate::types::NULL_PAGE),
            page_live_list: Arc::new(crate::livelist::LiveListState::new()),
            page_live_list_head_pid: AtomicU64::new(crate::types::NULL_PAGE),
            page_live_list_tail_pid: AtomicU64::new(crate::types::NULL_PAGE),
            promoted_log_head_pid: AtomicU64::new(crate::types::NULL_PAGE),
            promoted_log_tail_pid: AtomicU64::new(crate::types::NULL_PAGE),
            parent_vol_ord: parking_lot::RwLock::new(None),
            branched_at_lsn: 0,
            promotion_cursor: parking_lot::RwLock::new(None),
        }
    }

    /// Variant of [`with_dead_list_anchor`] that also carries clone-lineage
    /// fields. Used by `Db::open` to seed clones from the persisted
    /// `VolumeEntry` and by `Db::clone_volume` to mark a freshly-minted clone
    /// before its background promotion walker starts. `branched_at_lsn` is
    /// immutable for the volume's lifetime; the other fields move only on
    /// explicit lineage events.
    ///
    /// Also carries the persisted `flags` so the sticky
    /// `VOLUME_FLAG_CLONE_LINEAGE` survives reopen, the per-clone page-livelist
    /// anchors, and `clone_birth_lsn`: the sticky capture threshold armed on
    /// every shard tree so the COW witness logs ALLOC/FREE for pages born after
    /// the clone branched.
    #[allow(clippy::too_many_arguments)]
    fn with_lineage(
        ord: VolumeOrdinal,
        shards: Vec<L2pShard>,
        created_lsn: Lsn,
        flags: u8,
        head_pid: crate::types::PageId,
        tail_pid: crate::types::PageId,
        parent_vol_ord: Option<VolumeOrdinal>,
        branched_at_lsn: Lsn,
        promotion_cursor: Option<crate::types::Lba>,
        page_dead_list_head_pid: crate::types::PageId,
        page_dead_list_tail_pid: crate::types::PageId,
        page_live_list_head_pid: crate::types::PageId,
        page_live_list_tail_pid: crate::types::PageId,
        promoted_log_head_pid: crate::types::PageId,
        promoted_log_tail_pid: crate::types::PageId,
        clone_birth_lsn: Option<Lsn>,
    ) -> Self {
        // Arm the per-shard livelist capture threshold. Non-clones pass
        // `None` (zero overhead); clones (and promoted ex-clones) pass
        // `Some(branched_at_lsn)` so every shard tree records ALLOC/FREE
        // for its clone-private (`birth > B`) pages.
        if let Some(b) = clone_birth_lsn {
            for shard in &shards {
                shard.tree.write().set_clone_birth_lsn(Some(b));
            }
        }
        let nshards = shards.len();
        Self {
            ord,
            shards,
            created_lsn,
            flags: AtomicU8::new(flags),
            dead_list: Arc::new(crate::deadlist::DeadListState::new()),
            dead_list_head_pid: AtomicU64::new(head_pid),
            dead_list_tail_pid: AtomicU64::new(tail_pid),
            page_dead_list: fresh_page_dead_lists(nshards),
            page_dead_list_head_pid: AtomicU64::new(page_dead_list_head_pid),
            page_dead_list_tail_pid: AtomicU64::new(page_dead_list_tail_pid),
            page_live_list: Arc::new(crate::livelist::LiveListState::new()),
            page_live_list_head_pid: AtomicU64::new(page_live_list_head_pid),
            page_live_list_tail_pid: AtomicU64::new(page_live_list_tail_pid),
            promoted_log_head_pid: AtomicU64::new(promoted_log_head_pid),
            promoted_log_tail_pid: AtomicU64::new(promoted_log_tail_pid),
            parent_vol_ord: parking_lot::RwLock::new(parent_vol_ord),
            branched_at_lsn,
            promotion_cursor: parking_lot::RwLock::new(promotion_cursor),
        }
    }
}

struct DedupManifestUpdate {
    /// Per-shard old `dedup_index` level heads, one entry per shard in
    /// shard order. Captured before [`prepare_dedup_manifest_update`]
    /// rewrites the manifest field; passed to
    /// [`finish_dedup_manifest_update`] which frees them only after
    /// the manifest commit has made the new heads durable.
    old_dedup_heads: Vec<Vec<PageId>>,
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

/// Iterator over every live `(Hash8, DedupValue)` entry in the
/// dedup forward index. Tombstoned rows are hidden. Output is sorted
/// by hash.
pub struct DbDedupIter {
    inner: std::vec::IntoIter<(Hash8, DedupValue)>,
}

impl Iterator for DbDedupIter {
    type Item = Result<(Hash8, DedupValue)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

/// Opaque resume cursor for [`Db::scan_dedup_from`]. `Default` starts a fresh
/// pass at the beginning of the index. The fields are private cuckoo page-table
/// internals — callers only obtain a cursor from `Default` or a returned
/// [`DedupScanBatch`] and pass it back, so the cuckoo layout can change without
/// touching client code.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DedupScanCursor {
    page_idx: u64,
    slot: u32,
}

/// One bounded batch from [`Db::scan_dedup_from`].
pub struct DedupScanBatch {
    pub entries: Vec<(Hash8, DedupValue)>,
    /// Cursor to pass to the next call to continue the pass.
    pub next: DedupScanCursor,
    /// True when this batch reached the end of the index (a full pass
    /// completed); `next` is then reset to the start.
    pub wrapped: bool,
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

/// Directory that holds the lifecycle-log segments
/// ([[buffer-as-sole-journal-c3]]). Only created when the embedder runs
/// in a non-WAL [`crate::config::MetaDbJournalMode`].
pub(crate) fn lifecycle_log_dir(root: &Path) -> PathBuf {
    root.join("lifecycle_log")
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
mod async_reclaim;
mod bfg_quiesce;
mod bfg_sync;
mod commit;
mod helpers;
mod indexes;
mod l2p;
mod l2p_buffer;
mod l2p_prefold;
mod lifecycle;
mod lineage_gc;
mod livelist_condense;
mod promotion;
mod snapshot;
mod streaming_flush;
mod verify_audit;
mod volume;

pub use commit::DeferredOutcomeHandle;
pub use snapshot::{DropReport, RestoreReport, SnapshotView};

use apply::*;
use helpers::*;
pub use volume::DropVolumeReport;

impl Drop for Db {
    fn drop(&mut self) {
        // Stop the L2P streaming writeback worker FIRST. It holds
        // `Arc<PageStore>` + iterates `volumes`, both of which are
        // about to drop below; joining the thread here makes sure no
        // shard `tree.write()` / `page_store.write_sealed_page_runs`
        // call is in flight when those fields go away.
        if let Some(mut flusher) = self.l2p_writeback.lock().take() {
            flusher.stop();
        }
        if let Some(mut prefold) = self.l2p_prefold.lock().take() {
            prefold.stop();
        }
        // Stop the BFG worker pair before volume trees / page cache go away.
        // Quiesce stops first so no new group enters Syncing; sync drains any
        // in-flight cycle. Inert when `bfg_threads_enabled = false`.
        self.stop_bfg_threads();
        // Stop the Lineage GC driver before page_store / refcount / dedup
        // teardown — its cycle calls `run_lineage_gc_cycle_inner` which
        // touches commit_ops, page_store, the refcount shards and the
        // manifest. Joining here guarantees no GC cycle is mid-flight when
        // those fields drop.
        if let Some(mut worker) = self.lineage_gc_worker.lock().take() {
            worker.stop();
        }
        // Inline-delivery mode parks nothing in the aggregator, so poison_all is
        // a no-op kept only for API parity.
        self.deferred_outcomes
            .poison_all("metadb: db shutting down");
        // Stop the livelist-condense worker before page_store / page_cache go
        // away and before async reclaim. A completed condense can enqueue old
        // chain pages into deferred_free, so it is a reclaim producer.
        if let Some(mut worker) = self.livelist_condense.lock().take() {
            worker.stop();
        }
        // Detach the async dedup-index drainers. Each worker holds an
        // `Arc<DedupIndex>` (= self.dedup_index), so without joining
        // them here the index (and the page_store it references) would
        // never drop and the threads would run forever — the same
        // circular-shutdown shape as the refcount drainers below.
        self.dedup_index.detach_drainers();
        // Stop the async reclaim worker only after every independent producer
        // above has joined. The explicit shutdown path drains durably via
        // `drain_deferred_reclaim_durable`; Drop merely prevents IO racing field
        // teardown when a caller omitted that terminal operation.
        if let Some(mut worker) = self.async_reclaim.lock().take() {
            worker.stop();
        }
        // The refcount fold is inline + per-BFG-slot now — no background rc
        // drainer threads to detach (see `refcount::shard`).
        // ApplyLanes have their own Drop that joins their workers;
        // they fire automatically when the Box goes out of scope.
    }
}

impl Db {
    /// Whether refcount is the authoritative live-L2P-reference count (so onyx
    /// GC reclaim can free on `rc==0` alone and skip the full-volume
    /// `referenced_extents` reverify scan). Mirrors `Config::rc_authoritative_reclaim`.
    pub fn rc_authoritative_reclaim(&self) -> bool {
        self.rc_authoritative_reclaim
    }

    /// Test helper: synchronously drain every L2P shard's BFG ring buffer into
    /// the on-disk tree on the caller thread. This is a thin alias for
    /// [`Db::force_compact_l2p_buffers`], the same helper used by snapshot,
    /// range_delete, and inline flush paths before they read tree state. Kept
    /// under the `test_*` name so existing proptest harnesses continue to
    /// compile without modification.
    pub fn test_force_compact_pass(&self) {
        // Inert when `l2p_buffer_enabled = false`; otherwise loops
        // shards and drains their slots into the tree.
        let _ = self.force_compact_l2p_buffers();
    }

    /// BFG inspection hook used by integration tests. Returns the in-memory
    /// state machine's `checkpoint_bfg` snapshot.
    pub fn bfg_checkpoint_for_test(&self) -> u64 {
        self.bfg.checkpoint_bfg()
    }

    /// Test helper: directly commit a `PromotionChunk` lifecycle record
    /// with caller-supplied PBAs and cursor. Production code drives
    /// this path through [`Db::run_promotion_chunk`], which derives
    /// both from the clone's L2P; this shim lets integration tests pin
    /// specific PBAs / cursors (e.g. simulating a crash mid-walk)
    /// without requiring MAX_PROMOTION_CHUNK_PBAS-many real L2P
    /// entries.
    pub fn test_commit_promotion_chunk(
        &self,
        vol_ord: VolumeOrdinal,
        pba_increfs: Vec<Pba>,
        next_cursor: Option<Lba>,
    ) -> Result<()> {
        self.commit_promotion_chunk(vol_ord, pba_increfs, next_cursor)
    }

    /// Test helper: directly commit a `PromotionComplete` lifecycle
    /// record. See [`Db::test_commit_promotion_chunk`] for rationale.
    pub fn test_commit_promotion_complete(&self, vol_ord: VolumeOrdinal) -> Result<()> {
        self.commit_promotion_complete(vol_ord)
    }
}

#[cfg(test)]
impl Db {
    /// Test helper: drain and return the in-memory dead-list buffer for one
    /// volume, so tests can inspect per-emit records without going through a
    /// flush.
    pub(crate) fn test_drain_dead_list(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Option<Vec<crate::deadlist::DeadRecord>> {
        self.volumes
            .read()
            .get(&vol_ord)
            .map(|v| v.dead_list.drain())
    }

    /// Test helper: snapshot the volume's `(dead_list_head_pid,
    /// dead_list_tail_pid)` anchors. Used to assert that a flush
    /// advanced them and that a chain extension picked up the prior
    /// tail as the new segment's `prev_seg_pid`.
    pub(crate) fn test_dead_list_anchors(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Option<(PageId, PageId)> {
        self.volumes.read().get(&vol_ord).map(|v| {
            (
                v.dead_list_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                v.dead_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }

    /// Test helper: number of buffered records in the HEAD page-deadlist
    /// (L2P-page deaths captured but not yet drained to a segment). Used to
    /// prove the COW witness actually populates.
    pub(crate) fn test_page_dead_list_len(&self, vol_ord: VolumeOrdinal) -> Option<usize> {
        self.volumes
            .read()
            .get(&vol_ord)
            // H1: accumulator fans out per shard — sum across them.
            .map(|v| v.page_dead_list.iter().map(|a| a.len()).sum())
    }

    /// Test helper: the volume's HEAD page-deadlist `(head_pid, tail_pid)`
    /// anchors.
    pub(crate) fn test_page_dead_list_anchors(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Option<(PageId, PageId)> {
        self.volumes.read().get(&vol_ord).map(|v| {
            (
                v.page_dead_list_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                v.page_dead_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }

    /// Test helper: number of buffered records in the per-clone page-livelist
    /// (ALLOC/FREE captured but not yet drained to a segment). `None` for an
    /// unknown volume; `Some(0)` for a non-clone.
    pub(crate) fn test_page_live_list_len(&self, vol_ord: VolumeOrdinal) -> Option<usize> {
        self.volumes
            .read()
            .get(&vol_ord)
            .map(|v| v.page_live_list.len())
    }

    /// Test helper: the volume's page-livelist `(head_pid, tail_pid)` anchors.
    pub(crate) fn test_page_live_list_anchors(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Option<(PageId, PageId)> {
        self.volumes.read().get(&vol_ord).map(|v| {
            (
                v.page_live_list_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                v.page_live_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }

    /// Test helper: drain + reconstruct the live-ALLOC set (ALLOC minus matched
    /// FREE) from a volume's page-livelist — both the durable chain and the
    /// not-yet-drained in-memory buffer — as a set of `(pid, birth)` pairs.
    /// Lets a test assert clone-private reachability without going through
    /// `verify_path`.
    pub(crate) fn test_clone_live_allocs(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Result<std::collections::HashSet<(PageId, Lsn)>> {
        let vol = self.volume(vol_ord)?;
        let mut records = crate::livelist::read_chain_records(
            vol.page_live_list_tail_pid
                .load(std::sync::atomic::Ordering::Acquire),
            |p| self.page_store.read_page(p),
        )?;
        records.extend(vol.page_live_list.peek());
        Ok(crate::livelist::live_allocs(records)?
            .into_iter()
            .map(|r| (r.pid, r.birth_lsn))
            .collect())
    }

    /// Test helper: raw PBAs recorded in a volume's promoted-PBA log chain
    /// (the global-rc edges promotion incref'd), most-recent segment first.
    /// Reads the durable chain via the in-memory tail anchor.
    #[cfg(test)]
    pub(crate) fn test_promoted_log_pbas(&self, vol_ord: VolumeOrdinal) -> Result<Vec<Pba>> {
        let vol = self.volume(vol_ord)?;
        let tail = vol
            .promoted_log_tail_pid
            .load(std::sync::atomic::Ordering::Acquire);
        let records = crate::livelist::read_chain_records(tail, |p| self.page_store.read_page(p))?;
        // The promoted-PBA log reuses the LiveListSegment codec with the raw
        // `Pba` stored in the record's `pid` slot.
        Ok(records.into_iter().map(|r| r.pid).collect())
    }

    /// Test helper: the volume's promoted-PBA log `(head, tail)` anchors.
    #[cfg(test)]
    pub(crate) fn test_promoted_log_anchors(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Option<(PageId, PageId)> {
        self.volumes.read().get(&vol_ord).map(|v| {
            (
                v.promoted_log_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                v.promoted_log_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }

    /// Test helper: run the read-only clone COW-kill birth-operand audit over
    /// the current in-memory manifest + page store, returning safety-direction
    /// findings. Empty means the birth-based clone operand is safe for the
    /// current state. Flush first so the manifest reflects committed roots.
    #[cfg(test)]
    pub(crate) fn test_clone_birth_shadow_findings(&self) -> Result<Vec<String>> {
        crate::verify::clone_birth_shadow_findings(
            &self.page_store,
            &self.manifest_state.lock().manifest,
        )
    }

    /// Test helper: a snapshot's sealed page-deadlist tail anchor, by snapshot
    /// id.
    pub(crate) fn test_snapshot_page_dead_list_tail(&self, id: SnapshotId) -> Option<PageId> {
        self.manifest_state
            .lock()
            .manifest
            .snapshots
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.page_dead_list_tail_pid)
    }

    /// Test helper: read a page through the underlying `PageStore` so
    /// dead-list tests can inspect raw segment bytes without depending
    /// on a higher-level decoder.
    pub(crate) fn test_read_page(&self, pid: PageId) -> Result<crate::page::Page> {
        self.page_store.read_page(pid)
    }

    pub(crate) fn test_clear_parent_vol_ord(&self, vol_ord: VolumeOrdinal) {
        if let Some(vol) = self.volumes.read().get(&vol_ord) {
            *vol.parent_vol_ord.write() = None;
            *vol.promotion_cursor.write() = None;
        }
        let mut mst = self.manifest_state.lock();
        if let Some(entry) = mst.manifest.volumes.iter_mut().find(|e| e.ord == vol_ord) {
            entry.parent_vol_ord = None;
            entry.promotion_cursor = None;
        }
    }

    /// Test helper: corrupt a volume's manifest `branched_at_lsn` so the
    /// offline `clone_birth_shadow` operand drops that descendant from another
    /// clone's pinner set without touching the reachability ground truth.
    /// Manifest only; the in-memory `Volume.branched_at_lsn` is immutable so it
    /// is left alone.
    #[cfg(test)]
    pub(crate) fn test_set_manifest_branched_at_lsn(&self, vol_ord: VolumeOrdinal, lsn: Lsn) {
        let mut mst = self.manifest_state.lock();
        if let Some(entry) = mst.manifest.volumes.iter_mut().find(|e| e.ord == vol_ord) {
            entry.branched_at_lsn = lsn;
        }
    }

    /// Test helper: drive one step of the promotion walker for `vol_ord`.
    /// Returns the step outcome so tests can assert whether a chunk was
    /// committed, completion has been reached, or the volume isn't a clone.
    ///
    /// The production walker would call [`Db::run_promotion_chunk`] in
    /// a loop until [`promotion::PromotionStep::Completed`]; tests
    /// invoke it step-by-step so they can assert intermediate cursor /
    /// rc state.
    #[cfg(test)]
    pub(crate) fn test_run_promotion_chunk(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Result<promotion::PromotionStep> {
        self.run_promotion_chunk(vol_ord)
    }

    /// Test helper: synchronously run one livelist condense scan with the given
    /// segment threshold, so tests assert chain shrinkage deterministically
    /// without racing the background worker.
    #[cfg(test)]
    pub(crate) fn test_run_livelist_condense(&self, min_segments: usize) -> Result<()> {
        livelist_condense::run_condense_scan_once(
            self.page_store.clone(),
            self.page_cache.clone(),
            self.manifest_state.clone(),
            self.apply_gate.clone(),
            self.volumes.clone(),
            self.faults.clone(),
            livelist_condense::LivelistCondenseParams {
                min_segments,
                idle_interval_ms: 0,
            },
        )
    }

    /// Test helper: synchronously drive one Lineage GC cycle and
    /// return the number of volumes whose `head_pid` advanced. Used by
    /// `db::tests::lineage_gc` to assert chain truncation without
    /// racing the background worker.
    ///
    /// In rc-neutral lineage mode, each volume's plan + execute is interleaved
    /// with a `commit_free_pbas` call carrying the segment's dead-record PBAs.
    /// The plan's `rc==0` gate remains load-bearing because ordinary shared
    /// refcounts should not reach zero unless the refcount event ledger has
    /// already released them.
    pub(crate) fn test_run_lineage_gc_cycle(&self) -> Result<usize> {
        self.run_lineage_gc_cycle_inner()
    }
}

/// Production (non-`cfg(test)`) impl. The FreePbas-emitting Lineage GC
/// driver must be reachable from [`crate::db::lineage_gc::LineageGcWorker`]
/// in real builds, so it lives here rather than in the `#[cfg(test)]`
/// impl block above.
impl Db {
    pub(crate) fn run_lineage_gc_cycle_inner(&self) -> Result<usize> {
        let ctx = async_reclaim::LineageGcCtx {
            volumes: self.volumes.clone(),
            manifest_state: self.manifest_state.clone(),
            apply_gate: self.apply_gate.clone(),
            refcount_shards_rc: self
                .refcount_shards
                .iter()
                .map(|shard| shard.rc.clone())
                .collect(),
            faults: self.faults.clone(),
            metrics: self.metrics.clone(),
            emit_freepbas: self.lineage_gc_emit_freepbas,
            drop_dedup_shared: self.lineage_gc_drop_dedup_shared,
        };
        if !self.lineage_gc_emit_freepbas {
            return Err(MetaDbError::InvalidArgument(
                "lineage_gc_emit_freepbas=false is no longer supported: rc-neutral \
                 lineage GC requires FreePbas emission"
                    .into(),
            ));
        }

        // Per-volume plan → commit_free_pbas → execute. We can't fold
        // FreePbas across volumes because the apply path is keyed on
        // `vol_ord`; multi-vol cycles emit one `commit_free_pbas` per
        // advancing volume.
        let vol_handles: Vec<(crate::types::VolumeOrdinal, Arc<Volume>)> = {
            let guard = self.volumes.read();
            guard.iter().map(|(k, v)| (*k, v.clone())).collect()
        };
        let mut advanced = 0;
        for (vol_ord, vol) in vol_handles {
            let plan =
                match async_reclaim::gc_plan_head_advance(&self.page_store, &ctx, &vol, vol_ord) {
                    Ok(Some(p)) => p,
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::warn!(
                            vol_ord = vol_ord,
                            error = %err,
                            "lineage GC: rc-neutral plan failed"
                        );
                        continue;
                    }
                };
            if !plan.dead_pbas.is_empty() {
                // Order matters: FreePbas must commit BEFORE chain
                // truncation. If we truncate first and then crash
                // before FreePbas, the retire signal is lost (the
                // dead-list records are gone, onyx has no way to
                // know the PBAs are reclaimable). Committing first
                // means a crash before truncate is recoverable:
                // next GC cycle re-runs the plan against the still-
                // intact chain and re-emits FreePbas; apply
                // `apply_free_pbas` re-surfaces the same PBAs (rc
                // already 0 → exclusive branch). Onyx consumes these
                // via `PbaLifecycle::free_lineage_gc_proven`, which
                // absorbs a duplicate surface idempotently with an
                // `is_extent_free`/`is_retired` precheck (NOT a
                // set-typed retire — onyx now direct-frees lineage
                // PBAs). Safe because this commit precedes the chain
                // truncate, so a re-surface cannot name a PBA that was
                // already freed, reallocated, and made live again.
                let pbas = plan.dead_pbas.clone().into_boxed_slice();
                let outcome = self.commit_free_pbas(vol_ord, &pbas)?;
                self.dispatch_freed_pbas_outcomes(vol_ord, vec![outcome]);
            }
            if let Err(err) =
                async_reclaim::gc_execute_head_advance(&self.page_store, &ctx, &vol, vol_ord, &plan)
            {
                tracing::warn!(
                    vol_ord = vol_ord,
                    error = %err,
                    "lineage GC: execute failed after FreePbas commit"
                );
                continue;
            }
            advanced += 1;
        }
        Ok(advanced)
    }
}

#[cfg(test)]
mod tests;
