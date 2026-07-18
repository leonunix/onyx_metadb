//! Configuration knobs exposed to the embedder.
//!
//! Defaults picked to match the values documented in `docs/DESIGN.md §11`.
//! Field docs describe the runtime behavior, not the historical rollout order.

use std::path::PathBuf;

/// Upper bound on [`Config::dedup_shards`]. Sized so the manifest's
/// per-shard level-head section never starves the snapshot/volume
/// region.
pub const MAX_DEDUP_SHARDS: u32 = 64;

/// Storage cap for per-shard apply-lane metric arrays. Sized to match
/// [`MAX_DEDUP_SHARDS`] and to comfortably fit the practical L2P /
/// refcount shard counts (default 16, configurable via
/// [`Config::shards_per_partition`]). Lane ordinals at or beyond this
/// cap fall back to aggregate-only accounting so over-sized configs
/// stay observable, just less detailed.
pub const MAX_APPLY_LANE_SHARDS: usize = 64;

/// Page size in bytes. Fixed for v1; not a runtime choice.
///
/// Changing this would require re-encoding every on-disk format constant,
/// so it lives here as a compile-time constant rather than a config field.
pub const PAGE_SIZE: usize = 4096;

/// Buffer-as-sole-journal selector. `Buffer` is the only remaining variant.
/// The enum and [`Config::journal_mode`] field survive as a single-arm shape so
/// the on-disk config wire format is unchanged for onyx and the toml
/// deserialiser still accepts the historical `"buffer"` string.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MetaDbJournalMode {
    /// Data-plane commits skip any metadb-side journal entirely. Onyx's
    /// LV2 buffer replays uncheckpointed mutations through `commit_ops`
    /// on reopen. Lifecycle ops live in `lifecycle_log/` and are
    /// replayed by `Db::open` directly.
    Buffer,
}

/// Embedder-provided configuration for opening a database.
#[derive(Clone, Debug)]
pub struct Config {
    /// Directory that will hold the page file, WAL segments, and manifest.
    pub path: PathBuf,

    /// Number of shards per partition for the L2P B+tree.
    ///
    /// "Partition" is the old name for what the rest of metadb now calls a
    /// volume. New code should read [`Config::shards_per_volume`]; both names
    /// resolve to the same numeric value.
    pub shards_per_partition: u32,

    /// Upper bound on the number of volumes a single `Db` can hold. Used to
    /// validate manifest capacity at create / open time; exceeding it is a
    /// configuration error, not a runtime drift. Defaults to 1024.
    pub max_volumes: u32,

    /// Size of a single WAL segment before rotation.
    pub wal_segment_bytes: u64,

    /// Number of WAL writer lanes used by `Db`. Each lane has its own
    /// writer thread, segment directory, and fsync stream. Global LSNs
    /// are still assigned from one short critical section so recovery
    /// can merge lanes into the existing total order. Default to one
    /// lane so synchronous Onyx flush writers actually coalesce into
    /// group commits; raise this only on storage where parallel fsync
    /// streams beat larger batches.
    pub wal_lanes: u32,

    /// Experimental: allow selected embedder transactions to reserve an LSN
    /// and apply without writing a WAL record. The embedder must keep an
    /// independent durable replay source until checkpoint. Default off.
    pub unlogged_commits_enabled: bool,

    /// When true (default), `commit_ops` detects L2P-only commits (no rc /
    /// dedup buckets, all target L2P shards in buffered mode) and applies their
    /// L2P buckets directly on the caller thread instead of enqueueing closures
    /// onto per-shard apply-lane workers. Eliminates ~230 us of channel +
    /// queue-wait + worker scheduling overhead per commit on the seqwrite hot
    /// path.
    ///
    /// Falls back to the lane path automatically when:
    ///   * the commit touches any dedup bucket,
    ///   * any L2P shard is not running in `use_buffer` mode, or
    ///   * the commit is in the serial-apply branch (lifecycle ops,
    ///     guarded remap on a snapshot-bearing volume).
    ///
    /// Behaviour is byte-equivalent to the lane path; this is purely a
    /// concurrency / latency optimisation. Set to false to force the lane path
    /// for differential debugging.
    pub commit_direct_apply_enabled: bool,

    /// When true, `commit_ops_deferred` parks the freshly-computed
    /// `Vec<ApplyOutcome>` in the [`crate::DeferredOutcomeAggregator`] keyed by
    /// LSN and returns immediately. The L2P compactor's per-pass loop drains
    /// every staged entry whose touched `(volume, shard)` pairs have all been
    /// folded into the on-disk tree (the metadb equivalent of a BFG sync). When
    /// false, `commit_ops_deferred` is a thin wrapper that delivers outcomes
    /// synchronously; on-disk format is unchanged.
    ///
    /// This flag governs whether the staging path is exercised at all; the
    /// deferred API is always compiled in. Behaviour is byte-equivalent to the
    /// sync path: outcomes for a given LSN match exactly; only delivery timing
    /// shifts from caller thread to compactor boundary.
    pub commit_deferred_outcomes_enabled: bool,

    /// Upper bound on a single group-commit batch, in bytes.
    pub group_commit_max_batch_bytes: usize,

    /// How long the group-commit thread waits for more work before flushing
    /// a partial batch, in microseconds.
    pub group_commit_timeout_us: u64,

    /// When true, `commit_ops_deferred` submits to the WAL with
    /// `synchronous=false`. The writer thread writes the batch to OS page cache
    /// and acks the caller without calling fsync. Durability is consolidated at
    /// the next `flush_with_gate`, which fans an `Op::FsyncAll` to every WAL
    /// lane before promoting the manifest's `checkpoint_lsn`.
    ///
    /// Safety relies on two invariants outside metadb:
    /// 1. The onyx LV2 write buffer fsyncs every user write before ack,
    ///    acting as the ZIL equivalent. Anything past the last metadb
    ///    BFG sync is re-driven by the onyx commit_worker on restart.
    /// 2. The metadb `apply_gate.write()` barrier in `flush_with_gate`
    ///    sampled `last_applied_lsn` only after every in-flight commit
    ///    completed `finish_global_apply`, which is itself gated on
    ///    `wal.submit*().wait()` returning — so the writer thread has at
    ///    minimum appended the body to OS page cache. `fsync_all_lanes`
    ///    after the sample makes those bytes durable.
    ///
    /// **Requires `commit_deferred_outcomes_enabled = true`.** Async
    /// WAL without deferred outcomes is an unsupported combination; the
    /// `commit_ops_deferred` guard rejects the mismatch.
    pub wal_async_commits_enabled: bool,

    /// Deadline (microseconds) for group-commit batches whose submits
    /// are all `synchronous=false`. Only consulted when the batch is
    /// async-only; the moment a `synchronous=true` submit lands, the
    /// deadline collapses back to `group_commit_timeout_us` so sync
    /// hot-path latency is protected.
    ///
    /// Default 1000 (1 ms) — large enough to amortise the rare BFG
    /// fsync over many async commits, small enough to keep the inflight
    /// WAL-on-OS-page-cache window bounded.
    pub wal_async_group_commit_window_us: u64,

    /// Buffer-as-sole-journal selector. See [`MetaDbJournalMode`]. This is a
    /// single-variant enum today; the field is preserved so the on-disk config
    /// wire format remains compatible.
    pub journal_mode: MetaDbJournalMode,

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
    /// a runtime tunable. Default is 1, matching the original single-shard
    /// layout.
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
    /// full")`. Page-table chaining (`crate::paged_meta`) lifts the old
    /// single-meta-page cap, so the bucket count can now scale with
    /// the working set. Recorded in the manifest at `Db::create`.
    pub dedup_cuckoo_buckets: u64,

    /// Maximum number of entries kept in the dedup_index L1 hot
    /// cache. Each entry costs roughly 64 B (`(fp, hash, value, lru
    /// link)`), so 1 M entries ≈ 64 MiB. Larger caches reduce L3
    /// IOPS but cost RAM 1:1.
    pub dedup_l1_cache_entries: usize,

    /// Run a per-dedup-shard background drainer that absorbs staged
    /// `(hash → value)` dedup-index mutations into the on-disk cuckoo
    /// table outside the commit/apply critical path. When enabled, the
    /// hot-path `apply_dedup_*` arms only merge into an in-RAM staging
    /// map (last-LSN-wins) + warm L0/L1; the blocking 4 KiB cuckoo
    /// page write happens on the drainer thread, and the checkpoint
    /// barrier final-drains staging before sampling `checkpoint_lsn`.
    /// When disabled (the default), `stage_put`/`stage_delete` fall
    /// through to the verbatim eager `dedup_index.put`/`delete` —
    /// byte-identical to the pre-drainer behaviour. The `rc.stage(±1)`
    /// that pairs with every dedup mutation stays inline regardless, so
    /// refcount semantics are unchanged.
    pub dedup_drainer_enabled: bool,

    /// Drainer cycle interval. Each shard's drainer parks for at most
    /// this long before checking whether its staging map has work.
    /// Threshold-driven wakeups fire sooner.
    pub dedup_drainer_interval_ms: u64,

    /// Staging-map size that wakes the drainer ahead of the next timer
    /// tick. Sized so the in-gate final-drain catch-up at the
    /// checkpoint barrier stays bounded.
    pub dedup_drainer_threshold_entries: usize,

    /// Hard cap on staged entries processed by a single drainer cycle.
    /// Excess rolls into the next cycle.
    pub dedup_drainer_max_entries_per_cycle: usize,

    /// Backpressure trigger. When a shard's `active` staging map
    /// exceeds this, `stage_put`/`stage_delete` synchronously drains
    /// that shard (bounded) before returning, capping staging RAM.
    /// Safe because cuckoo put/delete is idempotent (unlike the rc
    /// overlay, this cannot lose contributions).
    pub dedup_drainer_backpressure_entries: usize,

    /// Run a per-shard background drainer that absorbs `RcShard.delta`
    /// into a sealed-page staging overlay outside `apply_gate.write()`.
    /// When enabled (the default), `Db::flush()` sample-phase work
    /// shrinks to "preempt drainer + small final-drain catch-up +
    /// atomic overlay snapshot" instead of doing the heavy clone-and-
    /// apply loop in-gate. When disabled, `RcShard` reverts to the
    /// priority-1 path verbatim — kept as an escape hatch for soak
    /// bisection.
    pub refcount_drainer_enabled: bool,

    /// Drainer cycle interval. Each shard's drainer parks for at most
    /// this long before checking whether `delta_active` has any work.
    /// Threshold-driven wakeups (see `refcount_drainer_threshold_entries`)
    /// can fire sooner.
    pub refcount_drainer_interval_ms: u64,

    /// `delta_active` size that wakes the drainer ahead of the next
    /// timer tick. Sized so the in-gate final-drain catch-up at
    /// `begin_checkpoint` stays bounded.
    pub refcount_drainer_threshold_entries: usize,

    /// Hard cap on entries processed by a single drainer cycle.
    /// Prevents a single cycle from holding overlay state for too long
    /// when the drainer is far behind. Excess entries roll into the
    /// next cycle.
    pub refcount_drainer_max_entries_per_cycle: usize,

    /// Batch size for refilling the per-shard `PagePool`. Each refill
    /// calls [`PageStore::allocate_run`](crate::page_store::PageStore)
    /// once, collapsing N per-page lock acquisitions into ⌈N / size⌉.
    pub refcount_drainer_alloc_run_size: usize,

    /// Backpressure trigger. When
    /// `delta_active.len() + delta_draining.len() + overlay.pages.len()`
    /// crosses this at `begin_checkpoint`, the in-gate path falls back
    /// to the priority-1 synchronous drain instead of trying to absorb
    /// a huge final batch into the overlay.
    pub refcount_drainer_backpressure_pages: usize,

    /// Enable the in-memory L2P buffer + periodic compaction path.
    /// When `false` (default), commits mutate the paged radix tree in-line.
    /// When `true`, commits insert into the per-shard `L2pBuffer` and a
    /// background compactor folds it into the tree on its own cadence. See
    /// [`crate::db::l2p_buffer`] + [`docs/DESIGN.md §B2`].
    pub l2p_buffer_enabled: bool,

    /// Global L2P mutation budget for one Open BFG. The crossing batch is
    /// admitted and closes that generation to later commits until it rolls, so
    /// submitted work is bounded by `limit + max_single_batch - 1` rather than
    /// growing with checkpoint service time. Repeated writes to one LBA count
    /// repeatedly; this is a conservative O(1) hot-path bound that avoids
    /// scanning or locking all per-shard buffers. Work-driven rolling is
    /// suspended while snapshots are live; snapshot take/drop own their forced
    /// BFG boundaries and page-deadlist classification.
    pub l2p_buffer_soft_entries: usize,

    /// Reserved compatibility setting for a future per-shard hard threshold.
    /// It is currently parsed but not enforced; the global BFG admission bound
    /// above is the active backpressure mechanism.
    pub l2p_buffer_hard_entries: usize,

    /// Maximum wall time the compactor may wait between cycles even
    /// when no entry threshold is crossed. Bounds `checkpoint_lsn`
    /// lag and WAL retention on idle systems.
    pub l2p_buffer_max_interval_ms: u64,

    /// When `true`, spawn the [`crate::db::bfg_quiesce::BfgQuiesceThread`] and
    /// [`crate::db::bfg_sync::BfgSyncThread`] at `Db::open` time. When `false`,
    /// callers fold and flush through the inline path.
    pub bfg_threads_enabled: bool,

    /// Stream threads-on refcount checkpoint data pages in bounded chunks.
    /// When `false`, the sync worker uses the legacy one-shot
    /// `begin_checkpoint(bfg)` path and retains every sealed page until the
    /// global checkpoint write. This is an exact diagnostic A/B switch; it
    /// does not change BFG rolling, shard selection, or commit concurrency.
    pub rc_checkpoint_streaming_enabled: bool,

    /// Build the proposed immutable refcount delta-run encoding from each
    /// frozen BFG slot and discard it after recording size/CPU metrics. This is
    /// a shadow-only experiment: it does not allocate page ids, write pages,
    /// change manifest state, or alter the authoritative paged-array fold.
    pub rc_delta_run_shadow_enabled: bool,

    /// Fan the per-BFG L2P syncing-slot drain out across shards (one task
    /// per shard) instead of folding them serially on the single
    /// `metadb-bfg-sync` thread. Each shard is independent (own `tree`
    /// lock + `l2p_buffer` + per-shard page alloc pool); the serial fold
    /// was the bfg-sync drain bottleneck (~74% of that thread in
    /// `compact_drain_into_tree`), capping single-volume write throughput.
    /// Default `false`: an older nvme-box run regressed, so this remains an
    /// explicit diagnostic A/B rather than an assumed production win. The
    /// work-driven BFG bound is independent of this setting.
    pub parallel_l2p_drain_enabled: bool,

    /// Maximum number of L2P shard folds that may execute concurrently when
    /// [`Self::parallel_l2p_drain_enabled`] is true. `0` preserves the legacy
    /// unbounded fan-out (one worker per pending shard job); positive values are
    /// a hard concurrency cap. Ignored when parallel drain is disabled.
    pub parallel_l2p_drain_workers: usize,

    /// Bound on buffered entries folded per `tree.write()` acquisition
    /// in the per-BFG L2P syncing-slot drain. The one-shot fold held the
    /// shard's tree write lock for the whole slot (100k+ entries under
    /// sustained load) while `apply_l2p_remap` takes the same lock per
    /// commit op and dedup/read multi_gets take the read side — a proven
    /// multi-second commit stall. Chunking releases the lock between
    /// chunks so commits/reads interleave; correctness is unchanged
    /// (publish-before-clear keeps the slot authoritative until the
    /// final chunk publishes). `0` = unbounded (legacy one-shot fold,
    /// A/B fallback). Applies to both the serial and parallel drain.
    pub l2p_drain_chunk_entries: usize,

    /// Pipeline the next frozen BFG's serial L2P fold with the current BFG's
    /// checkpoint IO. The successor is folded only after the current dirty
    /// checkpoint has been frozen, and the sync worker waits for the prefold
    /// before allowing that successor to enter Syncing. Snapshot and clone
    /// lifecycles conservatively disable the overlap because their page-death
    /// accounting requires strict fold/checkpoint boundaries.
    pub l2p_checkpoint_pipeline_enabled: bool,

    /// Make PBA refcount the AUTHORITATIVE count of live L2P references
    /// (every L2P remap increfs its new head_pba; a packed N-LBA unit → +N),
    /// so onyx GC reclaim becomes a pure `rc==0` check and the full-volume
    /// `referenced_extents` reverify scan (which held `tree.read()` across the
    /// whole volume and stalled the BFG fold/checkpoint → multi-second commit
    /// spikes) is eliminated. Decrements ride the existing lineage deadlist;
    /// the increfs ride the existing off-gate refcount drainer. Default
    /// `false` (current rc-neutral behaviour). ⚠ Turning this on requires a
    /// FRESH metadb: an existing store has `rc==0` for all exclusive PBAs,
    /// which the authoritative reader would mass-premature-free — `Db::open`
    /// REFUSES a pre-`RC_AUTHORITATIVE` manifest when this is set.
    pub rc_authoritative_reclaim: bool,

    /// The quiesce thread rolls a BFG group at least this often even with no
    /// force_roll signal. Ignored when `bfg_threads_enabled = false`.
    pub bfg_timeout_ms: u64,

    /// Run a background L2P streaming writeback worker that continuously
    /// seals dirty pages and writes them through the centralised
    /// `IoSubmitter`, *outside* `apply_gate.write()`. The next `Db::flush`
    /// then samples a much smaller dirty set — checkpoint's gate-hold
    /// time becomes dominated by lifecycle bookkeeping rather than by
    /// clone/seal of accumulated dirty pages.
    ///
    /// Crash semantics: writeback is a content-only optimisation. Pages
    /// written by writeback do NOT advance the durable `checkpoint_lsn`;
    /// crash recovery still replays WAL from the last manifest-committed
    /// LSN. The on-disk bytes that the writeback path leaves behind are
    /// either re-overwritten by replay (if the page is touched after
    /// `checkpoint_lsn`) or already match the replayed state.
    pub l2p_writeback_enabled: bool,

    /// Microseconds the streaming writeback worker parks between idle
    /// cycles (no shard has dirty pages above the threshold). Active
    /// cycles run back-to-back without sleeping.
    pub l2p_writeback_idle_sleep_us: u64,

    /// Minimum dirty page count on a shard to trigger a writeback cycle.
    /// Skips shards with fewer dirty pages so a near-quiescent system
    /// doesn't pay per-cycle overhead.
    pub l2p_writeback_min_dirty_pages: usize,

    /// Hard cap on pages a single writeback cycle hands to
    /// `write_sealed_page_runs` per shard. Caps install-lock hold time:
    /// `install_writeback` runs under `tree.write()`, and longer holds
    /// directly delay foreground commit apply on the same shard. With
    /// the default 8 192, install rarely exceeds 5–10 ms even with
    /// PageBuf-shaped costs.
    pub l2p_writeback_max_pages_per_cycle: usize,

    /// Global L2P-dirty target that gates the streaming writeback worker.
    /// The worker stays parked while the per-shard dirty page total is
    /// below this value — keeps the writeback IO submitter quiet under
    /// light load and concentrates work above the target, where the
    /// next foreground checkpoint would otherwise pay a large sample.
    /// Paired with `flush_dirty_pages_threshold` on the onyx side
    /// (`target/trigger` duality): writeback wakes at target, the
    /// apply-gate flush still kicks in at the higher trigger.
    /// 0 disables the gate (writeback runs whenever `min_dirty_pages`
    /// per-shard is met). Only effective when
    /// `l2p_writeback_enabled = true`.
    pub flush_dirty_pages_target: usize,

    /// Cap on background-priority ops in flight at the centralised
    /// [`crate::io_submitter::IoSubmitter`]. Sync-priority ops (commit
    /// writes / fsync) always admit up to SQ capacity; background ops
    /// (L2P streaming writeback) wait in a deferred queue once
    /// `inflight_bg` reaches this cap. Keeps a sustained writeback
    /// burst from displacing commit writes from the SQ — without it,
    /// enabling `l2p_writeback_enabled` regresses commit-write p99
    /// (validated 2026-05-16 on nvme-box: writeback off vs on,
    /// READ p99 +50 % / WRITE p99 +117 % at SQ=16384).
    /// 0 disables the cap (admit bg ops freely; useful for regression A/Bs).
    pub io_submitter_bg_inflight_cap: usize,

    /// Per-flush budget on the sum of `(dirty_l2p_pages +
    /// pending_rc_deltas)` the sample phase will process. When the
    /// running total crosses this cap during shard selection, the
    /// remaining shards stay unselected and their roots /
    /// `last_flushed_lsn` carry over to the next flush. Combined
    /// with `flush_cursor` round-robin, partial sampling keeps a
    /// single flush short enough to interleave with commit apply.
    /// Set to 0 (or any value larger than the live working set) to
    /// disable partial sampling and force every flush to be full.
    /// `manifest.checkpoint_lsn` becomes
    /// `min(per-shard last_flushed_lsn)` so WAL prune / recovery
    /// remain correct even when most flushes are partial.
    pub flush_select_budget: usize,

    /// Run `page_store.try_reclaim_limit` + page-cache invalidation
    /// in a background worker instead of inline at the end of
    /// `flush_with_gate`. The worker drains `deferred_free` in
    /// `async_reclaim_max_pages_per_cycle`-sized chunks, woken by
    /// every successful flush. Off-critical-path reclaim removes
    /// the dominant ~35% slice of `flush_total_max` so the
    /// dispatcher can fire the next flush sooner.
    pub async_reclaim_enabled: bool,
    /// Worker per-cycle cap (pages). Larger reduces per-page
    /// overhead at the cost of longer per-cycle NVMe burst; smaller
    /// smooths the queue at the cost of more cycles. The default is
    /// tuned for the same NVMe profile that drove the in-line
    /// reclaim budget heuristic.
    pub async_reclaim_max_pages_per_cycle: usize,
    /// Max milliseconds the worker parks when `deferred_free` is
    /// empty. Notifications from `flush_with_gate` cut this short
    /// via a condvar, so under load the worker stays hot and only
    /// hits this on truly idle systems.
    pub async_reclaim_idle_interval_ms: u64,
    /// A clone's persisted page-livelist chain is condensed to a single segment
    /// once it reaches at least this many segments. `0` disables the background
    /// condense worker entirely. Independent of `async_reclaim_enabled`.
    pub livelist_condense_min_segments: usize,
    /// Max milliseconds the livelist-condense worker parks between scans.
    pub livelist_condense_idle_interval_ms: u64,
    /// Lineage GC emits a `FreePbas` lifecycle/dispatch record for every
    /// dead-list record it retires before truncating the chain. The old
    /// chain-truncation-only mode is no longer supported; create/open reject
    /// `false` because rc-neutral L2P remaps require FreePbas retire events.
    pub lineage_gc_emit_freepbas: bool,

    /// Run the background Lineage GC driver
    /// ([`crate::db::lineage_gc::LineageGcWorker`]). When true a worker
    /// parks on `lineage_gc_interval_ms` and drives
    /// [`Db::run_lineage_gc_cycle_inner`](crate::db::Db) so dead-list
    /// segments are surfaced as `FreePbas` and the per-volume dead-list
    /// chain actually advances. **This is the only production trigger for PBA
    /// reclaim** — the `async_reclaim` worker deliberately holds no `Arc<Db>`
    /// and cannot emit FreePbas. Without this thread dead-list chains grow
    /// without bound and no LV3 PBA is ever reclaimed
    /// (`gc_lineage_freed_blocks` stays 0).
    ///
    /// Defaults OFF in [`Config::new`]: metadb's own lineage_gc unit
    /// tests and `metadb-soak` drive GC synchronously via
    /// `test_run_lineage_gc_cycle`, and a background mutator of
    /// `dead_list_head_pid` would race them. Onyx — the production
    /// client — overrides it ON.
    pub lineage_gc_enabled: bool,
    /// Idle park (ms) between Lineage GC wakes once nothing more can
    /// advance. A wake also drives more cycles immediately when a backlog
    /// remains (see `lineage_gc_max_cycles_per_wake`).
    pub lineage_gc_interval_ms: u64,
    /// Per-wake budget: how many GC cycles (each advances at most one
    /// dead-list segment per volume) to drive before parking again.
    /// Bounds the `apply_gate.write()` pressure one wake puts on the
    /// foreground commit path while still letting a backlog drain over a
    /// few wakes.
    pub lineage_gc_max_cycles_per_wake: usize,

    /// Lineage GC head-advance: when a head dead-list segment contains a
    /// record whose PBA still has `rc > 0`, DROP that record (do not surface
    /// it, do not decref) and advance the head past it, surfacing only the
    /// `rc == 0` (exclusive) records. When false (the default) the head
    /// segment is left intact on the first `rc > 0` record — the historical
    /// whole-segment bail.
    ///
    /// ## Why dropping rc>0 is safe — and its hard precondition
    ///
    /// In the current rc-neutral mode, the only events that bump a PBA's global
    /// rc are `DedupPut`/`PromotionChunk` (L2P remaps are rc-neutral). So an
    /// `rc > 0` dead-list record is EITHER (a) a dedup-target PBA whose rc is
    /// pure dedup_index membership, reclaimed by the client's dedup
    /// orphan-reclaim path (DedupDelete → rc 0 → retire → confirm-scan free),
    /// OR (b) a clone/snapshot promotion-shared PBA with no dedup entry, whose
    /// ONLY reclaim path is the FreePbas shared-decref. Dropping is safe for
    /// (a) — the dead-list record is redundant; the dedup path owns it — but a
    /// PERMANENT LEAK for (b), because nothing else decrefs a promotion incref.
    /// metadb cannot cheaply tell (a) from (b) per-record (no reverse
    /// PBA→hash index). The snapshot / descendant-branch pin checks still bail
    /// the whole volume, which covers ACTIVE clones, but a clone that finished
    /// promotion (parent_vol_ord cleared) is no longer pinned — so this flag is
    /// only correct when the DB **never** creates snapshots or clones, i.e.
    /// every `rc > 0` is class (a). Onyx — which exposes no snapshot/clone in
    /// its CLI or meta layer — sets this true; metadb standalone (and any DB
    /// that uses snapshot/clone) MUST leave it false.
    pub lineage_gc_drop_dedup_shared: bool,
}

impl Config {
    /// Accessor for the per-volume shard count. Reads from
    /// [`Config::shards_per_partition`]; kept as a dedicated method so call
    /// sites can use the volume terminology while the serialized field name
    /// remains unchanged.
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
            unlogged_commits_enabled: false,
            // Direct L2P apply defaults on for L2P-only commits. Any commit
            // outside the eligibility check falls back to the lane path.
            commit_direct_apply_enabled: true,
            // Deferred outcomes are the production default; see the field doc
            // for the timing contract.
            commit_deferred_outcomes_enabled: true,
            group_commit_max_batch_bytes: 4 * 1024 * 1024,
            group_commit_timeout_us: 1,
            // Async WAL is the production default. Durability still relies on
            // the onyx LV2 buffer as the upstream replay source; see the field
            // doc for the exact contract.
            wal_async_commits_enabled: true,
            wal_async_group_commit_window_us: 1000,
            journal_mode: MetaDbJournalMode::Buffer,
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
            // 100 k matches the nvme-box sweep sweet spot for
            // `flush_dirty_pages_threshold`; partial sampling reuses
            // the same budget so the early trigger and the per-flush
            // cap stay aligned by default.
            flush_select_budget: 100_000,
            // Async reclaim defaults OFF. 2026-05-15 nvme-box A/B
            // showed two failure modes — neither version delivered
            // a usable win:
            //   v1 (tight-loop worker): correctness preserved but
            //   the worker continuously consumes NVMe bandwidth for
            //   zero-stamped Free page writes + punch_hole, starving
            //   foreground flush IO. READ IOPS -26 % vs the
            //   threshold-only baseline; io_max went 27 s → 67 s.
            //   v2 (one-cycle-per-notify worker): NVMe contention
            //   gone but introduced 72 967 `refcount underflow` /
            //   `rc.stage failed` errors within the first 15 s of
            //   the run. Decoupling reclaim cadence from flush
            //   cadence breaks an invariant somewhere in the
            //   page_store + L2P remap path (root cause not
            //   pinned down — see `async_reclaim_underflow`
            //   memory). The infrastructure is left in tree for
            //   future investigation; do NOT enable until the
            //   underflow is understood.
            async_reclaim_enabled: false,
            // 65 536 pages = 256 MiB per cycle. Caps single-cycle
            // NVMe burst while still amortising the per-page
            // overhead (lock + zero-write + cache invalidate).
            async_reclaim_max_pages_per_cycle: 65_536,
            // 50 ms idle parking: short enough that a quiet system
            // still drains `deferred_free` within ~50 ms after the
            // last flush even if `notify()` was missed; long
            // enough not to busy-loop on otherwise-idle pages.
            async_reclaim_idle_interval_ms: 50,
            // Condense a clone's livelist chain once it reaches 16 segments.
            // This only rewrites the clone-private log; it does not change any
            // free decision. Lazy threshold + 1 s idle scan keep the worker
            // near-free for non-clone / low-churn workloads.
            livelist_condense_min_segments: 16,
            livelist_condense_idle_interval_ms: 1000,
            // Hot-path RC writes are gone; Lineage GC is the sole producer of
            // PBA-free decisions, so FreePbas emission is mandatory.
            lineage_gc_emit_freepbas: true,
            // Background Lineage GC defaults OFF for standalone metadb:
            // the lineage_gc unit tests + metadb-soak drive GC via
            // `test_run_lineage_gc_cycle`, and a background mutator of
            // `dead_list_head_pid` would race them. Onyx overrides ON —
            // it is the only production trigger for PBA reclaim.
            lineage_gc_enabled: false,
            lineage_gc_interval_ms: 1000,
            lineage_gc_max_cycles_per_wake: 256,
            // Conservative default: only onyx (no snapshot/clone) flips this on.
            lineage_gc_drop_dedup_shared: false,
            rebuild_free_list_on_open: true,
            reclaim_orphans_on_open: true,
            // 1 M buckets × 4 entries = 4 M cuckoo capacity at load
            // factor 1.0 (target ~3.4 M at 0.85). Sized for soak +
            // small production deployments; large production should
            // override based on expected unique-hash count. The
            // chained meta-page layout grows automatically, so the
            // ceiling is page-store free space rather than a fixed
            // single-meta-page cap.
            dedup_cuckoo_buckets: 1_000_000,
            // 64 K L1 hot cache — sized to keep recently-touched
            // entries warm without dominating RAM. Each entry is
            // ~64 B, so 64 K ≈ 4 MiB. Production should bump in
            // lock-step with the working-set size.
            dedup_l1_cache_entries: 64_000,
            // Dedup drainer ships **default-off**, behind a flag (mirrors
            // parallel_l2p_drain, NOT the default-on refcount_drainer).
            // This changes the hottest path (stage_ops, ~90% of commit
            // apply) and read-after-write timing on the cuckoo, so it
            // stays off until the soak gate + onyx NVMe soak + crash
            // matrix pass; flipped on in a follow-up commit. Flag off ⇒
            // stage_put/stage_delete are verbatim eager put/delete.
            dedup_drainer_enabled: false,
            dedup_drainer_interval_ms: 50,
            dedup_drainer_threshold_entries: 4_096,
            dedup_drainer_max_entries_per_cycle: 65_536,
            dedup_drainer_backpressure_entries: 16_384,
            // Drainer ships **default-on**. The background drainer absorbs
            // `RcShard.delta_active` into
            // a sealed-page overlay outside `apply_gate.write()` so
            // `flush()`'s sample-phase rc_drain shrinks from ~2 s
            // (priority-1 verbatim) to <100 ms. Flip back to `false`
            // to recover the priority-1 path verbatim — overlay
            // memory and drainer threads are cleaned up on drop.
            refcount_drainer_enabled: true,
            refcount_drainer_interval_ms: 50,
            refcount_drainer_threshold_entries: 4_096,
            refcount_drainer_max_entries_per_cycle: 65_536,
            refcount_drainer_alloc_run_size: 64,
            refcount_drainer_backpressure_pages: 8_192,
            // The L2P buffer ships default-off in the generic config; onyx can
            // opt in once its workload wants buffered writes.
            l2p_buffer_enabled: false,
            // Work-driven BFG roll. This is a global per-generation admission
            // budget, not the old per-shard compactor wake threshold. The
            // nvme-box A/B validated 4 M mutations; 64 K rolls checkpoints too
            // aggressively for production profiles that enable buffered L2P.
            l2p_buffer_soft_entries: 4_000_000,
            // Reserved compatibility value for a future per-shard hard
            // trigger. This is parsed but not enforced; the global soft
            // budget above is the active BFG admission bound.
            l2p_buffer_hard_entries: 512_000,
            // 30 s wall-clock floor — even on an idle system the
            // compactor will fire at least this often so
            // `checkpoint_lsn` and WAL retention don't drift.
            l2p_buffer_max_interval_ms: 30_000,
            // BFG worker threads default-off in the generic config; see field doc.
            bfg_threads_enabled: false,
            // Preserve the production streaming checkpoint path whenever BFG
            // workers are enabled. Tests/benchmarks can disable it explicitly
            // to recover the legacy one-shot memory shape.
            rc_checkpoint_streaming_enabled: true,
            // L3 format experiment only. Keep disabled unless a controlled
            // aged run is measuring the exact frozen-slot record stream.
            rc_delta_run_shadow_enabled: false,
            // Parallel per-shard L2P drain default-OFF: an earlier nvme-box
            // run regressed 3-4x after its workers escaped the background CPU
            // domain, and L2P was not the only healthy-window gate. Keep this
            // as an explicit A/B until background-only placement is validated.
            parallel_l2p_drain_enabled: false,
            // Parallel drain remains opt-in, but enabling it should be bounded
            // by default. Zero is reserved for an explicit legacy fan-out A/B.
            parallel_l2p_drain_workers: 4,
            // Bounded fold lock-holds default-ON: semantics-preserving
            // (same lock, same op order, same publish point); 0 restores
            // the one-shot hold for A/B.
            l2p_drain_chunk_entries: 4096,
            // Opt-in until the pipeline passes production crash/load A/B.
            l2p_checkpoint_pipeline_enabled: false,
            rc_authoritative_reclaim: false,
            bfg_timeout_ms: 5_000,
            // Streaming writeback ships default-off in this generic
            // `Config::new` so unit tests that assert on page-allocator
            // / snapshot state observe a quiescent backend (no
            // background writes between explicit `flush()` calls).
            // Production embedders (Onyx) explicitly set this true in
            // their config — they want the worker to keep dirty
            // backlog bounded so `Db::flush`'s gate-hold stays small
            // under sustained mixed write load.
            l2p_writeback_enabled: false,
            l2p_writeback_idle_sleep_us: 500,
            l2p_writeback_min_dirty_pages: 64,
            // 512 pages per install gives a ~3–6 ms `tree.write()`
            // hold under typical PageBuf install costs — small enough
            // not to starve foreground commit apply on the same shard,
            // large enough that each `IORING_OP_WRITEV` run carries a
            // useful payload (512 × 4 KiB = 2 MiB).
            l2p_writeback_max_pages_per_cycle: 512,
            // Streaming writeback target gate default: 0 = disabled
            // (writeback runs whenever per-shard `min_dirty_pages` is
            // met). Production embedders set this to e.g. 100 k so
            // writeback amortises pageouts only above the level where
            // the next checkpoint sample would be expensive anyway.
            flush_dirty_pages_target: 0,
            // Default bg inflight cap matches DEFAULT_BG_INFLIGHT_CAP
            // in `io_submitter.rs` (~6 % of SQ=16384). Tunable via
            // `Config::io_submitter_bg_inflight_cap`; 0 disables.
            io_submitter_bg_inflight_cap: 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn default_l2p_buffer_budget_matches_production_admission_semantics() {
        assert_eq!(Config::new("unused").l2p_buffer_soft_entries, 4_000_000);
    }

    #[test]
    fn rc_delta_run_shadow_defaults_off() {
        assert!(!Config::new("unused").rc_delta_run_shadow_enabled);
    }
}
