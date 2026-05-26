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

    /// Experimental: allow selected embedder transactions to reserve an LSN
    /// and apply without writing a WAL record. The embedder must keep an
    /// independent durable replay source until checkpoint. Default off.
    pub unlogged_commits_enabled: bool,

    /// ZFS-TXG-clone Phase 1: when true (default), `commit_ops` detects
    /// L2P-only commits (no rc / dedup buckets, all target L2P shards in
    /// buffered mode) and applies their L2P buckets DIRECTLY on the caller
    /// thread instead of enqueuing closures onto per-shard apply-lane
    /// workers. Eliminates ~230 us of channel + queue-wait + worker
    /// scheduling overhead per commit on the seqwrite hot path.
    ///
    /// Falls back to the lane path automatically when:
    ///   * the commit touches any dedup bucket,
    ///   * any L2P shard is not running in `use_buffer` mode, or
    ///   * the commit is in the serial-apply branch (lifecycle ops,
    ///     guarded remap on a snapshot-bearing volume).
    ///
    /// Behaviour is byte-equivalent to the lane path; this is purely a
    /// concurrency / latency optimisation. See
    /// `/root/.claude/plans/zfs-txg-clone.md` Phase 1. Set to false to
    /// force the legacy lane path (e.g. for differential debugging).
    pub commit_direct_apply_enabled: bool,

    /// ZFS-TXG-clone Phase 2: when true, `commit_ops_deferred` parks
    /// the freshly-computed `Vec<ApplyOutcome>` in the
    /// [`crate::DeferredOutcomeAggregator`] keyed by LSN and returns
    /// immediately. The L2P compactor's per-pass loop drains every
    /// staged entry whose touched `(volume, shard)` pairs have all
    /// been folded into the on-disk tree (the metadb equivalent of a
    /// ZFS TXG sync). When false (default during the first-month
    /// soak), `commit_ops_deferred` is a thin wrapper that delivers
    /// outcomes synchronously — call sites then either upgrade
    /// transparently or are routed through the legacy
    /// `commit_ops_sync` shape; on-disk format is unchanged.
    ///
    /// This flag governs whether the staging path is exercised at all;
    /// the deferred API is *always* compiled in. Flip to `true` after
    /// the 8h `deferred_outcomes_proptest` soak gate on nvme-box
    /// passes. Behaviour is byte-equivalent to the sync path —
    /// outcomes for a given LSN match exactly; only delivery timing
    /// shifts (compactor boundary instead of caller thread).
    pub commit_deferred_outcomes_enabled: bool,

    /// Upper bound on a single group-commit batch, in bytes.
    pub group_commit_max_batch_bytes: usize,

    /// How long the group-commit thread waits for more work before flushing
    /// a partial batch, in microseconds.
    pub group_commit_timeout_us: u64,

    /// ZFS-TXG-clone Phase 3: when true, `commit_ops_deferred` submits to
    /// the WAL with `synchronous=false`. The writer thread writes the
    /// batch to OS page cache and acks the caller without calling fsync.
    /// Durability is consolidated at the next `flush_with_gate`, which
    /// fans an `Op::FsyncAll` to every WAL lane before promoting the
    /// manifest's `checkpoint_lsn`.
    ///
    /// Safety relies on two invariants outside metadb:
    /// 1. The onyx LV2 write buffer fsyncs every user write before ack,
    ///    acting as the ZIL equivalent. Anything past the last metadb
    ///    TXG sync is re-driven by the onyx commit_worker on restart.
    /// 2. The metadb `apply_gate.write()` barrier in `flush_with_gate`
    ///    sampled `last_applied_lsn` only after every in-flight commit
    ///    completed `finish_global_apply`, which is itself gated on
    ///    `wal.submit*().wait()` returning — so the writer thread has at
    ///    minimum appended the body to OS page cache. `fsync_all_lanes`
    ///    after the sample makes those bytes durable.
    ///
    /// **Requires `commit_deferred_outcomes_enabled = true`.** Async
    /// WAL without deferred outcomes is an untested combination — the
    /// commit_ops_deferred guard rejects the mismatch. Flip to `true`
    /// after the 24h nvme-box soak + 10 SIGKILLs + 5 sysrq power-loss
    /// matrix passes.
    pub wal_async_commits_enabled: bool,

    /// Deadline (microseconds) for group-commit batches whose submits
    /// are all `synchronous=false`. Only consulted when the batch is
    /// async-only; the moment a `synchronous=true` submit lands, the
    /// deadline collapses back to `group_commit_timeout_us` so sync
    /// hot-path latency is protected.
    ///
    /// Default 1000 (1 ms) — large enough to amortise the rare TXG
    /// fsync over many async commits, small enough to keep the inflight
    /// WAL-on-OS-page-cache window bounded.
    pub wal_async_group_commit_window_us: u64,

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
    /// full")`. Page-table chaining (`crate::paged_meta`) lifts the old
    /// single-meta-page cap, so the bucket count can now scale with
    /// the working set. Recorded in the manifest at `Db::create`.
    pub dedup_cuckoo_buckets: u64,

    /// Maximum number of entries kept in the dedup_index L1 hot
    /// cache. Each entry costs roughly 64 B (`(fp, hash, value, lru
    /// link)`), so 1 M entries ≈ 64 MiB. Larger caches reduce L3
    /// IOPS but cost RAM 1:1.
    pub dedup_l1_cache_entries: usize,

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

    /// Enable the B2 in-memory L2P buffer + periodic compaction path.
    /// When `false` (default), commits mutate the paged radix tree
    /// in-line (Phase 0 behaviour). When `true`, commits insert into
    /// the per-shard `L2pBuffer` and a background compactor folds it
    /// into the tree on its own cadence. See
    /// [`crate::db::l2p_buffer`] + [`docs/DESIGN.md §B2`].
    pub l2p_buffer_enabled: bool,

    /// Soft threshold (per-shard active entry count). When
    /// `active.len()` crosses this, the compactor is woken to run a
    /// cycle. Default `64_000` ≈ 5 MB/shard at ~80 B/entry.
    pub l2p_buffer_soft_entries: usize,

    /// Hard threshold (per-shard active entry count). When
    /// `active.len()` crosses this, commits to that shard block on a
    /// Condvar until the compactor swaps the active map. Bounds
    /// peak memory at the cost of brief commit stalls.
    pub l2p_buffer_hard_entries: usize,

    /// Maximum wall time the compactor may wait between cycles even
    /// when no entry threshold is crossed. Bounds `checkpoint_lsn`
    /// lag and WAL retention on idle systems.
    pub l2p_buffer_max_interval_ms: u64,

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
    /// 0 disables the cap (admit bg ops freely; matches pre-Tier-1.C
    /// behaviour for regression A/Bs).
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
    /// [[no-refcount-hot-path-design]] Phase 4 Step 4: when true, the
    /// Lineage GC pass emits a [`crate::wal::WalOp::FreePbas`] WAL
    /// record for every dead-list record it retires (decref each
    /// shared PBA via the global rc, surface exclusive PBAs
    /// directly) before truncating the chain. When false (the
    /// default), GC behaves exactly as in Phase 3 — chain
    /// truncation only, no PBA retire surface. Phase 5 will flip
    /// the default ON once the hot path stops maintaining rc.
    pub lineage_gc_emit_freepbas: bool,
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
            unlogged_commits_enabled: false,
            // ZFS-TXG-clone Phase 1 — default on. Direct L2P apply on
            // caller thread for L2P-only commits. Safe fallback to lane
            // path for any commit that doesn't match the eligibility
            // check (see `commit_direct_apply_enabled` doc).
            commit_direct_apply_enabled: true,
            // ZFS-TXG-clone Phase 2 — production default. Validated by
            // the 8h nvme-phase23-soak (verify-clean, zero underflow,
            // engine alive end-to-end). See `commit_deferred_outcomes_enabled` doc.
            commit_deferred_outcomes_enabled: true,
            group_commit_max_batch_bytes: 4 * 1024 * 1024,
            group_commit_timeout_us: 1,
            // ZFS-TXG-clone Phase 3 — production default. Validated by
            // the same 8h nvme-phase23-soak as Phase 2 (both flags ran
            // hot together). Async-WAL durability still relies on the
            // onyx LV2 buffer as the ZIL equivalent (see
            // `wal_async_commits_enabled` doc); 24h SIGKILL/sysrq crash
            // matrix is a later hardening gate, not a prerequisite.
            wal_async_commits_enabled: true,
            wal_async_group_commit_window_us: 1000,
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
            // [[no-refcount-hot-path-design]] Phase 5: hot-path RC
            // writes are gone — Lineage GC is the sole producer of
            // PBA-free decisions, so FreePbas emission must be on by
            // default. The Phase 3/4 default-off mode is preserved
            // only for offline tooling that explicitly opts out.
            lineage_gc_emit_freepbas: true,
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
            // Drainer ships **default-on** (Tier 1.A,
            // `/root/.claude/plans/ticklish-sparking-barto.md`). The
            // background drainer absorbs `RcShard.delta_active` into
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
            // B2 buffer ships default-off. Phase 1 lands the
            // infrastructure; Phase 3 flips behaviour (commit writes
            // buffer only); Phase 5 may flip default to true after
            // nvme-box validation. See
            // /root/.claude/plans/ticklish-sparking-barto.md.
            l2p_buffer_enabled: false,
            // 64 K entries soft trigger ≈ 5 MB / shard. Compactor
            // wakes when any shard crosses this. Same magnitude as
            // `refcount_drainer_threshold_entries`. Smaller values
            // (e.g. 16 K) appeared attractive ("fold often, fold
            // small") but the more frequent cycles compete with
            // flush's apply_gate.write hold and net regress p9999.
            l2p_buffer_soft_entries: 64_000,
            // 512 K entries hard trigger ≈ 40 MB / shard. Commit
            // backpressure kicks in past this; bounds peak per-shard
            // RAM at ~40 MB worst case.
            l2p_buffer_hard_entries: 512_000,
            // 30 s wall-clock floor — even on an idle system the
            // compactor will fire at least this often so
            // `checkpoint_lsn` and WAL retention don't drift.
            l2p_buffer_max_interval_ms: 30_000,
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
