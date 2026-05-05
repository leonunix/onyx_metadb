use super::*;
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, Debug)]
struct RcApplyAction {
    op_idx: usize,
    pba: Pba,
    delta: i64,
    standalone_refcount: bool,
    remap_freed_candidate: bool,
}

struct L2pBucketApplyResult {
    outcomes: Vec<(usize, ApplyOutcome)>,
    rc_actions: Vec<RcApplyAction>,
}

#[derive(Default)]
struct RcBucketApplyResult {
    refcount_outcomes: Vec<(usize, u32)>,
    remap_freed: Vec<(usize, Pba)>,
}

struct LaneDispatchPlan {
    l2p_sorted: Vec<((VolumeOrdinal, usize), Vec<usize>)>,
    rc_buckets: Vec<Vec<RcApplyAction>>,
    rc_enqueued: Vec<bool>,
    /// Dedup op indices grouped by shard. Outer length always equals
    /// `Db::dedup_lanes.len()`; an empty inner vec means that shard
    /// has no work in this commit and gets no `DispatchLaneKey::Dedup`
    /// footprint entry.
    dedup_buckets: Vec<Vec<usize>>,
}

struct QueuedLanePlan {
    ops: Arc<Vec<WalOp>>,
    l2p_receivers: Vec<crossbeam_channel::Receiver<Result<L2pBucketApplyResult>>>,
    rc_buckets: Vec<Vec<RcApplyAction>>,
    rc_pending: Vec<Option<PendingApplyWork>>,
    /// Per-shard dedup op indices. Drained into the per-shard apply
    /// closures during `apply_ops_laned`.
    dedup_buckets: Vec<Vec<usize>>,
    /// One pending slot per non-empty bucket (entries for empty
    /// buckets are `None`).
    dedup_pendings: Vec<Option<PendingApplyWork>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct LanedApplyTiming {
    l2p_wait: std::time::Duration,
    rc_enqueue: std::time::Duration,
    rc_wait: std::time::Duration,
    dedup_enqueue: std::time::Duration,
    dedup_wait: std::time::Duration,
}

#[derive(Clone, Copy, Debug, Default)]
struct CommitTiming {
    drop_gate_wait: std::time::Duration,
    plan: std::time::Duration,
    encode: std::time::Duration,
    wal_submit: std::time::Duration,
    dispatch_wait: std::time::Duration,
    apply_gate_wait: std::time::Duration,
    lane_enqueue: std::time::Duration,
    apply: std::time::Duration,
    laned: LanedApplyTiming,
    finish_global_wait: std::time::Duration,
}

struct ActiveApplyGuard<'a> {
    db: &'a Db,
    lsn: Lsn,
}

impl Drop for ActiveApplyGuard<'_> {
    fn drop(&mut self) {
        self.db.active_apply_lsns.lock().remove(&self.lsn);
    }
}

impl DispatchFootprint {
    pub(super) fn global() -> Self {
        Self {
            global: true,
            lanes: BTreeSet::new(),
        }
    }

    fn from_lane_plan(plan: &LaneDispatchPlan) -> Self {
        let mut lanes = BTreeSet::new();
        for ((vol_ord, sid), _) in &plan.l2p_sorted {
            lanes.insert(DispatchLaneKey::L2p(*vol_ord, *sid));
        }
        for (sid, enqueued) in plan.rc_enqueued.iter().copied().enumerate() {
            if enqueued {
                lanes.insert(DispatchLaneKey::Refcount(sid));
            }
        }
        for (sid, bucket) in plan.dedup_buckets.iter().enumerate() {
            if !bucket.is_empty() {
                lanes.insert(DispatchLaneKey::Dedup(sid as u32));
            }
        }
        Self {
            global: false,
            lanes,
        }
    }

    fn conflicts(&self, other: &Self) -> bool {
        self.global || other.global || self.lanes.iter().any(|lane| other.lanes.contains(lane))
    }
}

fn wal_lane_key(op: &WalOp) -> u64 {
    match op {
        WalOp::L2pPut { vol_ord, lba, .. }
        | WalOp::L2pDelete { vol_ord, lba }
        | WalOp::L2pRemap { vol_ord, lba, .. } => {
            let mut bytes = [0u8; 10];
            bytes[..2].copy_from_slice(&vol_ord.to_be_bytes());
            bytes[2..].copy_from_slice(&lba.to_be_bytes());
            xxh3_64(&bytes)
        }
        WalOp::L2pRangeDelete { vol_ord, start, .. } => {
            let mut bytes = [0u8; 10];
            bytes[..2].copy_from_slice(&vol_ord.to_be_bytes());
            bytes[2..].copy_from_slice(&start.to_be_bytes());
            xxh3_64(&bytes)
        }
        WalOp::DedupPut { hash, .. }
        | WalOp::DedupPutGuarded { hash, .. }
        | WalOp::DedupDelete { hash }
        | WalOp::DedupCompareDelete { hash, .. }
        | WalOp::DedupComparePut { hash, .. } => xxh3_64(hash),
        WalOp::DedupReversePut { pba, hash } | WalOp::DedupReverseDelete { pba, hash } => {
            let mut bytes = [0u8; 40];
            bytes[..8].copy_from_slice(&pba.to_be_bytes());
            bytes[8..].copy_from_slice(hash);
            xxh3_64(&bytes)
        }
        WalOp::Incref { pba, .. } | WalOp::Decref { pba, .. } => xxh3_64(&pba.to_be_bytes()),
        WalOp::DropSnapshot { id, .. } => xxh3_64(&id.to_be_bytes()),
        WalOp::CreateVolume { ord, .. } | WalOp::DropVolume { ord, .. } => {
            xxh3_64(&ord.to_be_bytes())
        }
        WalOp::CloneVolume { new_ord, .. } => xxh3_64(&new_ord.to_be_bytes()),
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn thread_cpu_time() -> Option<std::time::Duration> {
    let mut ts = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, ts.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let ts = unsafe { ts.assume_init() };
    Some(std::time::Duration::new(
        ts.tv_sec as u64,
        ts.tv_nsec as u32,
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn thread_cpu_time() -> Option<std::time::Duration> {
    None
}

fn duration_us(duration: std::time::Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn dispatch_ready(state: &DispatchState, lsn: Lsn) -> bool {
    let Some(entry) = state.pending.get(&lsn) else {
        return false;
    };
    entry.durable
        && state
            .pending
            .range(..lsn)
            .all(|(_, lower)| !entry.footprint.conflicts(&lower.footprint))
}

impl Db {
    // -------- transaction / commit --------------------------------------

    /// Start a new transaction that buffers ops until `commit()`.
    pub fn begin(&self) -> Transaction<'_> {
        Transaction::new(self)
    }

    /// LSN of the most recent successful commit.
    pub fn last_applied_lsn(&self) -> Lsn {
        *self.last_applied_lsn.lock()
    }

    fn enter_active_apply(&self, lsn: Lsn) -> ActiveApplyGuard<'_> {
        self.active_apply_lsns.lock().insert(lsn);
        ActiveApplyGuard { db: self, lsn }
    }

    fn has_higher_active_apply(&self, lsn: Lsn) -> bool {
        self.active_apply_lsns
            .lock()
            .iter()
            .next_back()
            .is_some_and(|active| *active > lsn)
    }

    fn acquire_commit_apply_gate(&self, lsn: Lsn) -> crate::apply_gate::ReadGuard<'_> {
        const RECHECK: std::time::Duration = std::time::Duration::from_micros(100);

        let started = std::time::Instant::now();
        let mut logged_wait = false;
        loop {
            if !self.apply_gate.has_writer_pending() {
                return self.apply_gate.read();
            }

            if self.has_higher_active_apply(lsn) {
                let waited = started.elapsed();
                if waited >= std::time::Duration::from_millis(1) {
                    tracing::debug!(
                        lsn,
                        wait_us = duration_us(waited),
                        "metadb: lower-LSN commit bypassing pending checkpoint to unblock global apply order"
                    );
                }
                return self.apply_gate.read_bypass_writer_pending();
            }

            if !logged_wait && started.elapsed() >= std::time::Duration::from_secs(1) {
                logged_wait = true;
                tracing::warn!(
                    lsn,
                    wait_ms = started.elapsed().as_millis() as u64,
                    "metadb: commit waiting behind checkpoint apply gate"
                );
            }
            std::thread::sleep(RECHECK);
        }
    }

    /// Internal: submit a set of ops to the WAL, apply them to indexes,
    /// and return the assigned LSN plus any per-op outcomes.
    ///
    /// Concurrency: WAL submission runs **outside** any Db-level lock so
    /// multiple submitters coalesce into one group-commit batch at the
    /// WAL writer. After fsync, dispatch is re-serialised by global LSN
    /// but does only short enqueue work: bucket ops, push each bucket into
    /// its shard lane, and advance `last_dispatched_lsn`. The expensive
    /// apply work then runs concurrently across lanes. A commit only waits
    /// for global LSN order again after its lanes finish, right before it
    /// advances the contiguous `last_applied_lsn`.
    pub(crate) fn commit_ops(&self, ops: &[WalOp]) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        if ops.is_empty() {
            self.metrics.record_commit_empty();
            return Ok((self.last_applied_lsn(), Vec::new()));
        }
        let commit_started = std::time::Instant::now();
        let cpu_started = thread_cpu_time();
        let mut timing = CommitTiming::default();
        self.metrics.record_commit_attempt(ops.len());
        // `drop_gate.read()` pairs with lifecycle paths' write acquire.
        // Hold it across submit + apply so `drop_snapshot` /
        // `range_delete` cannot wedge themselves between our LSN
        // assignment and apply. Using the read side is important:
        // ordinary commits must still submit concurrently so the WAL
        // writer can coalesce them into group commits.
        let drop_gate_started = std::time::Instant::now();
        let _drop_guard = self.drop_gate.read();
        timing.drop_gate_wait = drop_gate_started.elapsed();
        self.metrics
            .record_commit_drop_gate_wait(timing.drop_gate_wait);
        // Plan the lane footprint before LSN allocation. `submit_wal_ops`
        // registers this footprint while the WAL set still holds its
        // allocator mutex, so every lower LSN's footprint is known before
        // a higher LSN can be assigned.
        let plan_started = std::time::Instant::now();
        let volumes = self.volumes.read().clone();
        let serial_apply = self.batch_uses_serial_apply(ops);
        let plan = if serial_apply {
            None
        } else {
            match self.build_lane_dispatch_plan(&volumes, ops) {
                Ok(plan) => Some(plan),
                Err(err) => {
                    self.metrics.record_commit_error(commit_started.elapsed());
                    self.poison_commit_waiters(&err);
                    return Err(err);
                }
            }
        };
        timing.plan = plan_started.elapsed();
        let plan_l2p_lanes = plan.as_ref().map(|plan| plan.l2p_sorted.len()).unwrap_or(0);
        let plan_rc_lanes = plan
            .as_ref()
            .map(|plan| {
                plan.rc_enqueued
                    .iter()
                    .filter(|enqueued| **enqueued)
                    .count()
            })
            .unwrap_or(0);
        let plan_dedup_ops = plan
            .as_ref()
            .map(|plan| {
                plan.dedup_buckets
                    .iter()
                    .map(|bucket| bucket.len())
                    .sum::<usize>()
            })
            .unwrap_or(0);
        let dispatch_footprint = plan
            .as_ref()
            .map(DispatchFootprint::from_lane_plan)
            .unwrap_or_else(DispatchFootprint::global);
        let dispatch_lanes = dispatch_footprint.lanes.len();

        let encode_started = std::time::Instant::now();
        let body = match try_encode_body(ops) {
            Ok(body) => body,
            Err(err) => {
                self.metrics.record_commit_error(commit_started.elapsed());
                return Err(err);
            }
        };
        timing.encode = encode_started.elapsed();
        let wal_started = std::time::Instant::now();
        let lsn = match self.submit_wal_ops(ops, body, Some(dispatch_footprint)) {
            Ok(lsn) => {
                timing.wal_submit = wal_started.elapsed();
                self.metrics.record_commit_wal_submit(timing.wal_submit);
                lsn
            }
            Err(err) => {
                timing.wal_submit = wal_started.elapsed();
                self.metrics.record_commit_wal_submit(timing.wal_submit);
                self.metrics.record_commit_error(commit_started.elapsed());
                self.poison_commit_waiters(&err);
                return Err(err);
            }
        };
        if let Err(err) = self.faults.inject(FaultPoint::CommitPostWalBeforeApply) {
            self.metrics.record_commit_error(commit_started.elapsed());
            self.poison_commit_waiters(&err);
            return Err(err);
        }

        // Wait until every lower durable-or-in-flight LSN that touches one
        // of our lanes has dispatched. Disjoint lower LSNs do not block us.
        let wait_started = std::time::Instant::now();
        if let Err(err) = self.mark_wal_durable_and_wait_for_dispatch(lsn) {
            timing.dispatch_wait = wait_started.elapsed();
            self.metrics.record_commit_apply_wait(timing.dispatch_wait);
            self.metrics.record_commit_error(commit_started.elapsed());
            return Err(err);
        }
        timing.dispatch_wait = wait_started.elapsed();
        self.metrics.record_commit_apply_wait(timing.dispatch_wait);

        let apply_gate_started = std::time::Instant::now();
        let apply_guard = self.acquire_commit_apply_gate(lsn);
        let active_apply = self.enter_active_apply(lsn);
        timing.apply_gate_wait = apply_gate_started.elapsed();
        self.metrics
            .record_commit_apply_gate_wait(timing.apply_gate_wait);

        let apply_started = std::time::Instant::now();
        let outcomes = if let Some(plan) = plan {
            let enqueue_started = std::time::Instant::now();
            let queued_plan = self.enqueue_lane_plan(&volumes, lsn, plan, Arc::new(ops.to_vec()));
            timing.lane_enqueue = enqueue_started.elapsed();
            self.complete_retained_dispatch(lsn);
            match self.apply_ops_laned(lsn, ops.len(), queued_plan) {
                Ok((outcomes, laned_timing)) => {
                    timing.apply = apply_started.elapsed();
                    timing.laned = laned_timing;
                    self.metrics.record_commit_apply(timing.apply);
                    self.metrics.record_commit_apply_laned(
                        laned_timing.l2p_wait,
                        laned_timing.rc_enqueue,
                        laned_timing.rc_wait,
                        laned_timing.dedup_enqueue,
                        laned_timing.dedup_wait,
                    );
                    outcomes
                }
                Err(err) => {
                    timing.apply = apply_started.elapsed();
                    self.metrics.record_commit_apply(timing.apply);
                    self.metrics.record_commit_error(commit_started.elapsed());
                    self.poison_commit_waiters(&err);
                    return Err(err);
                }
            }
        } else {
            if let Err(err) = self.wait_for_global_apply_turn(lsn) {
                timing.apply = apply_started.elapsed();
                self.metrics.record_commit_apply(timing.apply);
                self.metrics.record_commit_error(commit_started.elapsed());
                return Err(err);
            }
            match self.apply_commit_batch(&volumes, lsn, ops) {
                Ok(outcomes) => {
                    timing.apply = apply_started.elapsed();
                    self.metrics.record_commit_apply(timing.apply);
                    outcomes
                }
                Err(err) => {
                    timing.apply = apply_started.elapsed();
                    self.metrics.record_commit_apply(timing.apply);
                    self.metrics.record_commit_error(commit_started.elapsed());
                    self.poison_commit_waiters(&err);
                    return Err(err);
                }
            }
        };
        if let Err(err) = self.faults.inject(FaultPoint::CommitPostApplyBeforeLsnBump) {
            self.metrics.record_commit_error(commit_started.elapsed());
            self.poison_commit_waiters(&err);
            return Err(err);
        }

        // Bump BEFORE dropping the gate: if we released the gate first
        // a concurrent flush could observe `last_applied_lsn = lsn - 1`
        // while trees already contain op `lsn`, causing recovery to
        // double-apply on restart (refcount incref is not idempotent).
        let finish_started = std::time::Instant::now();
        if let Err(err) = self.finish_global_apply(lsn) {
            self.metrics.record_commit_error(commit_started.elapsed());
            return Err(err);
        }
        timing.finish_global_wait = finish_started.elapsed();
        if serial_apply {
            self.complete_retained_dispatch(lsn);
        }
        drop(active_apply);
        drop(apply_guard);
        let total_elapsed = commit_started.elapsed();
        if total_elapsed >= std::time::Duration::from_secs(1) {
            let cpu_elapsed = cpu_started
                .and_then(|started| thread_cpu_time().map(|now| now.saturating_sub(started)));
            let pending = self.pending_state();
            let metrics = self.metrics.snapshot();
            tracing::warn!(
                lsn,
                ops = ops.len(),
                serial_apply,
                plan_l2p_lanes,
                plan_rc_lanes,
                plan_dedup_ops,
                dispatch_lanes,
                total_ms = total_elapsed.as_millis() as u64,
                thread_cpu_ms = cpu_elapsed.map(|d| d.as_millis() as u64),
                drop_gate_wait_us = duration_us(timing.drop_gate_wait),
                plan_us = duration_us(timing.plan),
                encode_us = duration_us(timing.encode),
                wal_submit_us = duration_us(timing.wal_submit),
                dispatch_wait_us = duration_us(timing.dispatch_wait),
                apply_gate_wait_us = duration_us(timing.apply_gate_wait),
                lane_enqueue_us = duration_us(timing.lane_enqueue),
                apply_us = duration_us(timing.apply),
                l2p_lane_wait_us = duration_us(timing.laned.l2p_wait),
                rc_lane_enqueue_us = duration_us(timing.laned.rc_enqueue),
                rc_lane_wait_us = duration_us(timing.laned.rc_wait),
                dedup_lane_enqueue_us = duration_us(timing.laned.dedup_enqueue),
                dedup_lane_wait_us = duration_us(timing.laned.dedup_wait),
                finish_global_wait_us = duration_us(timing.finish_global_wait),
                wal_submit_wait_max_us = metrics.wal_submit_wait_max_us,
                wal_write_max_us = metrics.wal_write_max_us,
                wal_fsync_max_us = metrics.wal_fsync_max_us,
                wal_batch_records_max = metrics.wal_batch_records_max,
                pending_dispatch = pending.dispatch_pending,
                pending_dedup_lane_q = pending.dedup_lane_queue,
                pending_l2p_apply_q = pending.l2p_apply_queue,
                pending_l2p_dirty = pending.l2p_pagebuf_dirty,
                pending_rc_apply_q = pending.rc_apply_queue,
                pending_rc_dirty = pending.rc_pagebuf_dirty,
                flush_total_max_us = metrics.flush_total_max_us,
                flush_io_max_us = metrics.flush_io_max_us,
                flush_install_max_us = metrics.flush_install_max_us,
                flush_reclaim_max_us = metrics.flush_reclaim_max_us,
                "metadb: slow commit_with_outcomes (>=1s)"
            );
        }
        self.metrics.record_commit_success(total_elapsed);
        Ok((lsn, outcomes))
    }

    fn apply_op(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        op: &WalOp,
    ) -> Result<ApplyOutcome> {
        let snap_lookup = |vol: VolumeOrdinal| -> Vec<SnapInfo> { self.snap_info_for_vol(vol) };
        let op_started = std::time::Instant::now();
        let outcome = apply_op_bare(
            volumes,
            &self.refcount_shards,
            &self.dedup_index,
            &self.dedup_reverse,
            &self.page_store,
            lsn,
            op,
            &snap_lookup,
        )?;
        record_per_op_apply(&self.metrics, op, op_started.elapsed());
        // DropSnapshot also mutates the in-memory manifest's snapshot
        // list; the page work lives in apply_op_bare so it can be
        // shared with the replay path. Lock order (apply_gate.read →
        // manifest_state) matches every other call site.
        if let WalOp::DropSnapshot { id, .. } = op {
            let dropped_vol = {
                let mut mstate = self.manifest_state.lock();
                let dropped = mstate
                    .manifest
                    .snapshots
                    .iter()
                    .find(|s| s.id == *id)
                    .map(|s| s.vol_ord);
                mstate.manifest.snapshots.retain(|s| s.id != *id);
                dropped
            };
            if let Some(vol) = dropped_vol {
                self.recompute_snap_info(vol);
            }
        }
        Ok(outcome)
    }

    pub(super) fn submit_wal_ops(
        &self,
        ops: &[WalOp],
        body: Vec<u8>,
        footprint: Option<DispatchFootprint>,
    ) -> Result<Lsn> {
        let lane = self.wal_lane_for_ops(ops);
        match footprint {
            Some(footprint) => self.wal.submit_to_reserved(lane, body, |lsn| {
                self.register_dispatch_intent(lsn, footprint);
            }),
            None => self.wal.submit_to(lane, body),
        }
    }

    fn wal_lane_for_ops(&self, ops: &[WalOp]) -> usize {
        let lanes = self.wal.lane_count();
        if lanes <= 1 {
            return 0;
        }
        let Some(op) = ops.first() else {
            return 0;
        };
        (wal_lane_key(op) as usize) % lanes
    }

    /// Apply a commit batch under `apply_gate.read()`. Large batches
    /// group ops by shard so each shard lock is taken once per (vol,
    /// shard) rather than once per op. Small batches fall through to
    /// the serial path — the bucketing overhead dominates below a
    /// handful of ops.
    ///
    /// Correctness:
    /// - Intra-bucket order is preserved (`Vec<usize>` of original op
    ///   indices), so multiple ops to the same (vol, lba) or same pba
    ///   apply in caller order.
    /// - Cross-bucket order is relaxed: L2P shards and refcount shards
    ///   live on disjoint trees, and same-LSN `cow_for_write` is
    ///   idempotent via `page.generation >= lsn`, so reordering between
    ///   two shards does not change the committed state.
    /// - Each shard lock is held for the span of its bucket only;
    ///   locks are taken one at a time in (vol_ord, shard_id) sorted
    ///   order, so the CLAUDE-documented "cross-shard ops take locks
    ///   in shard index order" invariant still holds.
    ///
    /// Defensive fallback: if the batch contains a lifecycle op
    /// (DropSnapshot, CreateVolume, DropVolume, CloneVolume), a guarded
    /// remap, or a remap against a volume with live snapshots, we fall
    /// back to serial apply. Lifecycle ops do not currently reach
    /// `commit_ops` from any caller — they have their own entry points
    /// with stronger locking — but the fallback keeps the bucketed path
    /// safe if a future caller routes them through here.
    fn apply_commit_batch(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        const BUCKET_THRESHOLD: usize = 8;
        if ops.len() < BUCKET_THRESHOLD || self.batch_requires_serial_apply(ops) {
            let mut outcomes = Vec::with_capacity(ops.len());
            for op in ops {
                outcomes.push(self.apply_op(volumes, lsn, op)?);
            }
            return Ok(outcomes);
        }
        self.apply_ops_grouped(volumes, lsn, ops)
    }

    fn batch_requires_serial_apply(&self, ops: &[WalOp]) -> bool {
        if batch_contains_lifecycle_op(ops) {
            return true;
        }

        // Guarded remaps used to fall back to serial apply because the
        // lane path's `apply_l2p_bucket` did not honour the guard. The
        // bucket now does the rc check inline (same lock order as the
        // serial path: L2P shard write → refcount shard lock), so guard
        // alone no longer forces serial. Snapshot-bearing volumes still
        // need the serial path for the snap-pin walk inside
        // `apply_l2p_remap`.
        let mut remap_vols = HashSet::new();
        for op in ops {
            if let WalOp::L2pRemap { vol_ord, .. } = op {
                remap_vols.insert(*vol_ord);
            }
        }

        remap_vols
            .into_iter()
            .any(|vol_ord| !self.snap_info_for_vol(vol_ord).is_empty())
    }

    fn batch_uses_serial_apply(&self, ops: &[WalOp]) -> bool {
        self.batch_requires_serial_apply(ops)
    }

    pub(super) fn poison_commit_waiters(&self, err: &MetaDbError) {
        let mut poison = self.commit_poison.lock();
        if poison.is_none() {
            *poison = Some(err.to_string());
        }
        self.dispatch_cvar.notify_all();
        self.commit_cvar.notify_all();
    }

    fn commit_poison_error(&self) -> Option<MetaDbError> {
        self.commit_poison
            .lock()
            .as_ref()
            .map(|msg| MetaDbError::Corruption(format!("commit pipeline failed: {msg}")))
    }

    fn register_dispatch_intent(&self, lsn: Lsn, footprint: DispatchFootprint) {
        let mut state = self.dispatch_state.lock();
        let old = state.pending.insert(
            lsn,
            DispatchEntry {
                footprint,
                durable: false,
            },
        );
        debug_assert!(
            old.is_none(),
            "duplicate dispatch reservation for LSN {lsn}"
        );
        self.dispatch_cvar.notify_all();
    }

    fn mark_wal_durable_and_wait_for_dispatch(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.dispatch_state.lock();
        let entry = state.pending.get_mut(&lsn).ok_or_else(|| {
            MetaDbError::Corruption(format!("missing dispatch reservation for LSN {lsn}"))
        })?;
        entry.durable = true;
        self.dispatch_cvar.notify_all();

        loop {
            if let Some(err) = self.commit_poison_error() {
                return Err(err);
            }
            if dispatch_ready(&state, lsn) {
                return Ok(());
            }
            self.dispatch_cvar.wait(&mut state);
        }
    }

    fn forget_dispatch_intent(&self, lsn: Lsn) {
        let mut state = self.dispatch_state.lock();
        state.pending.remove(&lsn);
        self.dispatch_cvar.notify_all();
    }

    fn complete_retained_dispatch(&self, lsn: Lsn) {
        self.forget_dispatch_intent(lsn);
    }

    pub(super) fn advance_dispatch_lsn(&self, lsn: Lsn) {
        // Lifecycle paths still call this at the old "dispatch complete"
        // point. They hold `drop_gate.write()` and `apply_gate.write()`,
        // so ordinary commits cannot have in-flight dispatch reservations
        // that need ordering against them.
        self.forget_dispatch_intent(lsn);
    }

    pub(super) fn wait_for_global_apply_turn(&self, lsn: Lsn) -> Result<()> {
        let mut applied = self.last_applied_lsn.lock();
        while *applied + 1 < lsn {
            if let Some(err) = self.commit_poison_error() {
                return Err(err);
            }
            self.commit_cvar.wait(&mut applied);
        }
        Ok(())
    }

    pub(super) fn finish_global_apply(&self, lsn: Lsn) -> Result<()> {
        let mut applied = self.last_applied_lsn.lock();
        while *applied + 1 < lsn {
            if let Some(err) = self.commit_poison_error() {
                return Err(err);
            }
            self.commit_cvar.wait(&mut applied);
        }
        debug_assert_eq!(
            *applied + 1,
            lsn,
            "global apply LSN advanced non-contiguously"
        );
        *applied = lsn;
        self.commit_cvar.notify_all();
        Ok(())
    }

    fn build_lane_dispatch_plan(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        ops: &[WalOp],
    ) -> Result<LaneDispatchPlan> {
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<usize>> = HashMap::new();
        let mut rc_buckets: Vec<Vec<RcApplyAction>> = (0..self.refcount_shards.len())
            .map(|_| Vec::new())
            .collect();
        let dedup_shard_count = self.dedup_lanes.len();
        let dedup_shard_count_u32 = dedup_shard_count as u32;
        let mut dedup_buckets: Vec<Vec<usize>> =
            (0..dedup_shard_count).map(|_| Vec::new()).collect();
        let mut remap_may_defer_refcount = false;

        for (idx, op) in ops.iter().enumerate() {
            match op {
                WalOp::L2pPut { vol_ord, lba, .. } | WalOp::L2pDelete { vol_ord, lba } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!("L2P op for unknown volume ord {vol_ord}"))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets.entry((*vol_ord, sid)).or_default().push(idx);
                }
                WalOp::L2pRemap { vol_ord, lba, .. } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!(
                            "L2pRemap for unknown volume ord {vol_ord}"
                        ))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets.entry((*vol_ord, sid)).or_default().push(idx);
                    remap_may_defer_refcount = true;
                }
                WalOp::Incref { pba, .. } | WalOp::Decref { pba, .. } => {
                    let sid = shard_for_key(&self.refcount_shards, *pba);
                    let delta = match op {
                        WalOp::Incref { delta, .. } => i64::from(*delta),
                        WalOp::Decref { delta, .. } => -i64::from(*delta),
                        _ => unreachable!(),
                    };
                    rc_buckets[sid].push(RcApplyAction {
                        op_idx: idx,
                        pba: *pba,
                        delta,
                        standalone_refcount: true,
                        remap_freed_candidate: false,
                    });
                }
                WalOp::DedupPut { hash, .. }
                | WalOp::DedupPutGuarded { hash, .. }
                | WalOp::DedupDelete { hash, .. }
                | WalOp::DedupCompareDelete { hash, .. }
                | WalOp::DedupComparePut { hash, .. } => {
                    let sid =
                        crate::dedup_types::shard_for_hash(hash, dedup_shard_count_u32) as usize;
                    dedup_buckets[sid].push(idx);
                }
                WalOp::DedupReversePut { hash, .. } | WalOp::DedupReverseDelete { hash, .. } => {
                    // Forward and reverse for the same `(hash, pba)`
                    // pair must land in the same shard so the dedup
                    // invariant survives a single commit. Route by
                    // hash here, not the encoded reverse key.
                    let sid =
                        crate::dedup_types::shard_for_hash(hash, dedup_shard_count_u32) as usize;
                    dedup_buckets[sid].push(idx);
                }
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRangeDelete { .. } => {
                    return Err(MetaDbError::Corruption(
                        "lifecycle op reached lane dispatch path".into(),
                    ));
                }
            }
        }

        let mut l2p_sorted: Vec<_> = l2p_buckets.into_iter().collect();
        l2p_sorted.sort_by_key(|((vol, sid), _)| (*vol, *sid));

        let mut rc_enqueued: Vec<bool> =
            rc_buckets.iter().map(|bucket| !bucket.is_empty()).collect();
        if remap_may_defer_refcount {
            rc_enqueued.fill(true);
        }

        Ok(LaneDispatchPlan {
            l2p_sorted,
            rc_buckets,
            rc_enqueued,
            dedup_buckets,
        })
    }

    fn enqueue_lane_plan(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        plan: LaneDispatchPlan,
        ops: Arc<Vec<WalOp>>,
    ) -> QueuedLanePlan {
        let mut l2p_receivers = Vec::with_capacity(plan.l2p_sorted.len());
        // Snapshot refcount shard handles once per commit so the per-lane
        // closures can do guarded-remap rc lookups (dedup hits) without
        // touching the Db struct from the worker thread.
        let refcount_shards_arc: Arc<Vec<Arc<crate::refcount::RcShard>>> =
            Arc::new(self.refcount_shards.iter().map(|s| s.rc.clone()).collect());
        for ((vol_ord, sid), indices) in plan.l2p_sorted {
            let volume = volumes
                .get(&vol_ord)
                .expect("volume presence checked during lane planning");
            let apply_volume = volume.clone();
            let apply_ops = ops.clone();
            let metrics = self.metrics.clone();
            let refcount_shards_arc = refcount_shards_arc.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            volume.shards[sid].apply_lane.enqueue_ready(
                lsn,
                Box::new(move || {
                    let result = Self::apply_l2p_bucket(
                        apply_volume,
                        sid,
                        indices,
                        lsn,
                        apply_ops.as_slice(),
                        refcount_shards_arc.as_slice(),
                        metrics.as_ref(),
                    );
                    let _ = tx.send(result);
                }),
            );
            l2p_receivers.push(rx);
        }
        let mut rc_pending: Vec<Option<PendingApplyWork>> =
            (0..plan.rc_enqueued.len()).map(|_| None).collect();
        for (sid, enqueued) in plan.rc_enqueued.iter().copied().enumerate() {
            if enqueued {
                rc_pending[sid] = Some(self.refcount_shards[sid].apply_lane.enqueue_pending(lsn));
            }
        }
        // One pending slot per non-empty dedup bucket. Empty buckets
        // get `None`, matching the per-shard layout of `dedup_buckets`.
        let mut dedup_pendings: Vec<Option<PendingApplyWork>> =
            (0..plan.dedup_buckets.len()).map(|_| None).collect();
        for (sid, bucket) in plan.dedup_buckets.iter().enumerate() {
            if !bucket.is_empty() {
                dedup_pendings[sid] = Some(self.dedup_lanes[sid].enqueue_pending(lsn));
            }
        }
        QueuedLanePlan {
            ops,
            l2p_receivers,
            rc_buckets: plan.rc_buckets,
            rc_pending,
            dedup_buckets: plan.dedup_buckets,
            dedup_pendings,
        }
    }

    fn apply_l2p_bucket(
        volume: Arc<Volume>,
        sid: usize,
        indices: Vec<usize>,
        lsn: Lsn,
        ops: &[WalOp],
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        metrics: &MetaMetrics,
    ) -> Result<L2pBucketApplyResult> {
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut rc_actions = Vec::new();
        let shard = &volume.shards[sid];
        let tree_lock_started = std::time::Instant::now();
        let mut tree = shard.tree.write();
        let tree_lock_wait = tree_lock_started.elapsed();
        let read_view_prepare_started = std::time::Instant::now();
        let mut read_view_guard = if shard.active_readers.load(Ordering::Acquire) == 0
            && let Some(mut guard) = shard.read_view.try_write()
        {
            if shard.active_readers.load(Ordering::Acquire) == 0 {
                *guard = Arc::new(crate::paged::ReadView::new(
                    tree.root(),
                    tree.root_level(),
                    crate::paged::ReadOverlay::empty_shared(),
                    tree.page_cache().clone(),
                ));
                tree.set_exclusive_read_overlay_mutation(true);
                Some(guard)
            } else {
                None
            }
        } else {
            None
        };
        let read_view_prepare = read_view_prepare_started.elapsed();
        let mut l2p_put_count = 0u64;
        let mut l2p_delete_count = 0u64;
        let mut l2p_remap_count = 0u64;
        let bucket_started = std::time::Instant::now();
        let ops_started = std::time::Instant::now();
        let ops_result = (|| -> Result<()> {
            for idx in indices {
                let outcome = match &ops[idx] {
                    WalOp::L2pPut { lba, value, .. } => {
                        let prev = tree.insert_at_lsn_deferred_finish(*lba, *value, lsn)?;
                        l2p_put_count += 1;
                        ApplyOutcome::L2pPrev(prev)
                    }
                    WalOp::L2pDelete { lba, .. } => {
                        let prev = tree.delete_at_lsn_deferred_finish(*lba, lsn)?;
                        l2p_delete_count += 1;
                        ApplyOutcome::L2pPrev(prev)
                    }
                    WalOp::L2pRemap {
                        lba,
                        new_value,
                        guard,
                        ..
                    } => {
                        // Guarded remaps (dedup hits): verify the target pba's
                        // refcount still satisfies `min_rc` before mutating
                        // L2P. Lock order matches the serial path
                        // (`apply_l2p_remap` in apply.rs): L2P shard write →
                        // refcount shard lock. Refcount lanes only ever take
                        // their own shard's lock and never touch L2P, so
                        // there is no L2P↔RC cycle.
                        if let Some((gp, min_rc)) = guard {
                            let gp_sid =
                                (xxh3_64(&gp.to_be_bytes()) as usize) % refcount_shards.len();
                            let cur = refcount_shards[gp_sid].get(*gp)?;
                            if cur < *min_rc {
                                l2p_remap_count += 1;
                                outcomes.push((
                                    idx,
                                    ApplyOutcome::L2pRemap {
                                        applied: false,
                                        prev: None,
                                        freed_pba: None,
                                    },
                                ));
                                continue;
                            }
                        }
                        let prev = tree.insert_at_lsn_deferred_finish(*lba, *new_value, lsn)?;
                        let value_changed = prev != Some(*new_value);
                        if value_changed {
                            let new_pba = new_value.head_pba();
                            let new_is_zero = new_value.0[27] & 0x02 != 0;
                            match prev {
                                Some(old_value) => {
                                    let old_pba = old_value.head_pba();
                                    let old_is_zero = old_value.0[27] & 0x02 != 0;
                                    if !old_is_zero && (old_pba != new_pba || new_is_zero) {
                                        rc_actions.push(RcApplyAction {
                                            op_idx: idx,
                                            pba: old_pba,
                                            delta: -1,
                                            standalone_refcount: false,
                                            remap_freed_candidate: true,
                                        });
                                    }
                                    if !new_is_zero && (old_pba != new_pba || old_is_zero) {
                                        rc_actions.push(RcApplyAction {
                                            op_idx: idx,
                                            pba: new_pba,
                                            delta: 1,
                                            standalone_refcount: false,
                                            remap_freed_candidate: false,
                                        });
                                    }
                                }
                                None => {
                                    if !new_is_zero {
                                        rc_actions.push(RcApplyAction {
                                            op_idx: idx,
                                            pba: new_pba,
                                            delta: 1,
                                            standalone_refcount: false,
                                            remap_freed_candidate: false,
                                        });
                                    }
                                }
                            }
                        }
                        l2p_remap_count += 1;
                        ApplyOutcome::L2pRemap {
                            applied: true,
                            prev,
                            freed_pba: None,
                        }
                    }
                    other => unreachable!("L2P bucket holds only L2P ops; saw {other:?}"),
                };
                outcomes.push((idx, outcome));
            }
            Ok(())
        })();
        let ops_elapsed = ops_started.elapsed();
        let finish_started = std::time::Instant::now();
        let apply_result = match ops_result {
            Ok(()) => tree.finish_batch_apply(),
            Err(err) => Err(err),
        };
        let finish_elapsed = finish_started.elapsed();
        tree.set_exclusive_read_overlay_mutation(false);
        let publish_started = std::time::Instant::now();
        if let Some(mut guard) = read_view_guard.take() {
            *guard = Arc::new(tree.snapshot_read_view());
        } else if apply_result.is_ok() {
            super::apply::publish_l2p_read_view(shard, &tree);
        }
        let publish_elapsed = publish_started.elapsed();
        apply_result?;
        let bucket_elapsed = bucket_started.elapsed();
        let total_l2p_ops = l2p_put_count + l2p_delete_count + l2p_remap_count;
        if bucket_elapsed.as_micros() >= 100_000
            || tree_lock_wait.as_micros() >= 100_000
            || read_view_prepare.as_micros() >= 100_000
            || ops_elapsed.as_micros() >= 100_000
            || publish_elapsed.as_micros() >= 100_000
        {
            tracing::warn!(
                vol_ord = volume.ord,
                shard = sid,
                lsn,
                indices = total_l2p_ops,
                put = l2p_put_count,
                delete = l2p_delete_count,
                remap = l2p_remap_count,
                total_us = duration_us(bucket_elapsed),
                tree_lock_wait_us = duration_us(tree_lock_wait),
                read_view_prepare_us = duration_us(read_view_prepare),
                ops_us = duration_us(ops_elapsed),
                finish_us = duration_us(finish_elapsed),
                publish_us = duration_us(publish_elapsed),
                "metadb: slow l2p apply bucket"
            );
        }
        if total_l2p_ops > 0 {
            let total_us = bucket_elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
            let put_us = total_us.saturating_mul(l2p_put_count) / total_l2p_ops;
            let delete_us = total_us.saturating_mul(l2p_delete_count) / total_l2p_ops;
            let remap_us = total_us.saturating_sub(put_us).saturating_sub(delete_us);
            metrics.record_apply_l2p_put_batch(
                l2p_put_count,
                std::time::Duration::from_micros(put_us),
            );
            metrics.record_apply_l2p_delete_batch(
                l2p_delete_count,
                std::time::Duration::from_micros(delete_us),
            );
            metrics.record_apply_l2p_remap_batch(
                l2p_remap_count,
                std::time::Duration::from_micros(remap_us),
            );
        }
        Ok(L2pBucketApplyResult {
            outcomes,
            rc_actions,
        })
    }

    fn apply_refcount_bucket_to_tree(
        rc: Arc<crate::refcount::RcShard>,
        metrics: Arc<MetaMetrics>,
        mut actions: Vec<RcApplyAction>,
        lsn: Lsn,
    ) -> Result<RcBucketApplyResult> {
        let mut result = RcBucketApplyResult::default();
        if actions.is_empty() {
            return Ok(result);
        }
        actions.sort_by_key(|action| action.op_idx);
        let mut by_pba: HashMap<Pba, Vec<RcApplyAction>> = HashMap::new();
        for action in actions {
            by_pba.entry(action.pba).or_default().push(action);
        }
        let mut by_pba: Vec<_> = by_pba.into_iter().collect();
        by_pba.sort_by_key(|(pba, _)| *pba);

        for (pba, group) in by_pba {
            let can_coalesce_remap = group.iter().all(|action| !action.standalone_refcount)
                && (group.iter().all(|action| action.delta > 0)
                    || group.iter().all(|action| action.delta < 0));

            if can_coalesce_remap {
                let delta: i64 = group.iter().map(|action| action.delta).sum();
                let op_started = std::time::Instant::now();
                let (pre, new) = rc.stage(pba, delta, lsn)?;
                metrics.record_apply_refcount(op_started.elapsed());
                if new == 0 && pre > 0 {
                    if let Some(action) = group
                        .iter()
                        .rev()
                        .find(|action| action.remap_freed_candidate)
                    {
                        result.remap_freed.push((action.op_idx, action.pba));
                    }
                }
                continue;
            }

            for action in group {
                let op_started = std::time::Instant::now();
                let (pre, new) = rc.stage(action.pba, action.delta, lsn)?;
                metrics.record_apply_refcount(op_started.elapsed());
                if action.remap_freed_candidate {
                    if new == 0 && pre > 0 {
                        result.remap_freed.push((action.op_idx, action.pba));
                    }
                } else if action.standalone_refcount {
                    result.refcount_outcomes.push((action.op_idx, new));
                }
            }
        }
        Ok(result)
    }

    fn apply_dedup_indices_to(
        dedup_index: &crate::dedup::DedupIndex,
        dedup_reverse: &crate::paged_reverse::PagedReverse,
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        metrics: &MetaMetrics,
        ops: &[WalOp],
        indices: Vec<usize>,
        lsn: Lsn,
    ) -> Result<Vec<(usize, ApplyOutcome)>> {
        let batch_started = std::time::Instant::now();
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut reverse_puts: HashMap<Pba, Vec<Hash8>> = HashMap::new();
        let flush_pba = |pba: Pba, reverse_puts: &mut HashMap<Pba, Vec<Hash8>>| -> Result<()> {
            let Some(hashes) = reverse_puts.remove(&pba) else {
                return Ok(());
            };
            let started = std::time::Instant::now();
            let count = hashes.len() as u64;
            dedup_reverse.put_many(pba, &hashes, lsn)?;
            metrics.record_dedup_reverse_put_batch(count, started.elapsed());
            Ok(())
        };
        let flush_all = |reverse_puts: &mut HashMap<Pba, Vec<Hash8>>| -> Result<()> {
            let pbas: Vec<Pba> = reverse_puts.keys().copied().collect();
            for pba in pbas {
                flush_pba(pba, reverse_puts)?;
            }
            Ok(())
        };

        for idx in indices {
            match &ops[idx] {
                WalOp::DedupPut { hash, value } => {
                    flush_all(&mut reverse_puts)?;
                    let started = std::time::Instant::now();
                    dedup_index.put(*hash, *value, lsn)?;
                    metrics.record_dedup_forward_put(started.elapsed());
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                WalOp::DedupPutGuarded {
                    hash,
                    value,
                    pba_guard,
                    min_rc,
                } => {
                    let guard_started = std::time::Instant::now();
                    let rc = refcount_shards
                        .get((xxh3_64(&pba_guard.to_be_bytes()) as usize) % refcount_shards.len())
                        .ok_or_else(|| MetaDbError::Corruption("missing refcount shard".into()))?
                        .get(*pba_guard)?;
                    metrics.record_dedup_guard(guard_started.elapsed());
                    if rc >= *min_rc {
                        let started = std::time::Instant::now();
                        dedup_index.put(*hash, *value, lsn)?;
                        metrics.record_dedup_forward_put(started.elapsed());
                        reverse_puts.entry(*pba_guard).or_default().push(*hash);
                    }
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                WalOp::DedupDelete { hash } => {
                    flush_all(&mut reverse_puts)?;
                    let started = std::time::Instant::now();
                    dedup_index.delete(hash, lsn)?;
                    metrics.record_dedup_forward_delete(started.elapsed());
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                WalOp::DedupCompareDelete { hash, old_value } => {
                    flush_all(&mut reverse_puts)?;
                    let started = std::time::Instant::now();
                    let applied = dedup_index.get(hash)?.as_ref() == Some(old_value);
                    if applied {
                        dedup_index.delete(hash, lsn)?;
                        metrics.record_dedup_forward_delete(started.elapsed());
                    }
                    outcomes.push((idx, ApplyOutcome::DedupCompare { applied }));
                }
                WalOp::DedupComparePut {
                    hash,
                    old_value,
                    new_value,
                } => {
                    flush_all(&mut reverse_puts)?;
                    let started = std::time::Instant::now();
                    let applied = dedup_index.get(hash)?.as_ref() == Some(old_value);
                    if applied {
                        dedup_index.put(*hash, *new_value, lsn)?;
                        metrics.record_dedup_forward_put(started.elapsed());
                    }
                    outcomes.push((idx, ApplyOutcome::DedupCompare { applied }));
                }
                WalOp::DedupReversePut { pba, hash } => {
                    reverse_puts.entry(*pba).or_default().push(*hash);
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                WalOp::DedupReverseDelete { pba, hash } => {
                    flush_pba(*pba, &mut reverse_puts)?;
                    let started = std::time::Instant::now();
                    dedup_reverse.delete(*pba, *hash, lsn)?;
                    metrics.record_dedup_reverse_delete(started.elapsed());
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                other => unreachable!("dedup bucket holds only dedup ops; saw {other:?}"),
            };
        }
        flush_all(&mut reverse_puts)?;
        metrics.record_apply_dedup_batch(outcomes.len() as u64, batch_started.elapsed());
        Ok(outcomes)
    }

    fn apply_ops_laned(
        &self,
        lsn: Lsn,
        op_count: usize,
        mut plan: QueuedLanePlan,
    ) -> Result<(Vec<ApplyOutcome>, LanedApplyTiming)> {
        let mut timing = LanedApplyTiming::default();
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..op_count).map(|_| None).collect();
        let mut first_error = None;

        let l2p_wait_started = std::time::Instant::now();
        for rx in plan.l2p_receivers.drain(..) {
            match rx.recv() {
                Ok(Ok(result)) => {
                    for (idx, outcome) in result.outcomes {
                        outcomes[idx] = Some(outcome);
                    }
                    for action in result.rc_actions {
                        let sid = shard_for_key(&self.refcount_shards, action.pba);
                        plan.rc_buckets[sid].push(action);
                    }
                }
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(MetaDbError::Corruption(
                            "persistent L2P lane worker failed to return a result".into(),
                        ));
                    }
                }
            }
        }
        timing.l2p_wait = l2p_wait_started.elapsed();
        if let Some(err) = first_error {
            return Err(err);
        }

        let mut rc_receivers = Vec::new();
        let rc_enqueue_started = std::time::Instant::now();
        for sid in 0..plan.rc_pending.len() {
            let Some(pending) = plan.rc_pending[sid].take() else {
                continue;
            };
            let actions = std::mem::take(&mut plan.rc_buckets[sid]);
            let rc = self.refcount_shards[sid].rc.clone();
            let metrics = self.metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            pending.set(Box::new(move || {
                let result = Self::apply_refcount_bucket_to_tree(rc, metrics, actions, lsn);
                let _ = tx.send(result);
            }));
            rc_receivers.push(rx);
        }
        timing.rc_enqueue = rc_enqueue_started.elapsed();

        let mut first_error = None;
        let rc_wait_started = std::time::Instant::now();
        for rx in rc_receivers {
            match rx.recv() {
                Ok(Ok(result)) => {
                    for (idx, new) in result.refcount_outcomes {
                        outcomes[idx] = Some(ApplyOutcome::RefcountNew(new));
                    }
                    for (idx, pba) in result.remap_freed {
                        match outcomes[idx].as_mut() {
                            Some(ApplyOutcome::L2pRemap { freed_pba, .. }) => {
                                *freed_pba = Some(pba);
                            }
                            other => {
                                unreachable!("remap rc action missing L2pRemap outcome: {other:?}")
                            }
                        }
                    }
                }
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(MetaDbError::Corruption(
                            "persistent refcount lane worker failed to return a result".into(),
                        ));
                    }
                }
            }
        }
        timing.rc_wait = rc_wait_started.elapsed();
        if let Some(err) = first_error {
            return Err(err);
        }

        // Fan dedup work out across shards. Each non-empty bucket
        // gets its own apply closure on its shard's lane; we collect
        // outcomes from all of them before returning.
        let mut dedup_receivers: Vec<
            crossbeam_channel::Receiver<Result<Vec<(usize, ApplyOutcome)>>>,
        > = Vec::new();
        let dedup_enqueue_started = std::time::Instant::now();
        let dedup_buckets = std::mem::take(&mut plan.dedup_buckets);
        let pendings = std::mem::take(&mut plan.dedup_pendings);
        let refcount_shards_arc: Arc<Vec<Arc<crate::refcount::RcShard>>> =
            Arc::new(self.refcount_shards.iter().map(|s| s.rc.clone()).collect());
        for (_sid, (pending_opt, bucket)) in pendings.into_iter().zip(dedup_buckets).enumerate() {
            let Some(pending) = pending_opt else { continue };
            let ready_at = std::time::Instant::now();
            let bucket_ops = bucket.len() as u64;
            let ops = plan.ops.clone();
            let dedup_index = self.dedup_index.clone();
            let dedup_reverse = self.dedup_reverse.clone();
            let refcount_shards_arc = refcount_shards_arc.clone();
            let metrics = self.metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            pending.set(Box::new(move || {
                let ready_queue_wait = ready_at.elapsed();
                let exec_started = std::time::Instant::now();
                let outcomes = Self::apply_dedup_indices_to(
                    dedup_index.as_ref(),
                    dedup_reverse.as_ref(),
                    refcount_shards_arc.as_slice(),
                    metrics.as_ref(),
                    ops.as_slice(),
                    bucket,
                    lsn,
                );
                metrics.record_dedup_lane_task(
                    bucket_ops,
                    ready_queue_wait,
                    exec_started.elapsed(),
                );
                let _ = tx.send(outcomes);
            }));
            dedup_receivers.push(rx);
        }
        timing.dedup_enqueue = dedup_enqueue_started.elapsed();
        let dedup_wait_started = std::time::Instant::now();
        for rx in dedup_receivers {
            let dedup_outcomes = rx.recv().map_err(|_| {
                MetaDbError::Corruption(
                    "persistent dedup lane worker failed to return a result".into(),
                )
            })??;
            for (idx, outcome) in dedup_outcomes {
                outcomes[idx] = Some(outcome);
            }
        }
        timing.dedup_wait = dedup_wait_started.elapsed();

        Ok((
            outcomes
                .into_iter()
                .map(|o| o.expect("every op index filled by exactly one lane"))
                .collect(),
            timing,
        ))
    }

    pub(super) fn apply_replay_batch(
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        refcount_shards: &[Shard],
        dedup_index: &Arc<crate::dedup::DedupIndex>,
        dedup_reverse: &Arc<crate::paged_reverse::PagedReverse>,
        page_store: &Arc<PageStore>,
        metrics: &Arc<MetaMetrics>,
        lsn: Lsn,
        ops: &[WalOp],
        snap_info_for_vol: &dyn Fn(VolumeOrdinal) -> Vec<SnapInfo>,
    ) -> Result<Vec<ApplyOutcome>> {
        const BUCKET_THRESHOLD: usize = 8;
        if ops.len() < BUCKET_THRESHOLD
            || replay_batch_requires_serial_apply(ops, snap_info_for_vol)
        {
            let mut outcomes = Vec::with_capacity(ops.len());
            for op in ops {
                outcomes.push(apply_op_bare(
                    volumes,
                    refcount_shards,
                    dedup_index.as_ref(),
                    dedup_reverse.as_ref(),
                    page_store,
                    lsn,
                    op,
                    snap_info_for_vol,
                )?);
            }
            return Ok(outcomes);
        }

        Self::apply_ops_grouped_to_lanes(
            volumes,
            refcount_shards,
            dedup_index,
            dedup_reverse,
            metrics,
            lsn,
            ops,
        )
    }

    fn apply_ops_grouped_to_lanes(
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        refcount_shards: &[Shard],
        dedup_index: &Arc<crate::dedup::DedupIndex>,
        dedup_reverse: &Arc<crate::paged_reverse::PagedReverse>,
        metrics: &Arc<MetaMetrics>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        let ops = Arc::new(ops.to_vec());
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..ops.len()).map(|_| None).collect();
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<usize>> = HashMap::new();
        let mut rc_buckets: Vec<Vec<RcApplyAction>> = vec![Vec::new(); refcount_shards.len()];
        let mut dedup_idxs: Vec<usize> = Vec::new();

        for (idx, op) in ops.iter().enumerate() {
            match op {
                WalOp::L2pPut { vol_ord, lba, .. }
                | WalOp::L2pDelete { vol_ord, lba }
                | WalOp::L2pRemap { vol_ord, lba, .. } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!("L2P op for unknown volume ord {vol_ord}"))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets.entry((*vol_ord, sid)).or_default().push(idx);
                }
                WalOp::Incref { pba, .. } | WalOp::Decref { pba, .. } => {
                    let sid = shard_for_key(refcount_shards, *pba);
                    let delta = match op {
                        WalOp::Incref { delta, .. } => i64::from(*delta),
                        WalOp::Decref { delta, .. } => -i64::from(*delta),
                        _ => unreachable!(),
                    };
                    rc_buckets[sid].push(RcApplyAction {
                        op_idx: idx,
                        pba: *pba,
                        delta,
                        standalone_refcount: true,
                        remap_freed_candidate: false,
                    });
                }
                WalOp::DedupPut { .. }
                | WalOp::DedupPutGuarded { .. }
                | WalOp::DedupDelete { .. }
                | WalOp::DedupCompareDelete { .. }
                | WalOp::DedupComparePut { .. }
                | WalOp::DedupReversePut { .. }
                | WalOp::DedupReverseDelete { .. } => {
                    dedup_idxs.push(idx);
                }
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRangeDelete { .. } => {
                    unreachable!("lifecycle ops must not reach apply_ops_grouped_to_lanes");
                }
            }
        }

        let refcount_shards_arc: Arc<Vec<Arc<crate::refcount::RcShard>>> = Arc::new(
            refcount_shards
                .iter()
                .map(|shard| shard.rc.clone())
                .collect(),
        );
        let mut l2p_sorted: Vec<_> = l2p_buckets.into_iter().collect();
        l2p_sorted.sort_by_key(|((vol, sid), _)| (*vol, *sid));

        let mut l2p_receivers = Vec::with_capacity(l2p_sorted.len());
        for ((vol_ord, sid), indices) in l2p_sorted {
            let volume = volumes
                .get(&vol_ord)
                .expect("volume presence checked during bucketing")
                .clone();
            let apply_volume = volume.clone();
            let apply_ops = ops.clone();
            let refcount_shards_arc = refcount_shards_arc.clone();
            let metrics = metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            volume.shards[sid].apply_lane.enqueue_ready(
                lsn,
                Box::new(move || {
                    let result = Self::apply_l2p_bucket(
                        apply_volume,
                        sid,
                        indices,
                        lsn,
                        apply_ops.as_slice(),
                        refcount_shards_arc.as_slice(),
                        metrics.as_ref(),
                    );
                    let _ = tx.send(result);
                }),
            );
            l2p_receivers.push(rx);
        }

        let mut first_error = None;
        for rx in l2p_receivers {
            match rx.recv() {
                Ok(Ok(result)) => {
                    for (idx, outcome) in result.outcomes {
                        outcomes[idx] = Some(outcome);
                    }
                    for action in result.rc_actions {
                        let sid = shard_for_key(refcount_shards, action.pba);
                        rc_buckets[sid].push(action);
                    }
                }
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(MetaDbError::Corruption(
                            "replay L2P lane worker failed to return a result".into(),
                        ));
                    }
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }

        let mut rc_receivers = Vec::new();
        for (sid, actions) in rc_buckets.into_iter().enumerate() {
            if actions.is_empty() {
                continue;
            }
            let rc = refcount_shards[sid].rc.clone();
            let metrics = metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            refcount_shards[sid].apply_lane.enqueue_ready(
                lsn,
                Box::new(move || {
                    let result = Self::apply_refcount_bucket_to_tree(rc, metrics, actions, lsn);
                    let _ = tx.send(result);
                }),
            );
            rc_receivers.push(rx);
        }

        let mut first_error = None;
        for rx in rc_receivers {
            match rx.recv() {
                Ok(Ok(result)) => {
                    for (idx, new) in result.refcount_outcomes {
                        outcomes[idx] = Some(ApplyOutcome::RefcountNew(new));
                    }
                    for (idx, pba) in result.remap_freed {
                        match outcomes[idx].as_mut() {
                            Some(ApplyOutcome::L2pRemap { freed_pba, .. }) => {
                                *freed_pba = Some(pba);
                            }
                            other => {
                                unreachable!("remap rc action missing L2pRemap outcome: {other:?}")
                            }
                        }
                    }
                }
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(MetaDbError::Corruption(
                            "replay refcount lane worker failed to return a result".into(),
                        ));
                    }
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }

        for (idx, outcome) in Self::apply_dedup_indices_to(
            dedup_index.as_ref(),
            dedup_reverse.as_ref(),
            refcount_shards_arc.as_slice(),
            metrics.as_ref(),
            ops.as_slice(),
            dedup_idxs,
            lsn,
        )? {
            outcomes[idx] = Some(outcome);
        }

        Ok(outcomes
            .into_iter()
            .map(|o| o.expect("every op index filled by exactly one replay lane"))
            .collect())
    }

    fn apply_ops_grouped_to(
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        refcount_shards: &[Shard],
        dedup_index: &Arc<crate::dedup::DedupIndex>,
        dedup_reverse: &Arc<crate::paged_reverse::PagedReverse>,
        metrics: &Arc<MetaMetrics>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..ops.len()).map(|_| None).collect();
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<usize>> = HashMap::new();
        let mut rc_buckets: Vec<Vec<RcApplyAction>> = vec![Vec::new(); refcount_shards.len()];
        let mut dedup_idxs: Vec<usize> = Vec::new();

        for (idx, op) in ops.iter().enumerate() {
            match op {
                WalOp::L2pPut { vol_ord, lba, .. }
                | WalOp::L2pDelete { vol_ord, lba }
                | WalOp::L2pRemap { vol_ord, lba, .. } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!("L2P op for unknown volume ord {vol_ord}"))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets.entry((*vol_ord, sid)).or_default().push(idx);
                }
                WalOp::Incref { pba, .. } | WalOp::Decref { pba, .. } => {
                    let sid = shard_for_key(refcount_shards, *pba);
                    let delta = match op {
                        WalOp::Incref { delta, .. } => i64::from(*delta),
                        WalOp::Decref { delta, .. } => -i64::from(*delta),
                        _ => unreachable!(),
                    };
                    rc_buckets[sid].push(RcApplyAction {
                        op_idx: idx,
                        pba: *pba,
                        delta,
                        standalone_refcount: true,
                        remap_freed_candidate: false,
                    });
                }
                WalOp::DedupPut { .. }
                | WalOp::DedupPutGuarded { .. }
                | WalOp::DedupDelete { .. }
                | WalOp::DedupCompareDelete { .. }
                | WalOp::DedupComparePut { .. }
                | WalOp::DedupReversePut { .. }
                | WalOp::DedupReverseDelete { .. } => {
                    dedup_idxs.push(idx);
                }
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRangeDelete { .. } => {
                    unreachable!("lifecycle ops must not reach apply_ops_grouped_to");
                }
            }
        }

        let refcount_shards_vec: Vec<Arc<crate::refcount::RcShard>> = refcount_shards
            .iter()
            .map(|shard| shard.rc.clone())
            .collect();
        let mut l2p_sorted: Vec<_> = l2p_buckets.into_iter().collect();
        l2p_sorted.sort_by_key(|((vol, sid), _)| (*vol, *sid));
        for ((vol_ord, sid), indices) in l2p_sorted {
            let volume = volumes
                .get(&vol_ord)
                .expect("volume presence checked during bucketing")
                .clone();
            let result = Self::apply_l2p_bucket(
                volume,
                sid,
                indices,
                lsn,
                ops,
                &refcount_shards_vec,
                metrics.as_ref(),
            )?;
            for (idx, outcome) in result.outcomes {
                outcomes[idx] = Some(outcome);
            }
            for action in result.rc_actions {
                let sid = shard_for_key(refcount_shards, action.pba);
                rc_buckets[sid].push(action);
            }
        }

        for (sid, actions) in rc_buckets.into_iter().enumerate() {
            if actions.is_empty() {
                continue;
            }
            let result = Self::apply_refcount_bucket_to_tree(
                refcount_shards[sid].rc.clone(),
                metrics.clone(),
                actions,
                lsn,
            )?;
            for (idx, new) in result.refcount_outcomes {
                outcomes[idx] = Some(ApplyOutcome::RefcountNew(new));
            }
            for (idx, pba) in result.remap_freed {
                match outcomes[idx].as_mut() {
                    Some(ApplyOutcome::L2pRemap { freed_pba, .. }) => {
                        *freed_pba = Some(pba);
                    }
                    other => {
                        unreachable!("remap rc action missing L2pRemap outcome: {other:?}")
                    }
                }
            }
        }

        for (idx, outcome) in Self::apply_dedup_indices_to(
            dedup_index.as_ref(),
            dedup_reverse.as_ref(),
            refcount_shards_vec.as_slice(),
            metrics.as_ref(),
            ops,
            dedup_idxs,
            lsn,
        )? {
            outcomes[idx] = Some(outcome);
        }

        Ok(outcomes
            .into_iter()
            .map(|o| o.expect("every op index filled by exactly one bucket"))
            .collect())
    }

    /// Bucketed batch-apply. Only invoked for sufficiently large
    /// batches composed entirely of bucketable ops (plain L2P,
    /// unguarded/no-snapshot remap, refcount, dedup). See
    /// [`apply_commit_batch`] for the dispatch rule.
    fn apply_ops_grouped(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        Self::apply_ops_grouped_to(
            volumes,
            &self.refcount_shards,
            &self.dedup_index,
            &self.dedup_reverse,
            &self.metrics,
            lsn,
            ops,
        )
    }
}

fn replay_batch_requires_serial_apply(
    ops: &[WalOp],
    snap_info_for_vol: &dyn Fn(VolumeOrdinal) -> Vec<SnapInfo>,
) -> bool {
    if batch_contains_lifecycle_op(ops) {
        return true;
    }

    let mut remap_vols = HashSet::new();
    for op in ops {
        if let WalOp::L2pRemap { vol_ord, .. } = op {
            remap_vols.insert(*vol_ord);
        }
    }
    remap_vols
        .into_iter()
        .any(|vol_ord| !snap_info_for_vol(vol_ord).is_empty())
}

/// Record per-op-type apply latency for the serial fallback path.
/// Lifecycle ops (DropSnapshot / CreateVolume / DropVolume / CloneVolume) are
/// rare and self-instrument elsewhere, so they're skipped here.
fn record_per_op_apply(metrics: &MetaMetrics, op: &WalOp, elapsed: std::time::Duration) {
    match op {
        WalOp::L2pPut { .. } => metrics.record_apply_l2p_put(elapsed),
        WalOp::L2pDelete { .. } => metrics.record_apply_l2p_delete(elapsed),
        WalOp::L2pRemap { .. } => metrics.record_apply_l2p_remap(elapsed),
        WalOp::L2pRangeDelete { .. } => metrics.record_apply_l2p_range_delete(elapsed),
        WalOp::Incref { .. } | WalOp::Decref { .. } => metrics.record_apply_refcount(elapsed),
        WalOp::DedupPut { .. }
        | WalOp::DedupPutGuarded { .. }
        | WalOp::DedupDelete { .. }
        | WalOp::DedupCompareDelete { .. }
        | WalOp::DedupComparePut { .. }
        | WalOp::DedupReversePut { .. }
        | WalOp::DedupReverseDelete { .. } => metrics.record_apply_dedup(elapsed),
        WalOp::DropSnapshot { .. }
        | WalOp::CreateVolume { .. }
        | WalOp::DropVolume { .. }
        | WalOp::CloneVolume { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn footprint(lanes: impl IntoIterator<Item = DispatchLaneKey>) -> DispatchFootprint {
        DispatchFootprint {
            global: false,
            lanes: lanes.into_iter().collect(),
        }
    }

    fn entry(footprint: DispatchFootprint, durable: bool) -> DispatchEntry {
        DispatchEntry { footprint, durable }
    }

    #[test]
    fn dispatch_scheduler_allows_disjoint_higher_lsn_to_bypass() {
        let mut state = DispatchState::default();
        state
            .pending
            .insert(10, entry(footprint([DispatchLaneKey::L2p(0, 0)]), false));
        state
            .pending
            .insert(11, entry(footprint([DispatchLaneKey::L2p(0, 1)]), true));

        assert!(
            dispatch_ready(&state, 11),
            "lower undurable work on another shard must not block dispatch"
        );
    }

    #[test]
    fn dispatch_scheduler_blocks_conflicting_higher_lsn() {
        let mut state = DispatchState::default();
        state
            .pending
            .insert(10, entry(footprint([DispatchLaneKey::Refcount(2)]), false));
        state
            .pending
            .insert(11, entry(footprint([DispatchLaneKey::Refcount(2)]), true));

        assert!(
            !dispatch_ready(&state, 11),
            "same lane must preserve WAL LSN dispatch order"
        );
        state.pending.remove(&10);
        assert!(dispatch_ready(&state, 11));
    }

    #[test]
    fn dispatch_scheduler_treats_global_as_conflicting_with_every_lane() {
        let mut state = DispatchState::default();
        state
            .pending
            .insert(10, entry(DispatchFootprint::global(), true));
        state
            .pending
            .insert(11, entry(footprint([DispatchLaneKey::Dedup(0)]), true));

        assert!(
            !dispatch_ready(&state, 11),
            "global serial work must retain the old FIFO barrier"
        );
    }

    #[test]
    fn small_remap_batches_use_lane_dispatch_not_global_serial() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Db::create(dir.path()).unwrap();
        let mut raw = [0u8; 28];
        raw[..8].copy_from_slice(&123_u64.to_be_bytes());

        let ops = [WalOp::L2pRemap {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 7,
            new_value: L2pValue(raw),
            guard: None,
        }];

        assert!(
            !db.batch_uses_serial_apply(&ops),
            "tiny remap commits must keep precise L2P/refcount footprints so they do not block dedup dispatch globally"
        );
        let plan = db
            .build_lane_dispatch_plan(&db.volumes.read().clone(), &ops)
            .unwrap();
        let footprint = DispatchFootprint::from_lane_plan(&plan);
        assert!(!footprint.global);
        assert!(
            footprint
                .lanes
                .iter()
                .any(|lane| matches!(lane, DispatchLaneKey::L2p(BOOTSTRAP_VOLUME_ORD, _)))
        );
        assert!(
            !footprint
                .lanes
                .iter()
                .any(|lane| matches!(lane, DispatchLaneKey::Dedup(_))),
            "remap-only commits should not conflict with dedup shards"
        );
    }
}
