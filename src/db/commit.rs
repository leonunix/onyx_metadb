use super::*;

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
    dedup_idxs: Vec<usize>,
}

struct QueuedLanePlan {
    ops: Arc<Vec<WalOp>>,
    l2p_receivers: Vec<crossbeam_channel::Receiver<Result<L2pBucketApplyResult>>>,
    rc_buckets: Vec<Vec<RcApplyAction>>,
    rc_pending: Vec<Option<PendingApplyWork>>,
    dedup_idxs: Vec<usize>,
    dedup_pending: Option<PendingApplyWork>,
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
        if !plan.dedup_idxs.is_empty() {
            lanes.insert(DispatchLaneKey::Dedup);
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
        WalOp::DedupPut { hash, .. } | WalOp::DedupDelete { hash } => xxh3_64(hash),
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
        self.metrics.record_commit_attempt(ops.len());
        // `drop_gate.read()` pairs with lifecycle paths' write acquire.
        // Hold it across submit + apply so `drop_snapshot` /
        // `range_delete` cannot wedge themselves between our LSN
        // assignment and apply. Using the read side is important:
        // ordinary commits must still submit concurrently so the WAL
        // writer can coalesce them into group commits.
        let drop_gate_started = std::time::Instant::now();
        let _drop_guard = self.drop_gate.read();
        self.metrics
            .record_commit_drop_gate_wait(drop_gate_started.elapsed());
        // Plan the lane footprint before LSN allocation. `submit_wal_ops`
        // registers this footprint while the WAL set still holds its
        // allocator mutex, so every lower LSN's footprint is known before
        // a higher LSN can be assigned.
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
        let dispatch_footprint = plan
            .as_ref()
            .map(DispatchFootprint::from_lane_plan)
            .unwrap_or_else(DispatchFootprint::global);

        let body = encode_body(ops);
        let wal_started = std::time::Instant::now();
        let lsn = match self.submit_wal_ops(ops, body, Some(dispatch_footprint)) {
            Ok(lsn) => {
                self.metrics.record_commit_wal_submit(wal_started.elapsed());
                lsn
            }
            Err(err) => {
                self.metrics.record_commit_wal_submit(wal_started.elapsed());
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
            self.metrics
                .record_commit_apply_wait(wait_started.elapsed());
            self.metrics.record_commit_error(commit_started.elapsed());
            return Err(err);
        }
        self.metrics
            .record_commit_apply_wait(wait_started.elapsed());

        let apply_gate_started = std::time::Instant::now();
        let apply_guard = self.apply_gate.read();
        self.metrics
            .record_commit_apply_gate_wait(apply_gate_started.elapsed());

        let apply_started = std::time::Instant::now();
        let outcomes = if let Some(plan) = plan {
            let queued_plan = self.enqueue_lane_plan(&volumes, lsn, plan, Arc::new(ops.to_vec()));
            self.complete_retained_dispatch(lsn);
            match self.apply_ops_laned(lsn, ops.len(), queued_plan) {
                Ok(outcomes) => {
                    self.metrics.record_commit_apply(apply_started.elapsed());
                    outcomes
                }
                Err(err) => {
                    self.metrics.record_commit_apply(apply_started.elapsed());
                    self.metrics.record_commit_error(commit_started.elapsed());
                    self.poison_commit_waiters(&err);
                    return Err(err);
                }
            }
        } else {
            if let Err(err) = self.wait_for_global_apply_turn(lsn) {
                self.metrics.record_commit_apply(apply_started.elapsed());
                self.metrics.record_commit_error(commit_started.elapsed());
                return Err(err);
            }
            match self.apply_commit_batch(&volumes, lsn, ops) {
                Ok(outcomes) => {
                    self.metrics.record_commit_apply(apply_started.elapsed());
                    outcomes
                }
                Err(err) => {
                    self.metrics.record_commit_apply(apply_started.elapsed());
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
        if let Err(err) = self.finish_global_apply(lsn) {
            self.metrics.record_commit_error(commit_started.elapsed());
            return Err(err);
        }
        if serial_apply {
            self.complete_retained_dispatch(lsn);
        }
        drop(apply_guard);
        self.metrics.record_commit_success(commit_started.elapsed());
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
        const SMALL_REMAP_LANE_THRESHOLD: usize = 8;
        if self.batch_requires_serial_apply(ops) {
            return true;
        }
        // Unguarded remaps can discover an old_pba on any refcount shard
        // after the L2P phase, so the lane path reserves every refcount lane
        // as a correctness placeholder. That is profitable for big flusher
        // batches, but far too much machinery for one-off diagnostic writes.
        ops.len() < SMALL_REMAP_LANE_THRESHOLD
            && ops.iter().any(|op| matches!(op, WalOp::L2pRemap { .. }))
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
        let mut dedup_idxs = Vec::new();
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
                WalOp::DedupPut { .. }
                | WalOp::DedupDelete { .. }
                | WalOp::DedupReversePut { .. }
                | WalOp::DedupReverseDelete { .. } => dedup_idxs.push(idx),
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
            dedup_idxs,
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
        // Snapshot refcount tree handles once per commit so the per-lane
        // closures can do guarded-remap rc lookups (dedup hits) without
        // touching the Db struct from the worker thread.
        let refcount_trees: Arc<Vec<Arc<Mutex<BTree>>>> = Arc::new(
            self.refcount_shards
                .iter()
                .map(|s| s.tree.clone())
                .collect(),
        );
        for ((vol_ord, sid), indices) in plan.l2p_sorted {
            let volume = volumes
                .get(&vol_ord)
                .expect("volume presence checked during lane planning");
            let apply_volume = volume.clone();
            let apply_ops = ops.clone();
            let metrics = self.metrics.clone();
            let refcount_trees = refcount_trees.clone();
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
                        refcount_trees.as_slice(),
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
        let dedup_pending = if plan.dedup_idxs.is_empty() {
            None
        } else {
            Some(self.dedup_lane.enqueue_pending(lsn))
        };
        QueuedLanePlan {
            ops,
            l2p_receivers,
            rc_buckets: plan.rc_buckets,
            rc_pending,
            dedup_idxs: plan.dedup_idxs,
            dedup_pending,
        }
    }

    fn apply_l2p_bucket(
        volume: Arc<Volume>,
        sid: usize,
        indices: Vec<usize>,
        lsn: Lsn,
        ops: &[WalOp],
        refcount_trees: &[Arc<Mutex<BTree>>],
        metrics: &MetaMetrics,
    ) -> Result<L2pBucketApplyResult> {
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut rc_actions = Vec::new();
        let mut tree = volume.shards[sid].tree.write();
        for idx in indices {
            let op_started = std::time::Instant::now();
            let outcome = match &ops[idx] {
                WalOp::L2pPut { lba, value, .. } => {
                    let prev = tree.insert_at_lsn(*lba, *value, lsn)?;
                    metrics.record_apply_l2p_put(op_started.elapsed());
                    ApplyOutcome::L2pPrev(prev)
                }
                WalOp::L2pDelete { lba, .. } => {
                    let prev = tree.delete_at_lsn(*lba, lsn)?;
                    metrics.record_apply_l2p_delete(op_started.elapsed());
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
                        let gp_sid = (xxh3_64(&gp.to_be_bytes()) as usize)
                            % refcount_trees.len();
                        let cur = {
                            let mut rc_tree = refcount_trees[gp_sid].lock();
                            rc_tree.get(*gp)?.map(|e| e.rc).unwrap_or(0)
                        };
                        if cur < *min_rc {
                            metrics.record_apply_l2p_remap(op_started.elapsed());
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
                    let prev = tree.insert_at_lsn(*lba, *new_value, lsn)?;
                    let value_changed = prev != Some(*new_value);
                    if value_changed {
                        let new_pba = new_value.head_pba();
                        match prev {
                            Some(old_value) => {
                                let old_pba = old_value.head_pba();
                                if old_pba != new_pba {
                                    rc_actions.push(RcApplyAction {
                                        op_idx: idx,
                                        pba: old_pba,
                                        delta: -1,
                                        standalone_refcount: false,
                                        remap_freed_candidate: true,
                                    });
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
                    metrics.record_apply_l2p_remap(op_started.elapsed());
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
        super::apply::publish_l2p_read_view(&volume.shards[sid], &tree);
        Ok(L2pBucketApplyResult {
            outcomes,
            rc_actions,
        })
    }

    fn apply_l2p_buckets(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
        l2p_sorted: Vec<((VolumeOrdinal, usize), Vec<usize>)>,
    ) -> Result<Vec<L2pBucketApplyResult>> {
        let metrics = self.metrics.as_ref();
        let refcount_trees: Vec<Arc<Mutex<BTree>>> = self
            .refcount_shards
            .iter()
            .map(|s| s.tree.clone())
            .collect();
        let mut results = Vec::with_capacity(l2p_sorted.len());
        for ((vol_ord, sid), indices) in l2p_sorted {
            let volume = volumes
                .get(&vol_ord)
                .expect("volume presence checked during bucketing")
                .clone();
            results.push(Self::apply_l2p_bucket(
                volume,
                sid,
                indices,
                lsn,
                ops,
                &refcount_trees,
                metrics,
            )?);
        }
        Ok(results)
    }

    fn apply_refcount_bucket(
        &self,
        sid: usize,
        actions: Vec<RcApplyAction>,
        lsn: Lsn,
    ) -> Result<RcBucketApplyResult> {
        Self::apply_refcount_bucket_to_tree(
            self.refcount_shards[sid].tree.clone(),
            self.metrics.clone(),
            actions,
            lsn,
        )
    }

    fn apply_refcount_bucket_to_tree(
        tree: Arc<Mutex<BTree>>,
        metrics: Arc<MetaMetrics>,
        mut actions: Vec<RcApplyAction>,
        lsn: Lsn,
    ) -> Result<RcBucketApplyResult> {
        let mut result = RcBucketApplyResult::default();
        if actions.is_empty() {
            return Ok(result);
        }
        actions.sort_by_key(|action| action.op_idx);
        let mut tree = tree.lock();
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
                let pre = if group.iter().any(|action| action.remap_freed_candidate) {
                    tree.get(pba)?.map(|e| e.rc).unwrap_or(0)
                } else {
                    0
                };
                let op_started = std::time::Instant::now();
                let new = refcount_apply_delta(&mut tree, pba, delta, lsn)?;
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
                let pre = if action.remap_freed_candidate {
                    tree.get(action.pba)?.map(|e| e.rc).unwrap_or(0)
                } else {
                    0
                };
                let op_started = std::time::Instant::now();
                let new = refcount_apply_delta(&mut tree, action.pba, action.delta, lsn)?;
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

    fn apply_refcount_buckets(
        &self,
        rc_buckets: Vec<Vec<RcApplyAction>>,
        lsn: Lsn,
        _op_count: usize,
    ) -> Result<Vec<RcBucketApplyResult>> {
        let non_empty = rc_buckets
            .iter()
            .filter(|actions| !actions.is_empty())
            .count();
        let mut results = Vec::with_capacity(non_empty);
        for (sid, actions) in rc_buckets.into_iter().enumerate() {
            if actions.is_empty() {
                continue;
            }
            results.push(self.apply_refcount_bucket(sid, actions, lsn)?);
        }
        Ok(results)
    }

    fn apply_dedup_indices(
        &self,
        ops: &[WalOp],
        indices: Vec<usize>,
    ) -> Vec<(usize, ApplyOutcome)> {
        Self::apply_dedup_indices_to(
            self.dedup_index.as_ref(),
            self.dedup_reverse.as_ref(),
            self.metrics.as_ref(),
            ops,
            indices,
        )
    }

    fn apply_dedup_indices_to(
        dedup_index: &Lsm,
        dedup_reverse: &Lsm,
        metrics: &MetaMetrics,
        ops: &[WalOp],
        indices: Vec<usize>,
    ) -> Vec<(usize, ApplyOutcome)> {
        let mut outcomes = Vec::with_capacity(indices.len());
        for idx in indices {
            let op_started = std::time::Instant::now();
            let outcome = match &ops[idx] {
                WalOp::DedupPut { hash, value } => {
                    dedup_index.put(*hash, *value);
                    ApplyOutcome::Dedup
                }
                WalOp::DedupDelete { hash } => {
                    dedup_index.delete(*hash);
                    ApplyOutcome::Dedup
                }
                WalOp::DedupReversePut { pba, hash } => {
                    let (key, value) = encode_reverse_entry(*pba, hash);
                    dedup_reverse.put(key, value);
                    ApplyOutcome::Dedup
                }
                WalOp::DedupReverseDelete { pba, hash } => {
                    let (key, _) = encode_reverse_entry(*pba, hash);
                    dedup_reverse.delete(key);
                    ApplyOutcome::Dedup
                }
                other => unreachable!("dedup bucket holds only dedup ops; saw {other:?}"),
            };
            metrics.record_apply_dedup(op_started.elapsed());
            outcomes.push((idx, outcome));
        }
        outcomes
    }

    fn apply_ops_laned(
        &self,
        lsn: Lsn,
        op_count: usize,
        mut plan: QueuedLanePlan,
    ) -> Result<Vec<ApplyOutcome>> {
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..op_count).map(|_| None).collect();
        let mut first_error = None;

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
        if let Some(err) = first_error {
            return Err(err);
        }

        let mut rc_receivers = Vec::new();
        for sid in 0..plan.rc_pending.len() {
            let Some(pending) = plan.rc_pending[sid].take() else {
                continue;
            };
            let actions = std::mem::take(&mut plan.rc_buckets[sid]);
            let tree = self.refcount_shards[sid].tree.clone();
            let metrics = self.metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            pending.set(Box::new(move || {
                let result = Self::apply_refcount_bucket_to_tree(tree, metrics, actions, lsn);
                let _ = tx.send(result);
            }));
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
                            "persistent refcount lane worker failed to return a result".into(),
                        ));
                    }
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }

        if let Some(pending) = plan.dedup_pending.take() {
            let ops = plan.ops.clone();
            let indices = std::mem::take(&mut plan.dedup_idxs);
            let dedup_index = self.dedup_index.clone();
            let dedup_reverse = self.dedup_reverse.clone();
            let metrics = self.metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            pending.set(Box::new(move || {
                let outcomes = Self::apply_dedup_indices_to(
                    dedup_index.as_ref(),
                    dedup_reverse.as_ref(),
                    metrics.as_ref(),
                    ops.as_slice(),
                    indices,
                );
                let _ = tx.send(outcomes);
            }));
            let dedup_outcomes = rx.recv().map_err(|_| {
                MetaDbError::Corruption(
                    "persistent dedup lane worker failed to return a result".into(),
                )
            })?;
            for (idx, outcome) in dedup_outcomes {
                outcomes[idx] = Some(outcome);
            }
        }

        Ok(outcomes
            .into_iter()
            .map(|o| o.expect("every op index filled by exactly one lane"))
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
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..ops.len()).map(|_| None).collect();
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<usize>> = HashMap::new();
        let mut rc_buckets: Vec<Vec<RcApplyAction>> = vec![Vec::new(); self.refcount_shards.len()];
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
                WalOp::DedupPut { .. }
                | WalOp::DedupDelete { .. }
                | WalOp::DedupReversePut { .. }
                | WalOp::DedupReverseDelete { .. } => {
                    dedup_idxs.push(idx);
                }
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRangeDelete { .. } => {
                    // Already filtered out by `batch_contains_lifecycle_op`.
                    unreachable!("lifecycle ops must not reach apply_ops_grouped");
                }
            }
        }

        // Apply L2P buckets in deterministic bucket order for small
        // commits, and in parallel for large commits. Buckets target
        // disjoint per-volume shards; any refcount side effects are
        // collected and applied after all L2P buckets finish.
        let mut l2p_sorted: Vec<_> = l2p_buckets.into_iter().collect();
        l2p_sorted.sort_by_key(|((vol, sid), _)| (*vol, *sid));
        for result in self.apply_l2p_buckets(volumes, lsn, ops, l2p_sorted)? {
            for (idx, outcome) in result.outcomes {
                outcomes[idx] = Some(outcome);
            }
            for action in result.rc_actions {
                let sid = shard_for_key(&self.refcount_shards, action.pba);
                rc_buckets[sid].push(action);
            }
        }

        // Apply refcount buckets. Each bucket owns one refcount shard,
        // so large remap batches can parallelise this phase too.
        for result in self.apply_refcount_buckets(rc_buckets, lsn, ops.len())? {
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

        // Dedup ops route through the LSM's own synchronisation; no
        // shard lock needed. Apply in original order — LSM puts on the
        // same key are last-write-wins, matching the serial path.
        for (idx, outcome) in self.apply_dedup_indices(ops, dedup_idxs) {
            outcomes[idx] = Some(outcome);
        }

        Ok(outcomes
            .into_iter()
            .map(|o| o.expect("every op index filled by exactly one bucket"))
            .collect())
    }
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
        | WalOp::DedupDelete { .. }
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
            .insert(11, entry(footprint([DispatchLaneKey::Dedup]), true));

        assert!(
            !dispatch_ready(&state, 11),
            "global serial work must retain the old FIFO barrier"
        );
    }
}
