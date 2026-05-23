use super::*;

mod dedup;
mod l2p;
mod refcount;
mod replay;

impl Db {
    pub(super) fn build_lane_dispatch_plan(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        ops: &[WalOp],
    ) -> Result<LaneDispatchPlan> {
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<L2pBucketEntry>> = HashMap::new();
        let rc_buckets: Vec<Vec<RcApplyAction>> = (0..self.refcount_shards.len())
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
                    l2p_buckets
                        .entry((*vol_ord, sid))
                        .or_default()
                        .push(L2pBucketEntry::Single(idx));
                }
                WalOp::L2pRemap { vol_ord, lba, .. } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!(
                            "L2pRemap for unknown volume ord {vol_ord}"
                        ))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets
                        .entry((*vol_ord, sid))
                        .or_default()
                        .push(L2pBucketEntry::Single(idx));
                    remap_may_defer_refcount = true;
                }
                WalOp::L2pRemapRange {
                    vol_ord,
                    start_lba,
                    values,
                } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!(
                            "L2pRemapRange for unknown volume ord {vol_ord}"
                        ))
                    })?;
                    // Count one range op now; per-shard LBA contribution
                    // (lbas + us) is recorded inside apply_l2p_bucket via
                    // record_apply_l2p_remap_range_lane_work.
                    self.metrics.record_apply_l2p_remap_range_lane_ops(1);
                    // Group LBA offsets by L2P shard. Typical
                    // passthrough range (32 contiguous LBAs aligned on
                    // a 128-LBA leaf) lands in one shard; mis-aligned
                    // ranges land in 1-2 shards. Each shard's slice
                    // becomes one `RangeSlice` bucket entry sharing
                    // the same `op_idx` — the outcomes merger combines
                    // the partial `ApplyOutcome::L2pRemapRange`s.
                    let mut per_shard: HashMap<usize, Vec<u32>> = HashMap::new();
                    for off in 0..values.len() {
                        let lba = start_lba + off as u64;
                        let sid = shard_for_key_l2p(&volume.shards, lba);
                        per_shard.entry(sid).or_default().push(off as u32);
                    }
                    for (sid, offsets) in per_shard {
                        l2p_buckets.entry((*vol_ord, sid)).or_default().push(
                            L2pBucketEntry::RangeSlice {
                                op_idx: idx,
                                lba_offsets: offsets.into_boxed_slice(),
                            },
                        );
                    }
                    remap_may_defer_refcount = true;
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
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRangeDelete { .. }
                | WalOp::FreePbas { .. }
                | WalOp::PromotionChunk { .. }
                | WalOp::PromotionComplete { .. } => {
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

    pub(super) fn enqueue_lane_plan(
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
            dedup_buckets: plan.dedup_buckets,
            dedup_pendings,
        }
    }

    pub(super) fn apply_ops_laned(
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
                        merge_l2p_outcome(&mut outcomes, idx, outcome);
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
        for sid in 0..plan.rc_buckets.len() {
            let actions = std::mem::take(&mut plan.rc_buckets[sid]);
            if actions.is_empty() {
                continue;
            }
            let rc = self.refcount_shards[sid].rc.clone();
            let metrics = self.metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            self.refcount_shards[sid].apply_lane.enqueue_ready(
                lsn,
                Box::new(move || {
                    let result = Self::apply_refcount_bucket_to_tree(rc, metrics, actions, lsn);
                    let _ = tx.send(result);
                }),
            );
            rc_receivers.push(rx);
        }
        timing.rc_enqueue = rc_enqueue_started.elapsed();
        self.complete_retained_dispatch(lsn);

        let mut first_error = None;
        let rc_wait_started = std::time::Instant::now();
        for rx in rc_receivers {
            match rx.recv() {
                Ok(Ok(result)) => {
                    for (idx, new) in result.refcount_outcomes {
                        outcomes[idx] = Some(ApplyOutcome::RefcountNew(new));
                    }
                    for (idx, pba, range_op) in result.remap_freed {
                        match (outcomes[idx].as_mut(), range_op) {
                            (Some(ApplyOutcome::L2pRemap { freed_pba, .. }), false) => {
                                *freed_pba = Some(pba);
                            }
                            (Some(ApplyOutcome::L2pRemapRange { freed_pbas, .. }), true) => {
                                freed_pbas.push(pba);
                            }
                            (other, range_op) => {
                                unreachable!(
                                    "remap rc action missing matching outcome: range_op={range_op} \
                                     outcome={other:?}"
                                )
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
        // gets its own apply closure on its shard's lane.
        //
        // The async path is valid only when:
        // 1. The commit carries a non-dedup op (L2P / refcount /
        //    volume lifecycle) whose outcome the caller is already
        //    blocked on. Pure-dedup commits — `db.put_dedup` /
        //    `db.delete_dedup` / `tx` with only dedup ops — must
        //    synchronise so the caller's follow-up `get_dedup`
        //    observes the put.
        // 2. No `DedupCompare*` ops are present — their `applied`
        //    flag is consumed by the caller (e.g. cleanup CAS path).
        //
        // When async-safe, hot-path callers only inspect `L2pRemap` /
        // `RefcountNew` outcome slots earlier in the vec; the
        // `ApplyOutcome::Dedup` marker for the async dedup ops is a
        // placeholder so the slot is filled.
        let has_non_dedup_op = plan.ops.iter().any(|op| {
            !matches!(
                op,
                WalOp::DedupPut { .. }
                    | WalOp::DedupPutGuarded { .. }
                    | WalOp::DedupDelete { .. }
                    | WalOp::DedupCompareDelete { .. }
                    | WalOp::DedupComparePut { .. }
            )
        });
        let no_sync_required_dedup_ops = plan.ops.iter().all(|op| {
            !matches!(
                op,
                WalOp::DedupCompareDelete { .. } | WalOp::DedupComparePut { .. }
            )
        });
        let dedup_async_safe = has_non_dedup_op && no_sync_required_dedup_ops;

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
            let refcount_shards_arc = refcount_shards_arc.clone();
            let metrics = self.metrics.clone();
            if dedup_async_safe {
                pending.set(Box::new(move || {
                    let ready_queue_wait = ready_at.elapsed();
                    let exec_started = std::time::Instant::now();
                    let _outcomes = Self::apply_dedup_indices_to(
                        dedup_index.as_ref(),
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
                }));
            } else {
                let (tx, rx) = crossbeam_channel::bounded(1);
                pending.set(Box::new(move || {
                    let ready_queue_wait = ready_at.elapsed();
                    let exec_started = std::time::Instant::now();
                    let outcomes = Self::apply_dedup_indices_to(
                        dedup_index.as_ref(),
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
        }
        timing.dedup_enqueue = dedup_enqueue_started.elapsed();
        let dedup_wait_started = std::time::Instant::now();
        if dedup_async_safe {
            for (idx, op) in plan.ops.iter().enumerate() {
                if outcomes[idx].is_none()
                    && matches!(
                        op,
                        WalOp::DedupPut { .. }
                            | WalOp::DedupPutGuarded { .. }
                            | WalOp::DedupDelete { .. }
                    )
                {
                    outcomes[idx] = Some(ApplyOutcome::Dedup);
                }
            }
        } else {
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
}
