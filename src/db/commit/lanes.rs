use super::*;

mod dedup;
mod l2p;
mod refcount;
mod replay;

/// Hash a PBA to a refcount shard index. Mirrors the routing math used
/// by `shard_for_key` in `apply.rs` and by the dedup apply lane's
/// inlined `rc_shard_for` closure. The planner and the apply lanes
/// MUST agree on this routing — divergence is a deadlock / underflow
/// hazard (planner says shard X is in the footprint, apply touches
/// shard Y → concurrent commit on Y races).
#[inline]
pub(super) fn rc_shard_of_pba(pba: Pba, num_shards: usize) -> usize {
    (xxh3_64(&pba.to_be_bytes()) as usize) % num_shards
}

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
        // Precise refcount footprint. The pre-planner used a
        // blanket `rc_enqueued.fill(true)` on any remap because the
        // hot-path L2pRemap drove rc deltas itself — that blanket made
        // every commit's footprint claim all 16 refcount lanes,
        // serializing every commit against every other commit on a
        // single volume regardless of which L2P shard each touched.
        //
        // made L2pRemap/L2pRemapRange
        // rc-neutral on the hot path — the rc-staging now rides
        // exclusively on the dedup ops (DedupPut/Guarded/Delete/Compare*)
        // via `lanes/dedup.rs::stage_rc{,_decref_if_live}`. So the
        // planner can compute the precise rc-shard set by walking those
        // ops and any guarded L2pRemap (which still READS rc[gp] inline
        // in `apply_l2p_bucket`; cross-LSN consistency requires footprint
        // claim).
        //
        // If a future op type adds rc-staging without updating this
        // walk, the consequence is: the planner says shard X is NOT in
        // the footprint, but the lane stages rc[Y] anyway. A concurrent
        // commit also touching rc[Y] races → underflow on the
        // non-atomic read-then-stage in `stage_rc_decref_if_live`.
        let num_rc_shards = self.refcount_shards.len();
        let mut rc_shards_touched: Vec<bool> = vec![false; num_rc_shards];
        let mut mark_rc = |pba: Pba, rc_shards_touched: &mut Vec<bool>| {
            rc_shards_touched[rc_shard_of_pba(pba, num_rc_shards)] = true;
        };
        // rc-authoritative: every applied L2P install increfs its new
        // head_pba AND decrefs the OLD head_pba it replaced (traditional,
        // inline, paired 1:1). The incref's pba is known here (`new_value`),
        // but the DECREF's pba is the *previous* mapping — discovered only at
        // apply time (`tree.get`), so the planner cannot name its rc shard.
        // To keep the dispatch footprint a superset of every rc shard the
        // commit will stage (a footprint/emit mismatch races the per-shard
        // apply → underflow), blanket-claim ALL rc shards when rc_auth. This
        // is the pre-behaviour (`rc_enqueued.fill(true)` on any
        // remap); it serializes commits on the rc lanes. The inline-rc cost
        // (this serialization + the per-LBA stages) is the explicit target of
        // the commit-perf optimization step — correctness first.
        let rc_auth = self.rc_authoritative_reclaim;

        for (idx, op) in ops.iter().enumerate() {
            match op {
                WalOp::L2pPut { vol_ord, lba, .. } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!("L2P op for unknown volume ord {vol_ord}"))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets
                        .entry((*vol_ord, sid))
                        .or_default()
                        .push(L2pBucketEntry::Single(idx));
                    // rc-authoritative incref+decref rc shards are blanket-
                    // claimed after this loop (decref pba unknown here).
                }
                WalOp::L2pDelete { vol_ord, lba } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!("L2P op for unknown volume ord {vol_ord}"))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets
                        .entry((*vol_ord, sid))
                        .or_default()
                        .push(L2pBucketEntry::Single(idx));
                    // Delete removes a reference; the decref rides the deadlist
                    // (record_dead in apply), NOT this commit's rc lane.
                }
                WalOp::L2pRemap {
                    vol_ord,
                    lba,
                    new_value: _,
                    guard,
                } => {
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
                    // Guarded L2pRemap reads rc[guard.0] inline in
                    // `apply_l2p_bucket` (lanes/l2p.rs:~280, ~540).
                    if let Some((gp, _)) = guard {
                        mark_rc(*gp, &mut rc_shards_touched);
                    }
                    // rc-authoritative incref(new)+decref(old) rc shards are
                    // blanket-claimed after this loop (decref pba unknown here).
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
                    // rc-authoritative incref+decref rc shards are blanket-
                    // claimed after this loop (decref pbas unknown here).
                }
                WalOp::DedupPut {
                    hash,
                    value,
                    old_pba,
                } => {
                    let sid =
                        crate::dedup_types::shard_for_hash(hash, dedup_shard_count_u32) as usize;
                    dedup_buckets[sid].push(idx);
                    // Apply stages: decref(old_pba) if Some + incref(new_pba)
                    // (lanes/dedup.rs:75-84). Pessimistically claim both
                    // even though the apply skips the decref/incref pair
                    // when old_pba == new_pba — that runtime decision
                    // must not change the dispatch footprint.
                    mark_rc(value.head_pba(), &mut rc_shards_touched);
                    if let Some(op) = old_pba {
                        mark_rc(*op, &mut rc_shards_touched);
                    }
                }
                WalOp::DedupPutGuarded {
                    hash,
                    value,
                    pba_guard,
                    old_pba,
                    ..
                } => {
                    let sid =
                        crate::dedup_types::shard_for_hash(hash, dedup_shard_count_u32) as usize;
                    dedup_buckets[sid].push(idx);
                    // Apply reads rc[pba_guard] then conditionally stages
                    // decref(old_pba) + incref(new_pba) (lanes/dedup.rs:97-123).
                    mark_rc(*pba_guard, &mut rc_shards_touched);
                    mark_rc(value.head_pba(), &mut rc_shards_touched);
                    if let Some(op) = old_pba {
                        mark_rc(*op, &mut rc_shards_touched);
                    }
                }
                WalOp::DedupDelete { hash, old_pba } => {
                    let sid =
                        crate::dedup_types::shard_for_hash(hash, dedup_shard_count_u32) as usize;
                    dedup_buckets[sid].push(idx);
                    // Apply stages decref(old_pba) if Some (lanes/dedup.rs:127-135).
                    if let Some(op) = old_pba {
                        mark_rc(*op, &mut rc_shards_touched);
                    }
                }
                WalOp::DedupCompareDelete { hash, old_value } => {
                    let sid =
                        crate::dedup_types::shard_for_hash(hash, dedup_shard_count_u32) as usize;
                    dedup_buckets[sid].push(idx);
                    // Apply stages decref(old_value.head_pba()) iff the
                    // CAS observes old_value matches current
                    // (lanes/dedup.rs:137-148). Pessimistic claim — the
                    // CAS decision can't change the dispatch footprint.
                    mark_rc(old_value.head_pba(), &mut rc_shards_touched);
                }
                WalOp::DedupComparePut {
                    hash,
                    old_value,
                    new_value,
                } => {
                    let sid =
                        crate::dedup_types::shard_for_hash(hash, dedup_shard_count_u32) as usize;
                    dedup_buckets[sid].push(idx);
                    // Apply stages decref(old)+incref(new) iff CAS hits
                    // (lanes/dedup.rs:149-171). Pessimistic claim.
                    mark_rc(old_value.head_pba(), &mut rc_shards_touched);
                    mark_rc(new_value.head_pba(), &mut rc_shards_touched);
                }
            }
        }

        // rc-authoritative: blanket-claim every rc shard (see the rationale
        // at `rc_auth` above — the inline decref's pba is unknown at plan
        // time). Off (): the precise per-op marks above stand.
        if rc_auth {
            for touched in rc_shards_touched.iter_mut() {
                *touched = true;
            }
        }

        let mut l2p_sorted: Vec<_> = l2p_buckets.into_iter().collect();
        l2p_sorted.sort_by_key(|((vol, sid), _)| (*vol, *sid));

        // Preserve the existing `!rc_buckets[sid].is_empty()` clause so
        // future ops that push directly into `rc_buckets` (replay path,
        // future direct refcount ops) still register their lanes. The
        // hot path uses `rc_shards_touched` exclusively.
        let rc_enqueued: Vec<bool> = rc_buckets
            .iter()
            .enumerate()
            .map(|(sid, bucket)| !bucket.is_empty() || rc_shards_touched[sid])
            .collect();

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
        bfg: crate::types::Bfg,
        plan: LaneDispatchPlan,
        ops: Arc<Vec<WalOp>>,
    ) -> QueuedLanePlan {
        let mut l2p_receivers = Vec::with_capacity(plan.l2p_sorted.len());
        // Snapshot refcount shard handles once per commit so the per-lane
        // closures can do guarded-remap rc lookups (dedup hits) without
        // touching the Db struct from the worker thread.
        let refcount_shards_arc: Arc<Vec<Arc<crate::refcount::RcShard>>> =
            Arc::new(self.refcount_shards.iter().map(|s| s.rc.clone()).collect());
        let rc_authoritative = self.rc_authoritative_reclaim;
        for ((vol_ord, sid), indices) in plan.l2p_sorted {
            let volume = volumes
                .get(&vol_ord)
                .expect("volume presence checked during lane planning");
            let apply_volume = volume.clone();
            let apply_ops = ops.clone();
            let metrics = self.metrics.clone();
            let refcount_shards_arc = refcount_shards_arc.clone();
            // BFG: capture this volume's live snapshot
            // capture-watermarks for the birth COW-kill + page-deadlist gate
            // before the lane runs.
            let snapshot_wms = self.snapshot_wms(vol_ord);
            // BFG: the clone COW-kill pinner set, built from the
            // SAME `volumes` snapshot this lane operates on (empty for non-clones).
            let clone_cow_pinners =
                crate::db::volume::clone_cow_pinners_from(volumes, vol_ord, snapshot_wms.clone());
            let (tx, rx) = crossbeam_channel::bounded(1);
            volume.shards[sid].apply_lane.enqueue_ready(
                lsn,
                Box::new(move || {
                    let result = Self::apply_l2p_bucket(
                        apply_volume,
                        sid,
                        indices,
                        lsn,
                        bfg,
                        apply_ops.as_slice(),
                        refcount_shards_arc.as_slice(),
                        metrics.as_ref(),
                        rc_authoritative,
                        snapshot_wms,
                        clone_cow_pinners,
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
            bfg,
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

        // BFG this commit is stamped to. The rc/dedup `rc.stage(bfg, …)`
        // below must land in this slot (kept frozen-open by the commit's
        // still-held `BfgGuard`, since we await every receiver here).
        let bfg = plan.bfg;
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
                    let result =
                        Self::apply_refcount_bucket_to_tree(rc, metrics, actions, lsn, bfg);
                    let _ = tx.send(result);
                }),
            );
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

        // Dedup table shards are independent, but their derived refcount
        // mutations are not: two different hashes can read or update the same
        // PBA. Preserve the old grouped apply's input order by running all
        // dedup ops for this commit in one coordinator closure. Pending tasks
        // are still installed on every touched dedup lane before dispatch, so
        // higher conflicting LSNs cannot pass any of those lane watermarks.
        //
        // The previous async dispatch (fire the dedup closure on its lane
        // and return without awaiting) is gone under the BFG-slot refcount
        // model: under `rc_authoritative` the dedup bucket stages derived
        // refcount deltas (`DedupPut` membership ±1), and those must land
        // in THIS commit's BFG slot to stay aligned with the dedup_index
        // entry's durability. If the closure ran after the commit returned
        // (and its `BfgGuard` dropped), this BFG could roll to Syncing and
        // its slot fold/clear before the stage landed → a foreign-slot rc
        // delta and a refcount-vs-dedup_index durability skew on replay.
        // Awaiting keeps the `BfgGuard` alive across `rc.stage(bfg, …)`, so
        // the slot stays frozen-open. The dedup_index cuckoo write itself
        // is already moved off the commit critical path by the dedup
        // drainer; awaiting here costs only the cheap `stage_put` + rc
        // stage. `DedupCompare*` ops were already on this awaited path
        // (their `applied` flag feeds the caller).
        let dedup_enqueue_started = std::time::Instant::now();
        let dedup_buckets = std::mem::take(&mut plan.dedup_buckets);
        let pendings = std::mem::take(&mut plan.dedup_pendings);
        let refcount_shards_arc: Arc<Vec<Arc<crate::refcount::RcShard>>> =
            Arc::new(self.refcount_shards.iter().map(|s| s.rc.clone()).collect());
        let mut leader: Option<(usize, PendingApplyWork)> = None;
        let mut followers = Vec::new();
        let mut dedup_indices = Vec::new();
        for (pending_opt, bucket) in pendings.into_iter().zip(dedup_buckets) {
            let Some(pending) = pending_opt else { continue };
            let first_idx = *bucket
                .first()
                .expect("a pending dedup task must have a non-empty bucket");
            dedup_indices.extend(bucket);
            match leader.take() {
                None => leader = Some((first_idx, pending)),
                Some((leader_idx, leader_pending)) if first_idx < leader_idx => {
                    followers.push(leader_pending);
                    leader = Some((first_idx, pending));
                }
                Some(existing) => {
                    followers.push(pending);
                    leader = Some(existing);
                }
            }
        }
        for pending in followers {
            pending.set(Box::new(|| {}));
        }

        let dedup_receiver = leader.map(|(_first_idx, pending)| {
            dedup_indices.sort_unstable();
            let ready_at = std::time::Instant::now();
            let bucket_ops = dedup_indices.len() as u64;
            let ops = plan.ops.clone();
            let dedup_index = self.dedup_index.clone();
            let refcount_shards_arc = refcount_shards_arc.clone();
            let metrics = self.metrics.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            pending.set(Box::new(move || {
                let ready_queue_wait = ready_at.elapsed();
                let exec_started = std::time::Instant::now();
                let outcomes = Self::apply_dedup_indices_to(
                    dedup_index.as_ref(),
                    refcount_shards_arc.as_slice(),
                    metrics.as_ref(),
                    ops.as_slice(),
                    dedup_indices,
                    lsn,
                    bfg,
                );
                metrics.record_dedup_lane_task(
                    bucket_ops,
                    ready_queue_wait,
                    exec_started.elapsed(),
                );
                let _ = tx.send(outcomes);
            }));
            rx
        });
        timing.dedup_enqueue = dedup_enqueue_started.elapsed();
        let dedup_wait_started = std::time::Instant::now();
        if let Some(rx) = dedup_receiver {
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
}
