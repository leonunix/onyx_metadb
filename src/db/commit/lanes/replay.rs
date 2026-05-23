use super::*;

impl Db {
    pub(in crate::db) fn apply_replay_batch(
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        refcount_shards: &[Shard],
        dedup_index: &Arc<crate::dedup::DedupIndex>,
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
                    page_store,
                    lsn,
                    op,
                    snap_info_for_vol,
                )?);
            }
            return Ok(outcomes);
        }

        Self::apply_ops_grouped_to_lanes(volumes, refcount_shards, dedup_index, metrics, lsn, ops)
    }

    pub(in crate::db::commit) fn apply_ops_grouped_to_lanes(
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        refcount_shards: &[Shard],
        dedup_index: &Arc<crate::dedup::DedupIndex>,
        metrics: &Arc<MetaMetrics>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        let ops = Arc::new(ops.to_vec());
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..ops.len()).map(|_| None).collect();
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<L2pBucketEntry>> = HashMap::new();
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
                    l2p_buckets
                        .entry((*vol_ord, sid))
                        .or_default()
                        .push(L2pBucketEntry::Single(idx));
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
                    metrics.record_apply_l2p_remap_range_lane_ops(1);
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
                }
                WalOp::DedupPut { .. }
                | WalOp::DedupPutGuarded { .. }
                | WalOp::DedupDelete { .. }
                | WalOp::DedupCompareDelete { .. }
                | WalOp::DedupComparePut { .. } => {
                    dedup_idxs.push(idx);
                }
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRangeDelete { .. }
                | WalOp::FreePbas { .. }
                | WalOp::PromotionChunk { .. }
                | WalOp::PromotionComplete { .. } => {
                    unreachable!("lifecycle op must not reach apply_ops_grouped_to_lanes");
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
                        merge_l2p_outcome(&mut outcomes, idx, outcome);
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

    pub(in crate::db::commit) fn apply_ops_grouped_to(
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        refcount_shards: &[Shard],
        dedup_index: &Arc<crate::dedup::DedupIndex>,
        metrics: &Arc<MetaMetrics>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..ops.len()).map(|_| None).collect();
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<L2pBucketEntry>> = HashMap::new();
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
                    l2p_buckets
                        .entry((*vol_ord, sid))
                        .or_default()
                        .push(L2pBucketEntry::Single(idx));
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
                    metrics.record_apply_l2p_remap_range_lane_ops(1);
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
                }
                WalOp::DedupPut { .. }
                | WalOp::DedupPutGuarded { .. }
                | WalOp::DedupDelete { .. }
                | WalOp::DedupCompareDelete { .. }
                | WalOp::DedupComparePut { .. } => {
                    dedup_idxs.push(idx);
                }
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRangeDelete { .. }
                | WalOp::FreePbas { .. }
                | WalOp::PromotionChunk { .. }
                | WalOp::PromotionComplete { .. } => {
                    unreachable!("lifecycle op must not reach apply_ops_grouped_to");
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
                merge_l2p_outcome(&mut outcomes, idx, outcome);
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

        for (idx, outcome) in Self::apply_dedup_indices_to(
            dedup_index.as_ref(),
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
    pub(in crate::db::commit) fn apply_ops_grouped(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        Self::apply_ops_grouped_to(
            volumes,
            &self.refcount_shards,
            &self.dedup_index,
            &self.metrics,
            lsn,
            ops,
        )
    }
}
