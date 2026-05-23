use super::*;
use crate::metrics::DedupPutStageTimings;
use std::sync::atomic::Ordering;

impl Db {
    pub(super) fn build_lane_dispatch_plan(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        ops: &[WalOp],
    ) -> Result<LaneDispatchPlan> {
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<L2pBucketEntry>> = HashMap::new();
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

    fn apply_l2p_bucket(
        volume: Arc<Volume>,
        sid: usize,
        indices: Vec<L2pBucketEntry>,
        lsn: Lsn,
        ops: &[WalOp],
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        metrics: &MetaMetrics,
    ) -> Result<L2pBucketApplyResult> {
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut rc_actions = Vec::new();
        let shard = &volume.shards[sid];
        if shard.use_buffer {
            return Self::apply_l2p_bucket_buffer(
                volume.clone(),
                sid,
                indices,
                lsn,
                ops,
                refcount_shards,
                metrics,
            );
        }
        let tree_lock_started = std::time::Instant::now();
        let mut tree = shard.tree.write();
        let tree_lock_wait = tree_lock_started.elapsed();
        let read_view_prepare_started = std::time::Instant::now();
        let mut read_view_guard = if shard.active_readers.load(Ordering::Acquire) == 0
            && let Some(guard) = shard.read_view.try_write()
        {
            // Holding the write guard blocks new readers until the
            // post-apply snapshot is published. Do not replace the current
            // view with an empty overlay here: the pre-apply root may already
            // reference dirty pages from earlier commits that have not been
            // checkpointed to the page store yet.
            if shard.active_readers.load(Ordering::Acquire) == 0 {
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
        let mut l2p_remap_range_lba_count = 0u64;
        let bucket_started = std::time::Instant::now();
        let ops_started = std::time::Instant::now();
        let ops_result = (|| -> Result<()> {
            // Stage 1.6 unification: flatten every unguarded `L2pRemap`
            // (`Single`) and every `RangeSlice` LBA into one batch list,
            // then leaf-run-batch ACROSS entry-kind boundaries. The
            // previous Stage 1.5 path leaf-run-grouped each RangeSlice
            // independently, so multiple range ops in the same bucket
            // landing on the same leaf each paid their own tree descend +
            // leaf-page modify. With sequential writes (the common
            // passthrough case) that doubled per-LBA cost vs. the baseline
            // per-LBA path.
            //
            // Layout:
            // * `batch_lbas[i] = (op_idx, lba, value, range_offset)`
            //   — `range_offset = Some(off)` flags a RangeSlice-sourced
            //   entry that should land in `range_partials[op_idx]` rather
            //   than as its own `L2pRemap` outcome.
            // * `range_partials[op_idx]` = pre-allocated
            //   `(applied, prevs)` boxes sized to the full range op (so
            //   slots in different shards land in the same positions).
            // * `nonbatch_positions` holds entries that can't leaf-run
            //   (Put/Delete/guarded L2pRemap); processed afterwards using
            //   the per-op fast paths.
            struct BatchLba {
                op_idx: usize,
                lba: u64,
                value: L2pValue,
                range_offset: Option<u32>,
            }
            let mut batch_lbas: Vec<BatchLba> = Vec::with_capacity(indices.len());
            let mut nonbatch_positions: Vec<usize> = Vec::new();
            let mut range_partials: std::collections::HashMap<
                usize,
                (Box<[bool]>, Vec<Option<L2pValue>>),
            > = std::collections::HashMap::new();

            for (pos, entry) in indices.iter().enumerate() {
                match entry {
                    L2pBucketEntry::Single(idx) => {
                        if let WalOp::L2pRemap {
                            lba,
                            new_value,
                            guard: None,
                            ..
                        } = &ops[*idx]
                        {
                            batch_lbas.push(BatchLba {
                                op_idx: *idx,
                                lba: *lba,
                                value: *new_value,
                                range_offset: None,
                            });
                        } else {
                            nonbatch_positions.push(pos);
                        }
                    }
                    L2pBucketEntry::RangeSlice {
                        op_idx,
                        lba_offsets,
                    } => {
                        let (start_lba, range_values) = match &ops[*op_idx] {
                            WalOp::L2pRemapRange {
                                start_lba, values, ..
                            } => (*start_lba, values.as_ref()),
                            other => {
                                unreachable!("RangeSlice op_idx points to non-range op: {other:?}")
                            }
                        };
                        let n = range_values.len();
                        range_partials
                            .entry(*op_idx)
                            .or_insert_with(|| (vec![false; n].into_boxed_slice(), vec![None; n]));
                        for &off in lba_offsets.iter() {
                            batch_lbas.push(BatchLba {
                                op_idx: *op_idx,
                                lba: start_lba + off as u64,
                                value: range_values[off as usize],
                                range_offset: Some(off),
                            });
                        }
                    }
                }
            }

            // Group batch_lbas by leaf_idx. Map preserves identity by
            // index into `batch_lbas` so per-leaf processing can recover
            // (op_idx, range_offset, etc).
            let mut leaf_groups: std::collections::HashMap<u64, Vec<usize>> =
                std::collections::HashMap::new();
            for (i, b) in batch_lbas.iter().enumerate() {
                let leaf_idx = b.lba >> crate::paged::format::LEAF_SHIFT;
                leaf_groups.entry(leaf_idx).or_default().push(i);
            }
            // Sort by leaf_idx for deterministic ordering (tests, replay).
            let mut leaf_order: Vec<u64> = leaf_groups.keys().copied().collect();
            leaf_order.sort_unstable();

            for leaf_idx in leaf_order {
                let group = leaf_groups
                    .remove(&leaf_idx)
                    .expect("leaf_idx came from leaf_groups keys");
                let mut entries: Vec<(u64, L2pValue)> = Vec::with_capacity(group.len());
                let mut accepted_batch_idx: Vec<usize> = Vec::with_capacity(group.len());
                for batch_i in &group {
                    let b = &batch_lbas[*batch_i];
                    let cur = tree.get_read_only(b.lba)?;
                    if super::apply::seq_guard_rejects(b.value.seq(), cur.as_ref()) {
                        match b.range_offset {
                            Some(off) => {
                                l2p_remap_range_lba_count += 1;
                                let (_, prevs) = range_partials
                                    .get_mut(&b.op_idx)
                                    .expect("range partial pre-allocated above");
                                prevs[off as usize] = cur;
                            }
                            None => {
                                l2p_remap_count += 1;
                                outcomes.push((
                                    b.op_idx,
                                    ApplyOutcome::L2pRemap {
                                        applied: false,
                                        prev: cur,
                                        freed_pba: None,
                                    },
                                ));
                            }
                        }
                        continue;
                    }
                    entries.push((b.lba, b.value));
                    accepted_batch_idx.push(*batch_i);
                }
                if entries.is_empty() {
                    continue;
                }
                let prev_values = tree.insert_leaf_run_at_lsn_deferred_finish(&entries, lsn)?;
                for ((batch_i, (_lba, _new_value)), prev) in accepted_batch_idx
                    .into_iter()
                    .zip(entries.into_iter())
                    .zip(prev_values.into_iter())
                {
                    let b = &batch_lbas[batch_i];
                    super::apply::record_dead(&volume, prev, lsn);
                    match b.range_offset {
                        Some(off) => {
                            l2p_remap_range_lba_count += 1;
                            let (applied, prevs) = range_partials
                                .get_mut(&b.op_idx)
                                .expect("range partial pre-allocated above");
                            applied[off as usize] = true;
                            prevs[off as usize] = prev;
                        }
                        None => {
                            l2p_remap_count += 1;
                            outcomes.push((
                                b.op_idx,
                                ApplyOutcome::L2pRemap {
                                    applied: true,
                                    prev,
                                    freed_pba: None,
                                },
                            ));
                        }
                    }
                }
            }

            // Finalize range_partials as L2pRemapRange outcomes. Ordering
            // is unspecified at the metadb apply layer; the
            // `apply_ops_laned` merger writes them into outcomes[idx]
            // (matching the `outcomes.len() == ops.len()` contract).
            for (op_idx, (applied, prevs)) in range_partials.drain() {
                outcomes.push((
                    op_idx,
                    ApplyOutcome::L2pRemapRange {
                        applied,
                        prevs: prevs.into_boxed_slice(),
                        freed_pbas: Vec::new(),
                    },
                ));
            }

            // Phase D: non-batchable ops (Put / Delete / guarded
            // L2pRemap). Each goes through its own tree primitive; no
            // sharing with leaf-run batching above.
            for pos in nonbatch_positions {
                let idx = match &indices[pos] {
                    L2pBucketEntry::Single(i) => *i,
                    L2pBucketEntry::RangeSlice { .. } => {
                        unreachable!("nonbatch slot contains only Single entries")
                    }
                };
                let outcome = match &ops[idx] {
                    WalOp::L2pPut { lba, value, .. } => {
                        let cur = tree.get_read_only(*lba)?;
                        if super::apply::seq_guard_rejects(value.seq(), cur.as_ref()) {
                            l2p_put_count += 1;
                            outcomes.push((idx, ApplyOutcome::L2pPrev(cur)));
                            continue;
                        }
                        let prev = tree.insert_at_lsn_deferred_finish(*lba, *value, lsn)?;
                        super::apply::record_dead(&volume, prev, lsn);
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
                        let cur = tree.get_read_only(*lba)?;
                        if super::apply::seq_guard_rejects(new_value.seq(), cur.as_ref()) {
                            l2p_remap_count += 1;
                            outcomes.push((
                                idx,
                                ApplyOutcome::L2pRemap {
                                    applied: false,
                                    prev: cur,
                                    freed_pba: None,
                                },
                            ));
                            continue;
                        }
                        let prev = tree.insert_at_lsn_deferred_finish(*lba, *new_value, lsn)?;
                        super::apply::record_dead(&volume, prev, lsn);
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
        if apply_result.is_ok() {
            if let Some(mut guard) = read_view_guard.take() {
                *guard = Arc::new(tree.snapshot_read_view());
            } else {
                super::apply::publish_l2p_read_view(shard, &tree);
            }
        }
        let publish_elapsed = publish_started.elapsed();
        apply_result?;
        let bucket_elapsed = bucket_started.elapsed();
        let total_l2p_ops =
            l2p_put_count + l2p_delete_count + l2p_remap_count + l2p_remap_range_lba_count;
        metrics.record_apply_l2p_bucket_stages(
            total_l2p_ops,
            bucket_elapsed,
            tree_lock_wait,
            read_view_prepare,
            ops_elapsed,
            finish_elapsed,
            publish_elapsed,
        );
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
                range_lbas = l2p_remap_range_lba_count,
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
            let remap_us = total_us.saturating_mul(l2p_remap_count) / total_l2p_ops;
            let range_us = total_us
                .saturating_sub(put_us)
                .saturating_sub(delete_us)
                .saturating_sub(remap_us);
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
            metrics.record_apply_l2p_remap_range_lane_work(
                l2p_remap_range_lba_count,
                std::time::Duration::from_micros(range_us),
            );
        }
        Ok(L2pBucketApplyResult {
            outcomes,
            rc_actions,
        })
    }

    /// B2 buffer variant of [`Self::apply_l2p_bucket`]. Same lane / per-shard
    /// dispatch contract, but writes land in `shard.l2p_buffer` instead of
    /// the paged tree. We don't hold `tree.write()` here: the buffer's own
    /// `Mutex<HashMap>` serialises concurrent inserts on this shard, and the
    /// background compactor is the only writer to the tree. RC bookkeeping
    /// is identical to the tree path — staged at commit time so that
    /// snapshot semantics and per-shard refcount apply lanes behave
    /// unchanged.
    /// B2 buffer variant of `apply_l2p_bucket`. Same caller contract:
    /// takes ownership of the bucket's entries; each `RangeSlice`
    /// entry contributes a partial `ApplyOutcome::L2pRemapRange` to be
    /// merged in `apply_ops_laned`.
    fn apply_l2p_bucket_buffer(
        volume: Arc<Volume>,
        sid: usize,
        indices: Vec<L2pBucketEntry>,
        lsn: Lsn,
        ops: &[WalOp],
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        metrics: &MetaMetrics,
    ) -> Result<L2pBucketApplyResult> {
        use super::l2p_buffer::BufferLookup;
        let shard = &volume.shards[sid];
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut rc_actions = Vec::new();
        // Snapshot the current published read view once. Lookups for `cur`
        // fall through to this view on buffer miss. Compactor publishes a
        // new view on each cycle; capturing once per-bucket is safe because
        // any committed-but-uncompacted state we'd miss in this view still
        // shows up in `shard.l2p_buffer.lookup`.
        let read_view_prepare_started = std::time::Instant::now();
        let read_view: Arc<crate::paged::ReadView> = shard.read_view.read().clone();
        let read_view_prepare = read_view_prepare_started.elapsed();
        let mut l2p_put_count = 0u64;
        let mut l2p_delete_count = 0u64;
        let mut l2p_remap_count = 0u64;
        let mut l2p_remap_range_lba_count = 0u64;
        let bucket_started = std::time::Instant::now();
        let ops_started = std::time::Instant::now();
        let ops_result = (|| -> Result<()> {
            for entry in &indices {
                if let L2pBucketEntry::RangeSlice {
                    op_idx,
                    lba_offsets,
                } = entry
                {
                    let (range_start_lba, range_values) = match &ops[*op_idx] {
                        WalOp::L2pRemapRange {
                            start_lba, values, ..
                        } => (*start_lba, values.as_ref()),
                        other => {
                            unreachable!("RangeSlice op_idx points to non-range op: {other:?}")
                        }
                    };
                    let n = range_values.len();
                    let mut applied_bits = vec![false; n].into_boxed_slice();
                    let mut prevs_box: Vec<Option<L2pValue>> = vec![None; n];

                    for &off in lba_offsets.iter() {
                        let lba = range_start_lba + off as u64;
                        let new_value = range_values[off as usize];
                        let cur = match shard.l2p_buffer.lookup(lba) {
                            BufferLookup::Present(v) => Some(v),
                            BufferLookup::Tombstone => None,
                            BufferLookup::Absent => read_view.get(lba)?,
                        };
                        if super::apply::seq_guard_rejects(new_value.seq(), cur.as_ref()) {
                            l2p_remap_range_lba_count += 1;
                            prevs_box[off as usize] = cur;
                            continue;
                        }
                        shard.l2p_buffer.insert(lba, new_value, lsn);
                        super::apply::record_dead(&volume, cur, lsn);
                        l2p_remap_range_lba_count += 1;
                        applied_bits[off as usize] = true;
                        prevs_box[off as usize] = cur;
                    }

                    outcomes.push((
                        *op_idx,
                        ApplyOutcome::L2pRemapRange {
                            applied: applied_bits,
                            prevs: prevs_box.into_boxed_slice(),
                            freed_pbas: Vec::new(),
                        },
                    ));
                    continue;
                }
                let idx = match entry {
                    L2pBucketEntry::Single(i) => *i,
                    L2pBucketEntry::RangeSlice { .. } => unreachable!("handled above"),
                };
                let outcome = match &ops[idx] {
                    WalOp::L2pPut { lba, value, .. } => {
                        let cur = match shard.l2p_buffer.lookup(*lba) {
                            BufferLookup::Present(v) => Some(v),
                            BufferLookup::Tombstone => None,
                            BufferLookup::Absent => read_view.get(*lba)?,
                        };
                        if super::apply::seq_guard_rejects(value.seq(), cur.as_ref()) {
                            l2p_put_count += 1;
                            outcomes.push((idx, ApplyOutcome::L2pPrev(cur)));
                            continue;
                        }
                        shard.l2p_buffer.insert(*lba, *value, lsn);
                        super::apply::record_dead(&volume, cur, lsn);
                        l2p_put_count += 1;
                        ApplyOutcome::L2pPrev(cur)
                    }
                    WalOp::L2pDelete { lba, .. } => {
                        let cur = match shard.l2p_buffer.lookup(*lba) {
                            BufferLookup::Present(v) => Some(v),
                            BufferLookup::Tombstone => None,
                            BufferLookup::Absent => read_view.get(*lba)?,
                        };
                        shard.l2p_buffer.insert_tombstone(*lba, lsn);
                        l2p_delete_count += 1;
                        ApplyOutcome::L2pPrev(cur)
                    }
                    WalOp::L2pRemap {
                        lba,
                        new_value,
                        guard,
                        ..
                    } => {
                        // RC-guarded remap: verify target pba refcount
                        // before mutating. Same lock-order rule as the
                        // tree-mode bucket (rc shard lookup only — buffer
                        // mutation needs no L2P lock).
                        if let Some((gp, min_rc)) = guard {
                            let gp_sid =
                                (xxh3_64(&gp.to_be_bytes()) as usize) % refcount_shards.len();
                            let cur_rc = refcount_shards[gp_sid].get(*gp)?;
                            if cur_rc < *min_rc {
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
                        let cur = match shard.l2p_buffer.lookup(*lba) {
                            BufferLookup::Present(v) => Some(v),
                            BufferLookup::Tombstone => None,
                            BufferLookup::Absent => read_view.get(*lba)?,
                        };
                        if super::apply::seq_guard_rejects(new_value.seq(), cur.as_ref()) {
                            l2p_remap_count += 1;
                            outcomes.push((
                                idx,
                                ApplyOutcome::L2pRemap {
                                    applied: false,
                                    prev: cur,
                                    freed_pba: None,
                                },
                            ));
                            continue;
                        }
                        shard.l2p_buffer.insert(*lba, *new_value, lsn);
                        super::apply::record_dead(&volume, cur, lsn);
                        l2p_remap_count += 1;
                        ApplyOutcome::L2pRemap {
                            applied: true,
                            prev: cur,
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
        ops_result?;
        let bucket_elapsed = bucket_started.elapsed();
        let total_l2p_ops =
            l2p_put_count + l2p_delete_count + l2p_remap_count + l2p_remap_range_lba_count;
        metrics.record_apply_l2p_bucket_stages(
            total_l2p_ops,
            bucket_elapsed,
            std::time::Duration::ZERO,
            read_view_prepare,
            ops_elapsed,
            std::time::Duration::ZERO,
            std::time::Duration::ZERO,
        );
        if bucket_elapsed.as_micros() >= 100_000 || ops_elapsed.as_micros() >= 100_000 {
            tracing::warn!(
                vol_ord = volume.ord,
                shard = sid,
                lsn,
                indices = total_l2p_ops,
                put = l2p_put_count,
                delete = l2p_delete_count,
                remap = l2p_remap_count,
                range_lbas = l2p_remap_range_lba_count,
                total_us = duration_us(bucket_elapsed),
                ops_us = duration_us(ops_elapsed),
                "metadb: slow l2p apply bucket (buffer)"
            );
        }
        if total_l2p_ops > 0 {
            let total_us = bucket_elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
            let put_us = total_us.saturating_mul(l2p_put_count) / total_l2p_ops;
            let delete_us = total_us.saturating_mul(l2p_delete_count) / total_l2p_ops;
            let remap_us = total_us.saturating_mul(l2p_remap_count) / total_l2p_ops;
            let range_us = total_us
                .saturating_sub(put_us)
                .saturating_sub(delete_us)
                .saturating_sub(remap_us);
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
            metrics.record_apply_l2p_remap_range_lane_work(
                l2p_remap_range_lba_count,
                std::time::Duration::from_micros(range_us),
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
                let op_idxs: Vec<usize> = group.iter().map(|a| a.op_idx).collect();
                // P0 diagnostic: trace every coalesced stage call so we can
                // confirm whether the +N at lsn=X actually reached the rc
                // shard. Filter via
                // `RUST_LOG=onyx_metadb::db::commit::rc_stage=trace`.
                tracing::trace!(
                    target: "onyx_metadb::db::commit::rc_stage",
                    pba,
                    coalesced_delta = delta,
                    ?op_idxs,
                    lsn,
                    "apply_refcount_bucket_to_tree: coalesced stage entry"
                );
                let (pre, new) = match rc.stage(pba, delta, lsn) {
                    Ok(r) => r,
                    Err(err) => {
                        tracing::error!(
                            target: "onyx_metadb::db::commit::rc_apply_underflow",
                            pba,
                            coalesced_delta = delta,
                            group_size = group.len(),
                            ?op_idxs,
                            lsn,
                            error = %err,
                            "apply_refcount_bucket_to_tree: coalesced group failed"
                        );
                        return Err(err);
                    }
                };
                tracing::trace!(
                    target: "onyx_metadb::db::commit::rc_stage",
                    pba,
                    coalesced_delta = delta,
                    pre,
                    new,
                    lsn,
                    "apply_refcount_bucket_to_tree: coalesced stage ok"
                );
                metrics.record_apply_refcount(op_started.elapsed());
                if new == 0 && pre > 0 {
                    if let Some(action) = group
                        .iter()
                        .rev()
                        .find(|action| action.remap_freed_candidate)
                    {
                        result
                            .remap_freed
                            .push((action.op_idx, action.pba, action.range_op));
                    }
                }
                continue;
            }

            for action in group {
                let op_started = std::time::Instant::now();
                let (pre, new) = match rc.stage(action.pba, action.delta, lsn) {
                    Ok(r) => r,
                    Err(err) => {
                        tracing::error!(
                            target: "onyx_metadb::db::commit::rc_apply_underflow",
                            pba = action.pba,
                            delta = action.delta,
                            op_idx = action.op_idx,
                            standalone = action.standalone_refcount,
                            remap_freed_candidate = action.remap_freed_candidate,
                            lsn,
                            error = %err,
                            "apply_refcount_bucket_to_tree: serial action failed"
                        );
                        return Err(err);
                    }
                };
                metrics.record_apply_refcount(op_started.elapsed());
                if action.remap_freed_candidate {
                    if new == 0 && pre > 0 {
                        result
                            .remap_freed
                            .push((action.op_idx, action.pba, action.range_op));
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
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        metrics: &MetaMetrics,
        ops: &[WalOp],
        indices: Vec<usize>,
        lsn: Lsn,
    ) -> Result<Vec<(usize, ApplyOutcome)>> {
        let batch_started = std::time::Instant::now();
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut pending_puts: Vec<(Hash8, DedupValue, Option<Pba>, usize)> = Vec::new();
        // Phase 5: dedup-table mutations drive the global PBA refcount
        // (shared-page bookkeeping). The hot path no longer stages rc
        // deltas on L2P remaps, so any rc movement for a shared pba has
        // to ride along with the DedupPut/Delete/Compare* that gained
        // or lost the reference. Helpers below stay close to
        // `apply_dedup_put_with_rc` / `apply_dedup_delete_with_rc` in
        // `apply.rs` and use the same routing math (xxh3 mod shards)
        // because this lane bucket holds `Arc<RcShard>` directly, not
        // the outer `Shard` wrapper that `shard_for_key` expects.
        let rc_shard_for =
            |pba: Pba| -> usize { (xxh3_64(&pba.to_be_bytes()) as usize) % refcount_shards.len() };
        let stage_rc = |pba: Pba, delta: i64| -> Result<()> {
            let sid = rc_shard_for(pba);
            refcount_shards[sid].stage(pba, delta, lsn)?;
            Ok(())
        };
        // Phase 5 stale-entry tolerance: a dedup_index row can point to
        // a PBA whose rc has been driven to 0 by lineage GC without
        // cleanup removing the row. The matching put/delete must then
        // skip the decref instead of underflowing the rc table — see
        // `apply_dedup_put_with_rc` for the full rationale. We pre-read
        // the current rc once per stale candidate; within this lane the
        // LSN-ordered apply gate prevents concurrent rc mutation on the
        // same pba, so the floor check is sound.
        let stage_rc_decref_if_live = |pba: Pba| -> Result<()> {
            let sid = rc_shard_for(pba);
            if refcount_shards[sid].get(pba)? > 0 {
                refcount_shards[sid].stage(pba, -1, lsn)?;
            }
            Ok(())
        };
        // [[no-refcount-hot-path-design]] Phase 5 (WAL schema 0xB8):
        // every dedup put / delete carries an embedded `old_pba`
        // captured at `Transaction::commit` time. The live apply path
        // here uses it directly so that replay and live apply produce
        // the same rc deltas — apply is deterministic from the WAL
        // alone, which is required because the on-disk dedup_index
        // data pages are written eagerly per op (only the meta page
        // is checkpoint-gated). Reading dedup_index here would also
        // work for the live path, but using the embedded value keeps
        // the live and replay paths bit-identical and saves a
        // cuckoo lookup per op.
        let flush_pending_puts =
            |pending_puts: &mut Vec<(Hash8, DedupValue, Option<Pba>, usize)>,
             outcomes: &mut Vec<(usize, ApplyOutcome)>|
             -> Result<()> {
                if pending_puts.is_empty() {
                    return Ok(());
                }
                let entries: Vec<(Hash8, DedupValue)> = pending_puts
                    .iter()
                    .map(|(hash, value, _, _)| (*hash, *value))
                    .collect();
                let mut put_timings = DedupPutStageTimings::default();
                let started = std::time::Instant::now();
                dedup_index.put_many_with_metrics(&entries, lsn, &mut put_timings)?;
                metrics
                    .record_dedup_forward_put_batch(pending_puts.len() as u64, started.elapsed());
                metrics.record_dedup_put_stages(put_timings);
                for (_, value, old_pba, idx) in pending_puts.drain(..) {
                    let new_pba = value.head_pba();
                    if old_pba != Some(new_pba) {
                        if let Some(op) = old_pba {
                            stage_rc_decref_if_live(op)?;
                        }
                        stage_rc(new_pba, 1)?;
                    }
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                Ok(())
            };

        for idx in indices {
            match &ops[idx] {
                WalOp::DedupPut {
                    hash,
                    value,
                    old_pba,
                } => {
                    pending_puts.push((*hash, *value, *old_pba, idx));
                }
                WalOp::DedupPutGuarded {
                    hash,
                    value,
                    pba_guard,
                    min_rc,
                    old_pba,
                } => {
                    flush_pending_puts(&mut pending_puts, &mut outcomes)?;
                    let guard_started = std::time::Instant::now();
                    let rc = refcount_shards
                        .get(rc_shard_for(*pba_guard))
                        .ok_or_else(|| MetaDbError::Corruption("missing refcount shard".into()))?
                        .get(*pba_guard)?;
                    metrics.record_dedup_guard(guard_started.elapsed());
                    if rc >= *min_rc {
                        let mut put_timings = DedupPutStageTimings::default();
                        let started = std::time::Instant::now();
                        dedup_index.put_with_metrics(*hash, *value, lsn, &mut put_timings)?;
                        metrics.record_dedup_forward_put(started.elapsed());
                        metrics.record_dedup_put_stages(put_timings);
                        let new_pba = value.head_pba();
                        if *old_pba != Some(new_pba) {
                            if let Some(op) = *old_pba {
                                stage_rc_decref_if_live(op)?;
                            }
                            stage_rc(new_pba, 1)?;
                        }
                    }
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                WalOp::DedupDelete { hash, old_pba } => {
                    flush_pending_puts(&mut pending_puts, &mut outcomes)?;
                    let started = std::time::Instant::now();
                    dedup_index.delete(hash, lsn)?;
                    metrics.record_dedup_forward_delete(started.elapsed());
                    if let Some(op) = *old_pba {
                        stage_rc_decref_if_live(op)?;
                    }
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                WalOp::DedupCompareDelete { hash, old_value } => {
                    flush_pending_puts(&mut pending_puts, &mut outcomes)?;
                    let started = std::time::Instant::now();
                    let cur = dedup_index.get(hash)?;
                    let applied = cur.as_ref() == Some(old_value);
                    if applied {
                        dedup_index.delete(hash, lsn)?;
                        metrics.record_dedup_forward_delete(started.elapsed());
                        stage_rc_decref_if_live(old_value.head_pba())?;
                    }
                    outcomes.push((idx, ApplyOutcome::DedupCompare { applied }));
                }
                WalOp::DedupComparePut {
                    hash,
                    old_value,
                    new_value,
                } => {
                    flush_pending_puts(&mut pending_puts, &mut outcomes)?;
                    let started = std::time::Instant::now();
                    let cur = dedup_index.get(hash)?;
                    let applied = cur.as_ref() == Some(old_value);
                    if applied {
                        let mut put_timings = DedupPutStageTimings::default();
                        dedup_index.put_with_metrics(*hash, *new_value, lsn, &mut put_timings)?;
                        metrics.record_dedup_forward_put(started.elapsed());
                        metrics.record_dedup_put_stages(put_timings);
                        let old_pba = old_value.head_pba();
                        let new_pba = new_value.head_pba();
                        if old_pba != new_pba {
                            stage_rc_decref_if_live(old_pba)?;
                            stage_rc(new_pba, 1)?;
                        }
                    }
                    outcomes.push((idx, ApplyOutcome::DedupCompare { applied }));
                }
                other => unreachable!("dedup bucket holds only dedup ops; saw {other:?}"),
            };
        }
        flush_pending_puts(&mut pending_puts, &mut outcomes)?;
        metrics.record_apply_dedup_batch(outcomes.len() as u64, batch_started.elapsed());
        Ok(outcomes)
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

    fn apply_ops_grouped_to_lanes(
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

    fn apply_ops_grouped_to(
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
    pub(super) fn apply_ops_grouped(
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
