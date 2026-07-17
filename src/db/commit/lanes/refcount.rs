use super::*;

impl Db {
    pub(in crate::db::commit) fn apply_refcount_bucket_to_tree(
        rc: Arc<crate::refcount::RcShard>,
        metrics: Arc<MetaMetrics>,
        mut actions: Vec<RcApplyAction>,
        lsn: Lsn,
        bfg: crate::types::Bfg,
    ) -> Result<RcBucketApplyResult> {
        let mut result = RcBucketApplyResult::default();
        if actions.is_empty() {
            return Ok(result);
        }
        let grouping_started = std::time::Instant::now();
        let action_count = actions.len();
        // Random overwrite workloads normally contribute one or two actions
        // per PBA. Grouping those through HashMap<Pba, Vec<_>> allocates a
        // large number of tiny Vecs and then still has to sort the map output.
        // One flat sort makes each PBA group contiguous while retaining the
        // original op order needed when selecting a freed-PBA outcome.
        actions.sort_unstable_by_key(|action| (action.pba, action.op_idx));

        // The normal rc-authoritative remap path contains only derived
        // (non-standalone) actions. Net-collapse those groups first, then
        // stage every distinct PBA with one RcShard batch so the four BFG
        // slot locks and refcount-page reads are amortized across the commit.
        if actions.iter().all(|action| !action.standalone_refcount) {
            let mut coalesced = Vec::new();
            for group in actions.chunk_by(|left, right| left.pba == right.pba) {
                let delta = group.iter().map(|action| action.delta).sum();
                if delta != 0 {
                    coalesced.push((group[0].pba, delta, group));
                }
            }
            let staged_actions: Vec<(Pba, i64)> = coalesced
                .iter()
                .map(|(pba, delta, _)| (*pba, *delta))
                .collect();
            let grouping_elapsed = grouping_started.elapsed();
            let batch_started = std::time::Instant::now();
            let (staged, stage_timings) = rc.stage_batch(bfg, &staged_actions, lsn)?;
            metrics.record_apply_refcount_batch(staged.len() as u64, batch_started.elapsed());
            metrics.record_apply_refcount_batch_breakdown(
                action_count as u64,
                staged_actions.len() as u64,
                grouping_elapsed,
                stage_timings.base_page_lookup,
                stage_timings.fold_lock_wait,
                stage_timings.slot_lock_wait,
                stage_timings.pending_slot_scan,
                stage_timings.delta_merge,
                stage_timings.base_lookup_attempts,
                stage_timings.epoch_retries,
                stage_timings.sampled_pbas,
            );
            for ((pba, _delta, group), (pre, new)) in coalesced.into_iter().zip(staged) {
                if new == 0
                    && pre > 0
                    && let Some(action) = group
                        .iter()
                        .rev()
                        .find(|action| action.remap_freed_candidate)
                {
                    result
                        .remap_freed
                        .push((action.op_idx, pba, action.range_op));
                }
            }
            return Ok(result);
        }

        for group in actions.chunk_by(|left, right| left.pba == right.pba) {
            let pba = group[0].pba;
            // Net-collapse every non-standalone group — including MIXED-sign
            // (rc-authoritative emits both incref(+1) of a new head_pba and
            // decref(-1) of an old one, and two LBAs in the same bucket can
            // hit the same pba with opposite signs, e.g. one LBA dedup-hits P
            // while another overwrites away from P). Summing the net delta and
            // staging ONCE is what keeps the freed-pba surfacing honest: a
            // serial +1 then -1 (or -1 then +1) would transiently cross rc==0
            // and wrongly surface a still-referenced pba as freed. Pre-
            // `apply_l2p_remap` collapsed per-pba net delta for exactly this
            // reason. Standalone refcount ops (which need individual
            // `RefcountNew` outcomes) keep the per-action serial path below.
            let can_coalesce_remap = group.iter().all(|action| !action.standalone_refcount);

            if can_coalesce_remap {
                let delta: i64 = group.iter().map(|action| action.delta).sum();
                if delta == 0 {
                    // Net no-op (e.g. same-pba +1/-1 cancel): do not touch rc,
                    // do not surface — the pba's live-ref count is unchanged.
                    continue;
                }
                let op_started = std::time::Instant::now();
                let op_idxs: Vec<usize> = group.iter().map(|a| a.op_idx).collect();
                // case diagnostic: trace every coalesced stage call so we can
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
                let (pre, new) = match rc.stage(bfg, pba, delta, lsn) {
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

            for action in group.iter().copied() {
                let op_started = std::time::Instant::now();
                let (pre, new) = match rc.stage(bfg, action.pba, action.delta, lsn) {
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
}
