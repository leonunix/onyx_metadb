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
        actions.sort_by_key(|action| action.op_idx);
        let mut by_pba: HashMap<Pba, Vec<RcApplyAction>> = HashMap::new();
        for action in actions {
            by_pba.entry(action.pba).or_default().push(action);
        }
        let mut by_pba: Vec<_> = by_pba.into_iter().collect();
        by_pba.sort_by_key(|(pba, _)| *pba);

        for (pba, group) in by_pba {
            // Net-collapse every non-standalone group — including MIXED-sign
            // (rc-authoritative emits both incref(+1) of a new head_pba and
            // decref(-1) of an old one, and two LBAs in the same bucket can
            // hit the same pba with opposite signs, e.g. one LBA dedup-hits P
            // while another overwrites away from P). Summing the net delta and
            // staging ONCE is what keeps the freed-pba surfacing honest: a
            // serial +1 then -1 (or -1 then +1) would transiently cross rc==0
            // and wrongly surface a still-referenced pba as freed. Pre-            // `apply_l2p_remap` collapsed per-pba net delta for exactly this
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

            for action in group {
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
