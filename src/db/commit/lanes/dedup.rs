use super::*;
use crate::metrics::DedupPutStageTimings;

impl Db {
    pub(in crate::db::commit) fn apply_dedup_indices_to(
        dedup_index: &crate::dedup::DedupIndex,
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        refcount_routing: crate::refcount::RefcountRouting,
        metrics: &MetaMetrics,
        ops: &[WalOp],
        indices: Vec<usize>,
        lsn: Lsn,
        bfg: crate::types::Bfg,
    ) -> Result<Vec<(usize, ApplyOutcome)>> {
        let batch_started = std::time::Instant::now();
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut pending_puts: Vec<(Hash8, DedupValue, Option<Pba>, usize)> = Vec::new();
        // dedup-table mutations drive the global PBA refcount
        // (shared-page bookkeeping). The hot path no longer stages rc
        // deltas on L2P remaps, so any rc movement for a shared pba has
        // to ride along with the DedupPut/Delete/Compare* that gained
        // or lost the reference. Helpers below use the shared
        // `rc_shard_of_pba` routing helper so the planner
        // (`build_lane_dispatch_plan`) and this apply lane agree on
        // which rc shard a PBA lives in — divergence is a deadlock /
        // underflow hazard.
        let rc_shard_for = |pba: Pba| -> usize {
            super::rc_shard_for_routing(refcount_routing, pba, refcount_shards.len())
        };
        let stage_rc = |pba: Pba, delta: i64| -> Result<()> {
            let sid = rc_shard_for(pba);
            refcount_shards[sid].stage(bfg, pba, delta, lsn)?;
            Ok(())
        };
        // Stale-entry tolerance: a dedup_index row can point to a PBA whose rc
        // is already 0. `stage_decref_if_positive` takes one fold-coherent
        // base+slots snapshot and makes a genuine zero a no-op. A preceding
        // plain get is not a safe guard because it can tear across
        // publish-before-clear and return a spurious zero, permanently dropping
        // a required decref.
        let stage_rc_decref_if_live = |pba: Pba| -> Result<()> {
            let sid = rc_shard_for(pba);
            refcount_shards[sid].stage_decref_if_positive(bfg, pba, lsn)?;
            Ok(())
        };
        //         // every dedup put / delete carries an embedded `old_pba`
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
                let dropped =
                    dedup_index.stage_put_many_with_metrics(&entries, lsn, &mut put_timings)?;
                metrics
                    .record_dedup_forward_put_batch(pending_puts.len() as u64, started.elapsed());
                metrics.record_dedup_put_stages(put_timings);
                for (hash, value, old_pba, idx) in pending_puts.drain(..) {
                    // A promote dropped on cuckoo saturation: skip its rc
                    // delta entirely and count it. Saturation only hits
                    // genuinely-new inserts (overwrites short-circuit before
                    // the eviction chain), so `old_pba` is None here and
                    // there is nothing to decref — skipping the +1 incref
                    // keeps rc consistent with the unchanged cuckoo. The
                    // co-committed L2P remap (separate op / lane) still
                    // lands; this is a future dedup miss, not a failed commit.
                    if !dropped.is_empty() && dropped.contains(&hash) {
                        metrics.record_dedup_promote_dropped_saturated(1);
                        outcomes.push((idx, ApplyOutcome::Dedup));
                        continue;
                    }
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
                        let placed = dedup_index.stage_put_with_metrics(
                            *hash,
                            *value,
                            lsn,
                            &mut put_timings,
                        )?;
                        metrics.record_dedup_forward_put(started.elapsed());
                        metrics.record_dedup_put_stages(put_timings);
                        if placed {
                            let new_pba = value.head_pba();
                            if *old_pba != Some(new_pba) {
                                if let Some(op) = *old_pba {
                                    stage_rc_decref_if_live(op)?;
                                }
                                stage_rc(new_pba, 1)?;
                            }
                        } else {
                            // Dropped on saturation: skip rc (new insert ⇒
                            // old_pba None ⇒ nothing to decref, no incref).
                            metrics.record_dedup_promote_dropped_saturated(1);
                        }
                    }
                    outcomes.push((idx, ApplyOutcome::Dedup));
                }
                WalOp::DedupDelete { hash, old_pba } => {
                    flush_pending_puts(&mut pending_puts, &mut outcomes)?;
                    let started = std::time::Instant::now();
                    dedup_index.stage_delete(hash, lsn)?;
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
                        dedup_index.stage_delete(hash, lsn)?;
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
                        let placed = dedup_index.stage_put_with_metrics(
                            *hash,
                            *new_value,
                            lsn,
                            &mut put_timings,
                        )?;
                        metrics.record_dedup_forward_put(started.elapsed());
                        metrics.record_dedup_put_stages(put_timings);
                        if placed {
                            let old_pba = old_value.head_pba();
                            let new_pba = new_value.head_pba();
                            if old_pba != new_pba {
                                stage_rc_decref_if_live(old_pba)?;
                                stage_rc(new_pba, 1)?;
                            }
                        } else {
                            // Defensive: a compare-put overwrites an existing
                            // (matched) entry, which cannot saturate. Count it
                            // and skip rc so we stay consistent if it ever does.
                            metrics.record_dedup_promote_dropped_saturated(1);
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
}
