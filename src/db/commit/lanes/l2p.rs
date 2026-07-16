use super::*;
use std::sync::atomic::Ordering;

/// rc-authoritative install (lane path): push incref(new head_pba) +
/// decref(old head_pba) as paired `RcApplyAction`s. The lane path only runs
/// for snapshot-free volumes — snapshot-bearing volumes route to the serial
/// `apply_l2p_remap`, which performs the snap-pin decref suppression — so the
/// decref is unconditional here. Net-collapsed per-pba in
/// `apply_refcount_bucket_to_tree`; the decref carries
/// `remap_freed_candidate` so a net `rc==0` surfaces `freed_pba` (consumed by
/// onyx's writer cleanup → retire → GcRunner `rc==0` reclaim). A `prev` with
/// the FLAG_ZERO placeholder bit is not a real mapping → no decref (mirrors
/// `record_dead`).
///
/// `surface`: whether the outcome can carry a freed pba — `true` for
/// `L2pRemap`/`L2pRemapRange` (the `apply_ops_laned` merger writes `freed_pba`
/// / `freed_pbas`), `false` for `L2pPut` whose `L2pPrev` outcome has no freed
/// slot (surfacing it would hit the merge's `unreachable!`). When `false` the
/// rc is still decref'd correctly; the freed pba is just not surfaced here and
/// is reclaimed by onyx's GC paths (rc stays authoritative either way).
#[inline]
fn push_rc_install(
    rc_actions: &mut Vec<RcApplyAction>,
    op_idx: usize,
    new_value: L2pValue,
    prev: Option<L2pValue>,
    range_op: bool,
    surface: bool,
) {
    rc_actions.push(RcApplyAction {
        op_idx,
        pba: new_value.head_pba(),
        delta: 1,
        standalone_refcount: false,
        remap_freed_candidate: false,
        range_op,
    });
    if let Some(old) = prev.filter(|p| p.0[27] & 0x02 == 0) {
        rc_actions.push(RcApplyAction {
            op_idx,
            pba: old.head_pba(),
            delta: -1,
            standalone_refcount: false,
            remap_freed_candidate: surface,
            range_op,
        });
    }
}

/// rc-authoritative delete (lane path): decref(old head_pba) only — a delete
/// installs no new reference. `remap_freed_candidate` is false: the
/// `L2pDelete` outcome (`L2pPrev`) has no `freed_pba` slot, so a delete that
/// drives rc to 0 is retired by onyx's discard / GC-compactor path rather
/// than surfaced here (rc stays correct either way; GcRunner's `rc==0` Gate-1
/// gates the actual free).
#[inline]
fn push_rc_delete(rc_actions: &mut Vec<RcApplyAction>, op_idx: usize, prev: Option<L2pValue>) {
    if let Some(old) = prev.filter(|p| p.0[27] & 0x02 == 0) {
        rc_actions.push(RcApplyAction {
            op_idx,
            pba: old.head_pba(),
            delta: -1,
            standalone_refcount: false,
            remap_freed_candidate: false,
            range_op: false,
        });
    }
}

impl Db {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::db::commit) fn apply_l2p_bucket(
        volume: Arc<Volume>,
        sid: usize,
        indices: Vec<L2pBucketEntry>,
        lsn: Lsn,
        bfg: crate::types::Bfg,
        ops: &[WalOp],
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        refcount_routing: crate::refcount::RefcountRouting,
        metrics: &MetaMetrics,
        // rc-authoritative: when true, every applied L2P-value install
        // (Put/Remap/RemapRange) pushes a +1 `RcApplyAction` for its new
        // head_pba so refcount counts live L2P references. Must mirror the
        // planner footprint in `build_lane_dispatch_plan`. Default-off path
        // () emits nothing — `rc_actions` stays empty.
        rc_authoritative: bool,
        // BFG: this volume's live snapshot `capture_watermark`s,
        // captured by the dispatcher before the lane runs. Arms the birth COW-kill
        // (filtered per dying-page lsn via `youngest_snap_below`) and gates which
        // COW-displaced L2P pages enter the HEAD page-deadlist. Empty (no live
        // snapshot, or the replay path) records nothing.
        snapshot_wms: Vec<Lsn>,
        // BFG: this volume's clone COW-kill pinner set
        // ({B_C} ∪ own-snap wms ∪ descendant branch points), captured by the
        // dispatcher. Empty for non-clones and on replay. Only read by the
        // tree-mode clone arm of `cow_for_write`; the buffered path returns early.
        clone_cow_pinners: Vec<Lsn>,
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
                bfg,
                ops,
                refcount_shards,
                refcount_routing,
                metrics,
                rc_authoritative,
            );
        }
        let tree_lock_started = std::time::Instant::now();
        let mut tree = shard.tree.write();
        // BFG: arm the birth-authoritative non-clone
        // COW-kill decision with this commit's youngest-snapshot lsn (captured
        // by the dispatcher, same value the matching `drain_page_deaths_into`
        // uses below). The buffered shards returned early above; this is the
        // tree-locked laned COW path.
        tree.set_snapshot_wms(snapshot_wms);
        // clone COW-kill: the page-rc-independent clone COW-kill pinner set (separate field
        // so the deadlist drain keeps reading `snapshot_wms`; empty for non-clones).
        tree.set_clone_cow_pinners(clone_cow_pinners);
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
            // lane unification unification: flatten every unguarded `L2pRemap`
            // (`Single`) and every `RangeSlice` LBA into one batch list,
            // then leaf-run-batch ACROSS entry-kind boundaries. The
            // previous previous range-slice path path leaf-run-grouped each RangeSlice
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
                for ((batch_i, (_lba, new_value)), prev) in accepted_batch_idx
                    .into_iter()
                    .zip(entries.into_iter())
                    .zip(prev_values.into_iter())
                {
                    let b = &batch_lbas[batch_i];
                    if rc_authoritative {
                        push_rc_install(
                            &mut rc_actions,
                            b.op_idx,
                            new_value,
                            prev,
                            b.range_offset.is_some(),
                            true,
                        );
                    } else {
                        super::apply::record_dead(&volume, prev, lsn);
                    }
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

            // lifecycle journal cutover: non-batchable ops (Put / Delete / guarded
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
                        if rc_authoritative {
                            push_rc_install(&mut rc_actions, idx, *value, prev, false, false);
                        } else {
                            super::apply::record_dead(&volume, prev, lsn);
                        }
                        l2p_put_count += 1;
                        ApplyOutcome::L2pPrev(prev)
                    }
                    WalOp::L2pDelete { lba, .. } => {
                        let prev = tree.delete_at_lsn_deferred_finish(*lba, lsn)?;
                        // rc-authoritative: decref the deleted reference's PBA
                        // inline (paired with the increfs at install).                         // (flag off): no rc movement; the deleted PBA's reclaim
                        // rode the now-removed reverify scan.
                        if rc_authoritative {
                            push_rc_delete(&mut rc_actions, idx, prev);
                        }
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
                            let gp_sid = refcount_routing.shard_for_pba(*gp, refcount_shards.len());
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
                        // rc-authoritative: incref(new) + decref(old), paired
                        // (guarded dedup-hit included — its L2P-pointer edge is
                        // independent of the DedupPut membership edge).
                        if rc_authoritative {
                            push_rc_install(&mut rc_actions, idx, *new_value, prev, false, true);
                        } else {
                            super::apply::record_dead(&volume, prev, lsn);
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
        if apply_result.is_ok() {
            if let Some(mut guard) = read_view_guard.take() {
                *guard = Arc::new(tree.snapshot_read_view());
            } else {
                super::apply::publish_l2p_read_view(shard, &tree);
            }
            // BFG: the laned tree-mode apply COWs each
            // root→leaf path; drain the page-death witness into the HEAD
            // page-deadlist. This is the dominant write path (single +
            // batched L2pPut/L2pDelete/unguarded L2pRemap all land here),
            // so it MUST drain — otherwise the witness leaks across ops.
            super::apply::drain_page_deaths_into(&volume.page_dead_list[sid], &mut tree);
            // BFG: same site for the per-clone page-livelist
            // witness (empty for non-clones).
            super::apply::drain_live_events_into(&volume.page_live_list, &mut tree);
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
    /// Visible to the rest of `db::commit` so the direct-apply
    /// fast path in `commit.rs` can invoke it on the caller thread,
    /// skipping the apply-lane channel hop. Buffer mode never produces
    /// `rc_actions`, so the returned `L2pBucketApplyResult.rc_actions`
    /// is always empty here.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::db::commit) fn apply_l2p_bucket_buffer(
        volume: Arc<Volume>,
        sid: usize,
        indices: Vec<L2pBucketEntry>,
        lsn: Lsn,
        bfg: crate::types::Bfg,
        ops: &[WalOp],
        refcount_shards: &[Arc<crate::refcount::RcShard>],
        refcount_routing: crate::refcount::RefcountRouting,
        metrics: &MetaMetrics,
        // See `apply_l2p_bucket`: rc-authoritative emits +1 per applied install.
        rc_authoritative: bool,
    ) -> Result<L2pBucketApplyResult> {
        use super::l2p_buffer::BufferLookup;
        let shard = &volume.shards[sid];
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut rc_actions = Vec::new();
        // Per-LBA `cur` reads consult [`shard.l2p_buffer`] first and fall
        // through to a freshly-loaded `shard.read_view` on Absent. Re-fetch
        // per fallthrough — capturing the read view once at function entry
        // is **not** race-free vs the background compactor: a compactor
        // cycle that runs after the snapshot finishes `swap_for_compaction`
        // → `publish_l2p_read_view` → `finish_compaction` between the
        // bucket start and a later `buffer.lookup`, at which point the
        // entry is gone from `draining` and the stale snapshot does not
        // yet see it in the tree, yielding a spurious None `prev`.
        // BFG deferred-outcome follow-up #3a reproducer hits this
        // deterministically with overlapping L2pRemapRange + an aggressive
        // compactor.
        let read_view_prepare = std::time::Duration::ZERO;
        let load_view = || -> Arc<crate::paged::ReadView> { shard.read_view.read().clone() };
        let mut l2p_put_count = 0u64;
        let mut l2p_delete_count = 0u64;
        let mut l2p_remap_count = 0u64;
        let mut l2p_remap_range_lba_count = 0u64;
        let bucket_started = std::time::Instant::now();
        let ops_started = std::time::Instant::now();
        let ops_result = (|| -> Result<()> {
            #[derive(Clone, Copy)]
            enum FlatKind {
                Put,
                Delete,
                Remap { guard: Option<(Pba, u32)> },
                Range { offset: u32 },
            }
            #[derive(Clone, Copy)]
            struct FlatOp {
                op_idx: usize,
                lba: Lba,
                value: Option<L2pValue>,
                kind: FlatKind,
            }

            let mut flat = Vec::new();
            let mut range_partials: std::collections::HashMap<
                usize,
                (Box<[bool]>, Vec<Option<L2pValue>>),
            > = std::collections::HashMap::new();
            for entry in &indices {
                match entry {
                    L2pBucketEntry::Single(idx) => match &ops[*idx] {
                        WalOp::L2pPut { lba, value, .. } => flat.push(FlatOp {
                            op_idx: *idx,
                            lba: *lba,
                            value: Some(*value),
                            kind: FlatKind::Put,
                        }),
                        WalOp::L2pDelete { lba, .. } => flat.push(FlatOp {
                            op_idx: *idx,
                            lba: *lba,
                            value: None,
                            kind: FlatKind::Delete,
                        }),
                        WalOp::L2pRemap {
                            lba,
                            new_value,
                            guard,
                            ..
                        } => flat.push(FlatOp {
                            op_idx: *idx,
                            lba: *lba,
                            value: Some(*new_value),
                            kind: FlatKind::Remap { guard: *guard },
                        }),
                        other => unreachable!("L2P bucket holds only L2P ops; saw {other:?}"),
                    },
                    L2pBucketEntry::RangeSlice {
                        op_idx,
                        lba_offsets,
                    } => {
                        let (start_lba, values) = match &ops[*op_idx] {
                            WalOp::L2pRemapRange {
                                start_lba, values, ..
                            } => (*start_lba, values.as_ref()),
                            other => {
                                unreachable!("RangeSlice op_idx points to non-range op: {other:?}")
                            }
                        };
                        range_partials.entry(*op_idx).or_insert_with(|| {
                            (
                                vec![false; values.len()].into_boxed_slice(),
                                vec![None; values.len()],
                            )
                        });
                        for &offset in lba_offsets.iter() {
                            flat.push(FlatOp {
                                op_idx: *op_idx,
                                lba: start_lba + u64::from(offset),
                                value: Some(values[offset as usize]),
                                kind: FlatKind::Range { offset },
                            });
                        }
                    }
                }
            }

            // Read the three visible BFG slots once each, then load one fresh
            // read-view for every miss. Loading the view after the buffer scan
            // preserves publish-before-clear against the concurrent compactor.
            let lbas: Vec<Lba> = flat.iter().map(|op| op.lba).collect();
            let buffer_values = shard.l2p_buffer.lookup_many_for_open_bfg(bfg, &lbas);
            let mut initial_values = vec![None; flat.len()];
            let mut absent = Vec::new();
            for (idx, lookup) in buffer_values.into_iter().enumerate() {
                match lookup {
                    BufferLookup::Present(value) => initial_values[idx] = Some(value),
                    BufferLookup::Tombstone => {}
                    BufferLookup::Absent => absent.push(idx),
                }
            }
            if !absent.is_empty() {
                load_view().multi_get_into(&lbas, &absent, &mut initial_values)?;
            }

            // Guard PBAs are immutable for this bucket's apply window (their
            // RC lanes are in the dispatch footprint), so read each distinct
            // PBA once per refcount shard.
            let mut guard_passed = vec![true; flat.len()];
            let mut guard_buckets: Vec<std::collections::HashMap<Pba, Vec<(usize, u32)>>> = (0
                ..refcount_shards.len())
                .map(|_| std::collections::HashMap::new())
                .collect();
            for (idx, op) in flat.iter().enumerate() {
                if let FlatKind::Remap {
                    guard: Some((pba, min_rc)),
                } = op.kind
                {
                    let rc_sid = refcount_routing.shard_for_pba(pba, refcount_shards.len());
                    guard_buckets[rc_sid]
                        .entry(pba)
                        .or_default()
                        .push((idx, min_rc));
                }
            }
            for (rc_sid, bucket) in guard_buckets.into_iter().enumerate() {
                if bucket.is_empty() {
                    continue;
                }
                let mut guards: Vec<_> = bucket.into_iter().collect();
                guards.sort_by_key(|(pba, _)| *pba);
                let pbas: Vec<Pba> = guards.iter().map(|(pba, _)| *pba).collect();
                let counts = refcount_shards[rc_sid].get_many(&pbas)?;
                for ((_, members), rc) in guards.into_iter().zip(counts) {
                    for (flat_idx, min_rc) in members {
                        guard_passed[flat_idx] = rc >= min_rc;
                    }
                }
            }

            let mut latest: std::collections::HashMap<Lba, Option<L2pValue>> =
                std::collections::HashMap::new();
            for (flat_idx, op) in flat.iter().copied().enumerate() {
                if !guard_passed[flat_idx] {
                    l2p_remap_count += 1;
                    outcomes.push((
                        op.op_idx,
                        ApplyOutcome::L2pRemap {
                            applied: false,
                            prev: None,
                            freed_pba: None,
                        },
                    ));
                    continue;
                }
                let cur = latest
                    .get(&op.lba)
                    .copied()
                    .unwrap_or(initial_values[flat_idx]);
                let seq_rejected = op.value.is_some_and(|value| {
                    !matches!(op.kind, FlatKind::Delete)
                        && super::apply::seq_guard_rejects(value.seq(), cur.as_ref())
                });

                match op.kind {
                    FlatKind::Put => {
                        l2p_put_count += 1;
                        outcomes.push((op.op_idx, ApplyOutcome::L2pPrev(cur)));
                        if seq_rejected {
                            continue;
                        }
                        let value = op.value.expect("put carries a value");
                        latest.insert(op.lba, Some(value));
                        if rc_authoritative {
                            push_rc_install(&mut rc_actions, op.op_idx, value, cur, false, false);
                        } else {
                            super::apply::record_dead(&volume, cur, lsn);
                        }
                    }
                    FlatKind::Delete => {
                        l2p_delete_count += 1;
                        outcomes.push((op.op_idx, ApplyOutcome::L2pPrev(cur)));
                        latest.insert(op.lba, None);
                        if rc_authoritative {
                            push_rc_delete(&mut rc_actions, op.op_idx, cur);
                        }
                    }
                    FlatKind::Remap { .. } => {
                        l2p_remap_count += 1;
                        outcomes.push((
                            op.op_idx,
                            ApplyOutcome::L2pRemap {
                                applied: !seq_rejected,
                                prev: cur,
                                freed_pba: None,
                            },
                        ));
                        if seq_rejected {
                            continue;
                        }
                        let value = op.value.expect("remap carries a value");
                        latest.insert(op.lba, Some(value));
                        if rc_authoritative {
                            push_rc_install(&mut rc_actions, op.op_idx, value, cur, false, true);
                        } else {
                            super::apply::record_dead(&volume, cur, lsn);
                        }
                    }
                    FlatKind::Range { offset } => {
                        l2p_remap_range_lba_count += 1;
                        let (applied, prevs) = range_partials
                            .get_mut(&op.op_idx)
                            .expect("range partial allocated while flattening");
                        prevs[offset as usize] = cur;
                        if seq_rejected {
                            continue;
                        }
                        let value = op.value.expect("range remap carries a value");
                        applied[offset as usize] = true;
                        latest.insert(op.lba, Some(value));
                        if rc_authoritative {
                            push_rc_install(&mut rc_actions, op.op_idx, value, cur, true, true);
                        } else {
                            super::apply::record_dead(&volume, cur, lsn);
                        }
                    }
                }
            }

            let mut mutations: Vec<_> = latest.into_iter().collect();
            mutations.sort_by_key(|(lba, _)| *lba);
            shard.l2p_buffer.apply_batch_at_bfg(bfg, &mutations, lsn);
            for (op_idx, (applied, prevs)) in range_partials {
                outcomes.push((
                    op_idx,
                    ApplyOutcome::L2pRemapRange {
                        applied,
                        prevs: prevs.into_boxed_slice(),
                        freed_pbas: Vec::new(),
                    },
                ));
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
}
