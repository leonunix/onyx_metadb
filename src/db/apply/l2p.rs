use super::*;

/// Apply-time CAS gate. Returns true iff `new_seq` is stale relative
/// to `cur` and the op must be skipped. `seq == 0` on either side is
/// the no-guard sentinel (legacy callers like `DedupScanner` and
/// direct `insert`) — the check is bypassed and the op applies. The
/// guard is strict less-than: `new_seq == cur_seq` accepts. Onyx's
/// buffer seq is globally monotonic per append, so equality only
/// happens when a recovered buffer entry replays its own previously
/// committed write (mark_flushed is memory-only — after a clean
/// shutdown + reopen, the recovered entry is re-flushed even though
/// L2P already carries its seq). Accepting on equality lets the
/// retry land instead of leaking the freshly-allocated PBA.
/// See `L2pValue::seq` for the wire layout.
#[inline]
pub(in crate::db) fn seq_guard_rejects(new_seq: u64, cur: Option<&L2pValue>) -> bool {
    if new_seq == 0 {
        return false;
    }
    match cur {
        Some(c) => {
            let cs = c.seq();
            cs != 0 && new_seq < cs
        }
        None => false,
    }
}

/// Stamp the incoming value with the current apply `lsn` as its
/// `birth_lsn` if the caller did not already attach one (sentinel 0).
/// Promote / dedup-hit / scanner-remap callers carry the source PBA's
/// original birth_lsn in the value and want it preserved; fresh writes
/// arrive with birth_lsn=0 and get stamped here so Phase 2's per-volume
/// dead-list emitter ([[no-refcount-hot-path-design]]) can read it
/// directly off `ApplyOutcome::L2pRemap.prev` without an extra
/// refcount-shard lookup.
#[inline]
pub(in crate::db) fn stamp_birth_lsn(value: L2pValue, lsn: Lsn) -> L2pValue {
    if value.birth_lsn() == 0 {
        value.with_birth_lsn(lsn)
    } else {
        value
    }
}

/// Emit `(prev.head_pba, prev.birth_lsn, death_lsn=lsn)` into the
/// volume's in-memory dead-list buffer if `prev` represents a real
/// mapping (i.e. not a `FLAG_ZERO` placeholder). The buffer is drained
/// at the next checkpoint flush; WAL replay re-emits whatever hasn't
/// been written to a segment yet, so this hook is safe to fire from
/// both live commit and recovery paths.
#[inline]
pub(in crate::db) fn record_dead(volume: &Volume, prev: Option<L2pValue>, death_lsn: Lsn) {
    if let Some(old) = prev {
        if old.0[27] & 0x02 != 0 {
            return;
        }
        volume.dead_list.push(crate::deadlist::DeadRecord {
            pba: old.head_pba(),
            birth_lsn: old.birth_lsn(),
            death_lsn,
        });
    }
}

/// Apply one [`WalOp::L2pRemap`]. Mutates the L2P shard
/// (buffer or tree, depending on the B2 toggle) and emits the dead-list
/// record for the previous mapping. Phase 5 rule: hot-path L2P remaps
/// are **rc-neutral**. The only events that bump global refcounts are
/// `PromotionChunk` (clone walker), `FreePbas` (Lineage GC), and the
/// `DedupPut`/`DedupDelete`/`Dedup{Compare,}{Put,Delete}` family — all
/// dispatched via [`apply_dedup_put_with_rc`] / [`apply_dedup_delete_with_rc`]
/// / [`apply_promotion_chunk`] / [`apply_free_pbas`]. See the
/// [[no-refcount-hot-path-design]] note in `apply_op_bare`.
///
/// `snap_infos` is currently unused: Phase 5's pinning is enforced via
/// `birth_lsn` (lineage GC consumer side) rather than the per-remap
/// snap-pin walk that the pre-Phase-5 hot path performed. Kept on the
/// signature so callers (`apply_op_bare`, replay) don't churn; remove
/// once all entry points migrate.
///
/// `guard` still applies — onyx-side dedup-hit promote relies on a
/// liveness floor read of a target PBA's rc to gate the remap.
pub(in crate::db) fn apply_l2p_remap(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    vol_ord: VolumeOrdinal,
    lba: Lba,
    new_value: L2pValue,
    guard: Option<(Pba, u32)>,
    _snap_infos: &[SnapInfo],
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("L2pRemap for unknown volume ord {vol_ord}"))
    })?;
    let l2p_sid = shard_for_key_l2p(&volume.shards, lba);
    let use_buffer = volume.shards[l2p_sid].use_buffer;

    // The L2P shard write lock (or, on the B2 path, its serialising
    // role against compactor cycles) brackets the guard read + L2P
    // write so the "guard passed" decision and the put land atomically
    // against concurrent ops on this (vol, lba). Per-shard apply
    // serialisation comes from the apply lane (one worker per shard);
    // this lock prevents commits from racing the compactor's
    // `tree.write()` mid-cycle.
    let mut tree = volume.shards[l2p_sid].tree.write();

    if let Some((gp, min_rc)) = guard {
        let gp_sid = shard_for_key(refcount_shards, gp);
        let cur = refcount_shards[gp_sid].rc.get(gp)?;
        if cur < min_rc {
            return Ok(ApplyOutcome::L2pRemap {
                applied: false,
                prev: None,
                freed_pba: None,
            });
        }
    }

    // Read current value: buffer-first when B2 is active, else tree.
    let cur = if use_buffer {
        match volume.shards[l2p_sid].l2p_buffer.lookup(lba) {
            crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
            crate::db::l2p_buffer::BufferLookup::Tombstone => None,
            crate::db::l2p_buffer::BufferLookup::Absent => tree.get(lba)?,
        }
    } else {
        tree.get(lba)?
    };
    if seq_guard_rejects(new_value.seq(), cur.as_ref()) {
        return Ok(ApplyOutcome::L2pRemap {
            applied: false,
            prev: cur,
            freed_pba: None,
        });
    }
    let new_value = stamp_birth_lsn(new_value, lsn);

    // Drive the mutation. In B2 the prev value we just read IS the
    // pre-write state — buffer.insert is a swap, so `cur` (read above)
    // equals what tree.insert_at_lsn would have returned.
    let prev = if use_buffer {
        volume.shards[l2p_sid]
            .l2p_buffer
            .insert(lba, new_value, lsn);
        cur
    } else {
        tree.insert_at_lsn(lba, new_value, lsn)?
    };
    record_dead(volume, prev, lsn);

    // B2 path: the compactor will publish on its next cycle, so commit
    // here only needs to make the buffer entry observable, which
    // `buffer.insert` did atomically above.
    if !use_buffer {
        publish_l2p_read_view(&volume.shards[l2p_sid], &tree);
    }

    Ok(ApplyOutcome::L2pRemap {
        applied: true,
        prev,
        freed_pba: None,
    })
}

/// Apply one [`WalOp::L2pRemapRange`]: per-LBA L2P remap semantics over
/// `[start_lba .. start_lba + values.len())` of one volume, all under
/// one apply call. Equivalent in net effect to N calls of
/// [`apply_l2p_remap`] with `guard = None` on each LBA, with three
/// amortizations:
///
/// 1. **Tree write lock per shard, not per LBA**: LBAs are bucketed by
///    L2P shard once, then each shard's tree is locked once for the
///    whole bucket. Onyx's passthrough caller produces contiguous LBAs
///    that usually land in one shard (and often one leaf, since
///    `shard_for_key_l2p` hashes `lba >> LEAF_SHIFT`).
/// 2. **Refcount net delta across the range**: incref/decref pairs that
///    cancel within the same range never touch the refcount shard.
///    Same per-PBA net-delta collapse that the per-LBA path uses, just
///    aggregated over more LBAs.
/// 3. **WAL / op-dispatch / bucket-assembly cost**: the entire range is
///    one record, one outcome slot, one apply-lane dispatch.
///
/// Range ops are always unguarded; the dedup-hit path that needs a
/// guard keeps emitting per-LBA `L2pRemap`. The snap-pin check stays
/// per-LBA inside the range — a range-aware snap-pin walk is the
/// Stage 2 amortization tracked as `metadb_leaf_pin_todo`.
#[allow(clippy::too_many_arguments)]
pub(in crate::db) fn apply_l2p_remap_range(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    vol_ord: VolumeOrdinal,
    start_lba: Lba,
    values: &[L2pValue],
    _snap_infos: &[SnapInfo],
) -> Result<ApplyOutcome> {
    let _ = refcount_shards;
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("L2pRemapRange for unknown volume ord {vol_ord}"))
    })?;

    let n = values.len();
    debug_assert!(n > 0, "L2pRemapRange with empty values reached apply");

    // Bucket LBAs by L2P shard so each shard's tree mutex is taken once
    // for the whole range. Mirrors apply_l2p_range_delete's shape.
    let shard_count = volume.shards.len();
    let mut shard_buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
    for i in 0..n {
        let lba = start_lba + i as u64;
        shard_buckets[shard_for_key_l2p(&volume.shards, lba)].push(i);
    }

    let mut prevs: Vec<Option<L2pValue>> = vec![None; n];
    let mut applied: Vec<bool> = vec![false; n];

    for (l2p_sid, indices) in shard_buckets.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let use_buffer = volume.shards[l2p_sid].use_buffer;
        let mut tree = volume.shards[l2p_sid].tree.write();

        for &i in indices {
            let lba = start_lba + i as u64;
            let new_value = values[i];

            // Buffer-first lookup matching apply_l2p_remap's path.
            let cur = if use_buffer {
                match volume.shards[l2p_sid].l2p_buffer.lookup(lba) {
                    crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
                    crate::db::l2p_buffer::BufferLookup::Tombstone => None,
                    crate::db::l2p_buffer::BufferLookup::Absent => tree.get(lba)?,
                }
            } else {
                tree.get(lba)?
            };

            if seq_guard_rejects(new_value.seq(), cur.as_ref()) {
                // Stale write — leave L2P untouched; surface the rejecting
                // value as prev so onyx-side cleanup logic still sees a
                // per-LBA outcome slot. applied stays false (default).
                prevs[i] = cur;
                continue;
            }
            let new_value = stamp_birth_lsn(new_value, lsn);

            let prev = if use_buffer {
                volume.shards[l2p_sid]
                    .l2p_buffer
                    .insert(lba, new_value, lsn);
                cur
            } else {
                tree.insert_at_lsn(lba, new_value, lsn)?
            };
            record_dead(volume, prev, lsn);
            prevs[i] = prev;
            applied[i] = true;
        }

        // Publish once per shard, after all LBAs in this bucket mutated.
        if !use_buffer {
            publish_l2p_read_view(&volume.shards[l2p_sid], &tree);
        }
    }

    // Phase 5: hot-path L2P remaps are rc-neutral (see `apply_l2p_remap`
    // for the rationale). `freed_pbas` is surfaced by `apply_free_pbas`
    // / lineage GC; this outcome always returns an empty vec.
    Ok(ApplyOutcome::L2pRemapRange {
        applied: applied.into_boxed_slice(),
        prevs: prevs.into_boxed_slice(),
        freed_pbas: Vec::new(),
    })
}

/// Apply one [`WalOp::L2pRangeDelete`]. Walks the `captured` list,
/// deleting each lba from its volume's L2P shard and emitting a
/// decref against `old_pba` — modulo birth/death LSN suppression: if
/// `min_alive_snap_lsn` is `Some(snap_lsn)` and `pba.birth_lsn <=
/// snap_lsn`, the snapshot may still reference this pba and the
/// decref is suppressed. `drop_snapshot` later compensates via the
/// deadlist (next session) or via `diff_subtrees`-derived
/// `pba_decrefs` (current).
///
/// Replay safety: the captured list is authoritative — both live
/// apply and replay consume the same (lba, pba) pairs, and birth_lsn
/// is part of the rc tree's persistent state, so suppress decisions
/// reproduce across restarts.
/// Range-delete's `Db::range_delete` caller uses apply_gate.write
/// (same pattern as `drop_snapshot`) to exclude concurrent commits
/// while plan + submit + apply run, so captured is consistent with
/// the tree state at apply time.
pub(in crate::db) fn apply_l2p_range_delete(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    vol_ord: VolumeOrdinal,
    captured: &[(Lba, L2pValue)],
    snap_infos: &[SnapInfo],
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("L2pRangeDelete for unknown volume ord {vol_ord}"))
    })?;

    // Bucket captured entries by L2P shard so each tree mutex is
    // taken once.
    let shard_count = volume.shards.len();
    let mut shard_buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
    for (idx, (lba, _)) in captured.iter().enumerate() {
        shard_buckets[shard_for_key_l2p(&volume.shards, *lba)].push(idx);
    }

    // For each captured (lba, value): did it have a live mapping? And
    // if so, does any live snap of V still hold the *exact same* full
    // value at that lba? Full-value compare matches audit semantics
    // (distinct `(V, lba, value_28B)` tuples). Both determined while
    // holding the L2P shard mutex; snap reads use `get_at` on the same
    // tree instance, different root.
    let mut was_mapped: Vec<bool> = vec![false; captured.len()];
    let mut snap_pinned: Vec<bool> = vec![false; captured.len()];
    for (sid, indices) in shard_buckets.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let mut tree = volume.shards[sid].tree.write();
        for &idx in indices {
            let (lba, captured_value) = captured[idx];
            let prev = tree.delete_at_lsn(lba, lsn)?;
            was_mapped[idx] = prev.is_some();
            if !snap_infos.is_empty() && was_mapped[idx] {
                // Per-snap fast filter via birth_lsn, same shape as
                // apply_l2p_remap. Look up birth_lsn first under the
                // rc shard mutex, then walk only candidate snaps.
                let captured_birth = lookup_birth_lsn(refcount_shards, captured_value.head_pba())?;
                for s in snap_infos {
                    if let Some(b) = captured_birth {
                        if b > s.created_lsn {
                            continue;
                        }
                    }
                    let snap_root = s.l2p_shard_roots[sid];
                    if let Some(snap_val) = tree.get_at(snap_root, lba)? {
                        if snap_val == captured_value {
                            snap_pinned[idx] = true;
                            break;
                        }
                    }
                }
            }
        }
        publish_l2p_read_view(&volume.shards[sid], &tree);
    }

    // Second pass: precise snap-pin suppress + decref. Bucket by
    // refcount shard so each shard mutex is taken at most once.
    let mut rc_bucket: Vec<Vec<usize>> = vec![Vec::new(); refcount_shards.len()];
    for (idx, (_, value)) in captured.iter().enumerate() {
        if !was_mapped[idx] || snap_pinned[idx] || value.0[27] & 0x02 != 0 {
            continue;
        }
        rc_bucket[shard_for_key(refcount_shards, value.head_pba())].push(idx);
    }

    let mut freed_pbas: Vec<Pba> = Vec::new();
    for (sid, indices) in rc_bucket.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let shard = &refcount_shards[sid];
        for &idx in indices {
            let (_, value) = captured[idx];
            let pba = value.head_pba();
            // Phase 5: hot-path L2pRemap is rc-neutral, so a captured
            // PBA may have `rc == 0` (fresh exclusive write that never
            // entered dedup_index nor was promoted). Decref'ing such a
            // PBA would underflow. The stale-entry skip mirrors the
            // pattern in `apply_dedup_put_with_rc`: only mutate rc when
            // it represents a live "shared via dedup_index /
            // PromotionChunk" reference. Same on replay — `rc.get`
            // observes the in-progress apply lane's state for this
            // LSN.
            if shard.rc.get(pba)? == 0 {
                continue;
            }
            let (pre, new) = shard.rc.stage(pba, -1, lsn)?;
            if new == 0 && pre > 0 {
                freed_pbas.push(pba);
            }
        }
    }

    Ok(ApplyOutcome::RangeDelete { freed_pbas })
}
