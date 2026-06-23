use super::*;

mod dedup;
mod l2p;
mod promotion;
mod volume;

pub(super) use dedup::{apply_dedup_delete_with_rc, apply_dedup_put_with_rc, apply_free_pbas};
pub(super) use l2p::{
    apply_l2p_range_delete, apply_l2p_remap, apply_l2p_remap_range, drain_live_events,
    drain_live_events_into, drain_page_deaths_into, record_dead, scan_l2p_range, seq_guard_rejects,
    stage_delete_rc, stage_remap_rc, stamp_birth_lsn,
};
pub(super) use promotion::{apply_promotion_chunk, apply_promotion_complete};
pub(super) use volume::{
    apply_clone_volume_incref, apply_create_volume, apply_drop_snapshot_pages_and_decrefs,
    apply_drop_volume, build_clone_volume_shards, collect_paged_refcounts_for_roots,
};

/// Must be invoked **before** the caller drops the tree write guard:
/// the guard's lifetime is what makes "tree.root() at publish time" a
/// well-defined moment that includes every just-mutated dirty page.
pub(super) fn publish_l2p_read_view(shard: &L2pShard, tree: &PagedL2p) {
    let view = Arc::new(tree.snapshot_read_view());
    *shard.read_view.write() = view;
}

/// Apply one [`WalOp`] to raw `Db` state. Used by both the live commit
/// path (through `self.apply_op`) and the WAL-replay path (before
/// `Self` exists). Takes individual references so it can run against
/// locally-constructed state during `open`. Private to this module
/// because `Shard` is.
///
/// `lsn` is the WAL record LSN the op is applying at; used only by
/// `DropSnapshot` to stamp page generations for idempotent replay.
/// Callers that don't have an exact LSN (e.g. `replay_into`, which
/// passes the enclosing record's LSN) just pass the best available
/// value — the only correctness requirement is that `lsn` strictly
/// increases across apply invocations, which is already guaranteed by
/// WAL LSN monotonicity.
///
/// `DropSnapshot` mutates the in-memory manifest; callers that need
/// that side effect must handle the `manifest.snapshots.retain(...)`
/// themselves after calling this function. This split keeps
/// `apply_op_bare` usable in the replay path (which owns a bare
/// `Manifest`) and the live path (which owns a `Mutex<ManifestState>`).
///
/// `snap_info_for_vol` is a per-volume callback returning the live
/// snapshot view info for that volume (empty `Vec` for no-snap case).
/// [`apply_l2p_remap`] / [`apply_l2p_range_delete`] consult it to gate
/// decref decisions:
///
/// 1. Fast filter: if `old_pba.birth_lsn > min(snap.created_lsn)`,
///    no live snap can pin this content → decref.
/// 2. Otherwise read each snap's L2P at `(V, lba)`; suppress decref
///    iff any snap has that lba mapping to `old_pba`.
///
/// Replaces the legacy `leaf_was_shared` proxy. Callers that don't
/// have a snap-aware state (unit tests) can pass `&|_| Vec::new()`.

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_op_bare(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    dedup_index: &crate::dedup::DedupIndex,
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    txg: crate::types::Txg,
    op: &WalOp,
    snap_info_for_vol: &dyn Fn(VolumeOrdinal) -> Vec<SnapInfo>,
    // rc-authoritative (serial / replay path): every applied L2P install does
    // the traditional inline incref(new)+decref(old) pair with snap-pin decref
    // suppression (`stage_remap_rc` / `stage_delete_rc`); `record_dead` is NOT
    // called (the dead-list / lineage GC stays dormant — inline rc is the sole
    // driver, balanced 1:1). Mirrors the lane path in lanes/l2p.rs. The inline
    // `rc.stage(±1)` is replay-safe under the same LSN-cutoff idempotency as
    // the existing dedup rc (see the DedupPut comment below).
    rc_authoritative: bool,
) -> Result<ApplyOutcome> {
    match op {
        WalOp::L2pPut {
            vol_ord,
            lba,
            value,
        } => {
            let volume = volumes.get(vol_ord).ok_or_else(|| {
                MetaDbError::Corruption(format!("L2pPut for unknown volume ord {vol_ord}"))
            })?;
            let sid = shard_for_key_l2p(&volume.shards, *lba);
            let use_buffer = volume.shards[sid].use_buffer;
            let mut tree = volume.shards[sid].tree.write();
            // ZFS port Phase 4 Step 4 (S1): fetch the volume's snapshots BEFORE
            // the COW so the birth-authoritative non-clone COW-kill can be armed;
            // reused for the page-deadlist drain below. An L2pPut takes no
            // snapshot, so the pre/post-put snapshot set is identical.
            let snap_infos = snap_info_for_vol(*vol_ord);
            // A3 cutover: tree-mode COW stages page-rc deltas into this
            // commit's TXG slot (no-op for the buffered path).
            tree.set_current_txg(txg);
            tree.set_snapshot_wms(l2p::snapshot_wms_of(&snap_infos));
            // L2pPut returns the rejecting `cur` as `L2pPrev(Some(cur))`;
            // caller distinguishes accept vs reject by comparing
            // `value.seq()` against `cur.seq()`.
            let cur = if use_buffer {
                match volume.shards[sid].l2p_buffer.lookup_for_open_txg(txg, *lba) {
                    crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
                    crate::db::l2p_buffer::BufferLookup::Tombstone => None,
                    crate::db::l2p_buffer::BufferLookup::Absent => tree.get(*lba)?,
                }
            } else {
                tree.get(*lba)?
            };
            if seq_guard_rejects(value.seq(), cur.as_ref()) {
                return Ok(ApplyOutcome::L2pPrev(cur));
            }
            let stamped = stamp_birth_lsn(*value, lsn);
            let prev = if use_buffer {
                volume.shards[sid]
                    .l2p_buffer
                    .insert_at_txg(txg, *lba, stamped, lsn);
                cur
            } else {
                let p = tree.insert_at_lsn(*lba, stamped, lsn)?;
                publish_l2p_read_view(&volume.shards[sid], &tree);
                p
            };
            // rc-authoritative: incref(new) + decref(old) inline, with
            // snap-pin decref suppression (`stage_remap_rc`). `L2pPrev` has no
            // freed-pba slot, so a net rc==0 here is reclaimed by onyx's GC
            // paths (rc stays authoritative). Phase-5 (flag off): dead-list +
            // lineage GC. (`snap_infos` hoisted above for the S1 COW-kill arm.)
            if rc_authoritative {
                stage_remap_rc(
                    refcount_shards,
                    txg,
                    &tree,
                    sid,
                    *lba,
                    stamped,
                    prev,
                    &snap_infos,
                    lsn,
                )?;
            } else {
                record_dead(volume, prev, lsn);
            }
            // ZFS port Phase 2: L2pPut COWs the root→leaf path on a direct
            // (non-buffer) write; record any page it displaced off the
            // head. Buffer mode COWs in the fold (witness empty here).
            l2p::drain_page_deaths(volume, &mut tree, &snap_infos);
            l2p::drain_live_events(volume, &mut tree);
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::L2pDelete { vol_ord, lba } => {
            let volume = volumes.get(vol_ord).ok_or_else(|| {
                MetaDbError::Corruption(format!("L2pDelete for unknown volume ord {vol_ord}"))
            })?;
            let sid = shard_for_key_l2p(&volume.shards, *lba);
            let use_buffer = volume.shards[sid].use_buffer;
            let mut tree = volume.shards[sid].tree.write();
            // ZFS port Phase 4 Step 4 (S1): snapshots before the COW (armed for
            // the birth-authoritative non-clone COW-kill, reused for the drain).
            let snap_infos = snap_info_for_vol(*vol_ord);
            // A3 cutover: tree-mode COW (delete) stages page-rc deltas here.
            tree.set_current_txg(txg);
            tree.set_snapshot_wms(l2p::snapshot_wms_of(&snap_infos));
            let prev = if use_buffer {
                let cur = match volume.shards[sid].l2p_buffer.lookup_for_open_txg(txg, *lba) {
                    crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
                    crate::db::l2p_buffer::BufferLookup::Tombstone => None,
                    crate::db::l2p_buffer::BufferLookup::Absent => tree.get(*lba)?,
                };
                volume.shards[sid]
                    .l2p_buffer
                    .insert_tombstone_at_txg(txg, *lba, lsn);
                cur
            } else {
                let p = tree.delete_at_lsn(*lba, lsn)?;
                publish_l2p_read_view(&volume.shards[sid], &tree);
                p
            };
            // rc-authoritative: decref the deleted reference inline, suppressed
            // when a live snapshot still pins it (`stage_delete_rc`). Phase-5
            // (flag off): dead-list + lineage GC drives reclaim. (`snap_infos`
            // hoisted above for the S1 COW-kill arm.)
            if rc_authoritative {
                stage_delete_rc(refcount_shards, txg, &tree, sid, *lba, prev, &snap_infos, lsn)?;
            }
            // ZFS port Phase 2: a direct delete COWs the path to the
            // deleted leaf; record any displaced page.
            l2p::drain_page_deaths(volume, &mut tree, &snap_infos);
            l2p::drain_live_events(volume, &mut tree);
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        // [[no-refcount-hot-path-design]] Phase 5: `DedupPut` is now the
        // only event that bumps global rc — the hot-path L2P apply arms
        // were stripped of inline incref/decref. The contract is:
        //   - inserting a fresh `(hash → pba)` entry         → incref(pba) by 1
        //   - replacing an existing entry at the same hash  → decref(old.pba), incref(new.pba)
        //     (no-op when old.pba == new.pba)
        //   - removing an entry                              → decref(pba)
        // Together with `apply_promotion_chunk` (clone walker) and
        // `apply_free_pbas` (lineage GC), this keeps `rc[pba] > 0` as
        // the "shared via dedup_index" signal that `apply_free_pbas`
        // relies on. Replay safety: dedup_index operations are
        // idempotent at the LSN-cutoff level (the WAL replay only
        // re-applies records strictly above `checkpoint_lsn`), so the
        // non-idempotent `rc.stage(±1)` here is safe under the same
        // invariant.
        WalOp::DedupPut {
            hash,
            value,
            old_pba,
        } => {
            apply_dedup_put_with_rc(
                dedup_index,
                refcount_shards,
                lsn,
                txg,
                *hash,
                *value,
                *old_pba,
            )?;
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupPutGuarded {
            hash,
            value,
            pba_guard,
            min_rc,
            old_pba,
        } => {
            let sid = shard_for_key(refcount_shards, *pba_guard);
            if refcount_shards[sid].rc.get(*pba_guard)? >= *min_rc {
                apply_dedup_put_with_rc(
                    dedup_index,
                    refcount_shards,
                    lsn,
                    txg,
                    *hash,
                    *value,
                    *old_pba,
                )?;
            }
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupDelete { hash, old_pba } => {
            apply_dedup_delete_with_rc(dedup_index, refcount_shards, lsn, txg, hash, *old_pba)?;
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupCompareDelete { hash, old_value } => {
            let cur = dedup_index.get(hash)?;
            let applied = cur.as_ref() == Some(old_value);
            if applied {
                // The compare ensured the on-disk entry equals
                // `old_value`, so its head_pba is the right pba to
                // decref. Compare ops don't carry a separate
                // `old_pba` slot (the value is sufficient).
                apply_dedup_delete_with_rc(
                    dedup_index,
                    refcount_shards,
                    lsn,
                    txg,
                    hash,
                    Some(old_value.head_pba()),
                )?;
            }
            Ok(ApplyOutcome::DedupCompare { applied })
        }
        WalOp::DedupComparePut {
            hash,
            old_value,
            new_value,
        } => {
            let cur = dedup_index.get(hash)?;
            let applied = cur.as_ref() == Some(old_value);
            if applied {
                apply_dedup_put_with_rc(
                    dedup_index,
                    refcount_shards,
                    lsn,
                    txg,
                    *hash,
                    *new_value,
                    Some(old_value.head_pba()),
                )?;
            }
            Ok(ApplyOutcome::DedupCompare { applied })
        }
        WalOp::L2pRemap {
            vol_ord,
            lba,
            new_value,
            guard,
        } => apply_l2p_remap(
            volumes,
            refcount_shards,
            lsn,
            txg,
            *vol_ord,
            *lba,
            *new_value,
            *guard,
            &snap_info_for_vol(*vol_ord),
            rc_authoritative,
        ),
        WalOp::L2pRemapRange {
            vol_ord,
            start_lba,
            values,
        } => apply_l2p_remap_range(
            volumes,
            refcount_shards,
            lsn,
            txg,
            *vol_ord,
            *start_lba,
            values,
            &snap_info_for_vol(*vol_ord),
            rc_authoritative,
        ),
    }
}

pub(super) fn shard_for_key(shards: &[Shard], key: u64) -> usize {
    (xxh3_64(&key.to_be_bytes()) as usize) % shards.len()
}

/// Read `pba`'s `birth_lsn` from the refcount shard. Returns `Some` if
/// the entry is live (rc > 0); `None` if rc == 0. Used by snap-pin
/// fast filters in [`apply_l2p_remap`] / [`apply_l2p_range_delete`].
///
/// The 0→1 birth_lsn stamp is what powers the birth/death LSN
/// suppression: a pba's birth_lsn equals the lsn of the op that
/// revived it from rc=0, so snapshots whose `created_lsn >= birth_lsn`
/// can be ruled out as having pinned this pba's content.
pub(super) fn lookup_birth_lsn(refcount_shards: &[Shard], pba: Pba) -> Result<Option<Lsn>> {
    let sid = shard_for_key(refcount_shards, pba);
    let entry = refcount_shards[sid].rc.get_entry(pba)?;
    Ok(if entry.rc > 0 {
        Some(entry.birth_lsn)
    } else {
        None
    })
}

pub(super) fn shard_for_key_l2p(shards: &[L2pShard], key: u64) -> usize {
    // Keep one compact L2P leaf (128 consecutive LBAs) in one shard. The old
    // hash(lba) router scattered a single 128 KiB/256 KiB user read across
    // almost every shard, so read-side multi_get had to walk 16 independent
    // trees before issuing the LV3 reads. Hashing leaf_idx preserves random
    // write distribution while making spatially-local reads and remap batches
    // shard-local.
    let leaf_idx = key >> crate::paged::format::LEAF_SHIFT;
    (xxh3_64(&leaf_idx.to_be_bytes()) as usize) % shards.len()
}

/// Returns true if the batch contains any op whose apply has manifest
/// or volume-lifecycle side effects. With Buffer-as-sole-journal D.5b
/// the only ops `commit_ops` accepts are data-plane variants, so this
/// is always `false`. The function is kept so the bucketed path's
/// defensive `if batch_contains_lifecycle_op { serial }` fallback
/// continues to compile; any future op variant with manifest side
/// effects should be added here.
pub(super) fn batch_contains_lifecycle_op(_ops: &[WalOp]) -> bool {
    false
}
