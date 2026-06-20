use super::*;

pub(in crate::db::lifecycle) fn refresh_manifest_from_checkpoints(
    manifest: &mut Manifest,
    volumes: &[Arc<Volume>],
    l2p_checkpoints: &[Vec<Option<crate::paged::tree::Checkpoint>>],
    dead_list_overrides: &HashMap<VolumeOrdinal, (PageId, PageId)>,
    page_dead_list_overrides: &HashMap<VolumeOrdinal, (PageId, PageId)>,
    page_live_list_overrides: &HashMap<VolumeOrdinal, (PageId, PageId)>,
) -> Result<()> {
    manifest.body_version = MANIFEST_BODY_VERSION;
    if volumes.len() != l2p_checkpoints.len() {
        return Err(MetaDbError::Corruption(format!(
            "checkpoint volume count {} does not match checkpoint groups {}",
            volumes.len(),
            l2p_checkpoints.len()
        )));
    }
    // Snapshot the prior root for every (vol_ord, shard) so we can
    // fall back on it for unselected shards. Vec lookup is O(volumes)
    // but the live volume count is small (≤ max_volumes).
    let mut new_entries = Vec::with_capacity(volumes.len());
    for (volume, checkpoints) in volumes.iter().zip(l2p_checkpoints.iter()) {
        if volume.shards.len() != checkpoints.len() {
            return Err(MetaDbError::Corruption(format!(
                "checkpoint shard count {} does not match volume {} shard count {}",
                checkpoints.len(),
                volume.ord,
                volume.shards.len()
            )));
        }
        // The previous manifest already records this volume's roots
        // (volume create / clone pushed an entry before any flush
        // committed). Borrow that prior root slice for unselected
        // shards so the manifest reflects "this shard wasn't
        // re-flushed this round". A volume that's brand-new and
        // genuinely missing from the prior manifest falls back to
        // the in-memory tree root (must be readable while holding
        // `apply_gate.write()`).
        let prior_roots: Option<&[PageId]> = manifest
            .volumes
            .iter()
            .find(|e| e.ord == volume.ord)
            .map(|e| e.l2p_shard_roots.as_ref());
        let mut roots = Vec::with_capacity(volume.shards.len());
        for (s_idx, ck_opt) in checkpoints.iter().enumerate() {
            let root = match ck_opt {
                Some(ck) => ck.root,
                None => prior_roots
                    .and_then(|pr| pr.get(s_idx).copied())
                    .unwrap_or_else(|| {
                        volume.shards[s_idx]
                            .tree
                            .try_read()
                            .map(|t| t.root())
                            .unwrap_or(crate::types::NULL_PAGE)
                    }),
            };
            roots.push(root);
        }
        // Preserve the prior per-shard durable_seq for the moment;
        // `refresh_manifest_durable_seq` below overwrites each entry
        // with the post-flush per-shard value (selected →
        // wal_checkpoint, unselected → prior atomic). We start from
        // the prior manifest's array (or zeros if missing) so
        // unselected shards have a sensible value even if the live
        // atomics haven't been advanced yet for this round.
        let prior_durable_seq: Option<&[Lsn]> = manifest
            .volumes
            .iter()
            .find(|e| e.ord == volume.ord)
            .map(|e| e.l2p_shard_durable_seq.as_ref());
        let durable_seq: Box<[Lsn]> = match prior_durable_seq {
            Some(seqs) if seqs.len() == volume.shards.len() => seqs.to_vec().into_boxed_slice(),
            _ => vec![0; volume.shards.len()].into_boxed_slice(),
        };
        let (dead_list_head_pid, dead_list_tail_pid) = match dead_list_overrides.get(&volume.ord) {
            Some((h, t)) => (*h, *t),
            None => (
                volume
                    .dead_list_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                volume
                    .dead_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
        };
        // v18 HEAD page-deadlist anchors (ZFS port Phase 2), mirroring
        // the PBA `dead_list_overrides` logic above: this flush's freshly
        // drained page segment is in the override map; volumes with no
        // page drain this round fall back to the live atomics (promoted
        // post-commit, so they reflect the durable chain).
        let (page_dead_list_head_pid, page_dead_list_tail_pid) =
            match page_dead_list_overrides.get(&volume.ord) {
                Some((h, t)) => (*h, *t),
                None => (
                    volume
                        .page_dead_list_head_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                    volume
                        .page_dead_list_tail_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                ),
            };
        // v19 per-clone page-livelist anchors (ZFS port Phase 3b), same
        // override-or-atomics fallback as the page-deadlist above.
        let (page_live_list_head_pid, page_live_list_tail_pid) =
            match page_live_list_overrides.get(&volume.ord) {
                Some((h, t)) => (*h, *t),
                None => (
                    volume
                        .page_live_list_head_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                    volume
                        .page_live_list_tail_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                ),
            };
        new_entries.push(VolumeEntry {
            ord: volume.ord,
            shard_count: volume.shards.len() as u32,
            l2p_shard_roots: roots.into_boxed_slice(),
            l2p_shard_durable_seq: durable_seq,
            created_lsn: volume.created_lsn,
            flags: volume.flags.load(std::sync::atomic::Ordering::Relaxed),
            dead_list_head_pid,
            dead_list_tail_pid,
            parent_vol_ord: *volume.parent_vol_ord.read(),
            branched_at_lsn: volume.branched_at_lsn,
            promotion_cursor: *volume.promotion_cursor.read(),
            page_dead_list_head_pid,
            page_dead_list_tail_pid,
            page_live_list_head_pid,
            page_live_list_tail_pid,
        });
    }
    manifest.volumes = new_entries;
    // refcount_shard_roots are stamped at create/open time and never
    // change across flushes (paged-array meta page id is stable);
    // leave whatever the manifest already carries untouched.
    Ok(())
}

/// Tier 2.B Stage 1: rewrite every per-shard `durable_seq` in the
/// manifest to reflect the durable state we're about to commit. This
/// mirrors the inputs to [`Db::compute_min_last_flushed_lsn_after`]:
/// selected shards advance to `wal_checkpoint`; unselected shards keep
/// their existing atomic. Must be called AFTER
/// [`refresh_manifest_from_checkpoints`] so the per-volume entries are
/// already shaped correctly, and BEFORE the manifest store commit so
/// the new arrays land on disk.
///
/// Stage 1 invariant: `checkpoint_lsn == min(all durable_seq[])`. The
/// `Manifest::assert_durable_seq_invariant` tripwire fires on the next
/// `encode` if this is violated.
pub(in crate::db::lifecycle) fn refresh_manifest_durable_seq(
    manifest: &mut Manifest,
    volumes: &[Arc<Volume>],
    refcount_shards: &[Shard],
    l2p_page_rc: &crate::l2p_page_rc::L2pPageRc,
    selected: &SelectedShards,
    wal_checkpoint: Lsn,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    if manifest.volumes.len() != volumes.len() {
        return Err(MetaDbError::Corruption(format!(
            "refresh_manifest_durable_seq: manifest volume count {} != live count {}",
            manifest.volumes.len(),
            volumes.len(),
        )));
    }
    for (v_idx, vol) in volumes.iter().enumerate() {
        let entry = &mut manifest.volumes[v_idx];
        if entry.ord != vol.ord {
            return Err(MetaDbError::Corruption(format!(
                "refresh_manifest_durable_seq: manifest volume[{v_idx}] ord {} != live ord {}",
                entry.ord, vol.ord,
            )));
        }
        if entry.l2p_shard_durable_seq.len() != vol.shards.len() {
            entry.l2p_shard_durable_seq = vec![0; vol.shards.len()].into_boxed_slice();
        }
        for (s_idx, shard) in vol.shards.iter().enumerate() {
            let prev = shard.last_flushed_lsn.load(Ordering::Acquire);
            // Phase 4 gate-shrink: `wal_checkpoint = slot_max_lsn(txg)`
            // can be 0 if the Syncing slot had no commits. Use
            // `max(wal_checkpoint, prev)` for selected shards so
            // durable_seq is monotonically non-decreasing — matches
            // the projection in `compute_min_last_flushed_lsn_after`
            // and the `fetch_max` atomic store post-manifest.
            let tree_lsn = if selected.l2p[v_idx][s_idx] {
                wal_checkpoint.max(prev)
            } else {
                prev
            };
            // Same B2 buffer term as `compute_min_last_flushed_lsn_after`:
            // any uncompacted buffer entry represents committed-but-
            // not-tree-durable state; WAL replay rebuilds it, so the
            // per-shard durable_seq must not advance past
            // `buffer.compacted_lsn`.
            let lsn = if shard.use_buffer {
                tree_lsn.min(shard.l2p_buffer.compacted_lsn())
            } else {
                tree_lsn
            };
            entry.l2p_shard_durable_seq[s_idx] = lsn;
        }
    }
    if manifest.refcount_durable_seq.len() != refcount_shards.len() {
        manifest.refcount_durable_seq = vec![0; refcount_shards.len()].into_boxed_slice();
    }
    for (s_idx, shard) in refcount_shards.iter().enumerate() {
        let prev = shard.last_flushed_lsn.load(Ordering::Acquire);
        // The per-TXG-slot rc fold makes a selected rc shard durable to
        // `wal_checkpoint` (like a non-buffered L2P shard), so no buffer-term
        // cap is needed — matches `compute_min_last_flushed_lsn_after` so the
        // `min(durable_seq[]) == checkpoint_lsn` invariant holds.
        let lsn = if selected.rc[s_idx] {
            wal_checkpoint.max(prev)
        } else {
            prev
        };
        manifest.refcount_durable_seq[s_idx] = lsn;
    }
    // Phase A2: L2P-page-rc shards, selected 1:1 with refcount via
    // `selected.rc`. Same `wal_checkpoint.max(prev)` projection as
    // refcount, and the post-manifest `fetch_max` advances the atomics
    // identically — so the page-rc durable_seq array exactly mirrors the
    // refcount one and the `min(durable_seq[]) == checkpoint_lsn`
    // invariant is preserved.
    let prc_count = l2p_page_rc.shard_count();
    if manifest.l2p_page_rc_durable_seq.len() != prc_count {
        manifest.l2p_page_rc_durable_seq = vec![0; prc_count].into_boxed_slice();
    }
    for s_idx in 0..prc_count {
        let prev = l2p_page_rc.last_flushed_lsn(s_idx);
        let lsn = if selected.rc[s_idx] {
            wal_checkpoint.max(prev)
        } else {
            prev
        };
        manifest.l2p_page_rc_durable_seq[s_idx] = lsn;
    }
    Ok(())
}
