use super::*;

/// [[no-refcount-hot-path-design]] Phase 4 Step 5 walker chunk apply.
/// Increfs each PBA in `pba_increfs` by 1 against the global refcount
/// table (the increfs are batched into the WAL-record group commit,
/// not into a separate per-incref record). Advances the volume's
/// in-memory `promotion_cursor` to `next_cursor`.
///
/// Idempotency: WAL replay only applies records strictly above the
/// previous `checkpoint_lsn`, and the walker emits at most one chunk
/// per volume per LSN, so a record at LSN `L` is applied at most once
/// across crashes. Within a record the per-PBA `rc.stage(pba, 1, lsn)`
/// is non-idempotent — re-staging would double-count — but the
/// "checkpoint_lsn cutoff" makes that condition unreachable.
pub(in crate::db) fn apply_promotion_chunk(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    vol_ord: VolumeOrdinal,
    pba_increfs: &[Pba],
    next_cursor: Option<crate::types::Lba>,
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("PromotionChunk for unknown volume ord {vol_ord}"))
    })?;
    let mut applied = 0usize;
    for &pba in pba_increfs {
        let sid = shard_for_key(refcount_shards, pba);
        refcount_shards[sid].rc.stage(pba, 1, lsn)?;
        applied += 1;
    }
    *volume.promotion_cursor.write() = next_cursor;
    Ok(ApplyOutcome::PromotionChunk {
        increfs_applied: applied,
        cursor_advanced_to: next_cursor,
    })
}

/// [[no-refcount-hot-path-design]] Phase 4 Step 5 walker finish apply.
/// Clears the clone's in-memory `parent_vol_ord` and `promotion_cursor`
/// — after this record the parent volume's Lineage GC stops treating
/// this clone's `branched_at_lsn` as a pin point.
///
/// Idempotent: re-applying after a crash sees both fields already
/// `None` and is a no-op. `branched_at_lsn` deliberately stays — the
/// clone's COW-shared L2P still records data born below that LSN, and
/// reset-to-zero would be misleading even if no GC consumer reads it
/// post-promotion.
pub(in crate::db) fn apply_promotion_complete(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    vol_ord: VolumeOrdinal,
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!(
            "PromotionComplete for unknown volume ord {vol_ord}"
        ))
    })?;
    *volume.parent_vol_ord.write() = None;
    *volume.promotion_cursor.write() = None;
    Ok(ApplyOutcome::PromotionComplete)
}
