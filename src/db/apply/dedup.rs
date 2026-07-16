use super::*;

/// helper: put a `(hash, value)`
/// into the dedup_index and reconcile the global refcount of the head
/// PBA. `DedupPut` is one of the three remaining rc-mutating events
/// (the others are `PromotionChunk` and `FreePbas`), so this helper is
/// the single place where the "DedupPut bumps rc" contract lives.
///
/// Behavior given the WAL-embedded `old_pba`:
///
/// | `old_pba` | Same as `new_pba`? | rc work |
/// |-----------|--------------------|---------|
/// | `None`    | —                  | incref(new) |
/// | `Some(p)` | yes                | none (idempotent re-put) |
/// | `Some(p)` | no                 | decref(p) + incref(new) |
///
/// `old_pba` is captured at `Transaction::commit` time and embedded in
/// the `WalOp::DedupPut` record (WAL schema 0xB8), so the apply path is
/// deterministic from the WAL alone — no dedup_index read here. That
/// matters for replay: dedup_index data pages are written eagerly per
/// op (only the meta page is checkpoint-gated; see
/// `lifecycle.rs::open_with_config_and_faults`), so reading the on-disk
/// dedup_index during replay would observe a state already reflecting
/// ops above `checkpoint_lsn` and stage the wrong rc deltas.
pub(in crate::db) fn apply_dedup_put_with_rc(
    dedup_index: &crate::dedup::DedupIndex,
    refcount_shards: &[Shard],
    metrics: &MetaMetrics,
    lsn: Lsn,
    bfg: crate::types::Bfg,
    hash: Hash8,
    value: DedupValue,
    old_pba: Option<Pba>,
) -> Result<()> {
    let new_pba = value.head_pba();
    // Put the cuckoo entry FIRST so we know whether it was placed or dropped on
    // saturation, then reconcile rc only for a real placement. A saturating
    // drop only hits a genuinely-new insert (an overwrite of an existing hash
    // short-circuits before the eviction chain), so on a drop `old_pba` is
    // `None` and there is nothing to decref — skipping the +1 incref leaves rc
    // consistent with the (unchanged) cuckoo. A dropped promote is a future
    // dedup miss, never a failed apply, so this path can never hard-error and
    // wedge WAL replay. This matches the lane path's `flush_pending_puts`
    // (put-many first, rc after, skip rc for dropped hashes).
    //
    // Ordering note: the cuckoo write no longer errors on saturation, so
    // doing it before the rc stage is safe — the only remaining error source
    // (an rc-shard fault) is a genuine corruption, not the routine full-table
    // case the old "rc before put" ordering was guarding against.
    let placed = dedup_index.stage_put(hash, value, lsn)?;
    if !placed {
        // Dropped on cuckoo saturation. Same accounting as the lane path's
        // `flush_pending_puts`: a saturating drop is a genuinely-new insert
        // (`old_pba == None`), so there is no decref, and skipping the +1
        // incref keeps rc consistent with the unchanged cuckoo. Count it so the
        // drop is observable (it otherwise looks like a silent success to the
        // onyx promote caller).
        metrics.record_dedup_promote_dropped_saturated(1);
        return Ok(());
    }
    if old_pba != Some(new_pba) {
        if let Some(op) = old_pba {
            let sid = shard_for_key(refcount_shards, op);
            // dedup_index entries can become stale when
            // lineage GC's `FreePbas` decrefs a PBA without first
            // invoking onyx-side `delete_dedup_index_if_matches`
            // cleanup (the cleanup path is best-effort). A subsequent
            // `DedupPut` on the stale hash should not double-decref
            // — the entry never represented a live shared reference
            // by the time we got here.
            refcount_shards[sid]
                .rc
                .stage_decref_if_positive(bfg, op, lsn)?;
        }
        let sid = shard_for_key(refcount_shards, new_pba);
        refcount_shards[sid].rc.stage(bfg, new_pba, 1, lsn)?;
    }
    Ok(())
}

/// helper: drop a hash from the
/// dedup_index and decref the head PBA's global refcount. Pair with
/// [`apply_dedup_put_with_rc`]. `old_pba` is the captured `head_pba()`
/// from the WAL record; `None` means there was no prior entry, in
/// which case the delete is a no-op on the rc side.
pub(in crate::db) fn apply_dedup_delete_with_rc(
    dedup_index: &crate::dedup::DedupIndex,
    refcount_shards: &[Shard],
    lsn: Lsn,
    bfg: crate::types::Bfg,
    hash: &Hash8,
    old_pba: Option<Pba>,
) -> Result<()> {
    if let Some(pba) = old_pba {
        let sid = shard_for_key(refcount_shards, pba);
        // See `apply_dedup_put_with_rc` for the stale-entry contract:
        // a dedup_index row pointing to a PBA whose rc has been driven
        // to 0 by lineage GC is stale, and removing it must not
        // underflow the rc table.
        refcount_shards[sid]
            .rc
            .stage_decref_if_positive(bfg, pba, lsn)?;
    }
    dedup_index.stage_delete(hash, lsn)?;
    Ok(())
}

/// Apply a batched `commit_free_pbas` with exclusive/shared split
/// ().
///
/// For each PBA we classify by **current refcount**:
///
/// - **Shared** (`rc > 0`): some other lineage still references this
///   PBA via the dedup index or a clone-promotion edge. Decref by 1; if
///   the staged result reaches 0 the PBA surfaces in `freed_pbas` and
///   onyx-side cleanup retires it + deletes the dedup_index entry.
/// - **Exclusive** (`rc == 0`): no other lineage references this PBA
///   — it was never put into `dedup_index` and the hot path never
///   bumped its rc, or a previous retire already brought it to 0. Surface
///   directly **without** touching rc. Touching it would underflow.
///
/// In normal lineage-GC flow, records for exclusive PBAs arrive here with
/// rc already 0. They take the **exclusive** branch and surface so onyx can do
/// the retire that used to ride on the `L2pRemap` outcome.
///
/// Replay idempotency: re-applying the same `commit_free_pbas` after a
/// crash sees rc=0 for everything it already drained on the first
/// pass; those PBAs surface again. Onyx consumes the surface via
/// `PbaLifecycle::free_lineage_gc_proven`, which absorbs a duplicate
/// idempotently (`is_extent_free`/`is_retired` precheck), so duplicate
/// surfaces are harmless. The old defensive `if cur == 0 { continue }` guard
/// collapsed exclusive PBAs with already-freed shared PBAs into a single
/// "skip"; the current path keeps exclusive PBAs visible because that surface is
/// now the primary retire signal.
///
/// We don't query `dedup_index.contains(pba)` here even though the
/// design says `is_shared = dedup_index.contains(pba) || rc > 0`:
/// the on-disk dedup_index is hash-keyed with no PBA-reverse lookup,
/// and only `DedupPut` plus clone promotion bump rc, so `rc > 0` is already the
/// durable "shared elsewhere" signal.
pub(in crate::db) fn apply_free_pbas(
    refcount_shards: &[Shard],
    lsn: Lsn,
    bfg: crate::types::Bfg,
    pbas: &[Pba],
) -> Result<ApplyOutcome> {
    let mut freed: Vec<Pba> = Vec::new();
    for &pba in pbas {
        let sid = shard_for_key(refcount_shards, pba);
        // One fold-coherent stage both classifies and applies the decref. At
        // rc=0 it clamps to `(0, 0)`, preserving the exclusive/duplicate
        // surface semantics without a racy read-then-stage gap.
        let (_, new) = refcount_shards[sid]
            .rc
            .stage_decref_if_positive(bfg, pba, lsn)?;
        if new == 0 {
            freed.push(pba);
        }
    }
    Ok(ApplyOutcome::FreePbas {
        freed_pbas: freed.into_boxed_slice(),
    })
}
