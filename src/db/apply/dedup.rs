use super::*;

/// [[no-refcount-hot-path-design]] Phase 5 helper: put a `(hash, value)`
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
    lsn: Lsn,
    hash: Hash8,
    value: DedupValue,
    old_pba: Option<Pba>,
) -> Result<()> {
    let new_pba = value.head_pba();
    // Stage rc deltas BEFORE the dedup_index put so a stale-entry skip
    // (see below) doesn't leave the index ahead of the rc table on
    // error. Within a single apply LSN every op is serialized for the
    // hash's shard, so the `get(old_pba)` floor check below cannot race
    // with a concurrent rc mutation.
    if old_pba != Some(new_pba) {
        if let Some(op) = old_pba {
            let sid = shard_for_key(refcount_shards, op);
            // Phase 5: dedup_index entries can become stale when
            // lineage GC's `FreePbas` decrefs a PBA without first
            // invoking onyx-side `delete_dedup_index_if_matches`
            // cleanup (the cleanup path is best-effort). A subsequent
            // `DedupPut` on the stale hash should not double-decref
            // — the entry never represented a live shared reference
            // by the time we got here.
            if refcount_shards[sid].rc.get(op)? > 0 {
                refcount_shards[sid].rc.stage(op, -1, lsn)?;
            }
        }
        let sid = shard_for_key(refcount_shards, new_pba);
        refcount_shards[sid].rc.stage(new_pba, 1, lsn)?;
    }
    // rc deltas above stay inline (correctness); only the cuckoo write is
    // deferred. `stage_put` is the eager `put` verbatim when the drainer
    // is disabled.
    dedup_index.stage_put(hash, value, lsn)?;
    Ok(())
}

/// [[no-refcount-hot-path-design]] Phase 5 helper: drop a hash from the
/// dedup_index and decref the head PBA's global refcount. Pair with
/// [`apply_dedup_put_with_rc`]. `old_pba` is the captured `head_pba()`
/// from the WAL record; `None` means there was no prior entry, in
/// which case the delete is a no-op on the rc side.
pub(in crate::db) fn apply_dedup_delete_with_rc(
    dedup_index: &crate::dedup::DedupIndex,
    refcount_shards: &[Shard],
    lsn: Lsn,
    hash: &Hash8,
    old_pba: Option<Pba>,
) -> Result<()> {
    if let Some(pba) = old_pba {
        let sid = shard_for_key(refcount_shards, pba);
        // See `apply_dedup_put_with_rc` for the stale-entry contract:
        // a dedup_index row pointing to a PBA whose rc has been driven
        // to 0 by lineage GC is stale, and removing it must not
        // underflow the rc table.
        if refcount_shards[sid].rc.get(pba)? > 0 {
            refcount_shards[sid].rc.stage(pba, -1, lsn)?;
        }
    }
    dedup_index.stage_delete(hash, lsn)?;
    Ok(())
}

/// Apply a batched `commit_free_pbas` with exclusive/shared split
/// ([[no-refcount-hot-path-design]] Phase 4 Step 3).
///
/// For each PBA we classify by **current refcount**:
///
/// - **Shared** (`rc > 0`): some other lineage still references this
///   PBA via the dedup_index (in Phase 5 only `DedupPut` and the
///   promotion walker bump global rc, so `rc > 0` is the definitive
///   "shared via dedup" signal). Decref by 1; if the staged result
///   reaches 0 the PBA surfaces in `freed_pbas` and onyx-side cleanup
///   retires it + deletes the dedup_index entry.
/// - **Exclusive** (`rc == 0`): no other lineage references this PBA
///   — it was never put into `dedup_index` and the hot path never
///   bumped its rc (Phase 5) or has already brought it to 0 (Phase
///   4). Surface directly **without** touching rc. Touching it would
///   underflow.
///
/// Phase 4 default: hot-path RC is still on, so a Phase-4-built GC
/// segment's records arrive here with rc already 0 (hot path's
/// `L2pRemap` decref ran). They take the **exclusive** branch and
/// surface so onyx can do the retire that previously rode on the
/// `L2pRemap` outcome.
///
/// Replay idempotency: re-applying the same `commit_free_pbas` after a
/// crash sees rc=0 for everything it already drained on the first
/// pass; those PBAs surface again. Onyx consumes the surface via
/// `PbaLifecycle::free_lineage_gc_proven`, which absorbs a duplicate
/// idempotently (`is_extent_free`/`is_retired` precheck), so duplicate
/// surfaces are harmless. The previous Phase 3 defensive
/// `if cur == 0 { continue }` guard collapsed exclusive PBAs with
/// already-freed shared PBAs into a single "skip" — Phase 4 separates
/// them because Phase 5 will need the exclusive surface to be the
/// **primary** retire signal (the hot-path `L2pRemap` retire path
/// goes away).
///
/// We don't query `dedup_index.contains(pba)` here even though the
/// design says `is_shared = dedup_index.contains(pba) || rc > 0`:
/// the on-disk dedup_index is hash-keyed with no PBA-reverse lookup,
/// and in Phase 5 only `DedupPut` bumps rc, so `rc > 0` already
/// implies "in dedup_index". The OR is redundant given the
/// promotion-walker discipline planned in Step 5.
pub(in crate::db) fn apply_free_pbas(
    refcount_shards: &[Shard],
    lsn: Lsn,
    pbas: &[Pba],
) -> Result<ApplyOutcome> {
    let mut freed: Vec<Pba> = Vec::new();
    for &pba in pbas {
        let sid = shard_for_key(refcount_shards, pba);
        // Peek current rc (pending merged with base). `rc.get` does
        // not mutate state and never underflows.
        let cur = refcount_shards[sid].rc.get(pba)?;
        if cur == 0 {
            // Exclusive — never refcounted (no dedup entry, no hot-
            // path bump still pending). Surface without touching rc.
            freed.push(pba);
            continue;
        }
        let (_, new) = refcount_shards[sid].rc.stage(pba, -1, lsn)?;
        if new == 0 {
            freed.push(pba);
        }
    }
    Ok(ApplyOutcome::FreePbas {
        freed_pbas: freed.into_boxed_slice(),
    })
}
