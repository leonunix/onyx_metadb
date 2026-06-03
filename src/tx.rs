//! Transaction API.
//!
//! A `Transaction` buffers a sequence of [`WalOp`]s and commits them
//! together as a single WAL record. The commit path is:
//!
//! 1. Serialize the ops into a record body.
//! 2. Submit the body to the WAL (no Db-level lock held). Multiple
//!    concurrent submits coalesce into one group-commit fsync.
//! 3. Wait for global-LSN dispatch order, enqueue bucketable work into
//!    per-shard apply lanes, then apply under `apply_gate.read()`.
//! 4. After all touched lanes finish, wait only for the contiguous
//!    global completion point and bump `last_applied_lsn` before
//!    dropping the apply gate — otherwise a concurrent flush could
//!    observe trees whose state is ahead of `last_applied_lsn` and
//!    recovery would double-apply on restart (refcount incref is not
//!    idempotent).
//! 5. Return the LSN (and, for auto-commit wrappers, any per-op
//!    pre-image the caller expected).
//!
//! Phase 6 shipped this path under a single `commit_lock` that wrapped
//! both the WAL submit and the apply, giving an MVP-simple "LSN order
//! == apply order trivially" proof. Phase 8b replaced that with the
//! LSN-ordered condvar queue described above so WAL group commit can
//! actually form batches.
//!
//! [`WalOp`]: crate::op::WalOp

use std::collections::HashMap;

use crate::dedup_types::{DedupValue, Hash8};
use crate::error::Result;
use crate::paged::L2pValue;
use crate::types::{Lba, Lsn, Pba, VolumeOrdinal};
use crate::op::WalOp;

/// Per-op outcome returned from the apply phase. Auto-commit wrappers
/// around `Transaction` use these to surface pre-images through the
/// existing `Db::insert` / `Db::delete` / … signatures.
///
/// Phase A reserves variants for the onyx-adapter ops that land in
/// sessions S2–S4 of [`docs/ONYX_INTEGRATION_PLAN.md`]:
/// * [`ApplyOutcome::L2pRemap`] — landed in S2 (`WalOp::L2pRemap`).
/// * [`ApplyOutcome::RangeDelete`] — landed in S3 (lifecycle `Discard`).
/// * The `freed_pbas` field on [`ApplyOutcome::DropSnapshot`] —
///   retained for lifecycle-log compatibility; Phase 5 leaves it empty
///   because DropSnapshot is PBA rc-neutral.
///
/// S1 declares the shape so apply-path plumbing is stable for the
/// follow-up sessions; each session fills in its own producer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// L2P put/delete; returns the previous value, if any.
    L2pPrev(Option<L2pValue>),
    /// [[no-refcount-hot-path-design]] Phase 5: standalone refcount
    /// ops were retired (WAL schema 0xB7), and the hot path no longer
    /// emits implicit refcount deltas from L2P remaps. This variant
    /// stays only because the dispatch plumbing still threads a
    /// per-shard rc lane that produces zero outcomes. Future cleanup
    /// can remove both the lane and this variant together.
    RefcountNew(u32),
    /// Dedup put/delete; no pre-image surfaced (LSM reads are not
    /// constant-time, and callers don't need the old value).
    Dedup,
    /// Conditional dedup-index compare operation. `applied=true`
    /// means the forward index still matched the expected old value
    /// at apply time and the mutation landed.
    DedupCompare { applied: bool },
    /// DropSnapshot result: every leaf value that was freed (i.e. whose
    /// owning `PagedLeaf` page hit rc=0 during apply) and the number of
    /// pages pushed onto the free list.
    ///
    /// Collected during the live apply only. Replay-path applies reuse
    /// the same arm but the collected vec is discarded — the numbers
    /// aren't load-bearing for recovery.
    ///
    /// `freed_pbas` is retained for the historical S4 lifecycle shape.
    /// Phase 5 ignores `pba_decrefs`, so DropSnapshot never surfaces PBA
    /// frees here.
    DropSnapshot {
        freed_leaf_values: Vec<L2pValue>,
        pages_freed: usize,
        freed_pbas: Vec<Pba>,
    },
    /// Outcome of `WalOp::L2pRemap` (the onyx-adapter hot path that
    /// fuses L2P put + refcount decref(old) + refcount incref(new) into
    /// a single WAL record). Reserved in S1; populated in S2.
    ///
    /// * `applied=false` iff the op's liveness `guard` rejected the
    ///   remap; in that case `prev` and `freed_pba` are both `None`
    ///   and L2P / refcount are untouched.
    /// * `prev` is the pre-image L2pValue (head 8B is old.pba per the
    ///   Onyx `BlockmapValue` contract).
    /// * `freed_pba = Some(old.pba)` iff this op's decref drove
    ///   refcount(old.pba) from `>0` to `0`. Onyx uses this to pass
    ///   freed pbas to `SpaceAllocator` and to
    ///   `cleanup_dedup_for_dead_pbas`.
    L2pRemap {
        applied: bool,
        prev: Option<L2pValue>,
        freed_pba: Option<Pba>,
    },
    /// Outcome of the lifecycle `Discard` op — bulk delete across `[start,
    /// end)` for one volume, with per-(lba, pba) decrefs applied under
    /// the leaf-rc-suppress rule. Reserved in S1; populated in S3.
    ///
    /// `freed_pbas` lists pbas whose refcount transitioned from `>0`
    /// to `0` during the apply. Order is undefined.
    RangeDelete { freed_pbas: Vec<Pba> },
    /// Outcome of [`WalOp::L2pRemapRange`] — one outcome per range op
    /// (mirrors the `outcomes.len() == ops.len()` contract above).
    ///
    /// * `applied[i]` mirrors `L2pRemap.applied` per LBA: `false` iff
    ///   that LBA's `seq_guard` rejected the new value (a newer write
    ///   already landed). Range ops are always unguarded, so this is
    ///   the only source of per-LBA rejection.
    /// * `prevs[i]` is the pre-image at `start_lba + i`. On a rejected
    ///   LBA `prevs[i]` is the value that rejected the new one (i.e.
    ///   the current live mapping with `seq > new_seq`); on a success
    ///   it's the pre-mutation value (or `None` if the LBA was unmapped).
    /// * `freed_pbas` lists distinct PBAs whose refcount transitioned
    ///   from `>0` to `0` during this range's apply, aggregated across
    ///   all accepted LBAs. Onyx uses it to feed `SpaceAllocator` +
    ///   dedup-index cleanup, identical to the per-LBA
    ///   `L2pRemap.freed_pba` aggregation path.
    L2pRemapRange {
        applied: Box<[bool]>,
        prevs: Box<[Option<L2pValue>]>,
        freed_pbas: Vec<Pba>,
    },
    /// Outcome of [`Db::commit_free_pbas`] — [[no-refcount-hot-path-design]]
    /// Phase 4 Step 3 retire-surface for the Lineage GC consumer.
    /// `freed_pbas` is the union of:
    ///
    /// - **Shared PBAs** whose rc transitioned from `>0` to `0` during
    ///   this apply (the dedup-retire path: onyx-side cleanup also
    ///   deletes the dedup_index entry).
    /// - **Exclusive PBAs** that arrived with rc=0 and were surfaced
    ///   directly — no rc mutation, no dedup_index delete required.
    ///
    /// Onyx-side cleanup coalesces sorted PBAs into `retire_one` /
    /// `retire_extent` calls, mirroring the path that today serves
    /// `L2pRemap.freed_pba` / `RangeDelete.freed_pbas`. Duplicate
    /// surfaces (across replays or across cycles) are harmless because
    /// retire is a set operation.
    FreePbas { freed_pbas: Box<[Pba]> },
    /// Outcome of [`LifecycleOp::PromotionChunk`] —
    /// [[no-refcount-hot-path-design]] Phase 4 Step 5 background
    /// promotion walker progress record. The walker incref'd
    /// `increfs_applied` PBAs against the global refcount table and
    /// advanced the volume's `promotion_cursor` to `cursor_advanced_to`.
    ///
    /// `cursor_advanced_to == None` means "the walker finished its last
    /// chunk for this volume" (the next op will be `PromotionComplete`);
    /// `Some(lba)` is the next LBA the walker should resume at on the
    /// following cycle / after crash.
    PromotionChunk {
        increfs_applied: usize,
        cursor_advanced_to: Option<crate::types::Lba>,
    },
    /// Outcome of [`LifecycleOp::PromotionComplete`] —
    /// [[no-refcount-hot-path-design]] Phase 4 Step 5 walker finish
    /// record. Apply cleared the volume's `parent_vol_ord` and
    /// `promotion_cursor`. No data is carried; the outcome slot exists
    /// only to preserve the `outcomes.len() == ops.len()` contract.
    PromotionComplete,
}

/// A batch of ops to be committed atomically.
///
/// `Transaction` is single-use: call [`commit`](Transaction::commit) to
/// flush the ops to the WAL or drop it to discard. Dropping uncommitted
/// ops is silent — there is no "rollback"; the ops were never durable.
pub struct Transaction<'db> {
    /// Back-reference so the transaction can call into `Db::commit_ops`.
    pub(crate) db: &'db crate::db::Db,
    pub(crate) ops: Vec<WalOp>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(db: &'db crate::db::Db) -> Self {
        Self {
            db,
            ops: Vec::new(),
        }
    }

    /// Number of ops currently buffered.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// `true` if no ops are buffered.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Buffer an L2P put targeted at volume `vol_ord`. The apply path
    /// routes to the volume's shard group; an unknown ordinal fails the
    /// commit with `Corruption` during apply.
    pub fn insert(&mut self, vol_ord: VolumeOrdinal, lba: Lba, value: L2pValue) -> &mut Self {
        self.ops.push(WalOp::L2pPut {
            vol_ord,
            lba,
            value,
        });
        self
    }

    /// Buffer an L2P delete for `vol_ord`. See [`insert`](Self::insert).
    pub fn delete(&mut self, vol_ord: VolumeOrdinal, lba: Lba) -> &mut Self {
        self.ops.push(WalOp::L2pDelete { vol_ord, lba });
        self
    }

    /// Onyx adapter hot path: fused L2P put + refcount decref(old) +
    /// refcount incref(new) as a single WAL record. The pre-metadb
    /// onyx writer emitted `insert + incref + decref` for every remap;
    /// this cuts that to one record, and the apply path enforces the
    /// SPEC §3.1 "leaf-rc-suppress" decref decision table atomically.
    ///
    /// `new_value.head_pba()` is the PBA the mapping targets — metadb
    /// reads the first 8 bytes of the payload to drive the decref /
    /// incref decision, consistent with the `BlockmapValue` contract.
    ///
    /// `guard = Some((pba, min_rc))` reads `refcount(pba)` before
    /// applying; if the value is `< min_rc` the whole op is a no-op
    /// and `ApplyOutcome::L2pRemap { applied: false, .. }` is
    /// reported. `None` applies unconditionally.
    ///
    /// `insert` / `delete` stay for non-refcount L2P paths; the remap
    /// primitive is the canonical onyx write op.
    pub fn l2p_remap(
        &mut self,
        vol_ord: VolumeOrdinal,
        lba: Lba,
        new_value: crate::paged::L2pValue,
        guard: Option<(Pba, u32)>,
    ) -> &mut Self {
        self.ops.push(WalOp::L2pRemap {
            vol_ord,
            lba,
            new_value,
            guard,
        });
        self
    }

    /// Range-shaped variant of [`l2p_remap`](Self::l2p_remap): apply the
    /// same per-LBA remap semantics to `[start_lba .. start_lba + values.len())`
    /// of `vol_ord` in a single WAL record. Always unguarded; the dedup
    /// hit path keeps using [`l2p_remap`](Self::l2p_remap) with a guard.
    ///
    /// Onyx's passthrough writer emits exactly one range op per
    /// CompressedUnit (LBAs are contiguous by construction). This
    /// amortizes the per-op WAL bytes, tag-dispatch, seq-guard check,
    /// and commit-side bucket-assembly across the range — the apply
    /// path's existing leaf-run batching does the actual tree work
    /// (one descend + CoW cascade per `leaf_idx = lba >> 7`).
    ///
    /// `values.len() ≤ crate::op::MAX_REMAP_RANGE_LBAS`. The Box
    /// is moved into the op; callers building from a `Vec` should call
    /// `.into_boxed_slice()` once and forward.
    pub fn l2p_remap_range(
        &mut self,
        vol_ord: VolumeOrdinal,
        start_lba: Lba,
        values: Box<[L2pValue]>,
    ) -> &mut Self {
        debug_assert!(
            !values.is_empty(),
            "l2p_remap_range called with empty values",
        );
        self.ops.push(WalOp::L2pRemapRange {
            vol_ord,
            start_lba,
            values,
        });
        self
    }

    /// Buffer a dedup put. The `old_pba` slot is left as `None`
    /// here and resolved against the current dedup_index just before
    /// the WAL is encoded (see [`resolve_dedup_old_pbas`]); callers
    /// don't need to know the prior `head_pba()`.
    pub fn put_dedup(&mut self, hash: Hash8, value: DedupValue) -> &mut Self {
        self.ops.push(WalOp::DedupPut {
            hash,
            value,
            old_pba: None,
        });
        self
    }

    /// Buffer a dedup put guarded by the target PBA's current refcount.
    pub fn put_dedup_guarded(
        &mut self,
        hash: Hash8,
        value: DedupValue,
        pba_guard: Pba,
        min_rc: u32,
    ) -> &mut Self {
        self.ops.push(WalOp::DedupPutGuarded {
            hash,
            value,
            pba_guard,
            min_rc,
            old_pba: None,
        });
        self
    }

    /// Buffer a dedup tombstone.
    pub fn delete_dedup(&mut self, hash: Hash8) -> &mut Self {
        self.ops.push(WalOp::DedupDelete {
            hash,
            old_pba: None,
        });
        self
    }

    /// Delete a dedup entry only if its current value equals
    /// `old_value` at WAL apply time.
    pub fn compare_delete_dedup(&mut self, hash: Hash8, old_value: DedupValue) -> &mut Self {
        self.ops.push(WalOp::DedupCompareDelete { hash, old_value });
        self
    }

    /// Replace a dedup entry only if its current value equals
    /// `old_value` at WAL apply time.
    pub fn compare_put_dedup(
        &mut self,
        hash: Hash8,
        old_value: DedupValue,
        new_value: DedupValue,
    ) -> &mut Self {
        self.ops.push(WalOp::DedupComparePut {
            hash,
            old_value,
            new_value,
        });
        self
    }

    /// Commit the buffered ops. Returns the LSN assigned to the
    /// record. Nothing is written if the transaction is empty; we
    /// return the last applied LSN instead, so read-your-writes still
    /// works when a caller races a commit against an empty commit.
    pub fn commit(mut self) -> Result<Lsn> {
        self.resolve_dedup_old_pbas()?;
        self.db.commit_ops(&self.ops).map(|(lsn, _)| lsn)
    }

    /// Like [`commit`](Self::commit) but returns the per-op outcomes
    /// too. This is the main entry point used by the onyx adapter —
    /// [`commit`](Self::commit) is a convenience wrapper that calls this
    /// and discards the outcomes vec.
    ///
    /// **Invariant**: `outcomes.len() == ops.len()` holds strictly,
    /// including on the bucketed apply path (`apply_ops_grouped`). Each
    /// returned outcome slot is in the same index as the corresponding
    /// op in the input vec — callers rely on positional correspondence
    /// to route e.g. `ApplyOutcome::L2pRemap.freed_pba` back to the
    /// `WalOp::L2pRemap` that produced it. `apply_op_bare` must return
    /// `Ok(_)` for every op; a `debug_assert!` would fire otherwise.
    pub fn commit_with_outcomes(mut self) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        self.resolve_dedup_old_pbas()?;
        self.db.commit_ops(&self.ops)
    }

    /// Experimental embedder fast path: apply without writing a WAL record.
    ///
    /// The caller must keep an independent durable replay source until a
    /// metadb checkpoint persists the returned LSN. This is intended for
    /// Onyx LV2-backed flush commits, not lifecycle or standalone metadata
    /// operations.
    pub fn commit_unlogged_with_outcomes(mut self) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        self.resolve_dedup_old_pbas()?;
        self.db.commit_ops_unlogged(&self.ops)
    }

    /// ZFS-TXG-clone onyx-side stager. Like [`commit_unlogged_with_outcomes`]
    /// but bypasses the per-LSN dispatch wait (`mark_wal_durable_and_wait_for_dispatch`,
    /// ~614 µs/commit on nvme-box). Apply runs synchronously on the
    /// caller thread under a `TxgGuard`; durability is via the caller's
    /// LV2 buffer until the next metadb TXG sync covers this LSN.
    /// See [`Db::stage_ops`] for the full invariant list.
    pub fn commit_staged_with_outcomes(mut self) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        self.resolve_dedup_old_pbas()?;
        self.db.stage_ops(&self.ops)
    }

    /// ZFS-TXG-clone Phase 2: like
    /// [`commit_with_outcomes`](Self::commit_with_outcomes) but returns
    /// a [`crate::DeferredOutcomeHandle`] instead of the outcomes vec.
    /// The handle delivers the same outcomes the sync path would have,
    /// at the next L2P compactor pass when
    /// `Config::commit_deferred_outcomes_enabled = true`. With the
    /// flag off the handle is pre-populated and `recv()` returns
    /// immediately. See `commit/outcomes.rs` for the delivery model.
    pub fn commit_deferred_with_outcomes(
        mut self,
    ) -> Result<(Lsn, crate::DeferredOutcomeHandle)> {
        self.resolve_dedup_old_pbas()?;
        self.db.commit_ops_deferred(&self.ops)
    }

    /// Fill in the embedded `old_pba` on every `DedupPut` /
    /// `DedupPutGuarded` / `DedupDelete` op the caller buffered with
    /// `None`. Reads the on-disk dedup_index once per unique hash and
    /// honours intra-batch chains: a second op against the same hash
    /// in the same transaction sees the first op's `value.head_pba()`
    /// as its `old_pba`, matching what the live apply path computes
    /// from a serialized read view.
    ///
    /// [[no-refcount-hot-path-design]] Phase 5 needs apply to be
    /// deterministic from the WAL alone — the on-disk dedup_index
    /// data pages are written eagerly per op (only the meta page is
    /// checkpoint-gated), so replaying `apply_op_bare` against the
    /// post-crash dedup_index state previously observed a hash → value
    /// mapping that already reflected ops above `checkpoint_lsn` and
    /// computed the wrong rc deltas.
    ///
    /// Concurrent-commit note: in metadb today different writer threads
    /// can race two `tx.commit()`s targeting the same hash. The reads
    /// here are not serialized against another transaction's apply, so
    /// two concurrent puts on the same hash may both capture the same
    /// `old_pba`. The onyx writer is per-volume serialized and the
    /// hash key space is high-entropy, so this collides only on the
    /// pathological "two volumes write the same 4 KiB content at the
    /// same instant" case; if that becomes a hotspot the fix is a
    /// per-hash-shard serialization gate around the read + WAL submit.
    fn resolve_dedup_old_pbas(&mut self) -> Result<()> {
        let needs_resolution = self.ops.iter().any(|op| {
            matches!(
                op,
                WalOp::DedupPut { old_pba: None, .. }
                    | WalOp::DedupPutGuarded { old_pba: None, .. }
                    | WalOp::DedupDelete { old_pba: None, .. }
            )
        });
        if !needs_resolution {
            return Ok(());
        }
        let mut in_batch: HashMap<Hash8, Option<Pba>> = HashMap::new();
        for op in self.ops.iter_mut() {
            match op {
                WalOp::DedupPut {
                    hash,
                    value,
                    old_pba,
                } => {
                    if old_pba.is_none() {
                        let resolved = match in_batch.get(hash) {
                            Some(v) => *v,
                            None => self.db.dedup_lookup_head_pba(hash)?,
                        };
                        *old_pba = resolved;
                    }
                    in_batch.insert(*hash, Some(value.head_pba()));
                }
                WalOp::DedupPutGuarded {
                    hash,
                    value,
                    old_pba,
                    ..
                } => {
                    if old_pba.is_none() {
                        let resolved = match in_batch.get(hash) {
                            Some(v) => *v,
                            None => self.db.dedup_lookup_head_pba(hash)?,
                        };
                        *old_pba = resolved;
                    }
                    // Guarded puts may be skipped by apply; only update
                    // the intra-batch view once we know they applied.
                    // The conservative choice is to assume they did
                    // apply — a follow-up op against the same hash
                    // typically expects to see the guarded put's
                    // value. If the guard rejects, the apply-time
                    // refcount short-circuits and the leftover stale
                    // `old_pba` on the follow-up op is harmless: that
                    // follow-up's own guard / unguarded path will
                    // produce the right rc deltas based on its
                    // captured value.
                    in_batch.insert(*hash, Some(value.head_pba()));
                }
                WalOp::DedupDelete { hash, old_pba } => {
                    if old_pba.is_none() {
                        let resolved = match in_batch.get(hash) {
                            Some(v) => *v,
                            None => self.db.dedup_lookup_head_pba(hash)?,
                        };
                        *old_pba = resolved;
                    }
                    in_batch.insert(*hash, None);
                }
                WalOp::DedupComparePut {
                    hash, new_value, ..
                } => {
                    // Compare ops don't carry a separate `old_pba`
                    // slot — they already embed `old_value` and the
                    // apply path uses `old_value.head_pba()` directly
                    // when the compare succeeds. We still update the
                    // intra-batch view so a follow-up put/delete sees
                    // the right "prior" hash value.
                    in_batch.insert(*hash, Some(new_value.head_pba()));
                }
                WalOp::DedupCompareDelete { hash, .. } => {
                    in_batch.insert(*hash, None);
                }
                _ => {}
            }
        }
        Ok(())
    }
}
