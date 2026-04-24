//! Transaction API.
//!
//! A `Transaction` buffers a sequence of [`WalOp`]s and commits them
//! together as a single WAL record. The commit path is:
//!
//! 1. Serialize the ops into a record body.
//! 2. Submit the body to the WAL (no Db-level lock held). Multiple
//!    concurrent submits coalesce into one group-commit fsync.
//! 3. Wait on `commit_cvar` until every lower LSN has applied, then
//!    apply our ops under `apply_gate.read()`.
//! 4. Bump `last_applied_lsn` to our LSN and `notify_all`, **before**
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
//! [`WalOp`]: crate::wal::WalOp

use crate::error::Result;
use crate::lsm::{DedupValue, Hash32};
use crate::paged::L2pValue;
use crate::types::{Lba, Lsn, Pba, VolumeOrdinal};
use crate::wal::WalOp;

/// Per-op outcome returned from the apply phase. Auto-commit wrappers
/// around `Transaction` use these to surface pre-images through the
/// existing `Db::insert` / `Db::incref_pba` / … signatures.
///
/// Phase A reserves variants for the onyx-adapter ops that land in
/// sessions S2–S4 of [`docs/ONYX_INTEGRATION_PLAN.md`]:
/// * [`ApplyOutcome::L2pRemap`] — landed in S2 (`WalOp::L2pRemap`).
/// * [`ApplyOutcome::RangeDelete`] — landed in S3 (`WalOp::L2pRangeDelete`).
/// * The `freed_pbas` field on [`ApplyOutcome::DropSnapshot`] —
///   populated by S4 when `WalOp::DropSnapshot.pba_decrefs` is added.
///
/// S1 declares the shape so apply-path plumbing is stable for the
/// follow-up sessions; each session fills in its own producer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// L2P put/delete; returns the previous value, if any.
    L2pPrev(Option<L2pValue>),
    /// Refcount incref/decref; returns the new refcount value.
    RefcountNew(u32),
    /// Dedup put/delete; no pre-image surfaced (LSM reads are not
    /// constant-time, and callers don't need the old value).
    Dedup,
    /// DropSnapshot result: every leaf value that was freed (i.e. whose
    /// owning `PagedLeaf` page hit rc=0 during apply) and the number of
    /// pages pushed onto the free list.
    ///
    /// Collected during the live apply only. Replay-path applies reuse
    /// the same arm but the collected vec is discarded — the numbers
    /// aren't load-bearing for recovery.
    ///
    /// `freed_pbas` is the new-in-S4 slot: the set of pbas whose
    /// refcount transitioned from `>0` to `0` while applying the
    /// drop_snapshot op's pba_decrefs list (the "snap has it, current
    /// has a different pba" diff). Empty until S4 wires it up.
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
    /// Outcome of `WalOp::L2pRangeDelete` — bulk delete across `[start,
    /// end)` for one volume, with per-(lba, pba) decrefs applied under
    /// the leaf-rc-suppress rule. Reserved in S1; populated in S3.
    ///
    /// `freed_pbas` lists pbas whose refcount transitioned from `>0`
    /// to `0` during the apply. Order is undefined.
    RangeDelete { freed_pbas: Vec<Pba> },
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
    /// `insert` / `delete` / `incref_pba` / `decref_pba` stay for
    /// diagnostic / non-refcount paths; the remap primitive is the
    /// canonical onyx write op.
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

    /// Buffer a dedup put.
    pub fn put_dedup(&mut self, hash: Hash32, value: DedupValue) -> &mut Self {
        self.ops.push(WalOp::DedupPut { hash, value });
        self
    }

    /// Buffer a dedup tombstone.
    pub fn delete_dedup(&mut self, hash: Hash32) -> &mut Self {
        self.ops.push(WalOp::DedupDelete { hash });
        self
    }

    /// Buffer a `dedup_reverse` registration (`pba` owns `hash`).
    pub fn register_dedup_reverse(&mut self, pba: Pba, hash: Hash32) -> &mut Self {
        self.ops.push(WalOp::DedupReversePut { pba, hash });
        self
    }

    /// Buffer a `dedup_reverse` tombstone for `(pba, hash)`.
    pub fn unregister_dedup_reverse(&mut self, pba: Pba, hash: Hash32) -> &mut Self {
        self.ops.push(WalOp::DedupReverseDelete { pba, hash });
        self
    }

    /// Buffer a refcount increment. `delta == 0` is allowed.
    pub fn incref_pba(&mut self, pba: Pba, delta: u32) -> &mut Self {
        self.ops.push(WalOp::Incref { pba, delta });
        self
    }

    /// Buffer a refcount decrement.
    pub fn decref_pba(&mut self, pba: Pba, delta: u32) -> &mut Self {
        self.ops.push(WalOp::Decref { pba, delta });
        self
    }

    /// Commit the buffered ops. Returns the WAL LSN assigned to the
    /// record. Nothing is written if the transaction is empty; we
    /// return the last applied LSN instead, so read-your-writes still
    /// works when a caller races a commit against an empty commit.
    pub fn commit(self) -> Result<Lsn> {
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
    pub fn commit_with_outcomes(self) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        self.db.commit_ops(&self.ops)
    }
}
