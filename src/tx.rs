//! Transaction API.
//!
//! A `Transaction` buffers a sequence of [`WalOp`]s and commits them
//! together as a single WAL record. The commit path is:
//!
//! 1. Serialize the ops into a record body.
//! 2. Submit the body to the WAL; wait for fsync.
//! 3. Apply every op to in-memory state under a global commit lock.
//! 4. Return the LSN (and, for auto-commit wrappers, any per-op
//!    pre-image the caller expected).
//!
//! The `commit_lock` makes the apply phase single-threaded. That
//! sacrifices the group-commit throughput benefits of phase 1's WAL
//! writer for MVP-friendly correctness: LSN order equals apply order
//! trivially, so recovery always reproduces the same in-memory state as
//! normal operation. Phase 8 can replace this with an LSN-ordered
//! condvar apply path.
//!
//! [`WalOp`]: crate::wal::WalOp

use crate::paged::L2pValue;
use crate::error::Result;
use crate::lsm::{DedupValue, Hash32};
use crate::types::{Lba, Lsn, Pba};
use crate::wal::WalOp;

/// Per-op outcome returned from the apply phase. Auto-commit wrappers
/// around `Transaction` use these to surface pre-images through the
/// existing `Db::insert` / `Db::incref_pba` / … signatures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// L2P put/delete; returns the previous value, if any.
    L2pPrev(Option<L2pValue>),
    /// Refcount incref/decref; returns the new refcount value.
    RefcountNew(u32),
    /// Dedup put/delete; no pre-image surfaced (LSM reads are not
    /// constant-time, and callers don't need the old value).
    Dedup,
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

    /// Buffer an L2P put.
    pub fn insert(&mut self, lba: Lba, value: L2pValue) -> &mut Self {
        self.ops.push(WalOp::L2pPut { lba, value });
        self
    }

    /// Buffer an L2P delete.
    pub fn delete(&mut self, lba: Lba) -> &mut Self {
        self.ops.push(WalOp::L2pDelete { lba });
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
    /// too. Used by the auto-commit wrappers that preserve the
    /// pre-image in their return type.
    pub(crate) fn commit_with_outcomes(self) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        self.db.commit_ops(&self.ops)
    }
}
