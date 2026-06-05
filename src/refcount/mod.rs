//! Paged-array + delta refcount store.
//!
//! Each shard owns a paged array indexed by
//! `pba >> ENTRIES_PER_PAGE_SHIFT`; reads / writes go to a `DeltaMap`
//! first and fall back to the on-disk array on miss. Apply lanes
//! drain delta into pages at commit boundaries.
//!
//! # Concurrency model
//!
//! - One [`RcShard`] per shard, with its own delta + array.
//! - Apply path serialises through the shard's `ApplyLane`; the lane
//!   worker holds the delta lock briefly to merge ops, then the array
//!   lock to flush.
//! - Read path takes the delta lock (read), peeks pending value,
//!   falls back to the array on miss.

pub mod array;
pub mod delta;
pub mod overlay;
pub mod shard;

pub use array::{ENTRIES_PER_PAGE, PagedRefcountArray};
pub use delta::DeltaMap;
pub use shard::RcShard;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{MetaDbError, Result};
use crate::types::Lsn;

/// Per-PBA refcount entry. `rc` is the live reference count;
/// `birth_lsn` records the LSN at which the entry transitioned from
/// `rc=0` to `rc=1` — i.e. when the current content of this PBA was
/// first incref'd. Birth/death LSN suppression in
/// `crate::db::apply_l2p_remap` uses this to decide whether a
/// concurrent snapshot might still pin the PBA.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RcEntry {
    pub rc: u32,
    pub birth_lsn: Lsn,
}

impl RcEntry {
    /// Sentinel for "no entry": rc=0, birth_lsn=0. Returned by callers
    /// that fold a missing-key lookup into the same arithmetic path.
    pub const ZERO: Self = Self {
        rc: 0,
        birth_lsn: 0,
    };
}

impl std::fmt::Display for RcEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(rc={}, birth_lsn={})", self.rc, self.birth_lsn)
    }
}

/// Compute the new RcEntry from `(prev, delta, lsn)`. Pure
/// arithmetic — does not touch any state. Mirrors the semantics of
/// `crate::db::apply::refcount_apply_delta`.
///
/// `birth_lsn` semantics:
/// - 0→1 transition stamps `birth_lsn = lsn`
/// - rc>0 → rc>0 preserves the existing `birth_lsn`
/// - rc>0 → 0 returns [`RcEntry::ZERO`] (birth_lsn implicitly cleared)
pub fn apply_delta_pure(prev: RcEntry, delta: i64, lsn: Lsn) -> Result<RcEntry> {
    let new_rc: u32 = if delta >= 0 {
        let amount = u32::try_from(delta).map_err(|_| {
            MetaDbError::InvalidArgument(format!("refcount delta {delta} exceeds u32"))
        })?;
        prev.rc.checked_add(amount).ok_or_else(|| {
            MetaDbError::InvalidArgument(format!("refcount overflow: {} + {amount}", prev.rc))
        })?
    } else {
        let amount = u32::try_from(-delta).map_err(|_| {
            MetaDbError::InvalidArgument(format!("refcount delta {delta} exceeds u32"))
        })?;
        prev.rc.checked_sub(amount).ok_or_else(|| {
            MetaDbError::InvalidArgument(format!("refcount underflow: {} - {amount}", prev.rc))
        })?
    };
    if new_rc == 0 {
        Ok(RcEntry::ZERO)
    } else {
        let birth_lsn = if prev.rc == 0 { lsn } else { prev.birth_lsn };
        Ok(RcEntry {
            rc: new_rc,
            birth_lsn,
        })
    }
}

/// Count of decref-underflows clamped to a benign no-op, across both the
/// stage merge and the array-apply flush, all shards.
static UNDERFLOW_CLAMPED: AtomicU64 = AtomicU64::new(0);

/// Total decref-underflows skipped as benign double-decrefs. A non-zero
/// value means a double-decref of an already-freed PBA was absorbed
/// rather than poisoning the commit / checkpoint pipeline.
pub fn underflow_clamped_total() -> u64 {
    UNDERFLOW_CLAMPED.load(Ordering::Relaxed)
}

/// Count of read-side merge underflows floored to rc=0 (vs propagated as
/// an Err), across `RcShard::lookup_entry` (`get`) and `stage`'s
/// "current value" computation, all shards.
static READ_UNDERFLOW_FLOORED: AtomicU64 = AtomicU64::new(0);

/// Total read-side merge underflows floored to rc=0. A non-zero value
/// means a read observed a transiently-inconsistent `(delta_active,
/// array)` sample — the value's logical floor is 0 and the read returned
/// 0 instead of failing an unrelated commit's apply. See
/// [`merge_read_or_floor`].
pub fn read_underflow_floored_total() -> u64 {
    READ_UNDERFLOW_FLOORED.load(Ordering::Relaxed)
}

/// Read-side merge: compute `prev ⊕ delta`, flooring a decref-underflow
/// to rc=0 instead of erroring. Overflow (delta >= 0) still errors —
/// that is never benign.
///
/// Why this exists: `RcShard::lookup_entry` / `stage` sample the pending
/// delta (`delta_active` / `delta_draining`) and the base
/// (overlay / on-disk array) without a lock that also excludes the flush
/// checkpoint. `begin_checkpoint` drains the pending under
/// `delta_active.lock` but installs the folded result into the array /
/// overlay **without** that lock, and the onyx hot path (`stage_ops`)
/// applies its rc ops without holding `apply_gate.read()` — so a reader
/// can observe a base that already folded a pending decref while it still
/// holds the pre-drain pending, double-counting the decref and computing
/// a negative rc. The logical refcount floor is 0, so flooring is
/// correct; propagating the Err instead fails the dedup-hit / promote tx,
/// which onyx demotes to a fresh miss — a self-amplifying `commit_errors`
/// burst on hot re-overwritten PBAs. The actual decref still flows
/// through [`apply_delta_or_skip`] at stage time, so a genuine
/// over-decref is still counted, never silently lost.
#[inline]
pub(crate) fn merge_read_or_floor(prev: RcEntry, delta: i64, lsn: Lsn) -> Result<RcEntry> {
    match apply_delta_pure(prev, delta, lsn) {
        Ok(entry) => Ok(entry),
        Err(_) if delta < 0 => {
            let total = READ_UNDERFLOW_FLOORED.fetch_add(1, Ordering::Relaxed) + 1;
            // Power-of-two rate limit so a burst can't flood the log.
            if total.is_power_of_two() {
                tracing::warn!(
                    target: "onyx_metadb::refcount::read_floor",
                    delta,
                    lsn,
                    prev_rc = prev.rc,
                    floored_total = total,
                    "rc read merge underflow floored to 0 (transient checkpoint/apply race); \
                     value's logical floor is 0"
                );
            }
            Ok(RcEntry::ZERO)
        }
        Err(err) => Err(err),
    }
}

/// Read-side convenience over [`merge_read_or_floor`]: fold an optional
/// pending delta onto `base` (`None` leaves `base` unchanged). The shape
/// `RcShard::lookup_entry` / `stage` use to merge each of `delta_active`
/// / `delta_draining` onto the on-disk-or-overlay base.
#[inline]
pub(crate) fn merge_pending_read(base: RcEntry, pending: Option<delta::Pending>) -> Result<RcEntry> {
    match pending {
        Some(p) => merge_read_or_floor(base, p.delta, p.last_lsn),
        None => Ok(base),
    }
}

/// Record (and rate-limited-warn) that a decref past zero was skipped.
/// `site` distinguishes the stage merge ("stage") from the array-apply
/// flush ("array").
pub(crate) fn note_decref_underflow_skip(delta: i64, lsn: Lsn, prev_rc: u32, site: &'static str) {
    let total = UNDERFLOW_CLAMPED.fetch_add(1, Ordering::Relaxed) + 1;
    // Power-of-two rate limit so a storm of clamps can't flood the log.
    if total.is_power_of_two() {
        tracing::warn!(
            target: "onyx_metadb::refcount::stage_underflow",
            delta,
            lsn,
            prev_rc,
            site,
            clamped_total = total,
            "rc decref past zero skipped (benign double-decref); count left unchanged"
        );
    }
}

/// Like [`apply_delta_pure`] but a decref that would underflow is a
/// benign no-op: the count is left at `prev` (already its floor) rather
/// than erroring, so a double-decref of an already-freed PBA cannot
/// poison the apply / checkpoint pipeline. Overflow still errors. The
/// returned `bool` is `true` when an underflow was skipped, so the
/// caller can record it via [`note_decref_underflow_skip`].
pub(crate) fn apply_delta_or_skip(prev: RcEntry, delta: i64, lsn: Lsn) -> Result<(RcEntry, bool)> {
    match apply_delta_pure(prev, delta, lsn) {
        Ok(entry) => Ok((entry, false)),
        Err(_) if delta < 0 => Ok((prev, true)),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod apply_delta_tests {
    use super::*;

    #[test]
    fn zero_to_one_stamps_birth_lsn() {
        let r = apply_delta_pure(RcEntry::ZERO, 1, 42).unwrap();
        assert_eq!(
            r,
            RcEntry {
                rc: 1,
                birth_lsn: 42
            }
        );
    }

    #[test]
    fn preserves_birth_lsn_on_existing() {
        let prev = RcEntry {
            rc: 2,
            birth_lsn: 10,
        };
        let r = apply_delta_pure(prev, 3, 42).unwrap();
        assert_eq!(
            r,
            RcEntry {
                rc: 5,
                birth_lsn: 10
            }
        );
    }

    #[test]
    fn to_zero_resets_birth_lsn() {
        let prev = RcEntry {
            rc: 1,
            birth_lsn: 10,
        };
        let r = apply_delta_pure(prev, -1, 42).unwrap();
        assert_eq!(r, RcEntry::ZERO);
    }

    #[test]
    fn overflow_errors() {
        let prev = RcEntry {
            rc: u32::MAX,
            birth_lsn: 1,
        };
        assert!(apply_delta_pure(prev, 1, 42).is_err());
    }

    #[test]
    fn underflow_errors() {
        let prev = RcEntry {
            rc: 0,
            birth_lsn: 0,
        };
        assert!(apply_delta_pure(prev, -1, 42).is_err());
    }

    #[test]
    fn apply_delta_or_skip_skips_decref_underflow_but_errors_on_overflow() {
        // Decref past zero -> benign skip (count left at prev, flagged).
        let (entry, skipped) = apply_delta_or_skip(RcEntry::ZERO, -1, 5).unwrap();
        assert_eq!(entry, RcEntry::ZERO);
        assert!(skipped);
        let prev = RcEntry {
            rc: 1,
            birth_lsn: 7,
        };
        let (entry, skipped) = apply_delta_or_skip(prev, -3, 8).unwrap();
        assert_eq!(entry, prev); // left unchanged (leaked, never negative)
        assert!(skipped);

        // A decref that does NOT underflow applies normally.
        let prev = RcEntry {
            rc: 2,
            birth_lsn: 7,
        };
        let (entry, skipped) = apply_delta_or_skip(prev, -1, 9).unwrap();
        assert_eq!(entry.rc, 1);
        assert!(!skipped);

        // Overflow still errors — never silently absorbed.
        let max = RcEntry {
            rc: u32::MAX,
            birth_lsn: 1,
        };
        assert!(apply_delta_or_skip(max, 1, 10).is_err());
    }

    #[test]
    fn merge_read_or_floor_floors_underflow_to_zero_passes_through_normal() {
        // Decref past zero on a read merge -> rc=0 floor, counter bumps.
        let before = read_underflow_floored_total();
        let entry = merge_read_or_floor(RcEntry::ZERO, -1, 5).unwrap();
        assert_eq!(entry, RcEntry::ZERO);
        assert_eq!(read_underflow_floored_total(), before + 1);

        // A net-negative multi-decref also floors to 0 (rc can't be < 0).
        let prev = RcEntry {
            rc: 1,
            birth_lsn: 7,
        };
        let entry = merge_read_or_floor(prev, -3, 8).unwrap();
        assert_eq!(entry, RcEntry::ZERO);

        // A normal (non-underflowing) merge passes through unchanged and
        // does NOT bump the floor counter.
        let mid = read_underflow_floored_total();
        let prev = RcEntry {
            rc: 2,
            birth_lsn: 7,
        };
        let entry = merge_read_or_floor(prev, -1, 9).unwrap();
        assert_eq!(entry.rc, 1);
        assert_eq!(read_underflow_floored_total(), mid);

        // Overflow (delta >= 0) still errors — never floored.
        let max = RcEntry {
            rc: u32::MAX,
            birth_lsn: 1,
        };
        assert!(merge_read_or_floor(max, 1, 10).is_err());
    }
}
