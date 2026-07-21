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
pub(crate) mod delta_run;
pub mod overlay;
pub(crate) mod segment_dir;
pub mod shard;

pub use array::{ENTRIES_PER_PAGE, PagedRefcountArray};
pub use delta::DeltaMap;
pub use shard::RcShard;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{MetaDbError, Result};
use crate::types::Lsn;
use xxhash_rust::xxh3::xxh3_64;

/// Durable refcount-shard routing semantic selected by the manifest version.
///
/// The paged-array bytes are identical in both modes. What changes is which
/// shard owns a PBA, so this must never change while an existing manifest is
/// being rewritten.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefcountRouting {
    /// Manifest v25: hash every PBA independently.
    LegacyPbaHash,
    /// Manifest v26: hash the 336-entry refcount page index. All entries that
    /// share one logical refcount page therefore share one shard.
    PageAffine,
}

/// The divisor is part of manifest-v26's durable routing contract. Do not
/// derive it from the current array layout: changing this value requires a new
/// manifest routing version and an explicit migration policy.
pub(crate) const REFCOUNT_V26_ROUTING_ENTRIES_PER_PAGE: usize = 336;

const _: () = {
    assert!(array::ENTRIES_PER_PAGE == REFCOUNT_V26_ROUTING_ENTRIES_PER_PAGE);
};

impl RefcountRouting {
    pub(crate) fn from_manifest_version(version: u32) -> Option<Self> {
        match version {
            25 => Some(Self::LegacyPbaHash),
            // v26 and v27 (delta-run persist) share the page-affine routing —
            // v27 only ADDS the segment-directory heads array, it does not change
            // which shard owns a PBA. The delta-run codec's `routing_version`
            // byte therefore stays 26 (see `manifest_version`).
            26 | 27 => Some(Self::PageAffine),
            _ => None,
        }
    }

    pub(crate) const fn manifest_version(self) -> u32 {
        match self {
            Self::LegacyPbaHash => 25,
            Self::PageAffine => 26,
        }
    }

    #[inline]
    pub(crate) fn shard_for_pba(self, pba: crate::types::Pba, shard_count: usize) -> usize {
        debug_assert!(shard_count > 0);
        let routing_key = match self {
            Self::LegacyPbaHash => pba,
            Self::PageAffine => pba / REFCOUNT_V26_ROUTING_ENTRIES_PER_PAGE as u64,
        };
        (xxh3_64(&routing_key.to_be_bytes()) as usize) % shard_count
    }
}

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

/// Count of plain read-side merge underflows floored to rc=0 (vs propagated as
/// an Err), across `RcShard::lookup_entry` (`get` / `get_many`), all shards.
static READ_UNDERFLOW_FLOORED: AtomicU64 = AtomicU64::new(0);

/// Count of data-page reads served from the dirty-staged overlay
/// (staged-but-not-yet-durable checkpoint pages), all shards.
static STAGED_OVERLAY_HITS: AtomicU64 = AtomicU64::new(0);

/// Total rc data-page reads served from the dirty-staged overlay. Each
/// hit is a read that, pre-overlay, would have raced the checkpoint's
/// deferred page write: for a fresh page it would have read unwritten
/// disk zeros (`PageMagicMismatch` → commit error), for a re-staged page
/// it COULD have read pre-fold content if the LRU had evicted the staged
/// copy (silent rc under-count). Non-zero under load is expected and
/// healthy — it proves the overlay is absorbing the race.
pub fn staged_overlay_hits_total() -> u64 {
    STAGED_OVERLAY_HITS.load(Ordering::Relaxed)
}

#[inline]
pub(crate) fn note_staged_overlay_hit() {
    let total = STAGED_OVERLAY_HITS.fetch_add(1, Ordering::Relaxed) + 1;
    // Power-of-two rate limit: ~40 lines over a process lifetime, just
    // enough to confirm in logs that the overlay is absorbing the
    // checkpoint-window race (each hit was a PageMagicMismatch commit
    // error or a silent rc under-count pre-overlay).
    if total.is_power_of_two() {
        tracing::info!(
            target: "onyx_metadb::refcount::staged_overlay",
            hits_total = total,
            "rc read served from dirty-staged checkpoint overlay"
        );
    }
}

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
/// Why this exists: plain `RcShard::get` / `get_many` sample pending slots and
/// the array without holding `fold_lock`. During publish-before-clear they can
/// observe a base that already folded a pending decref while still seeing that
/// same pending delta, double-counting it and computing a negative rc. The
/// logical read floor is 0. Mutation paths do not use this helper: `stage` and
/// `stage_batch` validate `fold_epoch` under `fold_lock` and all slot guards, so
/// an underflow in their coherent sample is a real model error.
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
/// pending delta onto `base` (`None` leaves `base` unchanged). This mirrors the
/// read-only lookup shape; mutation staging uses a fold-coherent snapshot.
#[inline]
pub(crate) fn merge_pending_read(
    base: RcEntry,
    pending: Option<delta::Pending>,
) -> Result<RcEntry> {
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
