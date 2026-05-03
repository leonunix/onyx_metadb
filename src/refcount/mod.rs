//! Paged-array + delta refcount store.
//!
//! Replaces the COW B+tree (`crate::btree`). Each shard owns a paged
//! array indexed by `pba >> ENTRIES_PER_PAGE_SHIFT`; reads / writes go
//! to a `DeltaMap` first and fall back to the on-disk array on miss.
//! Apply lanes drain delta into pages at commit boundaries.
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
pub mod shard;

pub use array::{PagedRefcountArray, ENTRIES_PER_PAGE};
pub use delta::DeltaMap;
pub use shard::RcShard;

use crate::btree::format::RcEntry;
use crate::error::{MetaDbError, Result};
use crate::types::Lsn;

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

#[cfg(test)]
mod apply_delta_tests {
    use super::*;

    #[test]
    fn zero_to_one_stamps_birth_lsn() {
        let r = apply_delta_pure(RcEntry::ZERO, 1, 42).unwrap();
        assert_eq!(r, RcEntry { rc: 1, birth_lsn: 42 });
    }

    #[test]
    fn preserves_birth_lsn_on_existing() {
        let prev = RcEntry { rc: 2, birth_lsn: 10 };
        let r = apply_delta_pure(prev, 3, 42).unwrap();
        assert_eq!(r, RcEntry { rc: 5, birth_lsn: 10 });
    }

    #[test]
    fn to_zero_resets_birth_lsn() {
        let prev = RcEntry { rc: 1, birth_lsn: 10 };
        let r = apply_delta_pure(prev, -1, 42).unwrap();
        assert_eq!(r, RcEntry::ZERO);
    }

    #[test]
    fn overflow_errors() {
        let prev = RcEntry { rc: u32::MAX, birth_lsn: 1 };
        assert!(apply_delta_pure(prev, 1, 42).is_err());
    }

    #[test]
    fn underflow_errors() {
        let prev = RcEntry { rc: 0, birth_lsn: 0 };
        assert!(apply_delta_pure(prev, -1, 42).is_err());
    }
}
