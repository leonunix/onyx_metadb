//! Background clone-promotion walker.
//!
//! [[no-refcount-hot-path-design]] Phase 4 Step 5. A clone created by
//! [`Db::clone_volume`] starts life COW-sharing the parent snapshot's
//! L2P pages. Hot-path writes that diverge from the parent's mapping
//! force per-page COW on demand; pages the clone never touches stay
//! shared with the parent.
//!
//! Phase 4 keeps the legacy `incref(new) / decref(old)` on the hot path
//! unchanged, so global rc still reflects "how many distinct pointers
//! exist" for shared PBAs. The job of the promotion walker is purely
//! lineage bookkeeping: for every PBA the clone references at the
//! moment of cloning, bump global rc by one so the parent can
//! eventually be dropped without dragging the clone's data with it.
//! Phase 5 will then flip the hot path to skip the per-write rc
//! mutations, at which point this walker becomes the only producer of
//! "shared PBA" rc edges.
//!
//! The walker is intentionally simple:
//! * One [`WalOp::PromotionChunk`] per call carries up to
//!   `MAX_PROMOTION_CHUNK_PBAS` incref edges, ordered by ascending LBA.
//! * The volume's `promotion_cursor` advances to the LBA *just after*
//!   the largest LBA processed in the chunk; the next call resumes
//!   from there.
//! * When the scan from the current cursor yields nothing, the walker
//!   emits a final [`WalOp::PromotionComplete`] that clears the clone's
//!   `parent_vol_ord` + `promotion_cursor` and lets the parent's
//!   lineage GC stop treating this clone's `branched_at_lsn` as a pin
//!   point.
//!
//! Decision A (from the Phase 4 plan): the walker only mutates global
//! rc. Exclusive PBAs (not yet in `dedup_index`) are *not* promoted
//! into the dedup table here — that upgrade is deferred to the next
//! dedup-on-write hit. Keeping the walker purely metadb-internal
//! avoids cross-process hash recomputation and keeps each PromotionChunk
//! a single fsync.

use std::sync::Arc;

use crate::error::Result;
use crate::types::{Lba, Pba, VolumeOrdinal};
use crate::wal::WalOp;

use super::Db;

/// Cap on the number of incref edges packed into one
/// [`WalOp::PromotionChunk`] record. Sized so the encoded WAL body
/// stays well below the segment boundary while still amortising the
/// per-chunk fsync. Mirrors the runtime check in
/// [`crate::wal::op::MAX_PROMOTION_CHUNK_PBAS`].
pub(crate) const MAX_PROMOTION_CHUNK_PBAS: usize = crate::wal::op::MAX_PROMOTION_CHUNK_PBAS;

/// Outcome of one [`Db::run_promotion_chunk`] iteration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PromotionStep {
    /// `vol_ord` does not name a clone, or the walker has already
    /// finished (`parent_vol_ord = None`). Callers can move on to
    /// the next volume.
    NotApplicable,
    /// A [`WalOp::PromotionChunk`] was committed. `increfs` is the
    /// number of PBA edges in this chunk; `next_cursor` is what the
    /// volume's `promotion_cursor` was advanced to.
    ChunkApplied {
        increfs: usize,
        next_cursor: Option<Lba>,
    },
    /// Either this call found no remaining mappings from the cursor
    /// onward (and emitted a bare [`WalOp::PromotionComplete`]), or
    /// the prior chunk already exhausted the L2P (this call only
    /// emitted the trailing completion record). Either way the
    /// volume's `parent_vol_ord` is now `None`.
    Completed,
}

impl Db {
    /// Run one promotion step for `vol_ord`. Returns
    /// [`PromotionStep::NotApplicable`] when the volume isn't a clone
    /// (or has already been promoted). Otherwise commits a
    /// [`WalOp::PromotionChunk`] (and possibly a trailing
    /// [`WalOp::PromotionComplete`]) and reports what changed.
    ///
    /// Idempotent across crashes via the WAL `checkpoint_lsn` cutoff:
    /// records ≤ `checkpoint_lsn` aren't replayed, so per-PBA increfs
    /// land at most once per WAL record. A crash between chunks
    /// resumes from the persisted `promotion_cursor`.
    pub(crate) fn run_promotion_chunk(&self, vol_ord: VolumeOrdinal) -> Result<PromotionStep> {
        let volume = match self.volumes.read().get(&vol_ord).cloned() {
            Some(v) => v,
            None => return Ok(PromotionStep::NotApplicable),
        };
        if volume.parent_vol_ord.read().is_none() {
            return Ok(PromotionStep::NotApplicable);
        }
        let start_lba = volume.promotion_cursor.read().unwrap_or(0);

        // Collect up to MAX_PROMOTION_CHUNK_PBAS (lba, pba) pairs at
        // or above `start_lba`. The materialised range scan is bounded
        // by the volume's L2P size, not by the LBA space; a clone with
        // K live mappings costs O(K) memory across the whole walker
        // run, not per call. For the typical clone-then-promote
        // workload this is dwarfed by the WAL body bytes the walker
        // is about to emit anyway. Phase 5 can swap in a streaming
        // shard walker if production traces show the materialisation
        // is a problem.
        let chunk = collect_chunk(self, vol_ord, &volume, start_lba)?;

        if chunk.pbas.is_empty() {
            // Cursor is past every live mapping. Emit the completion
            // record and return; the apply path clears both
            // `parent_vol_ord` and `promotion_cursor`.
            self.commit_ops(&[WalOp::PromotionComplete { vol_ord }])?;
            return Ok(PromotionStep::Completed);
        }

        let next_cursor = chunk.next_cursor;
        let increfs = chunk.pbas.len();
        self.commit_ops(&[WalOp::PromotionChunk {
            vol_ord,
            pba_increfs: chunk.pbas.into_boxed_slice(),
            next_cursor,
        }])?;

        if next_cursor.is_none() {
            // Last chunk by construction (we drained all remaining
            // mappings). Emit the trailing completion record in the
            // same call so the caller doesn't need a follow-up
            // iteration just to clear the lineage edge.
            self.commit_ops(&[WalOp::PromotionComplete { vol_ord }])?;
            Ok(PromotionStep::Completed)
        } else {
            Ok(PromotionStep::ChunkApplied {
                increfs,
                next_cursor,
            })
        }
    }
}

struct CollectedChunk {
    pbas: Vec<Pba>,
    /// `Some(lba)` = next call should resume from `lba`. `None` =
    /// this chunk drained the volume's L2P; the caller emits
    /// `PromotionComplete` right after the chunk.
    next_cursor: Option<Lba>,
}

fn collect_chunk(
    db: &Db,
    vol_ord: VolumeOrdinal,
    _volume: &Arc<super::Volume>,
    start_lba: Lba,
) -> Result<CollectedChunk> {
    // `db.range` already locks-and-merges across the volume's shards
    // through the lock-free read view, so this can run under
    // `apply_gate.read()` alongside live commits without blocking
    // the hot path.
    let iter = db.range(vol_ord, start_lba..)?;
    let mut pbas: Vec<Pba> = Vec::new();
    let mut last_processed_lba: Option<Lba> = None;
    let mut exhausted = true;
    for item in iter {
        let (lba, value) = item?;
        if pbas.len() >= MAX_PROMOTION_CHUNK_PBAS {
            exhausted = false;
            break;
        }
        pbas.push(value.head_pba());
        last_processed_lba = Some(lba);
    }
    let next_cursor = if exhausted {
        None
    } else {
        // We hit the chunk cap before the iterator ran out. Resume at
        // the LBA just after the last one we consumed. `+ 1` cannot
        // overflow here because `last_processed_lba` was a valid `Lba`
        // we just saw and `u64::MAX` would not have left room for
        // another entry beyond it.
        last_processed_lba.map(|l| l.saturating_add(1))
    };
    Ok(CollectedChunk { pbas, next_cursor })
}
