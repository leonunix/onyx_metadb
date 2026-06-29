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
//! * One [`LifecycleOp::PromotionChunk`] per call carries up to
//!   `MAX_PROMOTION_CHUNK_PBAS` incref edges, ordered by ascending LBA.
//! * The volume's `promotion_cursor` advances to the LBA *just after*
//!   the largest LBA processed in the chunk; the next call resumes
//!   from there.
//! * When the scan from the current cursor yields nothing, the walker
//!   emits a final [`LifecycleOp::PromotionComplete`] that clears the
//!   clone's `parent_vol_ord` + `promotion_cursor` and lets the parent's
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
use crate::lifecycle_log::LifecycleOp;
use crate::types::{Lba, Pba, VolumeOrdinal};

use super::Db;

/// Cap on the number of incref edges packed into one
/// [`LifecycleOp::PromotionChunk`] record. Sized so the encoded
/// journal body stays well below the segment boundary while still
/// amortising the per-chunk fsync.
pub(crate) const MAX_PROMOTION_CHUNK_PBAS: usize = 65536;

/// Outcome of one [`Db::run_promotion_chunk`] iteration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PromotionStep {
    /// `vol_ord` does not name a clone, or the walker has already
    /// finished (`parent_vol_ord = None`). Callers can move on to
    /// the next volume.
    NotApplicable,
    /// A [`LifecycleOp::PromotionChunk`] was committed. `increfs` is the
    /// number of PBA edges in this chunk; `next_cursor` is what the
    /// volume's `promotion_cursor` was advanced to.
    ChunkApplied {
        increfs: usize,
        next_cursor: Option<Lba>,
    },
    /// Either this call found no remaining mappings from the cursor
    /// onward (and emitted a bare [`LifecycleOp::PromotionComplete`]),
    /// or the prior chunk already exhausted the L2P (this call only
    /// emitted the trailing completion record). Either way the
    /// volume's `parent_vol_ord` is now `None`.
    Completed,
}

impl Db {
    /// Run one promotion step for `vol_ord`. Returns
    /// [`PromotionStep::NotApplicable`] when the volume isn't a clone
    /// (or has already been promoted). Otherwise commits a
    /// [`LifecycleOp::PromotionChunk`] (and possibly a trailing
    /// [`LifecycleOp::PromotionComplete`]) and reports what changed.
    ///
    /// Idempotent across crashes via the lifecycle-journal
    /// `lifecycle_replay_seq` cutoff: records ≤ the persisted seq
    /// aren't replayed, so per-PBA increfs land at most once per
    /// lifecycle record. A crash between chunks resumes from the
    /// persisted `promotion_cursor`.
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
            self.commit_promotion_complete(vol_ord)?;
            return Ok(PromotionStep::Completed);
        }

        let next_cursor = chunk.next_cursor;
        let increfs = chunk.pbas.len();
        self.commit_promotion_chunk(vol_ord, chunk.pbas, next_cursor)?;

        if next_cursor.is_none() {
            // Last chunk by construction (we drained all remaining
            // mappings). Emit the trailing completion record in the
            // same call so the caller doesn't need a follow-up
            // iteration just to clear the lineage edge.
            self.commit_promotion_complete(vol_ord)?;
            Ok(PromotionStep::Completed)
        } else {
            Ok(PromotionStep::ChunkApplied {
                increfs,
                next_cursor,
            })
        }
    }

    /// Drive `vol_ord`'s clone-promotion walker to completion: commit every
    /// [`LifecycleOp::PromotionChunk`] and the trailing
    /// [`LifecycleOp::PromotionComplete`] that clears `parent_vol_ord`.
    /// Returns `true` if `vol_ord` was an un-promoted clone now made
    /// independent, `false` if it was not a clone / already promoted.
    ///
    /// Public so the `metadb-soak` harness (a separate binary crate that can
    /// only reach `pub` API) can drive the promote→drop interleave the
    /// runtime clone-drop livelist shadow needs (without it the soak issues
    /// zero promotions, so the shadow only ever sees non-promoted clone
    /// drops). Each chunk is its own WAL record + fsync; for a clone with
    /// `K` live mappings this is `O(K / MAX_PROMOTION_CHUNK_PBAS)` commits.
    pub fn promote_volume(&self, vol_ord: VolumeOrdinal) -> Result<bool> {
        let mut promoted = false;
        loop {
            match self.run_promotion_chunk(vol_ord)? {
                PromotionStep::NotApplicable => return Ok(promoted),
                PromotionStep::ChunkApplied { .. } => promoted = true,
                PromotionStep::Completed => return Ok(true),
            }
        }
    }

    pub(crate) fn commit_promotion_chunk(
        &self,
        vol_ord: VolumeOrdinal,
        pba_increfs: Vec<Pba>,
        next_cursor: Option<Lba>,
    ) -> Result<()> {
        // drop_gate.read() BEFORE txg.enter() — deadlock-free lock order (see
        // commit_free_pbas / commit_ops): a commit parked at drop_gate.read
        // must hold no txg inflight, else a force-syncing lifecycle op
        // (take_snapshot / create_volume) deadlocks on roll_to_quiescing's
        // inflight drain. promote_volume drives this autonomously.
        let _drop_guard = self.drop_gate.read();
        let _txg_guard = self.txg.enter();
        let lifecycle_op = LifecycleOp::PromotionChunk {
            vol_ord,
            pba_increfs: pba_increfs.clone(),
            next_cursor,
        };
        let lsn = self.submit_lifecycle_op_with_dispatch(&lifecycle_op)?;
        _txg_guard.record_lsn(lsn);

        let _apply_guard = self.acquire_commit_apply_gate(lsn);
        let _active = self.enter_active_apply(lsn);

        super::apply_promotion_chunk(
            &self.volumes.read(),
            &self.refcount_shards,
            lsn,
            _txg_guard.txg(),
            vol_ord,
            &pba_increfs,
            next_cursor,
        )?;

        // v20 (S0): durably record the PBAs this chunk incref'd into the
        // volume's promoted-PBA log so `drop_volume` can later decref them
        // survivor-gated (closing the promotion over-pin leak). Synchronous
        // append (promotion is an admin op, not the hot path); the segment
        // page is fsync'd here, the anchor persists at the next manifest flush.
        //
        // NON-FATAL by design: the increfs are already applied + WAL-durable
        // (`apply_promotion_chunk` above) and `finish_global_apply` MUST still
        // run, so an emit IO failure (ENOSPC/EIO) must NOT `?`-return here — that
        // would leave `last_applied_lsn` stuck at `lsn-1` (never advanced, never
        // poisoned) and hang every later lifecycle commit. A failed/dropped log
        // segment only means this chunk's edges are not yet decref-able at drop
        // = a residual over-pin leak (data-safe, == today's behaviour), never a
        // free of a live PBA.
        //
        // ⚠ Faithfulness window (deferred): the log is emitted only on the live
        // commit path, NOT on WAL replay (`open.rs` replays `apply_promotion_chunk`
        // = re-increfs rc, but does not re-emit the segment). So a promote that
        // is never sealed by a manifest flush before a restart loses this
        // chunk's records while the rc increfs persist via replay → residual
        // over-pin for those edges. The full fix is a buffered-seal-at-flush
        // (atomic with the manifest, like the page-livelist) or a replay
        // re-emit; both deferred — the leak is data-safe and strictly smaller
        // than the pre-S0 behaviour where NO promoted PBA was ever reclaimable.
        if let Err(e) = self.emit_promoted_pba_log(vol_ord, &pba_increfs, lsn) {
            tracing::warn!(
                vol_ord,
                lsn,
                error = %e,
                "promoted-PBA log emit failed; promotion increfs applied, this chunk's \
                 log segment skipped (residual over-pin leak for these edges, data-safe)"
            );
        }

        {
            let mut mstate = self.manifest_state.lock();
            if let Some(entry) = mstate
                .manifest
                .volumes
                .iter_mut()
                .find(|e| e.ord == vol_ord)
            {
                entry.promotion_cursor = next_cursor;
                // Mirror the promoted-log anchors the emit just advanced, so
                // the in-memory manifest stays consistent between flushes (the
                // flush's `refresh_manifest_*` re-reads them from the volume
                // atomics regardless).
                if let Some(vol) = self.volumes.read().get(&vol_ord) {
                    entry.promoted_log_head_pid = vol
                        .promoted_log_head_pid
                        .load(std::sync::atomic::Ordering::Acquire);
                    entry.promoted_log_tail_pid = vol
                        .promoted_log_tail_pid
                        .load(std::sync::atomic::Ordering::Acquire);
                }
            }
        }

        self.finish_global_apply(lsn)?;
        self.advance_dispatch_lsn(lsn);
        Ok(())
    }

    /// v20 (ZFS port Phase 4 Step 4 / S0): append `pbas` as one segment to the
    /// volume's promoted-PBA log chain (reusing the `LiveListSegment` codec with
    /// the raw `Pba` stored in the record's `pid` slot). Writes + fsyncs the
    /// segment page(s) and advances the in-memory `promoted_log_{head,tail}_pid`
    /// anchors; the durable anchor rides the next manifest flush. No-op for an
    /// empty chunk.
    fn emit_promoted_pba_log(
        &self,
        vol_ord: VolumeOrdinal,
        pbas: &[Pba],
        lsn: crate::types::Lsn,
    ) -> Result<()> {
        if pbas.is_empty() {
            return Ok(());
        }
        let vol = match self.volumes.read().get(&vol_ord) {
            Some(v) => v.clone(),
            None => return Ok(()),
        };
        let records: Vec<crate::livelist::LiveRecord> = pbas
            .iter()
            .map(|&pba| crate::livelist::LiveRecord {
                pid: pba,
                birth_lsn: 0,
                event_lsn: lsn,
                kind: crate::livelist::LiveKind::Alloc,
            })
            .collect();
        let page_count = crate::livelist::segment_pages_for(records.len());
        if page_count == 0 {
            return Ok(());
        }
        let prev_tail = vol
            .promoted_log_tail_pid
            .load(std::sync::atomic::Ordering::Acquire);
        let start_pid = self.page_store.allocate_run(page_count)?;
        let pages = crate::livelist::build_segment_pages(start_pid, &records, prev_tail, lsn);
        let sealed: Vec<(crate::types::PageId, std::sync::Arc<crate::page::Page>)> = pages
            .into_iter()
            .map(|(pid, page)| (pid, std::sync::Arc::new(page)))
            .collect();
        self.page_store.write_sealed_page_runs(sealed)?;
        self.page_store.sync()?;
        if prev_tail == crate::types::NULL_PAGE {
            vol.promoted_log_head_pid
                .store(start_pid, std::sync::atomic::Ordering::Release);
        }
        vol.promoted_log_tail_pid
            .store(start_pid, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub(crate) fn commit_promotion_complete(&self, vol_ord: VolumeOrdinal) -> Result<()> {
        // drop_gate.read() BEFORE txg.enter() — deadlock-free lock order (see
        // commit_free_pbas / commit_ops). PromotionComplete is emitted at the
        // tail of every promote_volume run; an enter-before-read here would
        // leave the deadlock live on the completion record.
        let _drop_guard = self.drop_gate.read();
        let _txg_guard = self.txg.enter();
        let lifecycle_op = LifecycleOp::PromotionComplete { vol_ord };
        let lsn = self.submit_lifecycle_op_with_dispatch(&lifecycle_op)?;
        _txg_guard.record_lsn(lsn);

        let _apply_guard = self.acquire_commit_apply_gate(lsn);
        let _active = self.enter_active_apply(lsn);

        super::apply_promotion_complete(&self.volumes.read(), vol_ord)?;

        {
            let mut mstate = self.manifest_state.lock();
            if let Some(entry) = mstate
                .manifest
                .volumes
                .iter_mut()
                .find(|e| e.ord == vol_ord)
            {
                entry.parent_vol_ord = None;
                entry.promotion_cursor = None;
            }
        }

        self.finish_global_apply(lsn)?;
        self.advance_dispatch_lsn(lsn);
        Ok(())
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
