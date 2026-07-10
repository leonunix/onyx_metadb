//! BFG — deferred commit outcome plumbing.
//!
//! ## Responsibility
//!
//! `Db::commit_ops` returns `(Lsn, Vec<ApplyOutcome>)` synchronously. The
//! deferred entry point `Db::commit_ops_deferred` returns
//! `(Lsn, DeferredOutcomeHandle)` once WAL is durable and the L2pBuffer
//! insertions have completed.
//!
//! ## simplification (post-`L2pCompactor` retirement)
//!
//! The original design parked outcomes in a per-LSN aggregator
//! and released them on the `L2pCompactor`'s per-pass drain. Once BFG retired
//! the compactor and moved real per-BFG flush work into `BfgSyncThread`, the
//! commit-frequency "wake the compactor" channel that used to drain
//! these handles no longer exists. Hooking the wake onto the sync
//! thread would tie outcome delivery to the 5 s BFG quiesce timer —
//! orders of magnitude slower than the onyx commit_worker pipeline
//! requires.
//!
//! Inline delivery is safe because by the time `stage()` is called:
//!
//! - The commit's apply phase has completed (the call site at the
//!   bottom of `commit_ops_deferred` waits on the sync-mode commit
//!   path before staging).
//! - `last_applied_lsn` has been bumped past the commit's LSN.
//! - The L2pBuffer insert is visible to any later reader on any thread
//!   (`finish_global_apply` publishes under the commit cvar).
//!
//! So a sync-mode caller would already observe identical state via a
//! fresh `get`. Returning the outcomes through a pre-populated
//! capacity-1 channel preserves the `DeferredOutcomeHandle` API
//! surface for callers without the parking-lot intermediate.

use std::sync::Arc;

use crossbeam_channel::{Receiver, bounded};
use parking_lot::Mutex;

use crate::error::{MetaDbError, Result};
use crate::metrics::MetaMetrics;
use crate::testing::faults::FaultController;
use crate::tx::ApplyOutcome;
use crate::types::Lsn;

/// One-shot receiver returned by [`crate::Db::commit_ops_deferred`].
///
/// Since the simplification, outcomes are populated before the
/// handle is constructed, so `recv` / `try_recv` never block under
/// normal operation. The handle remains a `Receiver`-backed type so
/// callers using the `recv()` API don't need to change.
pub struct DeferredOutcomeHandle {
    rx: Receiver<Result<Vec<ApplyOutcome>>>,
    lsn: Lsn,
}

impl DeferredOutcomeHandle {
    /// Build a handle whose channel is already populated. Used by both
    /// `commit_ops_deferred` paths (sync delivery and the
    /// deferred-but-now-inline aggregator stage).
    pub(crate) fn ready(lsn: Lsn, value: Result<Vec<ApplyOutcome>>) -> Self {
        let (tx, rx) = bounded(1);
        // Send must succeed because the channel has capacity 1 and we
        // own both ends. Drop the sender so a follow-up recv on the
        // receiver sees the value rather than blocking on the sender.
        let _ = tx.send(value);
        drop(tx);
        Self { rx, lsn }
    }

    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Non-consuming readiness probe. Returns `true` when the channel
    /// has the staged outcome (or has been disconnected — `recv` /
    /// `try_recv` will then resolve without blocking). With the     /// inline delivery this is always true; kept for API compatibility
    /// with onyx's commit_worker chunk drainer.
    pub fn is_ready(&self) -> bool {
        !self.rx.is_empty()
    }

    /// Non-blocking probe. Returns `Some(outcomes)` if the channel is
    /// already populated (always true under inline delivery);
    /// consumes the handle on `Some`.
    pub fn try_recv(self) -> std::result::Result<Result<Vec<ApplyOutcome>>, Self> {
        match self.rx.try_recv() {
            Ok(value) => Ok(value),
            Err(crossbeam_channel::TryRecvError::Empty) => Err(self),
            Err(crossbeam_channel::TryRecvError::Disconnected) => Ok(Err(
                MetaDbError::InvalidArgument("deferred outcome channel disconnected".into()),
            )),
        }
    }

    /// Blocking wait for the staged outcome. With inline
    /// delivery this returns immediately.
    pub fn recv(self) -> Result<Vec<ApplyOutcome>> {
        match self.rx.recv() {
            Ok(value) => value,
            Err(_) => Err(MetaDbError::InvalidArgument(
                "deferred outcome channel disconnected".into(),
            )),
        }
    }
}

/// thin shell over [`DeferredOutcomeHandle::ready`].
///
/// Pre-this owned a per-LSN parking lot drained by the
/// `L2pCompactor`. The compactor is gone; outcomes are delivered
/// inline. The aggregator is kept as a public type so the existing
/// `db.deferred_outcomes.stage(...)` call site (and a handful of
/// tests / metrics) stay compiling — its observable behaviour is
/// "instant delivery, zero pending depth".
pub(crate) struct DeferredOutcomeAggregator {
    metrics: Arc<MetaMetrics>,
    /// Fault controller kept for parity with the legacy API; no fault
    /// points fire on the inline path. Held under `Mutex` to avoid
    /// adding a generic lifetime to the aggregator.
    #[allow(dead_code)]
    faults: Arc<FaultController>,
    /// Kept so `pending_depth()` can return a stable 0 without an
    /// atomic load and so we can debug-assert no double-stage.
    #[allow(dead_code)]
    seen_lsns: Mutex<()>,
}

impl DeferredOutcomeAggregator {
    pub(crate) fn new(metrics: Arc<MetaMetrics>, faults: Arc<FaultController>) -> Self {
        Self {
            metrics,
            faults,
            seen_lsns: Mutex::new(()),
        }
    }

    /// Build a handle whose channel is already populated with
    /// `outcomes`. The pre-aggregator parked the entry and
    /// released it on the next compactor pass; with the compactor
    /// retired, delivery is inline (every guarantee the deferred path
    /// promised — apply complete, last_applied_lsn bumped, L2pBuffer
    /// insert visible — has already been established by the sync-mode
    /// commit that produced these outcomes).
    pub(crate) fn stage(&self, lsn: Lsn, outcomes: Vec<ApplyOutcome>) -> DeferredOutcomeHandle {
        self.metrics.record_deferred_outcomes_staged();
        // zero pending depth at all times.
        self.metrics.record_deferred_outcomes_pending(0);
        self.metrics.record_deferred_outcomes_released(1);
        DeferredOutcomeHandle::ready(lsn, Ok(outcomes))
    }

    /// Watermark-based drain. leaves this as a no-op since
    /// every entry is delivered inline. Kept for API parity with the
    /// pre-surface; always returns 0.
    pub(crate) fn drain_up_to_lsn(&self, _watermark: Lsn) -> usize {
        0
    }

    /// Always 0 under inline delivery.
    pub(crate) fn pending_depth(&self) -> usize {
        0
    }

    /// No-op under inline delivery (no waiters parked in this
    /// aggregator).
    pub(crate) fn poison_all(&self, _msg: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> Arc<MetaMetrics> {
        Arc::new(MetaMetrics::default())
    }

    fn fresh_agg() -> DeferredOutcomeAggregator {
        DeferredOutcomeAggregator::new(metrics(), FaultController::disabled())
    }

    /// stage delivers inline. The handle's channel resolves
    /// before stage returns; aggregator depth stays at 0.
    #[test]
    fn stage_delivers_inline() {
        let agg = fresh_agg();
        let h = agg.stage(10, vec![ApplyOutcome::L2pPrev(None)]);
        assert_eq!(h.lsn(), 10);
        assert!(h.is_ready());
        assert_eq!(agg.pending_depth(), 0);
        let outcomes = h.recv().expect("outcomes delivered");
        assert_eq!(outcomes.len(), 1);
    }

    /// drain_up_to_lsn is a no-op ().
    #[test]
    fn drain_up_to_lsn_is_noop() {
        let agg = fresh_agg();
        let _h = agg.stage(50, vec![ApplyOutcome::L2pPrev(None)]);
        assert_eq!(agg.drain_up_to_lsn(100), 0);
        assert_eq!(agg.pending_depth(), 0);
    }

    /// poison_all is a no-op ().
    #[test]
    fn poison_all_is_noop() {
        let agg = fresh_agg();
        let handle = agg.stage(30, vec![ApplyOutcome::L2pPrev(None)]);
        agg.poison_all("test shutdown");
        // Handle was already populated; poison can't take that back.
        let result = handle.recv();
        assert!(result.is_ok(), "inline-delivered handle survives poison");
    }

    #[test]
    fn dropped_handle_is_harmless() {
        let agg = fresh_agg();
        let handle = agg.stage(40, vec![ApplyOutcome::L2pPrev(None)]);
        drop(handle);
        // Aggregator state is unchanged; subsequent stage works.
        let h2 = agg.stage(41, vec![ApplyOutcome::L2pPrev(None)]);
        let outs = h2.recv().unwrap();
        assert_eq!(outs.len(), 1);
    }

    #[test]
    fn try_recv_returns_outcomes_immediately() {
        let agg = fresh_agg();
        let handle = agg.stage(50, vec![ApplyOutcome::L2pPrev(None)]);
        let outcomes = match handle.try_recv() {
            Ok(o) => o.expect("outcomes delivered"),
            Err(_) => panic!("handle should be ready immediately under inline delivery"),
        };
        assert_eq!(outcomes.len(), 1);
    }
}
