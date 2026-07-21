//! Fault-injection framework for crash / IO-error tests.
//!
//! # Model
//!
//! Every durability-critical code path calls [`FaultController::inject`]
//! with a named [`FaultPoint`]. When the controller has no trigger
//! installed (or every trigger has already fired), `inject` is one
//! `Acquire` atomic load and a return.
//!
//! When a trigger is installed, the controller fires its [`FaultAction`]
//! exactly once, on the `fire_on_hit`-th call to that point (1-indexed).
//! Subsequent calls after the trigger fires are recorded in the hit
//! counter but do not fire again.
//!
//! # Scope
//!
//! The controller is a plain `Arc`. Each component that opts into fault
//! injection holds an `Arc<FaultController>` handed to it by the embedder
//! (or by tests). There is no global state — two independent databases
//! under test in the same process do not interfere.
//!
//! # Actions
//!
//! - [`FaultAction::Error`]: returns [`MetaDbError::InjectedFault`] so the
//!   caller can exercise the error-handling path.
//! - [`FaultAction::Panic`]: unwinds with a panic to simulate an abrupt
//!   process kill. Used by recovery tests in combination with
//!   `catch_unwind`.

use parking_lot::{Condvar, Mutex};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::{MetaDbError, Result};

/// Named site in the engine at which a fault can be injected. Add a
/// variant whenever a new durability-critical call is introduced so that
/// test coverage can be measured by enumerating this enum.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    /// Immediately before the WAL fsync system call returns.
    WalFsyncBefore,
    /// Immediately after the WAL fsync system call returns.
    WalFsyncAfter,
    /// After the WAL submit/fsync completed for a commit but before any
    /// in-memory apply starts.
    CommitPostWalBeforeApply,
    /// After every op in a commit was applied to memory but before
    /// `last_applied_lsn` is bumped.
    CommitPostApplyBeforeLsnBump,
    /// Before a page is written to the page file.
    PageWriteBefore,
    /// After a page has been written to the page file.
    PageWriteAfter,
    /// Before the manifest slot is fsync'd.
    ManifestFsyncBefore,
    /// After the manifest slot is fsync'd.
    ManifestFsyncAfter,
    /// After new dedup level-head chains were written during `flush`,
    /// but before the manifest swap committed them durable.
    FlushPostLevelRewriteBeforeManifest,
    /// Midway through a COW cascade, after a new child page has been
    /// written but before its parent has been linked to it.
    CowCascadeMidParentLink,
    /// Inside `Db::create_volume`, after the WAL submit + apply (shard
    /// roots allocated, volume installed in the map) but before the
    /// in-memory manifest's volumes table has been extended. Exercises
    /// the window where the WAL holds a `CreateVolume` record but no
    /// manifest commit has captured it — recovery must reconstruct the
    /// volume from the replayed op.
    CreateVolumePostWalBeforeManifest,
    /// Inside `Db::drop_volume`, after the WAL submit records the drop
    /// but before `apply_drop_volume` has touched any page refcount.
    /// Exercises the replay path that has to re-run the rc-dependent
    /// page cascade from the WAL record's inlined page list.
    DropVolumePostWalBeforeApply,
    /// Mid-way through `apply_clone_volume_incref`: one shard root has
    /// been incref'd and sealed to disk, the rest have not. Exercises
    /// the generation-stamp idempotency guard on CloneVolume replay.
    CloneVolumeMidIncref,
    /// During [`flush_with_gate`], dead-list segment pages have been
    /// written + `page_store.sync()` returned successfully, but the
    /// manifest commit hasn't yet captured the new `dead_list_tail_pid`.
    /// A crash here leaves the segment pages as durable orphans — the
    /// next open's manifest still points at the old tail.     /// accepts the leak; the test fixture asserts L2P / RC state stays
    /// self-consistent.
    DeadListPostSegWriteBeforeManifest,
    /// During [`flush_with_gate`], the manifest commit succeeded with
    /// the new `dead_list_tail_pid`, but no subsequent flush has run
    /// yet. A crash here is the steady-state recovery case: WAL replay
    /// re-emits dead records since the new checkpoint, the buffer
    /// repopulates, and the next flush writes a fresh segment linked
    /// to the manifest's tail.
    DeadListPostManifestBeforeNextFlush,
    /// Lineage GC fault: the head
    /// segment's records have all been verified reclaim-eligible
    /// and the new manifest body has been assembled, but the
    /// durable commit hasn't fired yet. A crash here leaves the
    /// manifest pointing at the original head — recovery is
    /// identical to "GC pass never ran". No leak, no orphan.
    LineageGcPostFreePbasBeforeManifest,
    /// Lineage GC fault: the manifest commit landed with a
    /// new `dead_list_head_pid` and the atomics have been promoted,
    /// but the old head segment's pages haven't been
    /// `page_store.free_many`-ed yet. A crash here leaves the old
    /// segment pages allocated but unreferenced (orphans). The
    /// next `Db::open` rebuilds the free list by scanning the page
    /// file, so orphans land on the free list at startup and get
    /// reclaimed lazily. page_store reconciliation closes
    /// this hole tightly; accepts the lazy reclaim.
    LineageGcPostHeadAdvanceBeforeFree,
    /// Lineage GC fault: mid-way through reading a head
    /// segment's pages (e.g. one of the continuation pages errored
    /// or the worker was killed). No side effect — the GC pass
    /// hadn't yet mutated anything.
    LineageGcMidSegmentRead,
    /// BFG livelist-condense fault: the condensed single
    /// segment has been written + synced, but the manifest re-anchor
    /// commit hasn't fired yet. A crash here leaves the committed anchor
    /// pointing at the OLD chain (intact) + the new segment as an
    /// unreferenced orphan that `reclaim_orphan_pages` sweeps on the next
    /// open. Mirrors [`Self::LineageGcPostFreePbasBeforeManifest`].
    LivelistCondensePostSegWriteBeforeManifest,
    /// BFG livelist-condense fault: the re-anchor manifest
    /// commit landed and the atomics were promoted to the condensed
    /// segment, but the OLD chain pages haven't been `free_idempotent`-ed
    /// yet. A crash here leaves them allocated-but-unreferenced (orphans);
    /// the next open reclaims them. Mirrors
    /// [`Self::LineageGcPostHeadAdvanceBeforeFree`].
    LivelistCondensePostManifestBeforeFree,
    /// BFG fault: inside the L2P compactor's
    /// step-7 drain of [`crate::db::commit::DeferredOutcomeAggregator`],
    /// after the staged outcomes have been popped from the pending
    /// map but **before** any `sender.send(...)` has fired.
    /// `FaultAction::Panic` simulates a crash: every waiter parked
    /// on `recv()` resolves with the channel-disconnected error
    /// after the receiver side closes. On-disk state is unaffected
    /// (the apply happened during commit, well before stage). The
    /// drop_handle / poison_all path covers the next-Db-open case
    /// without needing recovery here. `FaultAction::Error` is
    /// treated as a Panic since `drain_up_to_lsn` returns `usize`
    /// (no Result to propagate).
    DeferredOutcomeDrainMidway,
    /// BFG fault: inside `flush_with_gate`, AFTER
    /// `WalSet::fsync_all_lanes` returned Ok (every async WAL byte
    /// is on durable storage) BUT BEFORE
    /// `manifest_state.store.commit()` has fsynced the new
    /// `checkpoint_lsn`. `FaultAction::Panic` simulates a crash in
    /// this window: WAL records up to `wal_checkpoint` are durable,
    /// the manifest still points at the OLD checkpoint, recovery
    /// replays from the old checkpoint and re-applies the records
    /// idempotently (apply guards on `page.generation >= lsn`,
    /// refcount delta merge, cuckoo put-if-absent). End state must
    /// be byte-equivalent to a clean flush.
    BfgSyncMidway,
    /// BFG fault: inside
    /// `BfgQuiesceThread::run_worker`, fires AFTER
    /// `state.roll_to_quiescing` has closed the Open BFG + advanced
    /// `open_bfg` to the next slot BUT BEFORE `promote_to_syncing`
    /// flips the Quiescing slot to Syncing. `FaultAction::Panic`
    /// simulates a crash in this window: the in-memory state machine
    /// is mid-quiesce; the on-disk manifest still records the old
    /// `checkpoint_bfg`. Recovery reconstructs the state machine from
    /// the durable manifest, replays WAL, and folds replayed ops into
    /// the post-recovery open BFG — the discarded mid-quiesce state
    /// must be unreachable (no on-disk visible side effect from a
    /// non-promoted BFG).
    BfgQuiesceMidway,
    /// BFG fault: inside the WAL writer thread,
    /// fires in lieu of `seg.append(&buf)` for an async-only batch.
    /// Simulates the strongest possible loss case: the kernel never
    /// flushes the OS page cache write for an async-submitted
    /// record. Tests the LV2-buffer-driven recovery path on the
    /// onyx side — the metadb-side contract is "anything past the
    /// last successful fsync is lost; downstream durable log must
    /// re-drive". `FaultAction::Error` returns a fault error from
    /// `seg.append`, propagating through `commit_batch` to every
    /// in-batch ack.
    WalSubmitAsyncDropped,
    /// v27 delta-run persist: the append checkpoint wrote its segment data pages
    /// durably, but the manifest commit hasn't captured the new directory head.
    /// A crash here leaves the segment + freshly-built directory pages as durable
    /// orphans — the old manifest never referenced them; the next open's
    /// free-list rebuild / offline verify reclaims them. rc is unchanged (onyx
    /// LV2 replay re-drives the deltas).
    RcDeltaRunPostSegWriteBeforeManifest,
    /// v27 delta-run persist: the manifest commit landed with the new directory
    /// head, but the OLD directory framing pages haven't been freed yet. A crash
    /// here orphans those old framing pages (reclaimed lazily); the new
    /// directory + segments are durable and correct.
    RcDeltaRunPostManifestBeforeFree,
    /// v27 condense: the base fold was written durably, but the manifest commit
    /// (recording the folded base + emptied directory head) hasn't fired. A crash
    /// here re-condenses on open — the segments are still anchored by the OLD
    /// manifest, and the folded base pages carry `generation >= record.last_lsn`
    /// so the `force=false` replay-skip drops them (idempotent).
    RcCondensePostWriteBeforeManifest,
    /// v27 condense: the manifest commit landed with the emptied directory head,
    /// but the folded segment data pages + old directory framing haven't been
    /// freed. A crash here orphans them (reclaimed lazily); rc is correct (the
    /// base carries the fold, the manifest anchors no directory).
    RcCondensePostManifestBeforeFree,
}

impl FaultPoint {
    /// Short, dotted, kebab-cased static label used in error text and
    /// test-run logs.
    pub fn name(self) -> &'static str {
        match self {
            Self::WalFsyncBefore => "wal.fsync.before",
            Self::WalFsyncAfter => "wal.fsync.after",
            Self::CommitPostWalBeforeApply => "commit.post_wal.before_apply",
            Self::CommitPostApplyBeforeLsnBump => "commit.post_apply.before_lsn_bump",
            Self::PageWriteBefore => "page.write.before",
            Self::PageWriteAfter => "page.write.after",
            Self::ManifestFsyncBefore => "manifest.fsync.before",
            Self::ManifestFsyncAfter => "manifest.fsync.after",
            Self::FlushPostLevelRewriteBeforeManifest => "flush.level_rewrite.before_manifest",
            Self::CowCascadeMidParentLink => "cow.cascade.mid_parent_link",
            Self::CreateVolumePostWalBeforeManifest => "create_volume.post_wal.before_manifest",
            Self::DropVolumePostWalBeforeApply => "drop_volume.post_wal.before_apply",
            Self::CloneVolumeMidIncref => "clone_volume.mid_incref",
            Self::DeadListPostSegWriteBeforeManifest => "deadlist.post_seg_write.before_manifest",
            Self::DeadListPostManifestBeforeNextFlush => "deadlist.post_manifest.before_next_flush",
            Self::LineageGcPostFreePbasBeforeManifest => {
                "lineage_gc.post_free_pbas.before_manifest"
            }
            Self::LineageGcPostHeadAdvanceBeforeFree => "lineage_gc.post_head_advance.before_free",
            Self::LineageGcMidSegmentRead => "lineage_gc.mid_segment_read",
            Self::LivelistCondensePostSegWriteBeforeManifest => {
                "livelist_condense.post_seg_write.before_manifest"
            }
            Self::LivelistCondensePostManifestBeforeFree => {
                "livelist_condense.post_manifest.before_free"
            }
            Self::DeferredOutcomeDrainMidway => "deferred_outcomes.drain.midway",
            Self::BfgSyncMidway => "flush.bfg_sync.midway",
            Self::BfgQuiesceMidway => "bfg.quiesce.midway",
            Self::WalSubmitAsyncDropped => "wal.submit_async.dropped",
            Self::RcDeltaRunPostSegWriteBeforeManifest => {
                "rc_delta_run.post_seg_write.before_manifest"
            }
            Self::RcDeltaRunPostManifestBeforeFree => "rc_delta_run.post_manifest.before_free",
            Self::RcCondensePostWriteBeforeManifest => "rc_condense.post_write.before_manifest",
            Self::RcCondensePostManifestBeforeFree => "rc_condense.post_manifest.before_free",
        }
    }
}

/// What the controller does when a trigger fires.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FaultAction {
    /// Return [`MetaDbError::InjectedFault`] from the `inject` call.
    Error,
    /// Unwind with a panic, simulating a crash.
    Panic,
    /// Block the calling thread inside `inject` until a test calls
    /// [`FaultController::release_block`] for the same point. Lets a test
    /// pin one thread at a precise mid-operation site and drive a second
    /// thread through a window that would otherwise be a non-deterministic
    /// race (e.g. flush's gateless livelist sample ↔ the background
    /// `LivelistCondenser` re-anchor). `inject` returns `Ok(())` once
    /// released — the blocked code path then continues normally, unlike
    /// `Error`/`Panic`.
    Block,
}

#[derive(Copy, Clone, Debug)]
struct FaultEntry {
    action: FaultAction,
    fire_on_hit: u64,
    fired: bool,
}

/// Per-instance fault controller. Share via `Arc` across the components
/// of one database; keep separate instances isolated by construction.
pub struct FaultController {
    enabled: AtomicBool,
    inner: Mutex<Inner>,
    /// Wakes threads parked by a [`FaultAction::Block`] trigger when a test
    /// calls [`FaultController::release_block`].
    block_cvar: Condvar,
}

struct Inner {
    triggers: HashMap<FaultPoint, FaultEntry>,
    counts: HashMap<FaultPoint, u64>,
    /// Points a test has released; a `Block`ed `inject` returns once its
    /// point is in this set.
    released: HashSet<FaultPoint>,
}

impl FaultController {
    /// Construct a fresh disabled controller.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                triggers: HashMap::new(),
                counts: HashMap::new(),
                released: HashSet::new(),
            }),
            block_cvar: Condvar::new(),
        })
    }

    /// Return a process-wide singleton "permanently disabled" controller,
    /// useful as a default when a component wants an `Arc<FaultController>`
    /// but the embedder does not care about fault injection. No allocation
    /// beyond the first call.
    pub fn disabled() -> Arc<Self> {
        static DISABLED: OnceLock<Arc<FaultController>> = OnceLock::new();
        DISABLED.get_or_init(Self::new).clone()
    }

    /// Install a trigger and enable the controller. `fire_on_hit` must be
    /// `>= 1`; the Nth call to [`inject`](Self::inject) with matching
    /// `point` takes `action`. Subsequent calls only bump the counter.
    ///
    /// Installing the same point twice replaces the earlier trigger and
    /// resets its fired state.
    pub fn install(&self, point: FaultPoint, fire_on_hit: u64, action: FaultAction) {
        assert!(fire_on_hit >= 1, "fire_on_hit must be >= 1");
        let mut inner = self.inner.lock();
        inner.triggers.insert(
            point,
            FaultEntry {
                action,
                fire_on_hit,
                fired: false,
            },
        );
        self.enabled.store(true, Ordering::Release);
    }

    /// Remove every trigger and reset every counter; the controller
    /// returns to its just-constructed state.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.triggers.clear();
        inner.counts.clear();
        inner.released.clear();
        self.enabled.store(false, Ordering::Release);
        // Wake any thread still parked on a Block trigger so `clear` can't
        // leave it stuck forever (the trigger is gone, so it would never be
        // released through the normal path).
        self.block_cvar.notify_all();
    }

    /// Release every thread parked by a [`FaultAction::Block`] trigger for
    /// `point` (and any that block on it afterward). Idempotent. Pairs with
    /// [`FaultAction::Block`] to build deterministic two-thread interleave
    /// tests: install `Block`, wait until [`hits`](Self::hits) shows the
    /// blocked thread arrived, drive the second thread, then `release_block`.
    pub fn release_block(&self, point: FaultPoint) {
        let mut inner = self.inner.lock();
        inner.released.insert(point);
        self.block_cvar.notify_all();
    }

    /// Total number of times `point` has been seen by [`inject`] since
    /// the last [`clear`].
    pub fn hits(&self, point: FaultPoint) -> u64 {
        self.inner.lock().counts.get(&point).copied().unwrap_or(0)
    }

    /// Whether the trigger for `point` has already fired. Useful in tests
    /// to confirm that the code path being exercised actually ran into
    /// the trigger.
    pub fn fired(&self, point: FaultPoint) -> bool {
        self.inner
            .lock()
            .triggers
            .get(&point)
            .map(|e| e.fired)
            .unwrap_or(false)
    }

    /// Called by instrumented code. Fast path: one `Acquire` load when no
    /// triggers are installed. Slow path takes the inner mutex.
    #[inline]
    pub fn inject(&self, point: FaultPoint) -> Result<()> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inject_slow(point)
    }

    #[cold]
    #[inline(never)]
    fn inject_slow(&self, point: FaultPoint) -> Result<()> {
        let fire_action = {
            let mut inner = self.inner.lock();
            let hit = {
                let c = inner.counts.entry(point).or_insert(0);
                *c += 1;
                *c
            };
            match inner.triggers.get_mut(&point) {
                Some(entry) if !entry.fired && hit == entry.fire_on_hit => {
                    entry.fired = true;
                    Some(entry.action)
                }
                _ => None,
            }
        };
        match fire_action {
            Some(FaultAction::Error) => Err(MetaDbError::InjectedFault(point.name())),
            Some(FaultAction::Panic) => panic!("fault injected at {}", point.name()),
            Some(FaultAction::Block) => {
                // The `fire_action` scope above already dropped the lock, so we
                // re-acquire it fresh and park on the cvar; `Condvar::wait`
                // atomically releases it while parked, letting `release_block`
                // (or `clear`) take the lock and wake us. Exit when the point is
                // released OR its trigger was cleared — without the latter,
                // `clear()` (which empties `released`) would re-park us forever.
                let mut inner = self.inner.lock();
                while !inner.released.contains(&point) && inner.triggers.contains_key(&point) {
                    self.block_cvar.wait(&mut inner);
                }
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn fresh_controller_is_noop() {
        let c = FaultController::new();
        for _ in 0..10 {
            assert!(c.inject(FaultPoint::WalFsyncBefore).is_ok());
        }
        // No triggers → the fast path skipped the counter entirely.
        assert_eq!(c.hits(FaultPoint::WalFsyncBefore), 0);
    }

    #[test]
    fn disabled_singleton_is_noop() {
        let c = FaultController::disabled();
        for _ in 0..10 {
            assert!(c.inject(FaultPoint::WalFsyncBefore).is_ok());
        }
        // Two handles must point at the same controller.
        assert!(Arc::ptr_eq(&c, &FaultController::disabled()));
    }

    #[test]
    fn error_fires_on_nth_hit_exactly() {
        let c = FaultController::new();
        c.install(FaultPoint::WalFsyncBefore, 3, FaultAction::Error);
        assert!(c.inject(FaultPoint::WalFsyncBefore).is_ok()); // hit 1
        assert!(c.inject(FaultPoint::WalFsyncBefore).is_ok()); // hit 2
        assert!(matches!(
            c.inject(FaultPoint::WalFsyncBefore),
            Err(MetaDbError::InjectedFault(_))
        )); // hit 3 -> fires
        assert!(c.inject(FaultPoint::WalFsyncBefore).is_ok()); // hit 4 -> already fired
        assert!(c.inject(FaultPoint::WalFsyncBefore).is_ok()); // hit 5 -> already fired
        assert_eq!(c.hits(FaultPoint::WalFsyncBefore), 5);
        assert!(c.fired(FaultPoint::WalFsyncBefore));
    }

    #[test]
    fn error_fires_on_first_hit() {
        let c = FaultController::new();
        c.install(FaultPoint::PageWriteBefore, 1, FaultAction::Error);
        assert!(matches!(
            c.inject(FaultPoint::PageWriteBefore),
            Err(MetaDbError::InjectedFault(_))
        ));
        assert!(c.inject(FaultPoint::PageWriteBefore).is_ok());
    }

    #[test]
    fn panic_action_unwinds() {
        let c = FaultController::new();
        c.install(FaultPoint::ManifestFsyncBefore, 1, FaultAction::Panic);
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = c.inject(FaultPoint::ManifestFsyncBefore);
        }));
        assert!(result.is_err(), "panic action must unwind");
        // Subsequent calls don't fire again.
        assert!(c.inject(FaultPoint::ManifestFsyncBefore).is_ok());
    }

    #[test]
    fn clear_resets_state() {
        let c = FaultController::new();
        c.install(FaultPoint::WalFsyncBefore, 1, FaultAction::Error);
        let _ = c.inject(FaultPoint::WalFsyncBefore); // fires
        assert!(c.fired(FaultPoint::WalFsyncBefore));
        c.clear();
        assert_eq!(c.hits(FaultPoint::WalFsyncBefore), 0);
        // Disabled again → fast-path no-op.
        for _ in 0..5 {
            assert!(c.inject(FaultPoint::WalFsyncBefore).is_ok());
        }
    }

    #[test]
    fn multiple_points_are_independent() {
        let c = FaultController::new();
        c.install(FaultPoint::WalFsyncBefore, 1, FaultAction::Error);
        c.install(FaultPoint::PageWriteBefore, 2, FaultAction::Error);
        assert!(c.inject(FaultPoint::PageWriteBefore).is_ok()); // page hit 1
        assert!(matches!(
            c.inject(FaultPoint::WalFsyncBefore),
            Err(MetaDbError::InjectedFault(_))
        )); // wal hit 1 -> fires
        assert!(matches!(
            c.inject(FaultPoint::PageWriteBefore),
            Err(MetaDbError::InjectedFault(_))
        )); // page hit 2 -> fires
    }

    #[test]
    fn reinstall_replaces_and_resets_fired() {
        let c = FaultController::new();
        c.install(FaultPoint::WalFsyncBefore, 1, FaultAction::Error);
        let _ = c.inject(FaultPoint::WalFsyncBefore);
        assert!(c.fired(FaultPoint::WalFsyncBefore));
        // Reinstall with different trigger; fired resets, counter stays.
        c.install(FaultPoint::WalFsyncBefore, 5, FaultAction::Error);
        assert!(!c.fired(FaultPoint::WalFsyncBefore));
    }

    #[test]
    fn concurrent_hits_are_accounted() {
        let c = FaultController::new();
        c.install(FaultPoint::WalFsyncBefore, 10_000, FaultAction::Error); // never fires
        let errors = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let c = c.clone();
                let errors = errors.clone();
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        if c.inject(FaultPoint::WalFsyncBefore).is_err() {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(c.hits(FaultPoint::WalFsyncBefore), 8_000);
        assert_eq!(errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn point_names_are_stable_strings() {
        let all = [
            FaultPoint::WalFsyncBefore,
            FaultPoint::WalFsyncAfter,
            FaultPoint::PageWriteBefore,
            FaultPoint::PageWriteAfter,
            FaultPoint::ManifestFsyncBefore,
            FaultPoint::ManifestFsyncAfter,
            FaultPoint::CowCascadeMidParentLink,
        ];
        for p in all {
            assert!(!p.name().is_empty());
            assert!(p.name().contains('.'));
        }
    }

    #[test]
    #[should_panic]
    fn install_with_zero_hit_panics() {
        let c = FaultController::new();
        c.install(FaultPoint::WalFsyncBefore, 0, FaultAction::Error);
    }

    #[test]
    fn block_action_parks_until_released() {
        let c = FaultController::new();
        c.install(FaultPoint::PageWriteBefore, 1, FaultAction::Block);
        let c2 = c.clone();
        let handle = std::thread::spawn(move || c2.inject(FaultPoint::PageWriteBefore));
        // Reaching the block bumps the hit counter; spin until the worker parks.
        while c.hits(FaultPoint::PageWriteBefore) < 1 {
            std::thread::yield_now();
        }
        // Release it and confirm the parked inject returns Ok (not Error/Panic).
        c.release_block(FaultPoint::PageWriteBefore);
        assert!(
            handle.join().unwrap().is_ok(),
            "a released Block inject must return Ok and continue"
        );
        // A later hit on the same (already-fired) point does not block again.
        assert!(c.inject(FaultPoint::PageWriteBefore).is_ok());
    }

    #[test]
    fn clear_wakes_blocked_thread() {
        let c = FaultController::new();
        c.install(FaultPoint::PageWriteBefore, 1, FaultAction::Block);
        let c2 = c.clone();
        let handle = std::thread::spawn(move || c2.inject(FaultPoint::PageWriteBefore));
        while c.hits(FaultPoint::PageWriteBefore) < 1 {
            std::thread::yield_now();
        }
        // `clear` removes the trigger; the parked thread's wait predicate sees
        // the trigger gone and returns Ok rather than re-parking forever.
        c.clear();
        assert!(
            handle.join().unwrap().is_ok(),
            "clear() must wake a thread parked on a Block trigger"
        );
    }
}
