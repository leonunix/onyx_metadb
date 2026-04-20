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

use parking_lot::Mutex;
use std::collections::HashMap;
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
}

struct Inner {
    triggers: HashMap<FaultPoint, FaultEntry>,
    counts: HashMap<FaultPoint, u64>,
}

impl FaultController {
    /// Construct a fresh disabled controller.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                triggers: HashMap::new(),
                counts: HashMap::new(),
            }),
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
        self.enabled.store(false, Ordering::Release);
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
}
