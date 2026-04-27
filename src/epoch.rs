//! Lightweight epoch-based reader/reclaimer barrier.
//!
//! Replaces the `apply_gate.read()` shortcut on the L2P read path. A
//! reader [`pin`](EpochManager::pin)s for the duration of one tree walk;
//! the reclaimer (`page_store::PageStore::try_reclaim`) holds back any
//! page tagged at an epoch that some still-pinned reader could observe.
//!
//! # Protocol (proof of safety)
//!
//! - Reader: program order is `pin` → `read_view.read()` → walk.
//! - Free: tag = `epoch.advance()` (returns pre-bump value, then bumps
//!   global by 1).
//! - Reclaim safe: `tag < min_active_pin`.
//!
//! Suppose a reader's walk dereferences pid X. Then the view it loaded
//! references X. That view either is the freshest published view (in
//! which case X cannot have been retired yet) or an older one whose
//! superseding publish has not yet happened in real time relative to
//! the reader's `read_view.read()`. In the latter case:
//!
//! ```text
//! reader.pin
//!   < reader.read_view.read()
//!   < apply.publish(V_newer that retires X)
//!   < flush.free(X)  // reads global → tag, then bumps
//! ```
//!
//! `global` only ever increases, so the value the reader saw at `pin`
//! is `≤` the value the freer saw at `free`. That is, `G_pin ≤ tag`.
//! Therefore `min_active_pin ≤ G_pin ≤ tag`, so `tag < min_active_pin`
//! is false and reclaim refuses. Once the reader drops, `min_active_pin`
//! advances and reclaim proceeds.
//!
//! # Slot table
//!
//! Pin epochs live in a fixed `Box<[AtomicU64]>`. Each thread that ever
//! pins claims one slot via `next_slot.fetch_add(1)` and keeps it for
//! the rest of its life — slot lookup is a thread-local pointer compare,
//! so the hot path is one atomic load + one atomic store. The bound is
//! [`DEFAULT_SLOT_COUNT`]; onyx's worker pools stay well under that.
//! Exhaustion panics rather than silently sharing slots (sharing would
//! let one thread's pin clobber another's, breaking `min_active_pin`).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Default slot table size. Each slot is 8 bytes; 256 covers any realistic
/// thread count for onyx (worker pool + ublk + admin) with margin.
pub const DEFAULT_SLOT_COUNT: usize = 256;

/// Sentinel for "slot is not currently pinned." The global epoch starts
/// at 1 so a real pin never collides with this value.
const UNPINNED: u64 = 0;

/// Source of unique manager ids. Used so a thread_local slot cache does
/// not survive across `EpochManager` instances (drop + reuse-of-address
/// would otherwise hand back a stale slot index).
static NEXT_MANAGER_ID: AtomicU64 = AtomicU64::new(1);

/// Coordinates short-lived reader pins against deferred page reclamation.
pub struct EpochManager {
    id: u64,
    global: AtomicU64,
    slots: Box<[AtomicU64]>,
    next_slot: AtomicUsize,
}

/// RAII handle. Drop releases the slot back to `UNPINNED`.
pub struct Guard<'a> {
    mgr: &'a EpochManager,
    slot_idx: usize,
}

impl EpochManager {
    /// Construct an empty epoch manager with [`DEFAULT_SLOT_COUNT`] slots.
    pub fn new() -> Self {
        Self::with_slots(DEFAULT_SLOT_COUNT)
    }

    /// Same as [`new`](Self::new) but with an explicit slot count.
    pub fn with_slots(slot_count: usize) -> Self {
        assert!(slot_count > 0, "epoch slot count must be > 0");
        let slots = (0..slot_count)
            .map(|_| AtomicU64::new(UNPINNED))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            id: NEXT_MANAGER_ID.fetch_add(1, Ordering::Relaxed),
            global: AtomicU64::new(1),
            slots,
            next_slot: AtomicUsize::new(0),
        }
    }

    /// Pin the current global epoch into this thread's slot until the
    /// returned guard drops. Cheap: one atomic load + one atomic store.
    pub fn pin(&self) -> Guard<'_> {
        let slot_idx = self.thread_slot();
        let epoch = self.global.load(Ordering::Acquire);
        // Release-store so a subsequent reclaimer that loads slots with
        // Acquire sees this pin published before its safety check.
        self.slots[slot_idx].store(epoch, Ordering::Release);
        Guard {
            mgr: self,
            slot_idx,
        }
    }

    /// Read-and-bump the global epoch. Returns the **pre-bump** value;
    /// callers tag deferred work with that value so any reader pinning
    /// after this call (whose `G_pin > pre`) can be confirmed safe.
    pub fn advance(&self) -> u64 {
        self.global.fetch_add(1, Ordering::AcqRel)
    }

    /// Current global epoch (for tagging without bumping).
    pub fn current(&self) -> u64 {
        self.global.load(Ordering::Acquire)
    }

    /// Smallest active pin epoch, or `u64::MAX` if no thread is pinned.
    /// Reclaim drains entries with `tag < min_active_pin`.
    pub fn min_active_pin(&self) -> u64 {
        let mut min = u64::MAX;
        for slot in self.slots.iter() {
            let v = slot.load(Ordering::Acquire);
            if v != UNPINNED && v < min {
                min = v;
            }
        }
        min
    }

    fn thread_slot(&self) -> usize {
        thread_local! {
            static SLOT: Cell<Option<(usize, u64)>> = const { Cell::new(None) };
        }
        SLOT.with(|s| {
            if let Some((idx, mgr_id)) = s.get() {
                if mgr_id == self.id {
                    return idx;
                }
            }
            let idx = self.next_slot.fetch_add(1, Ordering::Relaxed);
            assert!(
                idx < self.slots.len(),
                "epoch slot table exhausted (slot_count={}); raise DEFAULT_SLOT_COUNT or audit thread spawn pattern",
                self.slots.len()
            );
            s.set(Some((idx, self.id)));
            idx
        })
    }
}

impl Default for EpochManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EpochManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochManager")
            .field("id", &self.id)
            .field("global", &self.global.load(Ordering::Relaxed))
            .field("slots", &self.slots.len())
            .field("min_active_pin", &self.min_active_pin())
            .finish()
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.mgr.slots[self.slot_idx].store(UNPINNED, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn empty_manager_min_pin_is_max() {
        let m = EpochManager::new();
        assert_eq!(m.min_active_pin(), u64::MAX);
    }

    #[test]
    fn pin_records_current_epoch() {
        let m = EpochManager::new();
        assert_eq!(m.current(), 1);
        let _g = m.pin();
        assert_eq!(m.min_active_pin(), 1);
    }

    #[test]
    fn drop_releases_slot() {
        let m = EpochManager::new();
        {
            let _g = m.pin();
            assert_eq!(m.min_active_pin(), 1);
        }
        assert_eq!(m.min_active_pin(), u64::MAX);
    }

    #[test]
    fn advance_returns_pre_bump_value() {
        let m = EpochManager::new();
        assert_eq!(m.advance(), 1);
        assert_eq!(m.current(), 2);
        assert_eq!(m.advance(), 2);
        assert_eq!(m.current(), 3);
    }

    #[test]
    fn pin_after_advance_observes_new_epoch() {
        let m = EpochManager::new();
        let _ = m.advance();
        let _ = m.advance();
        let _g = m.pin();
        assert_eq!(m.min_active_pin(), 3);
    }

    #[test]
    fn min_active_pin_tracks_smallest() {
        // Two threads pin at different epochs; min must be the smaller.
        let m = Arc::new(EpochManager::new());
        let g1 = m.pin();
        let m2 = m.clone();
        let t = thread::spawn(move || {
            let _ = m2.advance();
            let _ = m2.advance();
            let _g = m2.pin();
            assert_eq!(m2.min_active_pin(), 1);
        });
        t.join().unwrap();
        drop(g1);
        assert_eq!(m.min_active_pin(), u64::MAX);
    }

    #[test]
    fn each_thread_owns_its_slot() {
        // Concurrent pins from many threads must not clobber each other.
        let m = Arc::new(EpochManager::with_slots(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let m = m.clone();
            handles.push(thread::spawn(move || {
                let _g = m.pin();
                std::thread::sleep(std::time::Duration::from_millis(5));
            }));
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        // While the worker threads are pinned, min_active_pin must be 1
        // (every pin observed global == 1, no advance happened).
        assert_eq!(m.min_active_pin(), 1);
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.min_active_pin(), u64::MAX);
    }

    #[test]
    fn separate_managers_use_independent_slot_pools() {
        let a = EpochManager::with_slots(2);
        let b = EpochManager::with_slots(2);
        let _ga = a.pin();
        let _gb = b.pin();
        assert_eq!(a.min_active_pin(), 1);
        assert_eq!(b.min_active_pin(), 1);
    }

    #[test]
    #[should_panic(expected = "epoch slot table exhausted")]
    fn exhausted_slot_table_panics() {
        let m = Arc::new(EpochManager::with_slots(1));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let m = m.clone();
            handles.push(thread::spawn(move || {
                let _g = m.pin();
                std::thread::sleep(std::time::Duration::from_millis(50));
            }));
        }
        for h in handles {
            // One of them must have panicked while trying to claim a slot.
            // Propagate that panic out so the test sees it.
            if let Err(payload) = h.join() {
                std::panic::resume_unwind(payload);
            }
        }
    }
}
