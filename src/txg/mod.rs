//! Three-state global TXG epoch state machine.
//!
//! Replaces the single-`compacted_lsn` / `checkpoint_lsn` progress point with
//! ZFS-style TXG (transaction group) accounting. At any moment up to three
//! TXGs are active in a ring of `TXG_SIZE = 4` slots (index = `txg & 3`):
//!
//! - **Open**: accepting new commits. Exactly one slot.
//! - **Quiescing**: closed to new commits, waiting for in-flight `TxgGuard`s
//!   to drop. At most one slot.
//! - **Syncing**: drained; `TxgSyncThread` is persisting it. At most one slot.
//! - **Empty**: ring slot available for the next roll.
//!
//! Commit hot path:
//! ```ignore
//! let guard = state.enter();          // 1 mutex acquire, ~50ns
//! // ... submit WAL, apply ops, stamp L2pBuffer slot[guard.txg & 3] ...
//! guard.record_lsn(lsn);              // 1 mutex acquire
//! drop(guard);                         // 1 mutex acquire
//! ```
//!
//! Quiesce thread (single, fires on 5 s timer or dirty-data trigger):
//! ```ignore
//! let txg = state.roll_to_quiescing();   // close current Open, open next
//! state.wait_quiesce_drained(txg);       // park until inflight == 0
//! state.promote_to_syncing(txg);
//! sync_notify.wake();
//! ```
//!
//! Sync thread (single, fires on quiesce notify):
//! ```ignore
//! // drain L2pBuffer slots, apply to tree, manifest commit, then:
//! state.mark_synced(txg);
//! ```
//!
//! ## Concurrency
//!
//! One mutex (`inner`) covers all state transitions; one condvar (`cv`)
//! covers all waiters. Critical sections are O(1) — slot manipulation is
//! bit-indexed. The mutex is contended only by the two background threads
//! plus the commit hot path; commits do not block each other on this mutex
//! beyond the trivial slot-counter bump.
//!
//! Lookup-side cheaper paths use `open_txg_atomic` / `checkpoint_txg_atomic`
//! snapshots so the L2P read path can walk the ring without taking the
//! inner mutex.
//!
//! ## Ring invariants (debug_asserted)
//!
//! - Exactly one slot in Open, at `open_txg & 3`.
//! - At most one slot in Quiescing, at `(open_txg - 1) & 3`.
//! - At most one slot in Syncing.
//! - `checkpoint_txg + 1 <= open_txg`
//! - `open_txg - checkpoint_txg <= TXG_SIZE - 1` (at most `TXG_CONCURRENT_STATES` active)

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Condvar, Mutex};

use crate::types::{Lsn, Txg};

/// Ring slot count. Must be a power of two (slot index = `txg & (TXG_SIZE - 1)`).
pub const TXG_SIZE: usize = 4;

/// Maximum number of concurrently-active TXGs (Open + Quiescing + Syncing).
/// One slot in the ring is always Empty, acting as the next-slot reservation
/// for the upcoming roll.
pub const TXG_CONCURRENT_STATES: usize = 3;

const TXG_INDEX_MASK: u64 = (TXG_SIZE as u64) - 1;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TxgState {
    Empty,
    Open,
    Quiescing,
    Syncing,
}

#[derive(Copy, Clone, Debug)]
struct Slot {
    state: TxgState,
    inflight: u64,
    max_lsn: Lsn,
}

impl Slot {
    const fn empty() -> Self {
        Self {
            state: TxgState::Empty,
            inflight: 0,
            max_lsn: 0,
        }
    }
}

struct Inner {
    slots: [Slot; TXG_SIZE],
    open_txg: Txg,
    quiescing_txg: Option<Txg>,
    syncing_txg: Option<Txg>,
    checkpoint_txg: Txg,
    shutdown: bool,
    /// Set by `roll_to_quiescing` after the next-slot wait completes and
    /// BEFORE the inflight-drain wait begins. `enter()` blocks while this
    /// is set so no new commits join the closing TXG. Cleared once the
    /// state flip + open_txg advance is done.
    ///
    /// Without this, the WAL allocator may hand out an LSN < L to a
    /// commit stamped to TXG N+1 (entered between the inflight-drain
    /// wakeup and the open_txg advance) — breaking the monotonicity
    /// invariant `max(LSN in TXG_n) <= min(LSN in TXG_n+1)` that
    /// `checkpoint_lsn = slot[K].max_lsn` relies on for WAL prune
    /// correctness.
    closing_open: bool,
}

pub struct TxgStateMachine {
    inner: Mutex<Inner>,
    cv: Condvar,
    open_txg_atomic: AtomicU64,
    checkpoint_txg_atomic: AtomicU64,
}

impl TxgStateMachine {
    /// Fresh state machine with all slots empty except the Open slot at
    /// `(initial_checkpoint_txg + 1) & 3`.
    pub fn new(initial_checkpoint_txg: Txg) -> Self {
        let mut slots = [Slot::empty(); TXG_SIZE];
        let open_txg = initial_checkpoint_txg + 1;
        slots[(open_txg & TXG_INDEX_MASK) as usize].state = TxgState::Open;
        Self {
            inner: Mutex::new(Inner {
                slots,
                open_txg,
                quiescing_txg: None,
                syncing_txg: None,
                checkpoint_txg: initial_checkpoint_txg,
                shutdown: false,
                closing_open: false,
            }),
            cv: Condvar::new(),
            open_txg_atomic: AtomicU64::new(open_txg),
            checkpoint_txg_atomic: AtomicU64::new(initial_checkpoint_txg),
        }
    }

    /// Stamp the caller's commit to the currently-Open TXG. Holds a refcount
    /// on that slot until the returned guard is dropped. **Hot path.**
    ///
    /// Blocks (briefly) while a `roll_to_quiescing` is in its drain window
    /// — without that block a freshly-entered commit could obtain a WAL
    /// LSN before the rolling TXG's last commit, breaking the
    /// `max(LSN in TXG_n) <= min(LSN in TXG_n+1)` monotonicity invariant
    /// the manifest WAL-prune logic depends on. The window is bounded by
    /// the in-flight commit count on the closing TXG, which is normally
    /// drained within milliseconds.
    pub fn enter(&self) -> TxgGuard<'_> {
        let mut g = self.inner.lock();
        while g.closing_open && !g.shutdown {
            self.cv.wait(&mut g);
        }
        let txg = g.open_txg;
        let idx = (txg & TXG_INDEX_MASK) as usize;
        debug_assert_eq!(g.slots[idx].state, TxgState::Open);
        g.slots[idx].inflight += 1;
        TxgGuard { sm: self, txg }
    }

    /// Bump the slot's `max_lsn` if the supplied LSN is larger. Called by the
    /// commit thread after `submit_wal_ops` returns the LSN. Must be called
    /// while a `TxgGuard` for this TXG is held — otherwise the slot may have
    /// already rolled and the write would land on a stale slot.
    pub fn record_lsn(&self, txg: Txg, lsn: Lsn) {
        let mut g = self.inner.lock();
        let idx = (txg & TXG_INDEX_MASK) as usize;
        if lsn > g.slots[idx].max_lsn {
            g.slots[idx].max_lsn = lsn;
        }
    }

    /// Close the currently-Open TXG, wait for its in-flight commits to
    /// drain, then advance `open_txg` to the next slot. Returns the now-
    /// Quiescing TXG.
    ///
    /// Three-phase wait under the inner mutex:
    ///   1. Ring-full wait — `slots[(cur+1) & 3]` must be Empty.
    ///   2. Close `enter()` to new commits via `closing_open = true`.
    ///   3. Drain wait — `slots[cur & 3].inflight` must hit 0.
    /// Only then flip states and advance `open_txg`. The drain wait
    /// happens BEFORE the advance (not after) so the WAL allocator
    /// cannot interleave a TXG_(N+1) commit's LSN below a TXG_N
    /// commit's LSN — preserving the
    /// `max(LSN in TXG_n) <= min(LSN in TXG_n+1)` invariant.
    ///
    /// Called only by `TxgQuiesceThread`. Idempotent under shutdown:
    /// returns the current `open_txg` without advancing.
    pub fn roll_to_quiescing(&self) -> Txg {
        let mut g = self.inner.lock();
        let cur = g.open_txg;
        let cur_idx = (cur & TXG_INDEX_MASK) as usize;
        let next = cur + 1;
        let next_idx = (next & TXG_INDEX_MASK) as usize;

        // (1) Ring-full wait.
        while g.slots[next_idx].state != TxgState::Empty && !g.shutdown {
            self.cv.wait(&mut g);
        }
        if g.shutdown {
            return cur;
        }
        debug_assert!(g.quiescing_txg.is_none(), "two TXGs in Quiescing");
        debug_assert_eq!(g.slots[cur_idx].state, TxgState::Open);

        // (2) Close enter() to new commits. Without this the drain wait
        // below would race against fresh commits joining the slot.
        g.closing_open = true;

        // (3) Drain wait: every commit that started enter() before step (2)
        // closed the door must finish submit + apply and drop its guard.
        while g.slots[cur_idx].inflight > 0 && !g.shutdown {
            self.cv.wait(&mut g);
        }
        if g.shutdown {
            // Leave closing_open = true so the shutdown path is observable;
            // shutdown() also notifies so the close flag does not matter
            // beyond this point.
            return cur;
        }

        // Flip states and advance open_txg.
        g.slots[cur_idx].state = TxgState::Quiescing;
        g.slots[next_idx].state = TxgState::Open;
        g.quiescing_txg = Some(cur);
        g.open_txg = next;
        self.open_txg_atomic.store(next, Ordering::Release);
        // Reopen the door — new commits now stamp to `next`.
        g.closing_open = false;
        self.cv.notify_all();
        cur
    }

    /// Promote a Quiescing TXG to Syncing. `inflight` must be zero.
    ///
    /// **Blocks until the previous Syncing TXG has been consumed**
    /// (`mark_synced` cleared `syncing_txg`), enforcing "at most one
    /// Syncing". The quiesce worker is allowed to roll + quiesce TXG N+1
    /// while TXG N is still syncing (ZFS-style 3-concurrent
    /// open/quiescing/syncing, bounded by the ring-full wait in
    /// `roll_to_quiescing`), but the handoff INTO Syncing must wait for
    /// N's sync to finish. Without this wait, a `TxgSyncThread` that lags
    /// the roll cadence (e.g. `write_dirty_pages` under load) would let
    /// the worker promote a second TXG into Syncing — previously a
    /// `debug_assert` that panicked the worker, orphaned the sync thread,
    /// and hung every `wait_until_synced` (a shutdown deadlock). This is
    /// the metadb equivalent of ZFS `txg_quiesce_thread` waiting on
    /// `txg_has_quiesced_to_sync`.
    ///
    /// On the threads-off inline flush path this never waits:
    /// `mark_synced` for the prior TXG has already run before the next
    /// `roll_to_quiescing` → `promote_to_syncing`. Returns without
    /// promoting under shutdown (the caller re-checks `quiescing_txg`).
    pub fn promote_to_syncing(&self, txg: Txg) {
        let mut g = self.inner.lock();
        let idx = (txg & TXG_INDEX_MASK) as usize;
        while g.syncing_txg.is_some() && !g.shutdown {
            self.cv.wait(&mut g);
        }
        if g.shutdown {
            return;
        }
        debug_assert_eq!(g.slots[idx].state, TxgState::Quiescing);
        debug_assert_eq!(g.slots[idx].inflight, 0);
        debug_assert_eq!(g.quiescing_txg, Some(txg));
        g.slots[idx].state = TxgState::Syncing;
        g.quiescing_txg = None;
        g.syncing_txg = Some(txg);
    }

    /// Mark a Syncing TXG complete; advance `checkpoint_txg`. Wakes ring-full
    /// roll waiters and any flush callers parked in `wait_until_synced`.
    pub fn mark_synced(&self, txg: Txg) {
        let mut g = self.inner.lock();
        let idx = (txg & TXG_INDEX_MASK) as usize;
        debug_assert_eq!(g.slots[idx].state, TxgState::Syncing);
        debug_assert_eq!(g.syncing_txg, Some(txg));
        g.slots[idx].state = TxgState::Empty;
        g.slots[idx].inflight = 0;
        g.slots[idx].max_lsn = 0;
        g.syncing_txg = None;
        if txg > g.checkpoint_txg {
            g.checkpoint_txg = txg;
            self.checkpoint_txg_atomic.store(txg, Ordering::Release);
        }
        self.cv.notify_all();
    }

    /// Read the slot's max LSN. Returns the live value; if the slot has just
    /// been marked Empty by `mark_synced` the returned value is 0.
    pub fn slot_max_lsn(&self, txg: Txg) -> Lsn {
        let g = self.inner.lock();
        let idx = (txg & TXG_INDEX_MASK) as usize;
        g.slots[idx].max_lsn
    }

    /// Park until `checkpoint_txg >= target`. Used by `flush_with_gate` after
    /// it has force-rolled a TXG.
    pub fn wait_until_synced(&self, target: Txg) {
        let mut g = self.inner.lock();
        while g.checkpoint_txg < target && !g.shutdown {
            self.cv.wait(&mut g);
        }
    }

    /// Lock-free snapshot of `open_txg`. L2P read paths walk the buffer ring
    /// starting from this value.
    pub fn open_txg(&self) -> Txg {
        self.open_txg_atomic.load(Ordering::Acquire)
    }

    /// Lock-free snapshot of `checkpoint_txg`.
    pub fn checkpoint_txg(&self) -> Txg {
        self.checkpoint_txg_atomic.load(Ordering::Acquire)
    }

    /// Observability / test snapshot of every slot's state.
    pub fn snapshot(&self) -> StateSnapshot {
        let g = self.inner.lock();
        StateSnapshot {
            slots: [
                (g.slots[0].state, g.slots[0].inflight, g.slots[0].max_lsn),
                (g.slots[1].state, g.slots[1].inflight, g.slots[1].max_lsn),
                (g.slots[2].state, g.slots[2].inflight, g.slots[2].max_lsn),
                (g.slots[3].state, g.slots[3].inflight, g.slots[3].max_lsn),
            ],
            open_txg: g.open_txg,
            quiescing_txg: g.quiescing_txg,
            syncing_txg: g.syncing_txg,
            checkpoint_txg: g.checkpoint_txg,
        }
    }

    /// Wake every waiter so they can re-check shutdown and exit. Called by
    /// `Db::close` after both background threads have been signalled.
    pub fn shutdown(&self) {
        let mut g = self.inner.lock();
        g.shutdown = true;
        self.cv.notify_all();
    }
}

pub struct TxgGuard<'a> {
    sm: &'a TxgStateMachine,
    pub txg: Txg,
}

impl TxgGuard<'_> {
    pub fn txg(&self) -> Txg {
        self.txg
    }

    /// Record the highest LSN observed against this guard's TXG.
    pub fn record_lsn(&self, lsn: Lsn) {
        self.sm.record_lsn(self.txg, lsn);
    }
}

impl Drop for TxgGuard<'_> {
    fn drop(&mut self) {
        let mut g = self.sm.inner.lock();
        let idx = (self.txg & TXG_INDEX_MASK) as usize;
        debug_assert!(g.slots[idx].inflight > 0);
        g.slots[idx].inflight -= 1;
        // Notify when the slot just emptied. `roll_to_quiescing` parks
        // here with `closing_open = true` and `state = Open` (the state
        // flip happens AFTER the drain), so we cannot key off
        // `state == Quiescing` like a naive implementation might.
        if g.slots[idx].inflight == 0 {
            self.sm.cv.notify_all();
        }
    }
}

#[derive(Clone, Debug)]
pub struct StateSnapshot {
    pub slots: [(TxgState, u64, Lsn); TXG_SIZE],
    pub open_txg: Txg,
    pub quiescing_txg: Option<Txg>,
    pub syncing_txg: Option<Txg>,
    pub checkpoint_txg: Txg,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn new_initial_state() {
        let sm = TxgStateMachine::new(0);
        let s = sm.snapshot();
        assert_eq!(s.checkpoint_txg, 0);
        assert_eq!(s.open_txg, 1);
        assert_eq!(s.quiescing_txg, None);
        assert_eq!(s.syncing_txg, None);
        // Slot 1 is Open; everything else Empty.
        assert_eq!(s.slots[1].0, TxgState::Open);
        assert_eq!(s.slots[0].0, TxgState::Empty);
        assert_eq!(s.slots[2].0, TxgState::Empty);
        assert_eq!(s.slots[3].0, TxgState::Empty);
        assert_eq!(sm.open_txg(), 1);
        assert_eq!(sm.checkpoint_txg(), 0);
    }

    #[test]
    fn new_with_nonzero_checkpoint() {
        let sm = TxgStateMachine::new(7);
        let s = sm.snapshot();
        assert_eq!(s.checkpoint_txg, 7);
        assert_eq!(s.open_txg, 8);
        // 8 & 3 == 0 → slot 0 is Open.
        assert_eq!(s.slots[0].0, TxgState::Open);
    }

    #[test]
    fn enter_increments_inflight() {
        let sm = TxgStateMachine::new(0);
        let g1 = sm.enter();
        let g2 = sm.enter();
        assert_eq!(g1.txg(), 1);
        assert_eq!(g2.txg(), 1);
        let s = sm.snapshot();
        assert_eq!(s.slots[1].1, 2);
        drop(g1);
        let s = sm.snapshot();
        assert_eq!(s.slots[1].1, 1);
        drop(g2);
        let s = sm.snapshot();
        assert_eq!(s.slots[1].1, 0);
    }

    #[test]
    fn record_lsn_keeps_max() {
        let sm = TxgStateMachine::new(0);
        let g = sm.enter();
        g.record_lsn(10);
        g.record_lsn(5);
        g.record_lsn(20);
        assert_eq!(sm.slot_max_lsn(g.txg()), 20);
    }

    #[test]
    fn roll_advances_open_txg() {
        let sm = TxgStateMachine::new(0);
        let q = sm.roll_to_quiescing();
        assert_eq!(q, 1);
        let s = sm.snapshot();
        assert_eq!(s.open_txg, 2);
        assert_eq!(s.quiescing_txg, Some(1));
        assert_eq!(s.slots[1].0, TxgState::Quiescing);
        assert_eq!(s.slots[2].0, TxgState::Open);
    }

    #[test]
    fn full_cycle_open_quiesce_sync_empty() {
        let sm = TxgStateMachine::new(0);
        // No inflight commits — roll completes its embedded drain wait
        // immediately and returns the now-Quiescing TXG.
        let q = sm.roll_to_quiescing();
        assert_eq!(q, 1);
        sm.promote_to_syncing(q);
        let s = sm.snapshot();
        assert_eq!(s.syncing_txg, Some(1));
        assert_eq!(s.quiescing_txg, None);
        assert_eq!(s.slots[1].0, TxgState::Syncing);
        sm.mark_synced(q);
        let s = sm.snapshot();
        assert_eq!(s.syncing_txg, None);
        assert_eq!(s.checkpoint_txg, 1);
        assert_eq!(s.slots[1].0, TxgState::Empty);
    }

    #[test]
    fn three_concurrent_txgs_then_ring_full() {
        let sm = TxgStateMachine::new(0);
        // Roll until 3 active TXGs.
        let q1 = sm.roll_to_quiescing();
        sm.promote_to_syncing(q1);
        // TXG 1 Syncing, TXG 2 Open.
        let q2 = sm.roll_to_quiescing();
        // TXG 2 Quiescing, TXG 3 Open, TXG 1 still Syncing.
        let s = sm.snapshot();
        assert_eq!(s.open_txg, 3);
        assert_eq!(s.quiescing_txg, Some(2));
        assert_eq!(s.syncing_txg, Some(1));
        assert_eq!(s.slots[1].0, TxgState::Syncing);
        assert_eq!(s.slots[2].0, TxgState::Quiescing);
        assert_eq!(s.slots[3].0, TxgState::Open);
        assert_eq!(s.slots[0].0, TxgState::Empty);
        let _ = q2;
    }

    #[test]
    fn promote_blocks_until_prior_sync_consumed() {
        // Regression for the threads-on shutdown deadlock: the quiesce
        // worker may roll + quiesce TXG 2 while TXG 1 is still Syncing
        // (3-concurrent), but promoting TXG 2 INTO Syncing must wait for
        // TXG 1's sync to be consumed. This used to be a debug_assert
        // ("two TXGs in Syncing") that panicked the quiesce worker when a
        // slow TxgSyncThread let it get ahead — orphaning the sync worker
        // and hanging every wait_until_synced.
        let sm = Arc::new(TxgStateMachine::new(0));
        let q1 = sm.roll_to_quiescing();
        sm.promote_to_syncing(q1);
        assert_eq!(sm.snapshot().syncing_txg, Some(1));
        // Roll TXG 2 -> Quiescing while TXG 1 is still Syncing: allowed
        // (ring-full has a free slot), this is the 3-concurrent state.
        let q2 = sm.roll_to_quiescing();
        assert_eq!(q2, 2);
        assert_eq!(sm.snapshot().quiescing_txg, Some(2));
        assert_eq!(sm.snapshot().syncing_txg, Some(1));
        // promote(2) must BLOCK until TXG 1's sync is consumed.
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.promote_to_syncing(2));
        thread::sleep(Duration::from_millis(40));
        assert!(
            !h.is_finished(),
            "promote_to_syncing must block while another TXG is still Syncing"
        );
        // Consume TXG 1's sync; the blocked promote now proceeds.
        sm.mark_synced(1);
        h.join().unwrap();
        let s = sm.snapshot();
        assert_eq!(s.syncing_txg, Some(2));
        assert_eq!(s.quiescing_txg, None);
    }

    // Ring-full block path (slot[(open+1) & 3] not Empty) is exercised by
    // integration tests once `TxgQuiesceThread` and `TxgSyncThread` are
    // wired together — that's where the 3-active-TXG state is reachable.
    // Single-threaded unit tests can't construct it without violating the
    // single-quiesce-thread invariant.

    #[test]
    fn roll_blocks_until_inflight_drains() {
        let sm = Arc::new(TxgStateMachine::new(0));
        let g = sm.enter();
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.roll_to_quiescing());
        thread::sleep(Duration::from_millis(30));
        assert!(!h.is_finished(), "roll should still be blocking on drain");
        // Verify the closing-window blocks new enter().
        let sm3 = Arc::clone(&sm);
        let h2 = thread::spawn(move || {
            let entered = sm3.enter();
            let txg = entered.txg();
            drop(entered);
            txg
        });
        thread::sleep(Duration::from_millis(30));
        assert!(!h2.is_finished(), "enter should be blocked by closing_open");
        drop(g);
        let q = h.join().unwrap();
        assert_eq!(q, 1);
        // After roll completes, the blocked enter() unblocks and stamps the
        // NEW open_txg (2), not the now-Quiescing one.
        let entered_txg = h2.join().unwrap();
        assert_eq!(entered_txg, 2);
    }

    #[test]
    fn wait_until_synced_blocks_then_releases() {
        let sm = Arc::new(TxgStateMachine::new(0));
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || {
            sm2.wait_until_synced(1);
        });
        thread::sleep(Duration::from_millis(30));
        assert!(!h.is_finished());
        let q = sm.roll_to_quiescing();
        sm.promote_to_syncing(q);
        sm.mark_synced(q);
        h.join().unwrap();
        assert_eq!(sm.checkpoint_txg(), 1);
    }

    #[test]
    fn slot_indexing_wraps_correctly() {
        // checkpoint_txg starts at 100; open_txg = 101, index = 101 & 3 = 1.
        let sm = TxgStateMachine::new(100);
        assert_eq!(sm.open_txg(), 101);
        assert_eq!(sm.snapshot().slots[1].0, TxgState::Open);
        for expected_open in 102..=110u64 {
            let q = sm.roll_to_quiescing();
            sm.promote_to_syncing(q);
            sm.mark_synced(q);
            assert_eq!(sm.open_txg(), expected_open);
            let s = sm.snapshot();
            assert_eq!(s.slots[(expected_open & 3) as usize].0, TxgState::Open);
        }
    }

    #[test]
    fn shutdown_wakes_blocked_roll() {
        let sm = Arc::new(TxgStateMachine::new(0));
        let g = sm.enter();
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.roll_to_quiescing());
        thread::sleep(Duration::from_millis(30));
        sm.shutdown();
        let returned = h.join().unwrap();
        // Under shutdown, roll returns current `open_txg` without
        // advancing — the closing_open flag stays set but no commit can
        // observe it because `enter()` also sees shutdown.
        assert_eq!(returned, 1);
        drop(g);
    }

    #[test]
    fn shutdown_wakes_sync_waiter() {
        let sm = Arc::new(TxgStateMachine::new(0));
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || {
            sm2.wait_until_synced(99);
        });
        thread::sleep(Duration::from_millis(30));
        sm.shutdown();
        h.join().unwrap();
    }

    #[test]
    fn open_txg_atomic_kept_in_sync() {
        let sm = TxgStateMachine::new(0);
        assert_eq!(sm.open_txg(), 1);
        let q = sm.roll_to_quiescing();
        assert_eq!(sm.open_txg(), 2);
        sm.promote_to_syncing(q);
        assert_eq!(sm.open_txg(), 2);
        sm.mark_synced(q);
        assert_eq!(sm.open_txg(), 2);
        assert_eq!(sm.checkpoint_txg(), 1);
    }

    #[test]
    fn enter_during_close_window_blocks_until_advance() {
        // Direct test of the closing_open gate: a roll that's waiting on
        // inflight must block fresh enter()s; the unblocked enter sees the
        // NEW open TXG, not the closing one.
        let sm = Arc::new(TxgStateMachine::new(0));
        let g0 = sm.enter(); // pin TXG 1
        let sm2 = Arc::clone(&sm);
        let roller = thread::spawn(move || sm2.roll_to_quiescing());
        thread::sleep(Duration::from_millis(15));
        let sm3 = Arc::clone(&sm);
        let entrant = thread::spawn(move || {
            let g = sm3.enter();
            let txg = g.txg();
            drop(g);
            txg
        });
        thread::sleep(Duration::from_millis(15));
        assert!(!entrant.is_finished());
        drop(g0);
        let q = roller.join().unwrap();
        assert_eq!(q, 1);
        let entered_txg = entrant.join().unwrap();
        assert_eq!(entered_txg, 2);
    }
}
