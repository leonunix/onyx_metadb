//! Three-state global BFG epoch state machine.
//!
//! BFG - Blueflame Gatling Group
//! Inspired by ZFS TXG, but shoots faster 🔵
//!
//! Replaces the single-`compacted_lsn` / `checkpoint_lsn` progress point with
//! BFG accounting. The code still uses `Bfg` for the epoch type because that
//! name is wired through configs, metrics, tests, and on-disk fields. At any
//! moment several groups are active in a ring of `BFG_SIZE = 8` slots
//! (index = `bfg & (BFG_SIZE - 1)`):
//!
//! - **Open**: accepting new commits. Exactly one slot.
//! - **Quiescing**: closed to new commits, frozen, awaiting promotion into
//!   Syncing. Legacy mode has at most one; pipeline mode
//!   (`bfg_admission_pipeline_enabled`) queues several in a FIFO behind the
//!   single Syncing generation so the quiesce worker never blocks on the fold.
//! - **Syncing**: drained; `BfgSyncThread` is persisting it. At most one slot.
//! - **Empty**: ring slot available for the next roll.
//!
//! Commit hot path:
//! ```ignore
//! let guard = state.admit_l2p_work(work, limit)?; // enter + account atomically
//! // ... submit WAL, apply ops, stamp L2pBuffer slot[guard.bfg & (BFG_SIZE - 1)] ...
//! guard.record_lsn(lsn);              // 1 mutex acquire
//! drop(guard);                         // 1 mutex acquire
//! ```
//!
//! Quiesce thread (single, fires on 5 s timer or dirty-data trigger):
//! ```ignore
//! let bfg = state.roll_to_quiescing();   // close current Open, open next
//! state.wait_quiesce_drained(bfg);       // park until inflight == 0
//! state.promote_to_syncing(bfg);
//! sync_notify.wake();
//! ```
//!
//! Sync thread (single, fires on quiesce notify):
//! ```ignore
//! // drain L2pBuffer slots, apply to tree, manifest commit, then:
//! state.mark_synced(bfg);
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
//! Lookup-side cheaper paths use `open_bfg_atomic` / `checkpoint_bfg_atomic`
//! snapshots so the L2P read path can walk the ring without taking the
//! inner mutex.
//!
//! ## Ring invariants (debug_asserted)
//!
//! - Exactly one slot in Open, at `open_bfg & (BFG_SIZE - 1)`.
//! - Legacy: at most one Quiescing, at `(open_bfg - 1) & (BFG_SIZE - 1)`.
//!   Pipeline: a FIFO of frozen Quiescing generations below `open_bfg`.
//! - At most one slot in Syncing (both modes).
//! - `checkpoint_bfg + 1 <= open_bfg`
//! - `open_bfg - checkpoint_bfg <= BFG_SIZE - 1` (at most `BFG_CONCURRENT_STATES` active)

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Condvar, Mutex};

use crate::types::{Bfg, Lsn};

/// Ring slot count. Must be a power of two (slot index = `bfg & (BFG_SIZE - 1)`).
///
/// Deepened 4 -> 8 for pipeline mode: the quiesce worker may roll ahead of the
/// fold, so several frozen (Quiescing) generations queue behind the single
/// Syncing one. Worst case sized from a 1.5 s fold at a ~6.3 M/s burst fill over
/// the 4 M soft limit ≈ 3 extra Open generations → 1 Syncing + 3 frozen + 1 Open
/// + 1 Empty = 6, rounded up to the next power of two. Legacy mode still uses at
/// most 3 active slots; the extra slots stay Empty.
pub const BFG_SIZE: usize = 8;

/// Maximum number of concurrently-active BFGs (Open + Quiescing + Syncing).
/// One slot in the ring is always Empty, acting as the next-slot reservation
/// for the upcoming roll, so this is `BFG_SIZE - 1`.
pub const BFG_CONCURRENT_STATES: usize = BFG_SIZE - 1;

const BFG_INDEX_MASK: u64 = (BFG_SIZE as u64) - 1;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BfgState {
    Empty,
    Open,
    Quiescing,
    Syncing,
}

#[derive(Copy, Clone, Debug)]
struct Slot {
    state: BfgState,
    inflight: u64,
    max_lsn: Lsn,
    /// Number of L2P mutations admitted to this BFG. This is an upper
    /// bound on the unique entries the syncing fold must consume.
    l2p_work: usize,
}

impl Slot {
    const fn empty() -> Self {
        Self {
            state: BfgState::Empty,
            inflight: 0,
            max_lsn: 0,
            l2p_work: 0,
        }
    }
}

struct Inner {
    slots: [Slot; BFG_SIZE],
    open_bfg: Bfg,
    /// FIFO of frozen (Quiescing) generations awaiting promotion into Syncing,
    /// oldest at the front. Legacy (pipeline-off) mode keeps this at length
    /// `<= 1`: the quiesce worker blocks in `promote_to_syncing` before rolling
    /// the next generation, so at most one BFG is ever Quiescing. Pipeline mode
    /// lets the quiesce worker roll ahead without blocking on the prior fold, so
    /// several frozen generations queue here while one is Syncing;
    /// `try_promote_next` drains the front after each `mark_synced`.
    quiescing: VecDeque<Bfg>,
    syncing_bfg: Option<Bfg>,
    checkpoint_bfg: Bfg,
    shutdown: bool,
    /// Set by `mark_aborted` when a forced BFG sync fails in a way that cannot
    /// be retried in this process, for example a faulted manifest fsync. A
    /// failed sync leaves its slot in `Syncing` forever because `mark_synced`
    /// never runs, so every waiter must wake and return instead of blocking on
    /// a slot that can never complete. Unlike `shutdown`, this is a poison
    /// state: `wait_until_synced` returns `false` so callers report "restart
    /// required". Recovery is a process restart + reopen.
    aborted: bool,
    /// Set by `roll_to_quiescing` after the next-slot wait completes and
    /// BEFORE the inflight-drain wait begins. `enter()` blocks while this
    /// is set so no new commits join the closing BFG. Cleared once the
    /// state flip + open_bfg advance is done.
    ///
    /// Without this, the WAL allocator may hand out an LSN < L to a
    /// commit stamped to BFG N+1 (entered between the inflight-drain
    /// wakeup and the open_bfg advance) — breaking the monotonicity
    /// invariant `max(LSN in BFG_n) <= min(LSN in BFG_n+1)` that
    /// `checkpoint_lsn = slot[K].max_lsn` relies on for WAL prune
    /// correctness.
    closing_open: bool,
    /// The Open BFG whose L2P work budget has been reached. New
    /// work-admitting commits park until `roll_to_quiescing` successfully
    /// opens the next generation. Keeping the generation in the gate (rather
    /// than a bare bool) makes a delayed force notification harmless.
    l2p_admission_closed_bfg: Option<Bfg>,
    /// When set, the quiesce/sync workers use the non-blocking pipelined
    /// promotion path (`try_promote_next`) and the ring may hold several
    /// Quiescing generations. When clear, the legacy at-most-one-Quiescing +
    /// blocking `promote_to_syncing` semantics hold unchanged.
    pipeline_enabled: bool,
    /// Sum of admitted-but-not-yet-synced L2P work across every active slot
    /// (incremented by `admit_l2p_work`, decremented by `mark_synced`). In
    /// pipeline mode `admit_l2p_work` parks once this reaches `l2p_hard_limit`,
    /// bounding RAM/WAL even though the soft limit no longer serializes commits
    /// on the fold. Tracked in both modes; only enforced when pipelined.
    outstanding_l2p_work: usize,
    /// Hard ceiling on `outstanding_l2p_work` (pipeline mode only). `usize::MAX`
    /// disables the ceiling (legacy mode, or an explicit unbounded A/B).
    l2p_hard_limit: usize,
}

/// Terminal reason why a work-admitting commit could not enter an Open BFG.
/// Callers map `Aborted` to the sync-poison error and `Shutdown` to a clean
/// teardown error instead of parking forever.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    Shutdown,
    Aborted,
}

pub struct BfgStateMachine {
    inner: Mutex<Inner>,
    cv: Condvar,
    open_bfg_atomic: AtomicU64,
    checkpoint_bfg_atomic: AtomicU64,
}

impl BfgStateMachine {
    /// Fresh state machine with all slots empty except the Open slot at
    /// `(initial_checkpoint_bfg + 1) & (BFG_SIZE - 1)`. Legacy (pipeline-off)
    /// semantics: at most one Quiescing, blocking `promote_to_syncing`, no
    /// outstanding-work ceiling. Used by unit tests and pre-pipeline callers.
    pub fn new(initial_checkpoint_bfg: Bfg) -> Self {
        Self::new_with_pipeline(initial_checkpoint_bfg, false, usize::MAX)
    }

    /// Fresh state machine with the pipelined-admission mode and outstanding
    /// L2P-work ceiling configured explicitly. `pipeline_enabled == false` +
    /// `l2p_hard_limit == usize::MAX` is exactly [`Self::new`].
    pub fn new_with_pipeline(
        initial_checkpoint_bfg: Bfg,
        pipeline_enabled: bool,
        l2p_hard_limit: usize,
    ) -> Self {
        let mut slots = [Slot::empty(); BFG_SIZE];
        let open_bfg = initial_checkpoint_bfg + 1;
        slots[(open_bfg & BFG_INDEX_MASK) as usize].state = BfgState::Open;
        Self {
            inner: Mutex::new(Inner {
                slots,
                open_bfg,
                quiescing: VecDeque::new(),
                syncing_bfg: None,
                checkpoint_bfg: initial_checkpoint_bfg,
                shutdown: false,
                aborted: false,
                closing_open: false,
                l2p_admission_closed_bfg: None,
                pipeline_enabled,
                outstanding_l2p_work: 0,
                l2p_hard_limit,
            }),
            cv: Condvar::new(),
            open_bfg_atomic: AtomicU64::new(open_bfg),
            checkpoint_bfg_atomic: AtomicU64::new(initial_checkpoint_bfg),
        }
    }

    /// Stamp the caller's commit to the currently-Open BFG. Holds a refcount
    /// on that slot until the returned guard is dropped. **Hot path.**
    ///
    /// Blocks (briefly) while a `roll_to_quiescing` is in its drain window
    /// — without that block a freshly-entered commit could obtain a WAL
    /// LSN before the rolling BFG's last commit, breaking the
    /// `max(LSN in BFG_n) <= min(LSN in BFG_n+1)` monotonicity invariant
    /// the manifest WAL-prune logic depends on. The window is bounded by
    /// the in-flight commit count on the closing BFG, which is normally
    /// drained within milliseconds.
    pub fn enter(&self) -> BfgGuard<'_> {
        let mut g = self.inner.lock();
        while g.closing_open && !g.shutdown && !g.aborted {
            self.cv.wait(&mut g);
        }
        let bfg = g.open_bfg;
        let idx = (bfg & BFG_INDEX_MASK) as usize;
        debug_assert_eq!(g.slots[idx].state, BfgState::Open);
        g.slots[idx].inflight += 1;
        BfgGuard {
            sm: self,
            bfg,
            l2p_limit_crossed: false,
        }
    }

    /// Atomically admit one commit and account its submitted L2P work before
    /// the caller reserves an LSN. The batch that crosses `limit` is admitted,
    /// then the door closes for later callers until a successful BFG roll.
    /// Therefore a BFG contains at most `limit + max_batch_work - 1` submitted
    /// L2P mutations (with saturating arithmetic at `usize::MAX`).
    ///
    /// `limit == 0` disables work accounting and cannot close the admission
    /// gate. An already-closed generation is still respected. This is used
    /// when BFG threads/L2P buffering are disabled and while snapshots are
    /// live; timer/lifecycle-driven rolls remain unchanged.
    pub fn admit_l2p_work(
        &self,
        work: usize,
        limit: usize,
    ) -> std::result::Result<BfgGuard<'_>, AdmissionError> {
        let mut g = self.inner.lock();
        // Park on: (a) a roll's close window, (b) this generation's soft-limit
        // gate, or (c) pipeline mode's outstanding-work hard ceiling. (c) bounds
        // RAM/WAL now that the soft limit no longer serializes commits on the
        // fold; a completed fold (`mark_synced`) lowers `outstanding_l2p_work`
        // and `notify_all`s these waiters.
        while (g.closing_open
            || g.l2p_admission_closed_bfg == Some(g.open_bfg)
            || (g.pipeline_enabled && g.outstanding_l2p_work >= g.l2p_hard_limit))
            && !g.shutdown
            && !g.aborted
        {
            self.cv.wait(&mut g);
        }
        if g.aborted {
            return Err(AdmissionError::Aborted);
        }
        if g.shutdown {
            return Err(AdmissionError::Shutdown);
        }

        let bfg = g.open_bfg;
        let idx = (bfg & BFG_INDEX_MASK) as usize;
        debug_assert_eq!(g.slots[idx].state, BfgState::Open);

        let mut crossed = false;
        if work != 0 && limit != 0 {
            let previous = g.slots[idx].l2p_work;
            let current = previous.saturating_add(work);
            g.slots[idx].l2p_work = current;
            // Keep the outstanding total in step with per-slot work so
            // `mark_synced` can subtract this generation's contribution exactly.
            g.outstanding_l2p_work = g.outstanding_l2p_work.saturating_add(work);
            if previous < limit && current >= limit {
                debug_assert!(g.l2p_admission_closed_bfg.is_none());
                g.l2p_admission_closed_bfg = Some(bfg);
                crossed = true;
            }
        }

        g.slots[idx].inflight += 1;
        Ok(BfgGuard {
            sm: self,
            bfg,
            l2p_limit_crossed: crossed,
        })
    }

    /// Bump the slot's `max_lsn` if the supplied LSN is larger. Called by the
    /// commit thread after `submit_wal_ops` returns the LSN. Must be called
    /// while a `BfgGuard` for this BFG is held — otherwise the slot may have
    /// already rolled and the write would land on a stale slot.
    pub fn record_lsn(&self, bfg: Bfg, lsn: Lsn) {
        let mut g = self.inner.lock();
        let idx = (bfg & BFG_INDEX_MASK) as usize;
        if lsn > g.slots[idx].max_lsn {
            g.slots[idx].max_lsn = lsn;
        }
    }

    /// Close the currently-Open BFG, wait for its in-flight commits to
    /// drain, then advance `open_bfg` to the next slot. Returns the now-
    /// Quiescing BFG.
    ///
    /// Three-phase wait under the inner mutex:
    ///   1. Ring-full wait — `slots[(cur+1) & (BFG_SIZE - 1)]` must be Empty.
    ///   2. Close `enter()` to new commits via `closing_open = true`.
    ///   3. Drain wait — `slots[cur & (BFG_SIZE - 1)].inflight` must hit 0.
    /// Only then flip states and advance `open_bfg`. The drain wait
    /// happens BEFORE the advance (not after) so the WAL allocator
    /// cannot interleave a BFG_(N+1) commit's LSN below a BFG_N
    /// commit's LSN — preserving the
    /// `max(LSN in BFG_n) <= min(LSN in BFG_n+1)` invariant.
    ///
    /// Called only by `BfgQuiesceThread`. Idempotent under shutdown:
    /// returns the current `open_bfg` without advancing.
    pub fn roll_to_quiescing(&self) -> Bfg {
        let mut g = self.inner.lock();
        let cur = g.open_bfg;
        let cur_idx = (cur & BFG_INDEX_MASK) as usize;
        let next = cur + 1;
        let next_idx = (next & BFG_INDEX_MASK) as usize;

        // (1) Ring-full wait. The next slot must be Empty AND the roll must not
        // push the active-generation count past `BFG_SIZE - 1`. The second
        // bound is load-bearing for read correctness: `L2pBuffer::lookup_*`
        // walks `BFG_SIZE - 1` slots back from `open_bfg`, so it only covers
        // every active generation (Open + all Quiescing + the one Syncing) if
        // at most `BFG_SIZE - 1` are active. Without it, pipeline mode could
        // reach `BFG_SIZE` active generations with zero Empty slots and a read
        // would miss the Syncing generation's still-unpublished slot → stale /
        // torn read. `checkpoint_bfg` advances via `mark_synced` (which
        // `notify_all`s), so this wait releases as the fold drains. Legacy mode
        // keeps at most 3 active, so this bound never waits there.
        while (g.slots[next_idx].state != BfgState::Empty
            || next.saturating_sub(g.checkpoint_bfg) > (BFG_SIZE as u64 - 1))
            && !g.shutdown
            && !g.aborted
        {
            self.cv.wait(&mut g);
        }
        if g.shutdown || g.aborted {
            // `aborted`: a failed sync pinned a slot in Syncing forever, so
            // the ring can never drain. Return without advancing (no
            // closing_open set yet) — the quiesce worker re-checks
            // `quiescing_bfg != Some(cur)` and skips promote/notify.
            return cur;
        }
        // Legacy mode keeps at most one Quiescing (the worker blocks in
        // `promote_to_syncing` before the next roll); pipeline mode intentionally
        // queues several frozen generations behind a single Syncing.
        debug_assert!(
            g.pipeline_enabled || g.quiescing.is_empty(),
            "two BFGs in Quiescing (legacy mode)"
        );
        debug_assert_eq!(g.slots[cur_idx].state, BfgState::Open);

        // (2) Close enter() to new commits. Without this the drain wait
        // below would race against fresh commits joining the slot.
        g.closing_open = true;

        // (3) Drain wait: every commit that started enter() before step (2)
        // closed the door must finish submit + apply and drop its guard.
        while g.slots[cur_idx].inflight > 0 && !g.shutdown && !g.aborted {
            self.cv.wait(&mut g);
        }
        if g.shutdown || g.aborted {
            // Leave closing_open = true so the shutdown/abort path is
            // observable; shutdown()/mark_aborted() both notify and `enter()`
            // breaks on either flag, so the close flag does not matter beyond
            // this point. Returning without flipping leaves quiescing_bfg
            // unset → the quiesce worker skips promote/notify.
            return cur;
        }

        // Flip states and advance open_bfg.
        g.slots[cur_idx].state = BfgState::Quiescing;
        g.slots[next_idx].state = BfgState::Open;
        debug_assert_eq!(g.slots[next_idx].l2p_work, 0);
        g.quiescing.push_back(cur);
        g.open_bfg = next;
        self.open_bfg_atomic.store(next, Ordering::Release);
        if g.l2p_admission_closed_bfg == Some(cur) {
            g.l2p_admission_closed_bfg = None;
        } else {
            debug_assert!(g.l2p_admission_closed_bfg.is_none());
        }
        // Reopen the door — new commits now stamp to `next`.
        g.closing_open = false;
        self.cv.notify_all();
        cur
    }

    /// Promote a Quiescing BFG to Syncing. `inflight` must be zero.
    ///
    /// **Blocks until the previous Syncing BFG has been consumed**
    /// (`mark_synced` cleared `syncing_bfg`), enforcing "at most one
    /// Syncing". The quiesce worker may roll BFG N+1 while BFG N is still
    /// syncing, but the handoff INTO Syncing must wait for N's sync to finish.
    /// Without this wait, a `BfgSyncThread` that lags the roll cadence (for
    /// example `write_dirty_pages` under load) could promote two groups into
    /// Syncing, orphan the sync thread, and hang every `wait_until_synced`.
    ///
    /// On the threads-off inline flush path this never waits:
    /// `mark_synced` for the prior BFG has already run before the next
    /// `roll_to_quiescing` → `promote_to_syncing`. Returns without
    /// promoting under shutdown (the caller re-checks `quiescing_bfg`).
    pub fn promote_to_syncing(&self, bfg: Bfg) {
        let mut g = self.inner.lock();
        let idx = (bfg & BFG_INDEX_MASK) as usize;
        while g.syncing_bfg.is_some() && !g.shutdown && !g.aborted {
            self.cv.wait(&mut g);
        }
        if g.shutdown || g.aborted {
            // Return WITHOUT promoting (caller re-checks via `is_aborted` /
            // `snapshot().syncing_bfg` before driving run_sync_cycle).
            return;
        }
        debug_assert_eq!(g.slots[idx].state, BfgState::Quiescing);
        debug_assert_eq!(g.slots[idx].inflight, 0);
        // Legacy mode promotes the sole Quiescing generation, which is the FIFO
        // front. (Pipeline mode uses `try_promote_next` instead of this path.)
        debug_assert_eq!(g.quiescing.front().copied(), Some(bfg));
        g.slots[idx].state = BfgState::Syncing;
        g.quiescing.pop_front();
        g.syncing_bfg = Some(bfg);
    }

    /// Non-blocking promotion for pipeline mode: if no BFG is currently Syncing,
    /// promote the oldest Quiescing generation (FIFO front) into Syncing and
    /// return `Some(bfg)`; otherwise return `None` without blocking. The caller
    /// (quiesce worker after a roll, or sync worker after `mark_synced`) notifies
    /// the sync worker when this returns `Some`.
    ///
    /// Preserves at-most-one-Syncing (gates on `syncing_bfg.is_none()`) and
    /// in-order folds (FIFO front is the lowest-numbered frozen generation).
    /// Returns `None` under shutdown/abort so the worker stops driving syncs.
    #[must_use]
    pub fn try_promote_next(&self) -> Option<Bfg> {
        let mut g = self.inner.lock();
        if g.syncing_bfg.is_some() || g.shutdown || g.aborted {
            return None;
        }
        let bfg = *g.quiescing.front()?;
        let idx = (bfg & BFG_INDEX_MASK) as usize;
        debug_assert_eq!(g.slots[idx].state, BfgState::Quiescing);
        debug_assert_eq!(g.slots[idx].inflight, 0);
        g.quiescing.pop_front();
        g.slots[idx].state = BfgState::Syncing;
        g.syncing_bfg = Some(bfg);
        Some(bfg)
    }

    /// Mark a Syncing BFG complete; advance `checkpoint_bfg`. Wakes ring-full
    /// roll waiters and any flush callers parked in `wait_until_synced`.
    pub fn mark_synced(&self, bfg: Bfg) {
        let mut g = self.inner.lock();
        let idx = (bfg & BFG_INDEX_MASK) as usize;
        debug_assert_eq!(g.slots[idx].state, BfgState::Syncing);
        debug_assert_eq!(g.syncing_bfg, Some(bfg));
        // Folds complete strictly in order (at-most-one-Syncing + FIFO promote),
        // so a completed sync advances the checkpoint by exactly one.
        debug_assert_eq!(bfg, g.checkpoint_bfg + 1);
        // This generation's admitted L2P work is now folded into the tree; drop
        // it from the outstanding total that bounds pipeline-mode admission.
        g.outstanding_l2p_work = g
            .outstanding_l2p_work
            .saturating_sub(g.slots[idx].l2p_work);
        g.slots[idx].state = BfgState::Empty;
        g.slots[idx].inflight = 0;
        g.slots[idx].max_lsn = 0;
        g.slots[idx].l2p_work = 0;
        g.syncing_bfg = None;
        if bfg > g.checkpoint_bfg {
            g.checkpoint_bfg = bfg;
            self.checkpoint_bfg_atomic.store(bfg, Ordering::Release);
        }
        self.cv.notify_all();
    }

    /// Read the slot's max LSN. Returns the live value; if the slot has just
    /// been marked Empty by `mark_synced` the returned value is 0.
    pub fn slot_max_lsn(&self, bfg: Bfg) -> Lsn {
        let g = self.inner.lock();
        let idx = (bfg & BFG_INDEX_MASK) as usize;
        g.slots[idx].max_lsn
    }

    pub fn slot_l2p_work(&self, bfg: Bfg) -> usize {
        let g = self.inner.lock();
        g.slots[(bfg & BFG_INDEX_MASK) as usize].l2p_work
    }

    /// Park until `checkpoint_bfg >= target`. Used by `flush_with_gate` after
    /// it has force-rolled a BFG. Returns `false` iff the wait was released by
    /// `aborted` (a failed sync; the target will never be reached) so the
    /// caller surfaces the sync-poison "restart required" error instead of
    /// treating it as a clean sync; `true` on a real sync OR on shutdown
    /// (clean teardown, unchanged semantics).
    #[must_use]
    pub fn wait_until_synced(&self, target: Bfg) -> bool {
        let mut g = self.inner.lock();
        while g.checkpoint_bfg < target && !g.shutdown && !g.aborted {
            self.cv.wait(&mut g);
        }
        !g.aborted
    }

    /// True once a forced sync cycle has failed non-recoverably (see
    /// [`Inner::aborted`] / `mark_aborted`). Callers driving a sync re-check
    /// this after `promote_to_syncing` to avoid running a cycle on a slot the
    /// abort left un-promoted.
    pub fn is_aborted(&self) -> bool {
        self.inner.lock().aborted
    }

    /// Lock-free snapshot of `open_bfg`. L2P read paths walk the buffer ring
    /// starting from this value.
    pub fn open_bfg(&self) -> Bfg {
        self.open_bfg_atomic.load(Ordering::Acquire)
    }

    /// Lock-free snapshot of `checkpoint_bfg`.
    pub fn checkpoint_bfg(&self) -> Bfg {
        self.checkpoint_bfg_atomic.load(Ordering::Acquire)
    }

    /// True when this state machine runs the pipelined-admission mode
    /// (`try_promote_next` promotion, multi-Quiescing ring). Read by the
    /// quiesce/sync workers to choose the promotion path.
    pub fn pipeline_enabled(&self) -> bool {
        self.inner.lock().pipeline_enabled
    }

    /// Observability / test snapshot of every slot's state.
    pub fn snapshot(&self) -> StateSnapshot {
        let g = self.inner.lock();
        let slots =
            std::array::from_fn(|i| (g.slots[i].state, g.slots[i].inflight, g.slots[i].max_lsn));
        let active_generations = g
            .slots
            .iter()
            .filter(|s| s.state != BfgState::Empty)
            .count();
        StateSnapshot {
            slots,
            open_bfg: g.open_bfg,
            quiescing_bfg: g.quiescing.front().copied(),
            quiescing_bfgs: g.quiescing.iter().copied().collect(),
            syncing_bfg: g.syncing_bfg,
            checkpoint_bfg: g.checkpoint_bfg,
            active_generations,
        }
    }

    /// Wake every waiter so they can re-check shutdown and exit. Called by
    /// `Db::close` after both background threads have been signalled.
    pub fn shutdown(&self) {
        let mut g = self.inner.lock();
        g.shutdown = true;
        self.cv.notify_all();
    }

    /// Mark the BFG sync subsystem permanently failed, for example after a
    /// manifest fsync fault leaves a slot stuck in Syncing. Mirrors `shutdown`:
    /// set the sticky `aborted` flag and wake every waiter (`enter`,
    /// `roll_to_quiescing`, `promote_to_syncing`, `wait_until_synced`) so they
    /// return instead of blocking on the never-to-complete slot. One-way;
    /// recovery is a process restart.
    pub fn mark_aborted(&self) {
        let mut g = self.inner.lock();
        g.aborted = true;
        self.cv.notify_all();
    }
}

pub struct BfgGuard<'a> {
    sm: &'a BfgStateMachine,
    pub bfg: Bfg,
    l2p_limit_crossed: bool,
}

impl BfgGuard<'_> {
    pub fn bfg(&self) -> Bfg {
        self.bfg
    }

    /// Record the highest LSN observed against this guard's BFG.
    pub fn record_lsn(&self, lsn: Lsn) {
        self.sm.record_lsn(self.bfg, lsn);
    }

    /// True only for the admitted batch that closed this BFG's L2P work gate.
    /// The caller should send a generation-tagged quiesce notification before
    /// reserving its LSN, while this guard keeps the crossing commit inflight.
    pub fn l2p_limit_crossed(&self) -> bool {
        self.l2p_limit_crossed
    }
}

impl Drop for BfgGuard<'_> {
    fn drop(&mut self) {
        let mut g = self.sm.inner.lock();
        let idx = (self.bfg & BFG_INDEX_MASK) as usize;
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
    pub slots: [(BfgState, u64, Lsn); BFG_SIZE],
    pub open_bfg: Bfg,
    /// Oldest Quiescing generation (FIFO front), or `None`. Legacy consumers
    /// that expect at most one Quiescing read this; pipeline-aware callers use
    /// `quiescing_bfgs` for the full queue.
    pub quiescing_bfg: Option<Bfg>,
    /// All frozen (Quiescing) generations, oldest first. Length `<= 1` in
    /// legacy mode; may be deeper in pipeline mode.
    pub quiescing_bfgs: Vec<Bfg>,
    pub syncing_bfg: Option<Bfg>,
    pub checkpoint_bfg: Bfg,
    /// Number of active (non-Empty) generations: Open + Quiescing + Syncing.
    /// Surfaced to onyx metrics as `bfg_active_generations`.
    pub active_generations: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn new_initial_state() {
        let sm = BfgStateMachine::new(0);
        let s = sm.snapshot();
        assert_eq!(s.checkpoint_bfg, 0);
        assert_eq!(s.open_bfg, 1);
        assert_eq!(s.quiescing_bfg, None);
        assert_eq!(s.syncing_bfg, None);
        // Slot 1 is Open; everything else Empty.
        assert_eq!(s.slots[1].0, BfgState::Open);
        assert_eq!(s.slots[0].0, BfgState::Empty);
        assert_eq!(s.slots[2].0, BfgState::Empty);
        assert_eq!(s.slots[3].0, BfgState::Empty);
        assert_eq!(sm.open_bfg(), 1);
        assert_eq!(sm.checkpoint_bfg(), 0);
    }

    #[test]
    fn new_with_nonzero_checkpoint() {
        let sm = BfgStateMachine::new(7);
        let s = sm.snapshot();
        assert_eq!(s.checkpoint_bfg, 7);
        assert_eq!(s.open_bfg, 8);
        // 8 & (BFG_SIZE - 1) == 0 → slot 0 is Open.
        assert_eq!(s.slots[0].0, BfgState::Open);
    }

    #[test]
    fn enter_increments_inflight() {
        let sm = BfgStateMachine::new(0);
        let g1 = sm.enter();
        let g2 = sm.enter();
        assert_eq!(g1.bfg(), 1);
        assert_eq!(g2.bfg(), 1);
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
        let sm = BfgStateMachine::new(0);
        let g = sm.enter();
        g.record_lsn(10);
        g.record_lsn(5);
        g.record_lsn(20);
        assert_eq!(sm.slot_max_lsn(g.bfg()), 20);
    }

    #[test]
    fn l2p_crossing_batch_closes_admission_until_next_bfg_opens() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let before = sm.admit_l2p_work(7, 8).unwrap();
        assert!(!before.l2p_limit_crossed());
        let crossing = sm.admit_l2p_work(5, 8).unwrap();
        assert!(crossing.l2p_limit_crossed());
        // The crossing batch is the only allowed overshoot: 8 + 5 - 1.
        assert_eq!(sm.slot_l2p_work(1), 12);

        let waiter_sm = Arc::clone(&sm);
        let waiter = thread::spawn(move || {
            let guard = waiter_sm.admit_l2p_work(1, 8).unwrap();
            let bfg = guard.bfg();
            drop(guard);
            bfg
        });
        thread::sleep(Duration::from_millis(30));
        assert!(!waiter.is_finished(), "post-crossing admission must park");

        drop(before);
        drop(crossing);
        let q = sm.roll_to_quiescing();
        assert_eq!(q, 1);
        assert_eq!(waiter.join().unwrap(), 2);
        assert_eq!(sm.slot_l2p_work(2), 1);
    }

    #[test]
    fn zero_l2p_work_limit_disables_threshold() {
        let sm = BfgStateMachine::new(0);
        let g = sm.admit_l2p_work(usize::MAX, 0).unwrap();
        assert!(!g.l2p_limit_crossed());
        assert_eq!(sm.slot_l2p_work(g.bfg()), 0);
        let second = sm.admit_l2p_work(usize::MAX, 0).unwrap();
        assert_eq!(second.bfg(), g.bfg());
    }

    #[test]
    fn oversized_l2p_batch_is_admitted_then_closes_gate() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let crossing = sm.admit_l2p_work(usize::MAX, 8).unwrap();
        assert!(crossing.l2p_limit_crossed());
        assert_eq!(sm.slot_l2p_work(1), usize::MAX);

        let waiter_sm = Arc::clone(&sm);
        let waiter = thread::spawn(move || waiter_sm.admit_l2p_work(1, 8).map(|g| g.bfg()));
        thread::sleep(Duration::from_millis(30));
        assert!(!waiter.is_finished());
        drop(crossing);
        assert_eq!(sm.roll_to_quiescing(), 1);
        assert_eq!(waiter.join().unwrap(), Ok(2));
    }

    #[test]
    fn roll_advances_open_bfg() {
        let sm = BfgStateMachine::new(0);
        let q = sm.roll_to_quiescing();
        assert_eq!(q, 1);
        let s = sm.snapshot();
        assert_eq!(s.open_bfg, 2);
        assert_eq!(s.quiescing_bfg, Some(1));
        assert_eq!(s.slots[1].0, BfgState::Quiescing);
        assert_eq!(s.slots[2].0, BfgState::Open);
    }

    #[test]
    fn full_cycle_open_quiesce_sync_empty() {
        let sm = BfgStateMachine::new(0);
        // No inflight commits — roll completes its embedded drain wait
        // immediately and returns the now-Quiescing BFG.
        let q = sm.roll_to_quiescing();
        assert_eq!(q, 1);
        sm.promote_to_syncing(q);
        let s = sm.snapshot();
        assert_eq!(s.syncing_bfg, Some(1));
        assert_eq!(s.quiescing_bfg, None);
        assert_eq!(s.slots[1].0, BfgState::Syncing);
        sm.mark_synced(q);
        let s = sm.snapshot();
        assert_eq!(s.syncing_bfg, None);
        assert_eq!(s.checkpoint_bfg, 1);
        assert_eq!(s.slots[1].0, BfgState::Empty);
    }

    #[test]
    fn three_concurrent_bfgs_then_ring_full() {
        let sm = BfgStateMachine::new(0);
        // Roll until 3 active BFGs.
        let q1 = sm.roll_to_quiescing();
        sm.promote_to_syncing(q1);
        // BFG 1 Syncing, BFG 2 Open.
        let q2 = sm.roll_to_quiescing();
        // BFG 2 Quiescing, BFG 3 Open, BFG 1 still Syncing.
        let s = sm.snapshot();
        assert_eq!(s.open_bfg, 3);
        assert_eq!(s.quiescing_bfg, Some(2));
        assert_eq!(s.syncing_bfg, Some(1));
        assert_eq!(s.slots[1].0, BfgState::Syncing);
        assert_eq!(s.slots[2].0, BfgState::Quiescing);
        assert_eq!(s.slots[3].0, BfgState::Open);
        assert_eq!(s.slots[0].0, BfgState::Empty);
        let _ = q2;
    }

    #[test]
    fn promote_blocks_until_prior_sync_consumed() {
        // Regression for the threads-on shutdown deadlock: the quiesce
        // worker may roll + quiesce BFG 2 while BFG 1 is still Syncing
        // (3-concurrent), but promoting BFG 2 INTO Syncing must wait for
        // BFG 1's sync to be consumed. This used to be a debug_assert
        // ("two BFGs in Syncing") that panicked the quiesce worker when a
        // slow BfgSyncThread let it get ahead — orphaning the sync worker
        // and hanging every wait_until_synced.
        let sm = Arc::new(BfgStateMachine::new(0));
        let q1 = sm.roll_to_quiescing();
        sm.promote_to_syncing(q1);
        assert_eq!(sm.snapshot().syncing_bfg, Some(1));
        // Roll BFG 2 -> Quiescing while BFG 1 is still Syncing: allowed
        // (ring-full has a free slot), this is the 3-concurrent state.
        let q2 = sm.roll_to_quiescing();
        assert_eq!(q2, 2);
        assert_eq!(sm.snapshot().quiescing_bfg, Some(2));
        assert_eq!(sm.snapshot().syncing_bfg, Some(1));
        // promote(2) must BLOCK until BFG 1's sync is consumed.
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.promote_to_syncing(2));
        thread::sleep(Duration::from_millis(40));
        assert!(
            !h.is_finished(),
            "promote_to_syncing must block while another BFG is still Syncing"
        );
        // Consume BFG 1's sync; the blocked promote now proceeds.
        sm.mark_synced(1);
        h.join().unwrap();
        let s = sm.snapshot();
        assert_eq!(s.syncing_bfg, Some(2));
        assert_eq!(s.quiescing_bfg, None);
    }

    #[test]
    fn l2p_admission_stays_closed_while_three_active_bfgs_block_promote() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let q1 = sm.roll_to_quiescing();
        sm.promote_to_syncing(q1);
        let q2 = sm.roll_to_quiescing();
        assert_eq!(sm.snapshot().open_bfg, 3);

        let promote_sm = Arc::clone(&sm);
        let promoter = thread::spawn(move || promote_sm.promote_to_syncing(q2));
        thread::sleep(Duration::from_millis(30));
        assert!(!promoter.is_finished());

        let crossing = sm.admit_l2p_work(8, 8).unwrap();
        assert!(crossing.l2p_limit_crossed());
        drop(crossing);
        let waiter_sm = Arc::clone(&sm);
        let waiter = thread::spawn(move || waiter_sm.admit_l2p_work(1, 8).map(|g| g.bfg()));
        thread::sleep(Duration::from_millis(30));
        assert!(
            !waiter.is_finished(),
            "BFG 3 admission must remain closed while promote(2) is blocked"
        );

        sm.mark_synced(q1);
        promoter.join().unwrap();
        // Promotion alone does not reopen BFG 3; only its successful roll can.
        thread::sleep(Duration::from_millis(20));
        assert!(!waiter.is_finished());
        sm.mark_synced(q2);
        assert_eq!(sm.roll_to_quiescing(), 3);
        assert_eq!(waiter.join().unwrap(), Ok(4));
    }

    // Ring-full block path (slot[(open+1) & (BFG_SIZE - 1)] not Empty) is exercised by
    // integration tests once `BfgQuiesceThread` and `BfgSyncThread` are
    // wired together — that's where the 3-active-BFG state is reachable.
    // Single-threaded unit tests can't construct it without violating the
    // single-quiesce-thread invariant.

    #[test]
    fn roll_blocks_until_inflight_drains() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let g = sm.enter();
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.roll_to_quiescing());
        thread::sleep(Duration::from_millis(30));
        assert!(!h.is_finished(), "roll should still be blocking on drain");
        // Verify the closing-window blocks new enter().
        let sm3 = Arc::clone(&sm);
        let h2 = thread::spawn(move || {
            let entered = sm3.enter();
            let bfg = entered.bfg();
            drop(entered);
            bfg
        });
        thread::sleep(Duration::from_millis(30));
        assert!(!h2.is_finished(), "enter should be blocked by closing_open");
        drop(g);
        let q = h.join().unwrap();
        assert_eq!(q, 1);
        // After roll completes, the blocked enter() unblocks and stamps the
        // NEW open_bfg (2), not the now-Quiescing one.
        let entered_bfg = h2.join().unwrap();
        assert_eq!(entered_bfg, 2);
    }

    #[test]
    fn wait_until_synced_blocks_then_releases() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || {
            assert!(sm2.wait_until_synced(1), "synced wake must return true");
        });
        thread::sleep(Duration::from_millis(30));
        assert!(!h.is_finished());
        let q = sm.roll_to_quiescing();
        sm.promote_to_syncing(q);
        sm.mark_synced(q);
        h.join().unwrap();
        assert_eq!(sm.checkpoint_bfg(), 1);
    }

    #[test]
    fn slot_indexing_wraps_correctly() {
        // checkpoint_bfg starts at 100; open_bfg = 101, index = 101 & (BFG_SIZE-1).
        let mask = (BFG_SIZE - 1) as u64;
        let sm = BfgStateMachine::new(100);
        assert_eq!(sm.open_bfg(), 101);
        assert_eq!(sm.snapshot().slots[(101 & mask) as usize].0, BfgState::Open);
        for expected_open in 102..=110u64 {
            let q = sm.roll_to_quiescing();
            sm.promote_to_syncing(q);
            sm.mark_synced(q);
            assert_eq!(sm.open_bfg(), expected_open);
            let s = sm.snapshot();
            assert_eq!(s.slots[(expected_open & mask) as usize].0, BfgState::Open);
        }
    }

    #[test]
    fn shutdown_wakes_blocked_roll() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let g = sm.enter();
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.roll_to_quiescing());
        thread::sleep(Duration::from_millis(30));
        sm.shutdown();
        let returned = h.join().unwrap();
        // Under shutdown, roll returns current `open_bfg` without
        // advancing — the closing_open flag stays set but no commit can
        // observe it because `enter()` also sees shutdown.
        assert_eq!(returned, 1);
        drop(g);
    }

    #[test]
    fn shutdown_wakes_blocked_l2p_admission_with_status() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let crossing = sm.admit_l2p_work(8, 8).unwrap();
        let waiter_sm = Arc::clone(&sm);
        let waiter = thread::spawn(move || waiter_sm.admit_l2p_work(1, 8).map(|g| g.bfg()));
        thread::sleep(Duration::from_millis(30));
        assert!(!waiter.is_finished());
        sm.shutdown();
        assert_eq!(waiter.join().unwrap(), Err(AdmissionError::Shutdown));
        drop(crossing);
    }

    #[test]
    fn shutdown_wakes_sync_waiter() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || {
            // Shutdown is a clean teardown wake, not an abort → returns true.
            assert!(
                sm2.wait_until_synced(99),
                "shutdown wake must return true (clean)"
            );
        });
        thread::sleep(Duration::from_millis(30));
        sm.shutdown();
        h.join().unwrap();
    }

    #[test]
    fn open_bfg_atomic_kept_in_sync() {
        let sm = BfgStateMachine::new(0);
        assert_eq!(sm.open_bfg(), 1);
        let q = sm.roll_to_quiescing();
        assert_eq!(sm.open_bfg(), 2);
        sm.promote_to_syncing(q);
        assert_eq!(sm.open_bfg(), 2);
        sm.mark_synced(q);
        assert_eq!(sm.open_bfg(), 2);
        assert_eq!(sm.checkpoint_bfg(), 1);
    }

    #[test]
    fn enter_during_close_window_blocks_until_advance() {
        // Direct test of the closing_open gate: a roll that's waiting on
        // inflight must block fresh enter()s; the unblocked enter sees the
        // NEW open BFG, not the closing one.
        let sm = Arc::new(BfgStateMachine::new(0));
        let g0 = sm.enter(); // pin BFG 1
        let sm2 = Arc::clone(&sm);
        let roller = thread::spawn(move || sm2.roll_to_quiescing());
        thread::sleep(Duration::from_millis(15));
        let sm3 = Arc::clone(&sm);
        let entrant = thread::spawn(move || {
            let g = sm3.enter();
            let bfg = g.bfg();
            drop(g);
            bfg
        });
        thread::sleep(Duration::from_millis(15));
        assert!(!entrant.is_finished());
        drop(g0);
        let q = roller.join().unwrap();
        assert_eq!(q, 1);
        let entered_bfg = entrant.join().unwrap();
        assert_eq!(entered_bfg, 2);
    }

    // ---- Abort flag wakes every BFG waiter ----

    #[test]
    fn abort_makes_wait_until_synced_return_false() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.wait_until_synced(99));
        thread::sleep(Duration::from_millis(30));
        assert!(!h.is_finished(), "should block before abort");
        sm.mark_aborted();
        let synced = h.join().unwrap();
        assert!(
            !synced,
            "aborted wait must return false (caller surfaces poison)"
        );
    }

    #[test]
    fn abort_wakes_blocked_l2p_admission_with_status() {
        let sm = Arc::new(BfgStateMachine::new(0));
        let crossing = sm.admit_l2p_work(8, 8).unwrap();
        let waiter_sm = Arc::clone(&sm);
        let waiter = thread::spawn(move || waiter_sm.admit_l2p_work(1, 8).map(|g| g.bfg()));
        thread::sleep(Duration::from_millis(30));
        assert!(!waiter.is_finished());
        sm.mark_aborted();
        assert_eq!(waiter.join().unwrap(), Err(AdmissionError::Aborted));
        drop(crossing);
    }

    #[test]
    fn abort_wakes_blocked_promote_without_promoting() {
        // q1 stuck in Syncing (never mark_synced — models a failed cycle);
        // promote(q2) blocks on syncing_bfg.is_some(); mark_aborted must wake it
        // and it must NOT promote q2 (slot stays Quiescing).
        let sm = Arc::new(BfgStateMachine::new(0));
        let q1 = sm.roll_to_quiescing();
        sm.promote_to_syncing(q1);
        assert_eq!(sm.snapshot().syncing_bfg, Some(1));
        let q2 = sm.roll_to_quiescing();
        assert_eq!(sm.snapshot().quiescing_bfg, Some(2));
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.promote_to_syncing(q2));
        thread::sleep(Duration::from_millis(40));
        assert!(
            !h.is_finished(),
            "promote should block while q1 stuck Syncing"
        );
        sm.mark_aborted();
        h.join().unwrap();
        // q2 was NOT promoted: still Quiescing, syncing_bfg still the stuck q1.
        assert_eq!(sm.snapshot().quiescing_bfg, Some(2));
        assert_eq!(sm.snapshot().syncing_bfg, Some(1));
        assert!(sm.is_aborted());
    }

    #[test]
    fn abort_breaks_blocked_roll() {
        // A held `enter()` guard pins inflight>0, so roll_to_quiescing blocks on
        // the inflight-drain wait (after setting closing_open). Abort must wake
        // it and return the current bfg WITHOUT advancing — the HANG-completeness
        // fix for a roll wedged behind a stuck/aborted subsystem.
        let sm = Arc::new(BfgStateMachine::new(0));
        let g = sm.enter(); // inflight=1 on bfg 1
        let sm2 = Arc::clone(&sm);
        let h = thread::spawn(move || sm2.roll_to_quiescing());
        thread::sleep(Duration::from_millis(40));
        assert!(
            !h.is_finished(),
            "roll should block on the inflight-drain wait"
        );
        sm.mark_aborted();
        let returned = h.join().unwrap();
        assert_eq!(returned, 1, "aborted roll returns cur without advancing");
        assert_eq!(
            sm.snapshot().quiescing_bfg,
            None,
            "aborted roll must not flip to Quiescing"
        );
        assert!(sm.is_aborted());
        drop(g);
    }
}
