//! Writer-priority gate around the L2P / refcount apply phase.
//!
//! `parking_lot::RwLock` favours readers: a steady stream of `read()`
//! calls can starve a pending `write()` indefinitely. The commit hot
//! path does exactly that — every `commit_ops` holds `apply_gate.read()`
//! across the whole apply (~88 ms avg, 1+ s tail), so the on-disk
//! checkpoint flush (which needs `apply_gate.write()` to sample a
//! consistent `last_applied_lsn` + tree roots) waited multiple seconds
//! per attempt and tens of seconds at tail. That backed dirty pages up
//! in the L2P / refcount caches until the page cache started thrashing
//! and the upstream write buffer stopped draining.
//!
//! `ApplyGate` is a thin wrapper that adds writer priority: when a
//! writer is in flight (waiting for the lock OR holding it), new
//! readers wait on a condvar before entering. Existing
//! readers (already holding the inner lock) are unaffected — they
//! drain naturally and the writer proceeds. Once the writer releases,
//! the condvar wakes every parked reader.
//!
//! Checkpoint manifest publication has one deliberate exception: a writer may
//! suspend into a reservation after freezing its generation. The reservation
//! preserves writer order but admits readers while immutable catalog/bitmap
//! pages are written; resuming the writer closes the gate for the final slot
//! publish.
//!
//! Trade-off: commits' `apply_gate_wait` increases (they politely yield
//! to flush) in exchange for flush completing in tens of milliseconds
//! instead of seconds. Since flush is what unblocks dirty-page reclaim
//! and WAL-segment prune, prioritising it improves overall steady-state
//! throughput even though individual commits wait slightly longer.
//!
//! API surface mirrors `RwLock<()>` so call sites keep
//! `apply_gate.read()` / `apply_gate.write()` unchanged.

use parking_lot::{Condvar, Mutex};

pub struct ApplyGate {
    state: Mutex<GateState>,
    cv: Condvar,
}

#[derive(Default)]
struct GateState {
    readers: usize,
    writers_pending: usize,
    writer_active: bool,
    writer_reserved: bool,
}

impl Default for ApplyGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplyGate {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(GateState::default()),
            cv: Condvar::new(),
        }
    }

    /// Acquire shared access. Blocks until no writer is pending.
    ///
    /// A reader that passes the writer-pending check then calls
    /// `inner.read()` may still race with a writer that requested
    /// between the check and the acquire — that race is bounded by one
    /// writer's hold time (parking_lot's existing semantics), not by
    /// indefinite starvation.
    pub fn read(&self) -> ReadGuard<'_> {
        let mut state = self.state.lock();
        while state.writer_active || state.writers_pending > 0 {
            self.cv.wait(&mut state);
        }
        state.readers += 1;
        ReadGuard { gate: self }
    }

    /// Acquire shared access even while a writer is pending.
    ///
    /// This is intentionally not the default: it exists for the Db commit
    /// scheduler's lower-LSN rescue path. If a higher-LSN commit already
    /// holds the read side and is waiting for global LSN order, a lower-LSN
    /// predecessor must be allowed through or the pending checkpoint writer
    /// can form a three-way deadlock.
    pub(crate) fn read_bypass_writer_pending(&self) -> ReadGuard<'_> {
        let mut state = self.state.lock();
        while state.writer_active {
            self.cv.wait(&mut state);
        }
        state.readers += 1;
        ReadGuard { gate: self }
    }

    pub(crate) fn has_writer_pending(&self) -> bool {
        let state = self.state.lock();
        state.writer_active || state.writers_pending > 0
    }

    /// Acquire exclusive access. Bumps `writer_pending` immediately so
    /// any reader entering `read()` after this point parks.
    pub fn write(&self) -> WriteGuard<'_> {
        let mut state = self.state.lock();
        state.writers_pending += 1;
        while state.writer_active || state.writer_reserved || state.readers > 0 {
            self.cv.wait(&mut state);
        }
        state.writers_pending -= 1;
        state.writer_active = true;
        WriteGuard {
            gate: self,
            active: true,
        }
    }

    /// Acquire exclusive access only if it is immediately available.
    ///
    /// Unlike [`write`](Self::write), this does not mark a writer as
    /// pending. Opportunistic checkpoint attempts can use it to avoid
    /// parking new commit readers behind a background maintenance task.
    pub fn try_write(&self) -> Option<WriteGuard<'_>> {
        let mut state = self.state.lock();
        if state.writer_active
            || state.writer_reserved
            || state.writers_pending > 0
            || state.readers > 0
        {
            return None;
        }
        state.writer_active = true;
        Some(WriteGuard {
            gate: self,
            active: true,
        })
    }
}

pub struct ReadGuard<'a> {
    gate: &'a ApplyGate,
}

pub struct WriteGuard<'a> {
    gate: &'a ApplyGate,
    active: bool,
}

/// Exclusive-writer ordering token that temporarily lets readers through.
///
/// A checkpoint uses this after freezing the manifest generation: data-plane
/// readers may continue while immutable catalog/bitmap pages are written, but
/// another lifecycle writer cannot enter and publish a competing generation.
pub struct WriteReservation<'a> {
    gate: &'a ApplyGate,
    active: bool,
}

impl<'a> WriteGuard<'a> {
    pub fn suspend(mut self) -> WriteReservation<'a> {
        let mut state = self.gate.state.lock();
        debug_assert!(state.writer_active);
        debug_assert!(!state.writer_reserved);
        state.writer_active = false;
        state.writer_reserved = true;
        self.active = false;
        self.gate.cv.notify_all();
        WriteReservation {
            gate: self.gate,
            active: true,
        }
    }
}

impl<'a> WriteReservation<'a> {
    pub fn resume(mut self) -> WriteGuard<'a> {
        let mut state = self.gate.state.lock();
        debug_assert!(state.writer_reserved);
        state.writers_pending += 1;
        while state.writer_active || state.readers > 0 {
            self.gate.cv.wait(&mut state);
        }
        state.writers_pending -= 1;
        state.writer_reserved = false;
        state.writer_active = true;
        self.active = false;
        WriteGuard {
            gate: self.gate,
            active: true,
        }
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        state.readers = state.readers.saturating_sub(1);
        if state.readers == 0 {
            self.gate.cv.notify_all();
        }
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.gate.state.lock();
        state.writer_active = false;
        self.gate.cv.notify_all();
    }
}

impl Drop for WriteReservation<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.gate.state.lock();
        state.writer_reserved = false;
        self.gate.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn read_blocks_when_writer_pending() {
        let gate = Arc::new(ApplyGate::new());
        // Hold one reader so writer parks.
        let reader_hold = gate.read();

        // Spawn a writer; it'll bump writer_pending then park on
        // inner.write() waiting for our reader to drop.
        let gate_w = gate.clone();
        let writer_in = Arc::new(AtomicBool::new(false));
        let writer_in_clone = writer_in.clone();
        let writer = thread::spawn(move || {
            let _g = gate_w.write();
            writer_in_clone.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(50));
        });

        // Give the writer time to enter the gate's write() and bump
        // writer_pending.
        thread::sleep(Duration::from_millis(20));
        assert!(gate.has_writer_pending());

        // A new reader from another thread must park (because
        // writer_pending > 0), even though inner is currently held by
        // our reader (so a normal reader-priority RwLock would let it in).
        let gate_r = gate.clone();
        let reader_in = Arc::new(AtomicBool::new(false));
        let reader_in_clone = reader_in.clone();
        let reader = thread::spawn(move || {
            let _g = gate_r.read();
            reader_in_clone.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(20));
        assert!(
            !reader_in.load(Ordering::SeqCst),
            "second reader entered while a writer was pending"
        );
        assert!(
            !writer_in.load(Ordering::SeqCst),
            "writer should still be parked behind the held reader"
        );

        drop(reader_hold);
        writer.join().unwrap();
        reader.join().unwrap();
        assert!(reader_in.load(Ordering::SeqCst));
        assert!(writer_in.load(Ordering::SeqCst));
        assert!(!gate.has_writer_pending());
    }

    #[test]
    fn writer_eventually_wins_against_reader_storm() {
        let gate = Arc::new(ApplyGate::new());
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();

        // 8 reader threads continually take and release the gate.
        for _ in 0..8 {
            let gate = gate.clone();
            let stop = stop.clone();
            handles.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _g = gate.read();
                    thread::sleep(Duration::from_micros(50));
                }
            }));
        }

        // Give readers time to ramp up.
        thread::sleep(Duration::from_millis(20));

        // Writer should acquire within a bounded time even with
        // continuous reader pressure.
        let start = Instant::now();
        let g = gate.write();
        let waited = start.elapsed();
        // Tell readers to stop, then drop the write guard so any
        // readers parked in `read()` (because writer_pending > 0) wake
        // up and exit on the next loop iteration.
        stop.store(true, Ordering::Relaxed);
        drop(g);
        for h in handles {
            h.join().unwrap();
        }
        assert!(
            waited < Duration::from_millis(500),
            "writer waited too long: {:?}",
            waited
        );
    }

    #[test]
    fn try_write_does_not_park_later_readers_when_busy() {
        let gate = ApplyGate::new();
        let first_reader = gate.read();
        assert!(gate.try_write().is_none());
        assert!(
            !gate.has_writer_pending(),
            "try_write must not advertise a pending writer"
        );
        let second_reader = gate.read();
        drop(second_reader);
        drop(first_reader);
        assert!(gate.try_write().is_some());
    }

    #[test]
    fn suspended_writer_allows_readers_but_blocks_other_writers() {
        let gate = Arc::new(ApplyGate::new());
        let reservation = gate.write().suspend();

        let reader = gate.read();
        let writer_entered = Arc::new(AtomicBool::new(false));
        let gate_w = gate.clone();
        let writer_entered_w = writer_entered.clone();
        let writer = thread::spawn(move || {
            let _guard = gate_w.write();
            writer_entered_w.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(20));
        assert!(!writer_entered.load(Ordering::SeqCst));
        drop(reader);

        let resumed = reservation.resume();
        assert!(!writer_entered.load(Ordering::SeqCst));
        drop(resumed);
        writer.join().unwrap();
        assert!(writer_entered.load(Ordering::SeqCst));
    }

    #[test]
    fn dropping_suspended_writer_releases_queued_writer() {
        let gate = Arc::new(ApplyGate::new());
        let reservation = gate.write().suspend();
        let writer_entered = Arc::new(AtomicBool::new(false));
        let gate_w = gate.clone();
        let writer_entered_w = writer_entered.clone();
        let writer = thread::spawn(move || {
            let _guard = gate_w.write();
            writer_entered_w.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(20));
        assert!(!writer_entered.load(Ordering::SeqCst));
        drop(reservation);
        writer.join().unwrap();
        assert!(writer_entered.load(Ordering::SeqCst));
    }

    #[test]
    fn many_concurrent_readers_allowed_when_no_writer() {
        let gate = Arc::new(ApplyGate::new());
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let gate = gate.clone();
            let entered = entered.clone();
            let release = release.clone();
            handles.push(thread::spawn(move || {
                let _g = gate.read();
                entered.fetch_add(1, Ordering::SeqCst);
                while !release.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(1));
                }
            }));
        }
        // Wait for all readers to enter concurrently.
        let deadline = Instant::now() + Duration::from_secs(2);
        while entered.load(Ordering::SeqCst) < 16 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(entered.load(Ordering::SeqCst), 16);
        release.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn bypass_reader_can_enter_behind_pending_writer_when_reader_active() {
        let gate = Arc::new(ApplyGate::new());
        let held = gate.read();

        let writer_waiting = Arc::new(AtomicBool::new(false));
        let writer_entered = Arc::new(AtomicBool::new(false));
        let writer_gate = gate.clone();
        let writer_waiting_c = writer_waiting.clone();
        let writer_entered_c = writer_entered.clone();
        let writer = thread::spawn(move || {
            writer_waiting_c.store(true, Ordering::SeqCst);
            let _guard = writer_gate.write();
            writer_entered_c.store(true, Ordering::SeqCst);
        });

        while !writer_waiting.load(Ordering::SeqCst) || !gate.has_writer_pending() {
            thread::yield_now();
        }

        let bypass_gate = gate.clone();
        let bypass_entered = Arc::new(AtomicBool::new(false));
        let bypass_entered_c = bypass_entered.clone();
        let bypass = thread::spawn(move || {
            let _guard = bypass_gate.read_bypass_writer_pending();
            bypass_entered_c.store(true, Ordering::SeqCst);
        });

        let deadline = Instant::now() + Duration::from_millis(200);
        while !bypass_entered.load(Ordering::SeqCst) && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            bypass_entered.load(Ordering::SeqCst),
            "bypass reader should enter while an older reader is draining"
        );
        assert!(
            !writer_entered.load(Ordering::SeqCst),
            "writer must still wait for the original reader"
        );

        bypass.join().unwrap();
        drop(held);
        writer.join().unwrap();
        assert!(writer_entered.load(Ordering::SeqCst));
    }
}
