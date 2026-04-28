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
//! readers wait on a condvar before calling the inner `read()`. Existing
//! readers (already holding the inner lock) are unaffected — they
//! drain naturally and the writer proceeds. Once the writer releases,
//! the condvar wakes every parked reader.
//!
//! Trade-off: commits' `apply_gate_wait` increases (they politely yield
//! to flush) in exchange for flush completing in tens of milliseconds
//! instead of seconds. Since flush is what unblocks dirty-page reclaim
//! and WAL-segment prune, prioritising it improves overall steady-state
//! throughput even though individual commits wait slightly longer.
//!
//! API surface mirrors `RwLock<()>` so call sites keep
//! `apply_gate.read()` / `apply_gate.write()` unchanged.

use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct ApplyGate {
    inner: RwLock<()>,
    /// Number of write requests currently waiting OR holding the gate.
    /// Bumped atomically by `write()` before acquiring `inner`, decremented
    /// after releasing. Readers consult this to decide whether to park.
    writer_pending: AtomicUsize,
    cv_lock: Mutex<()>,
    cv: Condvar,
}

impl Default for ApplyGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplyGate {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(()),
            writer_pending: AtomicUsize::new(0),
            cv_lock: Mutex::new(()),
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
        if self.writer_pending.load(Ordering::Acquire) > 0 {
            let mut guard = self.cv_lock.lock();
            while self.writer_pending.load(Ordering::Acquire) > 0 {
                self.cv.wait(&mut guard);
            }
        }
        ReadGuard {
            _inner: self.inner.read(),
        }
    }

    /// Acquire exclusive access. Bumps `writer_pending` immediately so
    /// any reader entering `read()` after this point parks.
    pub fn write(&self) -> WriteGuard<'_> {
        self.writer_pending.fetch_add(1, Ordering::AcqRel);
        let inner = self.inner.write();
        WriteGuard {
            inner,
            release: WriterRelease { gate: self },
        }
    }
}

pub struct ReadGuard<'a> {
    _inner: RwLockReadGuard<'a, ()>,
}

/// Field drop order: `inner` runs first (releases the inner write lock),
/// then `release` runs (decrements `writer_pending` and wakes parked
/// readers). Doing it in this order avoids waking readers that would
/// immediately re-block on `inner.read()`.
pub struct WriteGuard<'a> {
    inner: RwLockWriteGuard<'a, ()>,
    // Drop runs on this field after `inner` to wake parked readers.
    #[allow(dead_code)]
    release: WriterRelease<'a>,
}

struct WriterRelease<'a> {
    gate: &'a ApplyGate,
}

impl Drop for WriterRelease<'_> {
    fn drop(&mut self) {
        let prev = self.gate.writer_pending.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            // Take cv_lock briefly so notify_all happens after any
            // reader that just observed writer_pending > 0 has parked
            // — otherwise the notify could fire between the reader's
            // load and its `wait`, leaving the reader stuck until the
            // next writer wakes it.
            let _g = self.gate.cv_lock.lock();
            self.gate.cv.notify_all();
        }
    }
}

// `WriteGuard` deliberately has no `Drop` impl: the field declaration
// order guarantees `inner` drops before `release`.
impl<'a> WriteGuard<'a> {
    /// Read-only handle to the inner guard, for tests / debug.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &RwLockWriteGuard<'a, ()> {
        &self.inner
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
        assert!(gate.writer_pending.load(Ordering::Acquire) > 0);

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
        assert_eq!(gate.writer_pending.load(Ordering::Acquire), 0);
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
}
