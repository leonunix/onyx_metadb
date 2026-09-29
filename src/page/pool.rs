//! Process-wide recycling pool for [`Page`](super::Page) buffers.
//!
//! # The measurement this replaces
//!
//! A `Page` is a `Box<[u8; 4096]>`, and 4096 is a jemalloc small size class
//! with **one region per slab**: every page alloc/free is an extent operation,
//! and every freed page is a dirty extent that decay later purges with a
//! `madvise` — a TLB-shootdown broadcast to every CPU sharing the `mm`, 31-47
//! IPIs per call regardless of its length. nvme-box, 2026-09-25, LBR call
//! stacks: the checkpoint thread (`metadb-bfg-sync`) spent **22.4 %** of its
//! user CPU inside jemalloc, 62 % of that under `DirtySnapshot::seal` (a clone
//! per dirty page per checkpoint) and 29 % under `PageCache::invalidate`. The
//! checkpoint writes ~41k pages per cycle on average, so the page lifecycle is
//! allocator churn at the scale of tens of thousands of pages a second.
//!
//! # Design
//!
//! Buffers are ordinary heap `Box`es that, once released, are kept and handed
//! out again instead of being freed — so jemalloc never sees the free and never
//! builds a dirty extent to purge.
//!
//! - A thread-local cache of up to [`LOCAL_MAX`] buffers serves most takes and
//!   releases with no lock. It moves [`BATCH`] buffers at a time to or from one
//!   global stack, so the global lock is taken once per `BATCH` operations even
//!   in a single-threaded burst (a checkpoint seals its dirty pages in one
//!   tight loop on one thread).
//! - The global stack keeps at most `max_free_pages`; past that a release goes
//!   back to the heap and counts as `overflow`. The pool never pre-allocates:
//!   it only holds what was released, so it is bounded by the cap, not by the
//!   page cache.
//! - The pool hands out buffers with **stale contents**. `Page`'s constructors
//!   overwrite every byte (`zeroed` fills, `clone` / `from_raw_bytes` copy), so
//!   a previous page can never leak into a new one.
//! - Counters are tallied thread-locally and folded into the globals at batch
//!   boundaries: no atomic on the hot path.
//!
//! # Runtime switch
//!
//! [`set_page_pool_enabled`] flips the pool on a live engine for an
//! interleaved A/B. Off, every take is a fresh heap allocation and every
//! release a heap free — the pre-pool behaviour — and the cached buffers are
//! given back (the global stack at once, each thread's cache on its next
//! operation).

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

use crate::config::PAGE_SIZE;

pub(super) type PageBytes = Box<[u8; PAGE_SIZE]>;

/// Buffers one thread keeps without touching the global stack.
const LOCAL_MAX: usize = 64;
/// Buffers moved per refill / spill between a thread and the global stack.
const BATCH: usize = 32;
/// Thread-local operations between counter folds when no batch moves happen.
const TALLY_FOLD_OPS: u64 = 256;

/// Default cap on buffers held by the global stack: 1 GiB. The checkpoint seals
/// ~41k pages (160 MiB) per cycle on average and the pool must hold one cycle's
/// worth between the seal and the release of the originals; the largest
/// observed cycle was 427k pages, which overflows at the margin instead.
pub const DEFAULT_PAGE_POOL_MAX_FREE_BYTES: usize = 1 << 30;

static ENABLED: AtomicBool = AtomicBool::new(true);
static MAX_FREE_PAGES: AtomicUsize = AtomicUsize::new(DEFAULT_PAGE_POOL_MAX_FREE_BYTES / PAGE_SIZE);
static GLOBAL: Mutex<Vec<PageBytes>> = Mutex::new(Vec::new());
/// Mirror of `GLOBAL.len()` so stats never take the lock.
static GLOBAL_FREE: AtomicUsize = AtomicUsize::new(0);
/// High-water of `GLOBAL.len()` since process start: the swing the cap has to
/// cover, measured instead of estimated.
static PEAK_FREE: AtomicUsize = AtomicUsize::new(0);

static TAKES: AtomicU64 = AtomicU64::new(0);
static FRESH: AtomicU64 = AtomicU64::new(0);
static RELEASES: AtomicU64 = AtomicU64::new(0);
static OVERFLOW: AtomicU64 = AtomicU64::new(0);
static BYPASS: AtomicU64 = AtomicU64::new(0);

/// Process-wide pool counters. `takes - fresh` were served from the pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PagePoolStats {
    pub enabled: bool,
    /// Buffer requests while the pool was on.
    pub takes: u64,
    /// Of `takes`, the ones the pool could not serve (fresh heap allocation).
    pub fresh: u64,
    /// Buffers handed back while the pool was on.
    pub releases: u64,
    /// Of `releases`, the ones freed to the heap because the global stack was
    /// at its cap.
    pub overflow: u64,
    /// Buffer requests while the pool was off (every one a heap allocation).
    pub bypass: u64,
    /// Buffers in the global stack right now (thread-local caches excluded).
    pub global_free_pages: u64,
    /// Most buffers the global stack has held at once since process start.
    pub peak_free_pages: u64,
    pub max_free_pages: u64,
}

/// Whether `Page` buffers are recycled **right now**.
pub fn page_pool_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Flip the pool on a running engine (onyx IPC `metadb-page-pool`). Turning it
/// off gives the global stack back to the heap immediately; each thread's cache
/// follows on that thread's next page operation.
pub fn set_page_pool_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
    if !enabled {
        trim_global(0);
    }
}

/// Cap the global stack at `bytes` (rounded down to whole pages). Lowering the
/// cap gives the excess back to the heap at once.
pub fn set_page_pool_max_free_bytes(bytes: usize) {
    let pages = bytes / PAGE_SIZE;
    MAX_FREE_PAGES.store(pages, Ordering::Relaxed);
    trim_global(pages);
}

pub fn page_pool_stats() -> PagePoolStats {
    PagePoolStats {
        enabled: page_pool_enabled(),
        takes: TAKES.load(Ordering::Relaxed),
        fresh: FRESH.load(Ordering::Relaxed),
        releases: RELEASES.load(Ordering::Relaxed),
        overflow: OVERFLOW.load(Ordering::Relaxed),
        bypass: BYPASS.load(Ordering::Relaxed),
        global_free_pages: GLOBAL_FREE.load(Ordering::Relaxed) as u64,
        peak_free_pages: PEAK_FREE.load(Ordering::Relaxed) as u64,
        max_free_pages: MAX_FREE_PAGES.load(Ordering::Relaxed) as u64,
    }
}

fn trim_global(keep: usize) {
    let excess: Vec<PageBytes> = {
        let mut global = GLOBAL.lock();
        if global.len() <= keep {
            return;
        }
        let excess = global.split_off(keep);
        GLOBAL_FREE.store(global.len(), Ordering::Relaxed);
        excess
    };
    // Freed outside the lock.
    drop(excess);
}

#[derive(Default)]
struct Tally {
    takes: u64,
    fresh: u64,
    releases: u64,
    overflow: u64,
    bypass: u64,
    ops: u64,
}

impl Tally {
    fn fold(&mut self) {
        for (local, global) in [
            (&mut self.takes, &TAKES),
            (&mut self.fresh, &FRESH),
            (&mut self.releases, &RELEASES),
            (&mut self.overflow, &OVERFLOW),
            (&mut self.bypass, &BYPASS),
        ] {
            if *local != 0 {
                global.fetch_add(*local, Ordering::Relaxed);
                *local = 0;
            }
        }
        self.ops = 0;
    }

    fn tick(&mut self) {
        self.ops += 1;
        if self.ops >= TALLY_FOLD_OPS {
            self.fold();
        }
    }
}

struct Local {
    free: Vec<PageBytes>,
    tally: Tally,
}

impl Drop for Local {
    /// Thread exit: hand the cache to the global stack (up to the cap) and
    /// publish the last tallies.
    fn drop(&mut self) {
        if !self.free.is_empty() && page_pool_enabled() {
            let mut global = GLOBAL.lock();
            let room = MAX_FREE_PAGES
                .load(Ordering::Relaxed)
                .saturating_sub(global.len());
            let keep = self.free.len().min(room);
            let start = self.free.len() - keep;
            global.extend(self.free.drain(start..));
            GLOBAL_FREE.store(global.len(), Ordering::Relaxed);
            PEAK_FREE.fetch_max(global.len(), Ordering::Relaxed);
        }
        self.tally.fold();
    }
}

thread_local! {
    static LOCAL: RefCell<Local> = RefCell::new(Local {
        free: Vec::with_capacity(LOCAL_MAX),
        tally: Tally::default(),
    });
}

/// A recycled buffer with unspecified contents, or `None` when the caller must
/// allocate one itself (pool off, or nothing cached).
pub(super) fn take() -> Option<PageBytes> {
    let enabled = page_pool_enabled();
    LOCAL
        .try_with(|cell| {
            let mut local = cell.borrow_mut();
            if !enabled {
                // Off: give this thread's cache back and count the bypass.
                local.free.clear();
                local.tally.bypass += 1;
                local.tally.tick();
                return None;
            }
            local.tally.takes += 1;
            if let Some(buf) = local.free.pop() {
                local.tally.tick();
                return Some(buf);
            }
            {
                let mut global = GLOBAL.lock();
                let n = global.len().min(BATCH);
                let start = global.len() - n;
                local.free.extend(global.drain(start..));
                GLOBAL_FREE.store(global.len(), Ordering::Relaxed);
            }
            let buf = local.free.pop();
            if buf.is_none() {
                local.tally.fresh += 1;
            }
            local.tally.fold();
            buf
        })
        // Thread-local storage already torn down (a page built during thread
        // exit): let the caller allocate.
        .unwrap_or(None)
}

/// Hand a buffer back. Kept for reuse when the pool is on and has room;
/// freed otherwise.
pub(super) fn release(buf: PageBytes) {
    if !page_pool_enabled() {
        drop(buf);
        return;
    }
    let mut slot = Some(buf);
    let _ = LOCAL.try_with(|cell| {
        let mut local = cell.borrow_mut();
        local.tally.releases += 1;
        if local.free.len() < LOCAL_MAX {
            local.free.push(slot.take().expect("released buffer"));
            local.tally.tick();
            return;
        }
        {
            let mut global = GLOBAL.lock();
            let room = MAX_FREE_PAGES
                .load(Ordering::Relaxed)
                .saturating_sub(global.len());
            let n = BATCH.min(room);
            let start = local.free.len() - n;
            global.extend(local.free.drain(start..));
            GLOBAL_FREE.store(global.len(), Ordering::Relaxed);
            PEAK_FREE.fetch_max(global.len(), Ordering::Relaxed);
        }
        if local.free.len() < LOCAL_MAX {
            local.free.push(slot.take().expect("released buffer"));
        } else {
            local.tally.overflow += 1;
        }
        local.tally.fold();
    });
    // Either the global stack was full or thread-local storage is gone: the
    // buffer goes back to the heap.
    drop(slot);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    // The pool is process-global. These tests serialize among themselves and
    // assert only what concurrent page traffic from other tests cannot break:
    // thread-local results, caps, and monotonic lower bounds. Where a test needs
    // the global stack empty, it sets the cap to 0, which nobody can push past.
    static SERIAL: StdMutex<()> = StdMutex::new(());

    struct Restore {
        enabled: bool,
        max_free_pages: usize,
    }

    impl Drop for Restore {
        fn drop(&mut self) {
            set_page_pool_enabled(self.enabled);
            set_page_pool_max_free_bytes(self.max_free_pages * PAGE_SIZE);
        }
    }

    fn isolate() -> (std::sync::MutexGuard<'static, ()>, Restore) {
        let serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let restore = Restore {
            enabled: page_pool_enabled(),
            max_free_pages: MAX_FREE_PAGES.load(Ordering::Relaxed),
        };
        (serial, restore)
    }

    fn local_len() -> usize {
        LOCAL.with(|cell| cell.borrow().free.len())
    }

    fn empty_this_thread() {
        set_page_pool_enabled(false);
        assert!(take().is_none()); // off: drops this thread's cache
        set_page_pool_enabled(true);
        assert_eq!(local_len(), 0);
    }

    #[test]
    fn a_released_buffer_is_handed_out_again() {
        let _guard = isolate();
        empty_this_thread();
        let buf: PageBytes = Box::new([7u8; PAGE_SIZE]);
        let addr = &*buf as *const _ as usize;
        release(buf);
        let again = take().expect("the released buffer is cached");
        assert_eq!(&*again as *const _ as usize, addr, "the same allocation comes back");
        // Contents are stale by contract; the constructors overwrite them.
        assert_eq!(again[0], 7);
    }

    #[test]
    fn a_full_thread_cache_spills_to_the_global_stack_up_to_the_cap() {
        let _guard = isolate();
        empty_this_thread();
        set_page_pool_max_free_bytes(BATCH * PAGE_SIZE);
        let overflow_before = page_pool_stats().overflow;

        for _ in 0..LOCAL_MAX + 3 * BATCH {
            release(Box::new([0u8; PAGE_SIZE]));
            assert!(local_len() <= LOCAL_MAX);
        }
        let stats = page_pool_stats();
        assert!(stats.global_free_pages <= BATCH as u64, "the global stack stops at its cap");
        assert!(stats.overflow > overflow_before, "past the cap, releases go to the heap");
    }

    #[test]
    fn an_empty_pool_counts_a_fresh_allocation_and_off_counts_a_bypass() {
        let _guard = isolate();
        set_page_pool_max_free_bytes(0);
        empty_this_thread();
        let before = page_pool_stats();
        assert!(take().is_none(), "nothing cached anywhere under a zero cap");
        set_page_pool_enabled(false);
        assert!(take().is_none());
        release(Box::new([0u8; PAGE_SIZE]));
        assert_eq!(local_len(), 0, "off, a release is a plain free");
        LOCAL.with(|cell| cell.borrow_mut().tally.fold());
        let after = page_pool_stats();
        assert!(after.fresh > before.fresh);
        assert!(after.bypass > before.bypass);
        assert!(!after.enabled);
    }

    #[test]
    fn a_thread_exit_hands_its_cache_to_the_global_stack() {
        let _guard = isolate();
        set_page_pool_enabled(false);
        set_page_pool_enabled(true);
        let released: Vec<usize> = std::thread::spawn(|| {
            (0..8)
                .map(|_| {
                    let buf: PageBytes = Box::new([0u8; PAGE_SIZE]);
                    let addr = &*buf as *const _ as usize;
                    release(buf);
                    addr
                })
                .collect()
        })
        .join()
        .unwrap();
        assert!(page_pool_stats().global_free_pages >= 8);
        // Another thread takes them from the global stack (LIFO, so the exited
        // thread's buffers are on top unless other traffic intervened).
        let taken: Vec<usize> = std::thread::spawn(|| {
            (0..8)
                .filter_map(|_| take().map(|buf| &*buf as *const _ as usize))
                .collect()
        })
        .join()
        .unwrap();
        assert_eq!(taken.len(), 8);
        assert!(taken.iter().any(|addr| released.contains(addr)));
    }
}
