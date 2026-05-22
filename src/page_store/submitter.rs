use std::fs::File;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

use crate::io_submitter::IoSubmitter;

/// Default pool size. Pool>1 requires
/// [`AffinityConfig::io_submitter_cpus`] to pin each submitter to a
/// distinct CPU — without pinning, multiple submitter threads can
/// land on the same core and net-regress vs pool=1 (j8 d4 with
/// pool=3 unbound: 32k LBA/s vs 59k for pool=1). Once a CPU set is
/// available, callers can flip this up to one submitter per
/// [`IoLaneClass`] (currently three) for per-class SQ isolation;
/// SQ=16384 per ring stays the throughput knob.
pub const DEFAULT_IO_SUBMITTER_POOL_SIZE: usize = 1;

/// Per-IO routing class. The pool keeps one [`IoSubmitter`] per
/// variant so independent write streams cannot saturate one another's
/// SQ. The hot lane mapping:
///
/// * `L2p` — paged radix tree writes from `PagedL2p` (single most
///   numerous foreground writer; gates commit ack).
/// * `Refcount` — `paged_meta` writes from refcount delta apply and
///   the legacy dedup_reverse path (lower volume, also commit-sync).
/// * `Dedup` — cuckoo page writes from the dedup_index apply lane.
///   These run async w.r.t. commit ack, so bursts here must not stall
///   L2p / Refcount writes that commit threads still wait on.
///
/// Variant ordinals must be `0..N` and contiguous so the
/// [`super::PageStore::io_submitter_for_class`] lookup is a slice index. Add
/// new variants only when the pool size grows in lockstep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoLaneClass {
    L2p = 0,
    Refcount = 1,
    Dedup = 2,
}

impl IoLaneClass {
    pub(super) fn index(self) -> usize {
        self as usize
    }
}

pub(super) fn make_io_submitters(
    file: &File,
    count: usize,
    bg_inflight_cap: usize,
) -> Box<[IoSubmitter]> {
    #[cfg(target_os = "linux")]
    {
        let count = count.max(1);
        let mut subs: Vec<IoSubmitter> = Vec::with_capacity(count);
        for ordinal in 0..count {
            match IoSubmitter::start_with_ordinal(file.as_raw_fd(), ordinal, bg_inflight_cap) {
                Some(sub) => subs.push(sub),
                None => {
                    // First submitter failed — io_uring unavailable.
                    // Bail out empty so callers fall back to pwrite.
                    return Box::new([]);
                }
            }
        }
        subs.into_boxed_slice()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        let _ = count;
        let _ = bg_inflight_cap;
        Box::new([])
    }
}
