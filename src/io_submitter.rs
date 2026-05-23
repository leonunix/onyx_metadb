//! Centralised io_uring submitter for the page-store backing file.
//!
//! Why this exists
//! ===============
//!
//! Before this module the page store had three disjoint write paths:
//! - Single-page `write_page` → direct `pwrite` (no io_uring)
//! - `write_sealed_page_runs` / `write_page_runs_parallel` → one shared
//!   `Mutex<IoUring>`, `submit_and_wait(N)` per call, capped at
//!   `MAX_SEALED_WRITE_URING_RUNS = 16` SQEs per chunk
//! - `sync` → same shared mutex, single fsync SQE
//!
//! Empirical result on nvme-box (2026-05-08): 99.995% of metadb page
//! writes were going through the `pwrite` fallback because the lion's
//! share of small `write_page` traffic (cuckoo bucket updates,
//! `paged_meta::write_chain`, refcount staged pages) never hit the uring
//! path at all. NVMe utilisation peaked at ~1% / 92 MB/s while the disk
//! could sustain ~7 GB/s.
//!
//! This module funnels every page-file write into a single dedicated
//! kernel ring with deep SQ capacity. Producers stay synchronous from
//! the caller's perspective: `write_page` returns once the kernel has
//! acknowledged the write (CQE arrived). Behind that synchronous facade
//! the submitter batches arbitrary numbers of producers into a single
//! `submit()` per kernel transition.
//!
//! Invariants the design preserves
//! ===============================
//!
//! 1. **Durability after `Ok`** — when `submit_write` / `submit_fsync`
//!    return `Ok`, the corresponding op's CQE has already been
//!    harvested. Subsequent `fsync` (whether through this submitter or
//!    a direct `file.sync_data()` syscall) will durably persist the
//!    bytes, because the kernel was the entity reporting completion.
//!
//! 2. **Order per page** — the submitter does not reorder anything on
//!    the producer side; ordering of writes to the same page is the
//!    caller's responsibility (typically a per-page-shard mutex held
//!    around `write_page` plus a page-cache `replace_or_insert`).
//!
//! 3. **Failure isolation** — a single CQE with `result < 0` only fails
//!    its own op's reply. The submitter loop continues. Loss of the
//!    submitter thread (panic) drops the receiver, which causes
//!    every subsequent `Sender::send` and every blocked `recv()` on a
//!    reply oneshot to surface as `Err`.
//!
//! 4. **Bounded backpressure** — the channel is bounded; producers
//!    block on `send` once the submitter cannot drain fast enough.
//!    No unbounded queue accumulating dirty pages in user space.

#[cfg(target_os = "linux")]
use std::os::unix::io::RawFd;
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, bounded, select_biased};
use parking_lot::Mutex;

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::metrics::MetaMetrics;
use crate::page::Page;
use crate::types::PageId;

/// Submission-queue depth for the centralised ring. Sized for the
/// async-dedup + foreground-write IO mix: cuckoo page bursts run
/// concurrently with L2P / refcount writes on the same ring, so the
/// SQ has to hold a deep enough window to keep producers from
/// blocking on channel-full back-pressure. Sweep on the nvme-box j8
/// d4 workload showed throughput peaking at 16384 (≈ +10% over 8192,
/// +145% over the original 1024 ceiling) before SQ=32768 regressed —
/// once the per-iteration submit batch grows past what NVMe can
/// drain in one window, kernel kworkers backlog and the ring goes
/// idle for long stretches.
#[cfg(target_os = "linux")]
const SQ_ENTRIES: u32 = 16384;

/// Channel capacity. Sized to twice the SQ so a producer that arrives
/// while the submitter is mid-batch does not immediately block — the
/// submitter will drain channel + ring on the next iteration.
#[cfg(target_os = "linux")]
const CHANNEL_CAPACITY: usize = SQ_ENTRIES as usize * 2;

/// Default cap on background-priority ops in flight at any time.
///
/// Sync-priority ops (commit writes / fsync) are always admitted up to
/// SQ capacity; background ops (streaming L2P writeback) wait in a
/// deferred queue once `inflight_bg` reaches this cap. Keeping the cap
/// well below `SQ_ENTRIES` guarantees there is always headroom for the
/// sync path even under sustained background pressure.
///
/// 1024 is ~6 % of SQ (16384). Tunable via
/// [`crate::config::Config::io_submitter_bg_inflight_cap`]. 0 disables
/// the cap (background ops admit freely; matches pre-Tier-1.C behaviour
/// when paired with Sync routing for both classes).
#[cfg(target_os = "linux")]
pub(crate) const DEFAULT_BG_INFLIGHT_CAP: usize = 1024;

/// Priority class for an IO op submitted to the centralised submitter.
/// `Sync` ops are caller-blocking (commit-path writes, fsync); `Background`
/// ops are off-critical-path (L2P streaming writeback). Background ops
/// admit through a deferred queue gated by `inflight_bg < bg_cap`, so a
/// sustained writeback burst can never starve a commit write of an SQE
/// slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoPriority {
    Sync,
    Background,
}

/// One pending write, run, or fsync. Constructed by producers,
/// consumed by the single submitter thread. Sync vs Background routing
/// is tracked outside this enum (each priority has its own channel),
/// but the variants are stored uniformly in the submitter's deferred
/// queue when a Background op has to wait for an `inflight_bg` slot.
#[cfg(target_os = "linux")]
enum IoOp {
    Write {
        page_id: PageId,
        page: Arc<Page>,
        reply: Sender<Result<()>>,
    },
    /// Contiguous run of pages issued as a single `IORING_OP_WRITEV`
    /// SQE. Replaces the old "16 SQEs per submit_and_wait" cap on the
    /// shared write_uring path: each run is one SQE here, and the
    /// submitter batches arbitrarily many runs per ring transition.
    WriteRun {
        start_page: PageId,
        pages: Vec<Arc<Page>>,
        reply: Sender<Result<()>>,
    },
    Fsync {
        reply: Sender<Result<()>>,
    },
    Shutdown,
}

#[cfg(target_os = "linux")]
struct InflightOp {
    kind: InflightKind,
    reply: Sender<Result<()>>,
    /// Keep page bytes alive until the kernel has copied them.
    /// `IORING_OP_WRITE` / `_WRITEV` only borrow the buffers; the
    /// `Arc<Page>` references here are what stop them from being
    /// dropped before the CQE arrives. For Write this holds 1 entry,
    /// for WriteRun N entries, for Fsync none.
    _pages: Vec<Arc<Page>>,
    /// For Writev SQEs only: the iovec array that the SQE points at.
    /// Rust `Vec<T>` heap allocations are stable across `Vec` moves,
    /// so the pointer captured at SQE-build time stays valid as long
    /// as this vec lives in the InflightOp.
    _iovecs: Vec<libc::iovec>,
    /// Submitted with [`IoPriority::Background`]. Drives the
    /// `inflight_bg` decrement when this op's CQE arrives.
    is_bg: bool,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug)]
enum InflightKind {
    Write { expected_len: usize },
    WriteRun { expected_len: usize },
    Fsync,
}

/// Owned handle to the submitter. Drop joins the thread.
#[cfg(target_os = "linux")]
pub(crate) struct IoSubmitter {
    /// Producer channel for caller-blocking ops (commit-path writes,
    /// fsync). Always admitted to the SQ up to capacity.
    sender: Option<Sender<IoOp>>,
    /// Producer channel for off-critical-path ops (L2P streaming
    /// writeback). Gated by `inflight_bg < bg_cap` inside the submitter
    /// loop, with overflow held in a deferred queue.
    bg_sender: Option<Sender<IoOp>>,
    join: Mutex<Option<JoinHandle<()>>>,
    /// Shared metrics slot. PageStore attaches its `Arc<MetaMetrics>`
    /// after construction (Db::open populates the page store first, then
    /// hands the metrics handle in). The submitter thread captures a
    /// clone of this Arc at start time; once `set` it stays set.
    metrics: Arc<OnceLock<Arc<MetaMetrics>>>,
}

/// Stub used on platforms without io_uring. All entry points return
/// "unsupported" so callers fall back to `pwrite` / `fsync` syscalls.
#[cfg(not(target_os = "linux"))]
pub(crate) struct IoSubmitter;

#[cfg(target_os = "linux")]
impl IoSubmitter {
    /// Try to start a submitter against `fd`. Returns `None` if io_uring
    /// is unavailable (old kernel, sandboxed environment) so callers can
    /// fall back to the pwrite path.
    ///
    /// `fd` must outlive every op submitted through this submitter; in
    /// practice the caller is `PageStore`, which owns the `File`.
    pub(crate) fn start(fd: RawFd) -> Option<Self> {
        Self::start_with_ordinal(fd, 0, DEFAULT_BG_INFLIGHT_CAP)
    }

    /// Same as [`Self::start`] but pins the submitter thread to the
    /// `ordinal`-th CPU in the configured `io_submitter_cpus` set. With
    /// pool>1 each ordinal MUST be distinct so the kernel mq-block
    /// layer routes each submitter's IO to a different NVMe hardware
    /// queue. With pool=1 the ordinal is 0 (unbound if no config).
    ///
    /// `bg_inflight_cap` bounds the number of [`IoPriority::Background`]
    /// ops in flight. Background ops over the cap wait in a deferred
    /// queue inside the submitter and admit as in-flight bg slots
    /// retire. `0` admits background ops freely (no cap) — useful for
    /// regression-testing against pre-Tier-1.C behaviour.
    pub(crate) fn start_with_ordinal(
        fd: RawFd,
        ordinal: usize,
        bg_inflight_cap: usize,
    ) -> Option<Self> {
        let ring = match IoUring::new(SQ_ENTRIES) {
            Ok(ring) => ring,
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    "io_submitter unavailable; page writes will use pwrite fallback"
                );
                return None;
            }
        };
        let (sender, receiver) = bounded(CHANNEL_CAPACITY);
        let (bg_sender, bg_receiver) = bounded(CHANNEL_CAPACITY);
        let metrics: Arc<OnceLock<Arc<MetaMetrics>>> = Arc::new(OnceLock::new());
        let metrics_for_thread = Arc::clone(&metrics);
        let join = std::thread::Builder::new()
            .name(format!("metadb-io-submitter-{ordinal}"))
            .spawn(move || {
                crate::affinity::bind_current(crate::affinity::ThreadRole::IoSubmitter, ordinal);
                submitter_loop(
                    fd,
                    ring,
                    receiver,
                    bg_receiver,
                    bg_inflight_cap,
                    metrics_for_thread,
                )
            })
            .ok()?;
        Some(Self {
            sender: Some(sender),
            bg_sender: Some(bg_sender),
            join: Mutex::new(Some(join)),
            metrics,
        })
    }

    /// Attach the page store's metrics handle. Idempotent — extra calls
    /// after the first are silently dropped (OnceLock semantics).
    pub(crate) fn attach_metrics(&self, metrics: Arc<MetaMetrics>) {
        let _ = self.metrics.set(metrics);
    }

    /// Submit a page write and block until the kernel acknowledges it
    /// (CQE harvested). On Ok return, a subsequent fsync on the same
    /// fd is guaranteed to persist this page.
    pub(crate) fn submit_write(&self, page_id: PageId, page: Arc<Page>) -> Result<()> {
        self.submit_write_async(page_id, page)?
            .recv()
            .map_err(|_| submitter_dead())?
    }

    /// Submit a contiguous run of pages as a single `IORING_OP_WRITEV`
    /// SQE. The submitter batches arbitrarily many concurrent runs per
    /// ring transition (the old shared-uring path capped at 16
    /// `submit_and_wait` SQEs per chunk).
    pub(crate) fn submit_write_run(&self, start_page: PageId, pages: Vec<Arc<Page>>) -> Result<()> {
        self.submit_write_run_async(start_page, pages)?
            .recv()
            .map_err(|_| submitter_dead())?
    }

    /// Issue an `IORING_OP_FSYNC` with `IORING_FSYNC_DATASYNC`. Returns
    /// once the CQE has been harvested.
    pub(crate) fn submit_fsync(&self) -> Result<()> {
        self.submit_fsync_async()?
            .recv()
            .map_err(|_| submitter_dead())?
    }

    /// Async variant of [`Self::submit_write`]. Returns the reply
    /// receiver immediately so callers can fan out many writes and
    /// then collect replies in any order. Bounded channel backpressure
    /// still applies: send blocks if the submitter cannot keep up.
    pub(crate) fn submit_write_async(
        &self,
        page_id: PageId,
        page: Arc<Page>,
    ) -> Result<crossbeam_channel::Receiver<Result<()>>> {
        self.submit_write_async_with_priority(page_id, page, IoPriority::Sync)
    }

    /// Submit a single-page write at the given priority. Background
    /// writes admit through the deferred queue once `inflight_bg`
    /// reaches the configured cap, so they cannot displace Sync ops
    /// from the SQ.
    pub(crate) fn submit_write_async_with_priority(
        &self,
        page_id: PageId,
        page: Arc<Page>,
        priority: IoPriority,
    ) -> Result<crossbeam_channel::Receiver<Result<()>>> {
        let sender = self.sender_for(priority).ok_or_else(submitter_dead)?;
        let (reply_tx, reply_rx) = bounded(1);
        sender
            .send(IoOp::Write {
                page_id,
                page,
                reply: reply_tx,
            })
            .map_err(|_| submitter_dead())?;
        Ok(reply_rx)
    }

    /// Async variant of [`Self::submit_write_run`]. Used by
    /// `write_sealed_page_runs` to dispatch every coalesced run as one
    /// `Writev` SQE without serialising on a per-run reply.
    pub(crate) fn submit_write_run_async(
        &self,
        start_page: PageId,
        pages: Vec<Arc<Page>>,
    ) -> Result<crossbeam_channel::Receiver<Result<()>>> {
        self.submit_write_run_async_with_priority(start_page, pages, IoPriority::Sync)
    }

    /// Submit a contiguous run of pages at the given priority.
    /// Background runs queue behind sync ops; once admitted they execute
    /// the same single-`Writev` SQE as sync runs.
    pub(crate) fn submit_write_run_async_with_priority(
        &self,
        start_page: PageId,
        pages: Vec<Arc<Page>>,
        priority: IoPriority,
    ) -> Result<crossbeam_channel::Receiver<Result<()>>> {
        let sender = self.sender_for(priority).ok_or_else(submitter_dead)?;
        let (reply_tx, reply_rx) = bounded(1);
        sender
            .send(IoOp::WriteRun {
                start_page,
                pages,
                reply: reply_tx,
            })
            .map_err(|_| submitter_dead())?;
        Ok(reply_rx)
    }

    fn sender_for(&self, priority: IoPriority) -> Option<&Sender<IoOp>> {
        match priority {
            IoPriority::Sync => self.sender.as_ref(),
            IoPriority::Background => self.bg_sender.as_ref(),
        }
    }

    /// Async variant of [`Self::submit_fsync`].
    pub(crate) fn submit_fsync_async(&self) -> Result<crossbeam_channel::Receiver<Result<()>>> {
        let sender = self.sender.as_ref().ok_or_else(submitter_dead)?;
        let (reply_tx, reply_rx) = bounded(1);
        sender
            .send(IoOp::Fsync { reply: reply_tx })
            .map_err(|_| submitter_dead())?;
        Ok(reply_rx)
    }

    /// Tear down: signal the submitter, wait for it to drain in-flight
    /// CQEs, then join. Idempotent — extra calls are no-ops.
    fn shutdown(&mut self) {
        if let Some(sender) = self.sender.take() {
            // Best-effort: if the submitter already exited, the channel
            // is closed and `send` returns Err — that's fine.
            let _ = sender.send(IoOp::Shutdown);
            drop(sender);
        }
        // Drop the bg sender too so the submitter's `bg_rx` becomes
        // disconnected once it has drained any in-flight bg work.
        if let Some(bg_sender) = self.bg_sender.take() {
            drop(bg_sender);
        }
        if let Some(handle) = self.join.lock().take() {
            let _ = handle.join();
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for IoSubmitter {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(not(target_os = "linux"))]
impl IoSubmitter {
    pub(crate) fn start(_fd: i32) -> Option<Self> {
        None
    }

    pub(crate) fn start_with_ordinal(
        _fd: i32,
        _ordinal: usize,
        _bg_inflight_cap: usize,
    ) -> Option<Self> {
        None
    }

    pub(crate) fn submit_write(&self, _page_id: PageId, _page: Arc<Page>) -> Result<()> {
        Err(submitter_dead())
    }

    pub(crate) fn submit_write_run(
        &self,
        _start_page: PageId,
        _pages: Vec<Arc<Page>>,
    ) -> Result<()> {
        Err(submitter_dead())
    }

    pub(crate) fn submit_fsync(&self) -> Result<()> {
        Err(submitter_dead())
    }

    pub(crate) fn attach_metrics(&self, _metrics: Arc<MetaMetrics>) {}
}

fn submitter_dead() -> MetaDbError {
    MetaDbError::Io(std::io::Error::other("metadb io submitter unavailable"))
}

#[cfg(target_os = "linux")]
fn submitter_loop(
    fd: RawFd,
    mut ring: IoUring,
    rx: Receiver<IoOp>,
    bg_rx: Receiver<IoOp>,
    bg_inflight_cap: usize,
    metrics: Arc<OnceLock<Arc<MetaMetrics>>>,
) {
    use std::collections::{HashMap, VecDeque};
    use std::io;

    // user_data → in-flight op state. user_data is a monotonic counter
    // so collisions are impossible until ~2^64 ops (centuries at any
    // realistic rate).
    let mut inflight: HashMap<u64, InflightOp> = HashMap::with_capacity(SQ_ENTRIES as usize);
    let mut next_uid: u64 = 1;
    let mut shutdown_requested = false;
    let sq_capacity = SQ_ENTRIES as usize;

    // Background ops over the inflight cap wait here. The deferred
    // queue is drained back into the SQ at the top of every loop
    // iteration as bg slots retire (see Phase 0 below). Bounded by
    // CHANNEL_CAPACITY in practice: senders block on `bg_rx` once that
    // many bg ops are outstanding (channel + deferred).
    let mut deferred_bg: VecDeque<IoOp> = VecDeque::new();
    let mut inflight_bg: usize = 0;
    // `0` disables the cap (admit bg ops freely). Otherwise the bg
    // admission ceiling.
    let bg_uncapped = bg_inflight_cap == 0;

    'outer: loop {
        // -- Phase 0: drain the deferred-bg queue if there's room -----
        //
        // This runs FIRST so a bg op that arrived while bg-inflight was
        // saturated cannot be starved indefinitely by a busy sync
        // producer. Each iteration we promote as many deferred bg ops
        // into the SQ as the cap and SQ capacity allow.

        while !deferred_bg.is_empty()
            && inflight.len() < sq_capacity
            && (bg_uncapped || inflight_bg < bg_inflight_cap)
        {
            let op = deferred_bg.pop_front().expect("non-empty checked above");
            let admitted = handle_op(
                op,
                &mut ring,
                &mut inflight,
                &mut next_uid,
                fd,
                &mut shutdown_requested,
                /*is_bg=*/ true,
            );
            if admitted {
                inflight_bg += 1;
            }
        }

        // -- Phase 1: pull ops from channels into the SQ --------------
        //
        // If nothing is in flight we block on the first op so the
        // submitter doesn't busy-spin. Sync ops always admit straight
        // to the SQ. Bg ops admit only while `inflight_bg < cap`;
        // overflow is parked in `deferred_bg` for the next iteration.

        let want = sq_capacity.saturating_sub(inflight.len());
        let mut pulled_this_round = 0usize;
        // Snapshot channel depth before draining so the metric reflects
        // producer queueing pressure, not the residual after the pull.
        let channel_pending_at_loop_top = rx.len();

        if inflight.is_empty() && deferred_bg.is_empty() && !shutdown_requested {
            // Both inflight and deferred-bg are empty: there's nothing
            // to wait on. Block until either channel has an op, biased
            // toward sync so sync producers cannot be starved by a
            // long-running bg producer that holds the bg_rx sender.
            let op_and_class = select_biased! {
                recv(rx) -> op => op.map(|o| (o, false)),
                recv(bg_rx) -> op => op.map(|o| (o, true)),
            };
            match op_and_class {
                Ok((op, is_bg)) => {
                    let admitted = handle_op(
                        op,
                        &mut ring,
                        &mut inflight,
                        &mut next_uid,
                        fd,
                        &mut shutdown_requested,
                        is_bg,
                    );
                    if admitted && is_bg {
                        inflight_bg += 1;
                    }
                    pulled_this_round += 1;
                }
                Err(_) => {
                    // All senders dropped. Treat as shutdown.
                    break 'outer;
                }
            }
        }

        // Drain sync first so a saturated sync producer always wins
        // when both channels have work.
        while pulled_this_round < want && !shutdown_requested {
            match rx.try_recv() {
                Ok(op) => {
                    handle_op(
                        op,
                        &mut ring,
                        &mut inflight,
                        &mut next_uid,
                        fd,
                        &mut shutdown_requested,
                        /*is_bg=*/ false,
                    );
                    pulled_this_round += 1;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    shutdown_requested = true;
                    break;
                }
            }
        }

        // Drain bg up to the remaining SQ budget AND the bg inflight
        // cap. Bg ops over the cap go to `deferred_bg` so we never
        // block the loop waiting for cap headroom.
        while pulled_this_round < want && !shutdown_requested {
            match bg_rx.try_recv() {
                Ok(op) => {
                    if bg_uncapped || inflight_bg < bg_inflight_cap {
                        let admitted = handle_op(
                            op,
                            &mut ring,
                            &mut inflight,
                            &mut next_uid,
                            fd,
                            &mut shutdown_requested,
                            /*is_bg=*/ true,
                        );
                        if admitted {
                            inflight_bg += 1;
                        }
                        pulled_this_round += 1;
                    } else {
                        deferred_bg.push_back(op);
                        if let Some(m) = metrics.get() {
                            m.record_io_submitter_bg_deferred();
                        }
                        // Don't increment pulled_this_round: the op
                        // didn't actually reach the SQ. Continue
                        // pulling so we don't leave bg work in the
                        // channel.
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // Bg sender disconnected. Continue running so any
                    // remaining sync ops finish; sync disconnect will
                    // shut the loop down below.
                    break;
                }
            }
        }

        // -- Phase 2: submit and (if anything is in flight) wait -------

        if inflight.is_empty() {
            // Either we got nothing but a Shutdown sentinel, or all
            // pulled ops were Shutdown. Either way, exit cleanly.
            if shutdown_requested && deferred_bg.is_empty() {
                break 'outer;
            }
            // Defensive: should not normally hit this — recv guarantees
            // at least one op when not shut down. Loop back.
            continue;
        }

        // submit_and_wait(1) flushes the SQ to the kernel and blocks
        // until at least one CQE arrives. With deep SQ + many
        // producers this is the single point that batches ops into one
        // ring transition.
        if let Some(m) = metrics.get() {
            m.record_io_submitter_iteration(
                pulled_this_round,
                inflight.len(),
                channel_pending_at_loop_top,
            );
            m.record_io_submitter_bg_inflight(inflight_bg, deferred_bg.len());
        }
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(err) => {
                // Submission itself failed. Surface to whichever ops
                // we've registered as in-flight by failing them all,
                // then continue. (This typically means a permanent
                // kernel failure, but we don't poison the submitter —
                // the next op will retry.)
                inflight_bg = drain_inflight_bg(&inflight, &mut inflight_bg, &|slot| {
                    let _ = slot.reply.send(Err(MetaDbError::Io(io::Error::new(
                        err.kind(),
                        format!("io_uring submit failed: {err}"),
                    ))));
                });
                continue;
            }
        }

        // -- Phase 3: harvest CQEs ------------------------------------

        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let uid = cqe.user_data();
            let result = cqe.result();
            let Some(slot) = inflight.remove(&uid) else {
                tracing::error!(
                    user_data = uid,
                    result,
                    "io_submitter: CQE for unknown user_data; submitter state corrupted"
                );
                continue;
            };
            if slot.is_bg {
                inflight_bg = inflight_bg.saturating_sub(1);
            }
            let outcome = decode_cqe_result(slot.kind, result);
            // Producer may have already disconnected (e.g. dropped Arc
            // before recv). Best-effort send.
            let _ = slot.reply.send(outcome);
        }

        if shutdown_requested && inflight.is_empty() && deferred_bg.is_empty() {
            break 'outer;
        }
    }

    // Final drain: any in-flight ops we still own had their CQEs lost
    // (or were never submitted). Fail them so producers stop blocking.
    fail_all_inflight(&mut inflight, || submitter_dead());
    // Also fail any deferred-bg ops that never reached the SQ.
    for op in deferred_bg.drain(..) {
        match op {
            IoOp::Write { reply, .. } | IoOp::WriteRun { reply, .. } | IoOp::Fsync { reply } => {
                let _ = reply.send(Err(submitter_dead()));
            }
            IoOp::Shutdown => {}
        }
    }
    // Drain any straggler ops left in either channel after shutdown.
    for chan in [&rx, &bg_rx] {
        while let Ok(op) = chan.try_recv() {
            match op {
                IoOp::Write { reply, .. }
                | IoOp::WriteRun { reply, .. }
                | IoOp::Fsync { reply } => {
                    let _ = reply.send(Err(submitter_dead()));
                }
                IoOp::Shutdown => {}
            }
        }
    }
}

/// Iterate the in-flight map and fail every bg slot with `report`.
/// Currently used by the submit-failure recovery path; sync slots stay
/// inflight (we will retry; the SQE was never submitted).
///
/// Note: in practice submit-failure is rare enough that the existing
/// `fail_all_inflight` is preferred; this is the bg-aware analogue we
/// would need for partial-failure handling if we ever distinguish
/// transient from terminal kernel errors.
#[cfg(target_os = "linux")]
fn drain_inflight_bg<F>(
    inflight: &std::collections::HashMap<u64, InflightOp>,
    inflight_bg: &mut usize,
    _report: &F,
) -> usize
where
    F: Fn(&InflightOp),
{
    // We don't actually iterate `inflight` here on the failure path —
    // the caller invokes `fail_all_inflight` which already cleared the
    // map. We just zero the bg counter so the next iteration's
    // admission gate sees a fresh slate.
    let _ = inflight;
    *inflight_bg = 0;
    0
}

#[cfg(target_os = "linux")]
enum ControlFlow {
    Continue,
    Stop,
}

/// Submit `op` to the ring. Returns `true` iff the op was actually
/// admitted as an SQE (so the caller should increment its in-flight
/// counter). `false` covers Shutdown sentinels and the rare SQ-full
/// fast-error path.
#[cfg(target_os = "linux")]
fn handle_op(
    op: IoOp,
    ring: &mut IoUring,
    inflight: &mut std::collections::HashMap<u64, InflightOp>,
    next_uid: &mut u64,
    fd: RawFd,
    shutdown: &mut bool,
    is_bg: bool,
) -> bool {
    match op {
        IoOp::Shutdown => {
            *shutdown = true;
            false
        }
        IoOp::Write {
            page_id,
            page,
            reply,
        } => {
            let uid = *next_uid;
            *next_uid = next_uid.wrapping_add(1);
            let entry = opcode::Write::new(types::Fd(fd), page.bytes().as_ptr(), PAGE_SIZE as u32)
                .offset(page_id * PAGE_SIZE as u64)
                .build()
                .user_data(uid);
            // SAFETY: `page` (Arc<Page>) is moved into the InflightOp
            // below before we relinquish the SQE; the bytes outlive the
            // kernel's read of the buffer.
            let pushed = unsafe { ring.submission().push(&entry) };
            match pushed {
                Ok(()) => {
                    inflight.insert(
                        uid,
                        InflightOp {
                            kind: InflightKind::Write {
                                expected_len: PAGE_SIZE,
                            },
                            reply,
                            _pages: vec![page],
                            _iovecs: Vec::new(),
                            is_bg,
                        },
                    );
                    true
                }
                Err(_) => {
                    // SQ full despite our accounting — shouldn't happen
                    // because we cap pulls at sq_capacity, but if it
                    // does we surface an error instead of dropping the
                    // op silently.
                    let _ = reply.send(Err(MetaDbError::Io(std::io::Error::other(
                        "io_uring submission queue unexpectedly full",
                    ))));
                    false
                }
            }
        }
        IoOp::WriteRun {
            start_page,
            pages,
            reply,
        } => {
            if pages.is_empty() {
                let _ = reply.send(Ok(()));
                return false;
            }
            let uid = *next_uid;
            *next_uid = next_uid.wrapping_add(1);
            // Build the iovec array from the page buffers. Vec heap
            // allocation is stable across move, so capturing
            // `iovecs.as_ptr()` here and then moving `iovecs` into the
            // InflightOp keeps the kernel's view of the iovec array
            // valid for the lifetime of the SQE.
            let iovecs: Vec<libc::iovec> = pages
                .iter()
                .map(|p| libc::iovec {
                    iov_base: p.bytes().as_ptr() as *mut libc::c_void,
                    iov_len: PAGE_SIZE,
                })
                .collect();
            let iovcnt = iovecs.len() as u32;
            let expected_len = pages.len() * PAGE_SIZE;
            let entry = opcode::Writev::new(types::Fd(fd), iovecs.as_ptr(), iovcnt)
                .offset(start_page * PAGE_SIZE as u64)
                .build()
                .user_data(uid);
            // SAFETY: `iovecs` and `pages` are moved into the
            // InflightOp below; both buffers stay alive until the CQE
            // for `uid` is harvested, at which point the slot is
            // removed and the buffers are dropped.
            let pushed = unsafe { ring.submission().push(&entry) };
            match pushed {
                Ok(()) => {
                    inflight.insert(
                        uid,
                        InflightOp {
                            kind: InflightKind::WriteRun { expected_len },
                            reply,
                            _pages: pages,
                            _iovecs: iovecs,
                            is_bg,
                        },
                    );
                    true
                }
                Err(_) => {
                    let _ = reply.send(Err(MetaDbError::Io(std::io::Error::other(
                        "io_uring submission queue unexpectedly full",
                    ))));
                    false
                }
            }
        }
        IoOp::Fsync { reply } => {
            let uid = *next_uid;
            *next_uid = next_uid.wrapping_add(1);
            // No IOSQE_IO_DRAIN: callers already collect the write
            // CQEs before submitting fsync, so ordering is enforced at
            // the application level. DRAIN is purely an ordering
            // primitive (it forces serialisation through the SQ) and
            // does nothing for fsync's intrinsic device-level cost;
            // empirically it made `meta_io_fsync_max_us` *worse*
            // (1042 ms → 1453 ms on nvme-box) by stalling later W4/W5
            // SQEs behind the fsync.
            let entry = opcode::Fsync::new(types::Fd(fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(uid);
            // SAFETY: SQE only references the fd, which the caller owns
            // for the lifetime of the submitter.
            let pushed = unsafe { ring.submission().push(&entry) };
            match pushed {
                Ok(()) => {
                    inflight.insert(
                        uid,
                        InflightOp {
                            kind: InflightKind::Fsync,
                            reply,
                            _pages: Vec::new(),
                            _iovecs: Vec::new(),
                            is_bg,
                        },
                    );
                    true
                }
                Err(_) => {
                    let _ = reply.send(Err(MetaDbError::Io(std::io::Error::other(
                        "io_uring submission queue unexpectedly full",
                    ))));
                    false
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn decode_cqe_result(kind: InflightKind, result: i32) -> Result<()> {
    if result < 0 {
        return Err(MetaDbError::Io(std::io::Error::from_raw_os_error(-result)));
    }
    match kind {
        InflightKind::Write { expected_len } | InflightKind::WriteRun { expected_len } => {
            if result as usize != expected_len {
                return Err(MetaDbError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    format!("io_uring short write: got {result} of {expected_len}"),
                )));
            }
            Ok(())
        }
        InflightKind::Fsync => Ok(()),
    }
}

#[cfg(target_os = "linux")]
fn fail_all_inflight<F>(inflight: &mut std::collections::HashMap<u64, InflightOp>, err: F)
where
    F: Fn() -> MetaDbError,
{
    for (_, slot) in inflight.drain() {
        let _ = slot.reply.send(Err(err()));
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
