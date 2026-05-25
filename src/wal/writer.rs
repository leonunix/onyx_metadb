//! Group-commit WAL writer.
//!
//! Submitters hand the [`Wal`] facade a record body; a single writer
//! thread drains the submission queue, assigns monotonic LSNs, encodes
//! the batch, writes it to the current segment, `fsync`s once, and acks
//! every submitter with its assigned LSN.
//!
//! Batch formation follows `docs/DESIGN.md §8.2`:
//!
//! 1. Block until at least one record arrives.
//! 2. Opportunistically drain records that are already queued, then wait
//!    for more until either `group_commit_max_batch_bytes` is reached or
//!    `group_commit_timeout_us` elapses.
//! 3. If the pending batch plus the current segment would exceed
//!    `wal_segment_bytes`, rotate to a new segment first. A batch never
//!    spans two segments.
//! 4. Write the entire encoded batch with one `write_all`, then fsync.
//! 5. Ack every submitter in submit-order with its LSN (or with the
//!    error, if the commit failed).
//!
//! On fsync failure, the writer thread terminates: every submitter in
//! the failed batch receives a [`MetaDbError`], and subsequent submits
//! from any thread fail with a "writer exited" error. Recovery is the
//! caller's problem — the WAL makes no attempt to reopen itself after
//! a durability failure.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use parking_lot::Mutex;

use crate::config::Config;
use crate::error::{MetaDbError, Result};
use crate::metrics::MetaMetrics;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::Lsn;
use crate::wal::record::{WAL_HEADER_SIZE, WAL_MAX_BODY, try_encode};
use crate::wal::segment::SegmentFile;

/// Handle to the WAL writer thread. Clone-free by design — hand it
/// around by `Arc<Wal>` if multiple owners are needed.
pub struct Wal {
    sender: Sender<Op>,
    thread: Mutex<Option<JoinHandle<()>>>,
    metrics: Arc<MetaMetrics>,
}

pub(crate) struct PendingWalAck {
    ack: Receiver<Result<Lsn>>,
    started: Instant,
    metrics: Arc<MetaMetrics>,
}

impl PendingWalAck {
    pub(crate) fn wait(self) -> Result<Lsn> {
        let result = self.ack.recv().map_err(|_| writer_exited())?;
        self.metrics.record_wal_submit_wait(self.started.elapsed());
        result
    }
}

enum Op {
    Submit {
        assigned_lsn: Option<Lsn>,
        body: Vec<u8>,
        ack: Sender<Result<Lsn>>,
        /// ZFS-TXG-clone Phase 3: when false, the writer thread acks
        /// the submit after `seg.append` (body in OS page cache) and
        /// skips the per-batch fsync. Durability is reasserted at the
        /// next `Op::FsyncAll` (TXG sync boundary). A mixed batch
        /// (any submit `synchronous=true`) still fsyncs — see
        /// `commit_batch`.
        synchronous: bool,
    },
    Shutdown {
        ack: Sender<Result<()>>,
    },
    /// Force-fsync everything written to the current segment so far,
    /// then ack. Modelled on `Shutdown`'s mid-batch handling: drained
    /// in-flight submits get committed (forced sync), acked in LSN
    /// order, then the FsyncAll caller is acked. Used by the
    /// `WalSet::fsync_all_lanes` TXG-sync barrier.
    FsyncAll {
        ack: Sender<Result<()>>,
    },
}

impl Wal {
    /// Create a fresh WAL in `dir`, starting with a segment whose first
    /// record will carry LSN `start_lsn`. The directory is created if
    /// missing. Any pre-existing segments in the directory are left alone
    /// (recovery lives elsewhere).
    pub fn create(
        dir: &Path,
        config: &Config,
        start_lsn: Lsn,
        faults: Arc<FaultController>,
    ) -> Result<Self> {
        Self::create_with_metrics(dir, config, start_lsn, faults, Arc::new(MetaMetrics::new()))
    }

    pub(crate) fn create_with_metrics(
        dir: &Path,
        config: &Config,
        start_lsn: Lsn,
        faults: Arc<FaultController>,
        metrics: Arc<MetaMetrics>,
    ) -> Result<Self> {
        assert!(start_lsn >= 1, "start_lsn must be >= 1");
        std::fs::create_dir_all(dir)?;
        let state = WriterState::init(
            dir.to_path_buf(),
            config,
            start_lsn,
            faults,
            metrics.clone(),
        )?;
        let (sender, receiver) = crossbeam_channel::unbounded();
        let thread = std::thread::Builder::new()
            .name("onyx-metadb-wal".to_string())
            .spawn(move || {
                crate::affinity::bind_current(crate::affinity::ThreadRole::Wal, 0);
                writer_main(receiver, state)
            })?;
        Ok(Self {
            sender,
            thread: Mutex::new(Some(thread)),
            metrics,
        })
    }

    /// Submit a record and block until it has been fsynced. Returns the
    /// LSN assigned by the writer. Submission order across threads is
    /// not guaranteed, but LSN ordering always matches the order in
    /// which the writer thread dequeued the messages.
    pub fn submit(&self, body: Vec<u8>) -> Result<Lsn> {
        if body.len() > WAL_MAX_BODY {
            return Err(wal_body_too_large(body.len()));
        }
        let started = Instant::now();
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.sender
            .send(Op::Submit {
                assigned_lsn: None,
                body,
                ack: ack_tx,
                synchronous: true,
            })
            .map_err(|_| writer_exited())?;
        let result = ack_rx.recv().map_err(|_| writer_exited())?;
        self.metrics.record_wal_submit_wait(started.elapsed());
        result
    }

    /// Enqueue an already-assigned record and return an ack handle.
    /// `WalSet` calls this while holding its short LSN/order mutex so
    /// same-lane enqueue order matches global LSN order, then waits for
    /// the fsync ack outside that mutex.
    ///
    /// When `synchronous=false` (ZFS-TXG-clone Phase 3), the writer
    /// thread acks the caller after `seg.append` (body in OS page
    /// cache) and skips the per-batch fsync. A mixed batch (any submit
    /// `synchronous=true`) still fsyncs. Durability is reasserted at
    /// the next `fsync_pending` call.
    pub(crate) fn submit_assigned_async_with_options(
        &self,
        lsn: Lsn,
        body: Vec<u8>,
        synchronous: bool,
    ) -> Result<PendingWalAck> {
        if body.len() > WAL_MAX_BODY {
            return Err(wal_body_too_large(body.len()));
        }
        let started = Instant::now();
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.sender
            .send(Op::Submit {
                assigned_lsn: Some(lsn),
                body,
                ack: ack_tx,
                synchronous,
            })
            .map_err(|_| writer_exited())?;
        Ok(PendingWalAck {
            ack: ack_rx,
            started,
            metrics: Arc::clone(&self.metrics),
        })
    }

    /// Force-fsync everything appended to this lane's current segment
    /// so far. Returns when the fsync syscall returns (or fails). The
    /// `WalSet::fsync_all_lanes` TXG-sync barrier fans this across every
    /// lane before promoting `manifest.checkpoint_lsn`.
    ///
    /// Safety argument (non-local — read before touching):
    /// `flush_with_gate` samples `wal_checkpoint = last_applied_lsn`
    /// under `apply_gate.write()`. Every commit ≤ `wal_checkpoint` has
    /// completed `finish_global_apply`, which is gated on
    /// `wal.submit*().wait()` returning, which means the writer thread
    /// has at minimum executed `seg.append(&buf)` (body in OS page
    /// cache). `fsync_pending` after the sample makes those bytes
    /// durable. Without this barrier, an async commit could land in a
    /// checkpoint that no surviving WAL record covers, breaking
    /// recovery.
    pub(crate) fn fsync_pending(&self) -> Result<()> {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.sender
            .send(Op::FsyncAll { ack: ack_tx })
            .map_err(|_| writer_exited())?;
        ack_rx.recv().map_err(|_| writer_exited())?
    }

    /// Drain any in-flight batch and stop the writer thread. After
    /// calling this, future `submit` calls fail. Safe to call multiple
    /// times; only the first has effect.
    pub fn shutdown(&self) -> Result<()> {
        let handle_opt = self.thread.lock().take();
        let Some(handle) = handle_opt else {
            return Ok(()); // Already shut down.
        };
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        // If the channel is already closed (thread exited due to prior
        // error), treat shutdown as a no-op on the messaging side and
        // still join.
        let _ = self.sender.send(Op::Shutdown { ack: ack_tx });
        let ack_result = ack_rx.recv().ok();
        let _ = handle.join();
        match ack_result {
            Some(r) => r,
            None => Err(MetaDbError::Corruption(
                "wal writer exited before shutdown ack".into(),
            )),
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Best-effort cleanup. Errors are swallowed because Drop cannot
        // return them; callers who care about durability must call
        // `shutdown()` explicitly.
        let _ = self.shutdown();
    }
}

fn writer_exited() -> MetaDbError {
    MetaDbError::Corruption("wal writer thread exited".into())
}

fn wal_body_too_large(length: usize) -> MetaDbError {
    MetaDbError::InvalidArgument(format!(
        "WAL body too large: {length} bytes exceeds WAL_MAX_BODY {WAL_MAX_BODY}"
    ))
}

struct WriterState {
    dir: PathBuf,
    current: Option<SegmentFile>,
    next_lsn: Lsn,
    max_segment_bytes: u64,
    max_batch_bytes: usize,
    timeout: Duration,
    /// ZFS-TXG-clone Phase 3: deadline applied when every submit in
    /// the in-flight batch has `synchronous=false`. Defaults to
    /// `config.wal_async_group_commit_window_us` (1 ms). Collapses to
    /// `timeout` the moment a sync submit lands — see `writer_main`.
    async_timeout: Duration,
    faults: Arc<FaultController>,
    metrics: Arc<MetaMetrics>,
}

struct PendingSubmit {
    assigned_lsn: Option<Lsn>,
    body: Vec<u8>,
    ack: Sender<Result<Lsn>>,
    synchronous: bool,
}

impl WriterState {
    fn init(
        dir: PathBuf,
        config: &Config,
        start_lsn: Lsn,
        faults: Arc<FaultController>,
        metrics: Arc<MetaMetrics>,
    ) -> Result<Self> {
        // If the directory already has segments (e.g., from a previous
        // open that was recovered up to `start_lsn - 1`), append to the
        // newest one; otherwise create a fresh segment at `start_lsn`.
        let segments = crate::wal::segment::list_segments(&dir)?;
        let seg = match segments.last() {
            Some((existing_start, _)) if *existing_start < start_lsn => {
                SegmentFile::open_for_append(&dir, *existing_start)?
            }
            Some((existing_start, _)) if *existing_start == start_lsn => {
                SegmentFile::open_for_append(&dir, *existing_start)?
            }
            Some((existing_start, _)) => {
                return Err(MetaDbError::Corruption(format!(
                    "WAL segment wal-{existing_start:020} starts ahead of requested \
                     start_lsn {start_lsn}",
                )));
            }
            None => SegmentFile::create(&dir, start_lsn)?,
        };
        Ok(Self {
            dir,
            current: Some(seg),
            next_lsn: start_lsn,
            max_segment_bytes: config.wal_segment_bytes,
            max_batch_bytes: config.group_commit_max_batch_bytes,
            timeout: Duration::from_micros(config.group_commit_timeout_us),
            async_timeout: Duration::from_micros(config.wal_async_group_commit_window_us),
            faults,
            metrics,
        })
    }

    fn finalize(&mut self) -> Result<()> {
        if let Some(mut seg) = self.current.take() {
            seg.sync_all()?;
        }
        Ok(())
    }

    fn rotate_to(&mut self, start_lsn: Lsn) -> Result<()> {
        if let Some(mut old) = self.current.take() {
            old.sync_all()?;
        }
        let seg = SegmentFile::create(&self.dir, start_lsn)?;
        self.current = Some(seg);
        Ok(())
    }

    /// ZFS-TXG-clone Phase 3: data-fsync the current segment without
    /// writing anything. Used when `Op::FsyncAll` arrives with no
    /// in-flight submits — the batch path's normal `seg.sync()` covers
    /// the mid-batch case. Mirrors the data-only `seg.sync()` flavour
    /// to keep the XFS/ext4 fsync cost consistent with the per-batch
    /// path; `sync_all` (metadata) is reserved for finalize/rotate.
    fn fsync_current(&mut self) -> Result<()> {
        if let Some(seg) = self.current.as_mut() {
            self.faults.inject(FaultPoint::WalFsyncBefore)?;
            let started = Instant::now();
            seg.sync()?;
            self.metrics.record_wal_fsync(started.elapsed());
            self.faults.inject(FaultPoint::WalFsyncAfter)?;
        }
        Ok(())
    }
}

fn writer_main(receiver: crossbeam_channel::Receiver<Op>, mut state: WriterState) {
    loop {
        // Block until an op arrives or the channel closes (every sender
        // dropped — treat as implicit shutdown).
        let first = match receiver.recv() {
            Ok(op) => op,
            Err(_) => {
                let _ = state.finalize();
                return;
            }
        };

        // A shutdown at the head of the queue is routed directly.
        if let Op::Shutdown { ack } = first {
            let _ = ack.send(state.finalize());
            return;
        }

        // ZFS-TXG-clone Phase 3: an FsyncAll at the head with no submits
        // in flight is a no-op aside from the explicit segment sync.
        if let Op::FsyncAll { ack } = first {
            let _ = ack.send(state.fsync_current());
            continue;
        }

        // Start a batch with the submit we already dequeued.
        let first_len = match &first {
            Op::Submit { body, .. } => body.len() + WAL_HEADER_SIZE,
            Op::Shutdown { .. } | Op::FsyncAll { .. } => unreachable!(),
        };
        let mut submits: Vec<PendingSubmit> = Vec::new();
        match first {
            Op::Submit {
                assigned_lsn,
                body,
                ack,
                synchronous,
            } => submits.push(PendingSubmit {
                assigned_lsn,
                body,
                ack,
                synchronous,
            }),
            Op::Shutdown { .. } | Op::FsyncAll { .. } => unreachable!(),
        }
        let mut batch_bytes = first_len;

        // Drain more submits up to the byte cap or the timeout. Always
        // consume immediately queued records before looking at the clock, so
        // a tiny timeout still forms batches when submitters already lined up.
        //
        // Phase 3 dynamic deadline: while every submit is async, wait up
        // to `async_timeout` to amortise the eventual TXG-sync fsync; the
        // moment a sync submit lands, collapse to `timeout` so sync
        // hot-path latency is protected. Recomputed every loop iteration
        // — cheap and matches the head-arrival anchor.
        let base = Instant::now();
        let mut pending_shutdown: Option<Sender<Result<()>>> = None;
        let mut pending_fsync_all: Option<Sender<Result<()>>> = None;
        while batch_bytes < state.max_batch_bytes
            && pending_shutdown.is_none()
            && pending_fsync_all.is_none()
        {
            let all_async = submits.iter().all(|s| !s.synchronous);
            let timeout = if all_async {
                state.async_timeout
            } else {
                state.timeout
            };
            let deadline = base + timeout;

            match receiver.try_recv() {
                Ok(Op::Submit {
                    assigned_lsn,
                    body,
                    ack,
                    synchronous,
                }) => {
                    batch_bytes += body.len() + WAL_HEADER_SIZE;
                    submits.push(PendingSubmit {
                        assigned_lsn,
                        body,
                        ack,
                        synchronous,
                    });
                    continue;
                }
                Ok(Op::Shutdown { ack }) => {
                    pending_shutdown = Some(ack);
                    break;
                }
                Ok(Op::FsyncAll { ack }) => {
                    pending_fsync_all = Some(ack);
                    break;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => break,
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match receiver.recv_timeout(remaining) {
                Ok(Op::Submit {
                    assigned_lsn,
                    body,
                    ack,
                    synchronous,
                }) => {
                    batch_bytes += body.len() + WAL_HEADER_SIZE;
                    submits.push(PendingSubmit {
                        assigned_lsn,
                        body,
                        ack,
                        synchronous,
                    });
                }
                Ok(Op::Shutdown { ack }) => {
                    pending_shutdown = Some(ack);
                    break;
                }
                Ok(Op::FsyncAll { ack }) => {
                    pending_fsync_all = Some(ack);
                    break;
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        // Commit the batch atomically.  On success, ack in order.  On
        // failure, ack every submitter in the batch with a freshly-
        // constructed error and exit the thread.
        //
        // Phase 3 force_fsync: when FsyncAll arrived mid-batch, the
        // batch is promoted to synchronous so the FsyncAll caller can
        // ack with confidence that every pending byte is durable.
        let force_fsync = pending_fsync_all.is_some();
        let commit_result = commit_batch(&mut state, &mut submits, force_fsync);
        match commit_result {
            Ok(assigned) => {
                for (ack, lsn) in assigned {
                    let _ = ack.send(Ok(lsn));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                tracing::error!(
                    error = %msg,
                    records = submits.len(),
                    batch_bytes,
                    "metadb wal writer: commit batch failed; stopping writer"
                );
                for pending in submits.drain(..) {
                    let _ = pending.ack.send(Err(MetaDbError::Corruption(format!(
                        "wal commit failed: {msg}"
                    ))));
                }
                if let Some(ack) = pending_shutdown {
                    let _ = ack.send(Err(MetaDbError::Corruption(format!(
                        "wal shutdown after commit failure: {msg}"
                    ))));
                }
                if let Some(ack) = pending_fsync_all {
                    let _ = ack.send(Err(MetaDbError::Corruption(format!(
                        "wal fsync_all after commit failure: {msg}"
                    ))));
                }
                return;
            }
        }

        // FsyncAll ack only AFTER in-batch ack drain — preserves the
        // "ack order matches LSN order" invariant. force_fsync above
        // ensured the batch already fsynced; no extra syscall here.
        if let Some(ack) = pending_fsync_all {
            let _ = ack.send(Ok(()));
        }

        if let Some(ack) = pending_shutdown {
            let _ = ack.send(state.finalize());
            return;
        }
    }
}

/// Encode the batch, rotate if needed, write, conditionally fsync.
/// Returns the list of `(ack, lsn)` pairs so the caller can dispatch
/// on success.
///
/// `submits` is consumed on success (drained). On failure, `submits`
/// is left intact so the caller can iterate and send error acks.
///
/// `force_fsync=true` (ZFS-TXG-clone Phase 3) means the batch must
/// fsync regardless of submit flags — used when `Op::FsyncAll` arrived
/// mid-batch and promoted the batch to synchronous. Otherwise the
/// batch fsyncs iff any submit had `synchronous=true`. An all-async
/// batch returns without fsync; durability is reasserted at the next
/// FsyncAll (TXG sync barrier).
fn commit_batch(
    state: &mut WriterState,
    submits: &mut Vec<PendingSubmit>,
    force_fsync: bool,
) -> Result<Vec<(Sender<Result<Lsn>>, Lsn)>> {
    if submits.is_empty() {
        return Ok(Vec::new());
    }

    // Estimated byte count for the rotation check. Accurate because the
    // record header is fixed size.
    let batch_bytes: usize = submits.iter().map(|s| s.body.len() + WAL_HEADER_SIZE).sum();
    if let Some(oversized) = submits
        .iter()
        .map(|s| s.body.len())
        .find(|len| *len > WAL_MAX_BODY)
    {
        return Err(wal_body_too_large(oversized));
    }
    let first_lsn = submits[0].assigned_lsn.unwrap_or(state.next_lsn);

    // Rotate BEFORE assigning LSNs so the new segment's start_lsn
    // matches the LSN of the first record it will contain.
    let need_rotate = match state.current.as_ref() {
        Some(seg) => {
            seg.bytes_written() > 0
                && seg.bytes_written() + batch_bytes as u64 > state.max_segment_bytes
        }
        None => true,
    };
    if need_rotate {
        state.rotate_to(first_lsn)?;
        state.metrics.record_wal_rotate();
    }

    // Encode into a single buffer so the write is one syscall.
    let mut buf = Vec::with_capacity(batch_bytes);
    let mut assigned = Vec::with_capacity(submits.len());
    let mut any_sync = force_fsync;
    for pending in submits.drain(..) {
        let lsn = pending.assigned_lsn.unwrap_or(state.next_lsn);
        if lsn < state.next_lsn {
            return Err(MetaDbError::Corruption(format!(
                "wal assigned lsn {} is behind writer cursor {}",
                lsn, state.next_lsn
            )));
        }
        state.next_lsn = state
            .next_lsn
            .max(lsn)
            .checked_add(1)
            .ok_or(MetaDbError::OutOfSpace)?;
        try_encode(&mut buf, lsn, &pending.body).map_err(|len| wal_body_too_large(len as usize))?;
        any_sync |= pending.synchronous;
        assigned.push((pending.ack, lsn));
    }

    // Write + (maybe) fsync. Fault points straddle the fsync so tests
    // can simulate both a partial write (before fsync returns) and a
    // post-durability crash.
    let seg = state
        .current
        .as_mut()
        .expect("writer has no current segment");
    let write_started = Instant::now();
    seg.append(&buf)?;
    state.metrics.record_wal_write(write_started.elapsed());
    if any_sync {
        state.faults.inject(FaultPoint::WalFsyncBefore)?;
        let fsync_started = Instant::now();
        seg.sync()?;
        state.metrics.record_wal_fsync(fsync_started.elapsed());
        state.faults.inject(FaultPoint::WalFsyncAfter)?;
    }
    state.metrics.record_wal_batch(assigned.len(), buf.len());
    Ok(assigned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::faults::FaultAction;
    use crate::wal::record::WalRecordIter;
    use crate::wal::segment::{list_segments, read_segment};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    fn cfg_fast_batch() -> Config {
        let mut c = Config::new("unused");
        // Short timeout so tests don't idle for the production group window.
        c.group_commit_timeout_us = 50;
        c
    }

    fn read_all(dir: &Path) -> Vec<(Lsn, Vec<u8>)> {
        let mut out = Vec::new();
        for (_, path) in list_segments(dir).unwrap() {
            let buf = read_segment(&path).unwrap();
            for rec in WalRecordIter::new(&buf) {
                out.push((rec.lsn, rec.body.to_vec()));
            }
        }
        out
    }

    #[test]
    fn single_submit_round_trip() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults).unwrap();
        let lsn = wal.submit(b"hello".to_vec()).unwrap();
        assert_eq!(lsn, 1);
        wal.shutdown().unwrap();

        let all = read_all(dir.path());
        assert_eq!(all, vec![(1, b"hello".to_vec())]);
    }

    #[test]
    fn many_submits_get_monotonic_lsns() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults).unwrap();
        let mut lsns = Vec::new();
        for i in 0..100u64 {
            let lsn = wal.submit(format!("msg {i}").into_bytes()).unwrap();
            lsns.push(lsn);
        }
        wal.shutdown().unwrap();

        assert_eq!(lsns, (1..=100u64).collect::<Vec<_>>());
        let all = read_all(dir.path());
        for (i, (lsn, body)) in all.iter().enumerate() {
            assert_eq!(*lsn, i as u64 + 1);
            assert_eq!(body, format!("msg {i}").as_bytes());
        }
    }

    #[test]
    fn rotation_when_segment_fills() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let mut cfg = cfg_fast_batch();
        // Each record is 16B header + 100B body = 116B.  Force rotation
        // after roughly 5 records.
        cfg.wal_segment_bytes = 500;
        let wal = Wal::create(dir.path(), &cfg, 1, faults).unwrap();
        for i in 0..20u64 {
            let body = vec![i as u8; 100];
            let lsn = wal.submit(body).unwrap();
            assert_eq!(lsn, i + 1);
        }
        wal.shutdown().unwrap();

        let segs = list_segments(dir.path()).unwrap();
        assert!(segs.len() >= 4, "expected multiple segments, got {segs:?}");
        // Start LSNs strictly increasing.
        for pair in segs.windows(2) {
            assert!(pair[0].0 < pair[1].0);
        }
        // All 20 records readable across segments.
        let all = read_all(dir.path());
        assert_eq!(all.len(), 20);
        for (i, (lsn, body)) in all.iter().enumerate() {
            assert_eq!(*lsn, i as u64 + 1);
            assert_eq!(body, &vec![i as u8; 100]);
        }
    }

    #[test]
    fn concurrent_submits_all_land() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let wal = Arc::new(Wal::create(dir.path(), &cfg_fast_batch(), 1, faults).unwrap());

        let threads: Vec<_> = (0..8)
            .map(|t| {
                let wal = wal.clone();
                std::thread::spawn(move || {
                    let mut out = Vec::new();
                    for i in 0..50u64 {
                        let body = format!("t{t}-m{i}").into_bytes();
                        let lsn = wal.submit(body.clone()).unwrap();
                        out.push((lsn, body));
                    }
                    out
                })
            })
            .collect();

        let mut all_submitted: Vec<(Lsn, Vec<u8>)> = Vec::new();
        for t in threads {
            all_submitted.extend(t.join().unwrap());
        }
        Arc::try_unwrap(wal).ok().unwrap().shutdown().unwrap();

        // Every submitter got a unique LSN.
        let mut lsns: Vec<_> = all_submitted.iter().map(|(l, _)| *l).collect();
        lsns.sort();
        lsns.dedup();
        assert_eq!(lsns.len(), 8 * 50);
        assert_eq!(*lsns.first().unwrap(), 1);
        assert_eq!(*lsns.last().unwrap(), 8 * 50);

        // What's on disk matches what callers were told.
        let on_disk = read_all(dir.path());
        let mut expected = all_submitted.clone();
        expected.sort_by_key(|(l, _)| *l);
        assert_eq!(on_disk, expected);
    }

    #[test]
    fn fsync_before_error_propagates_to_submitter() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        faults.install(FaultPoint::WalFsyncBefore, 1, FaultAction::Error);
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults.clone()).unwrap();

        // First submit hits the trigger and fails.
        match wal.submit(b"doomed".to_vec()) {
            Err(MetaDbError::Corruption(msg)) => {
                assert!(msg.contains("wal"), "expected wal error, got: {msg}");
            }
            other => panic!("expected commit failure, got {other:?}"),
        }
        // Writer thread has exited; any subsequent submit fails with a
        // "writer exited" error.
        match wal.submit(b"later".to_vec()) {
            Err(MetaDbError::Corruption(msg)) => {
                assert!(msg.contains("writer"), "expected writer error, got: {msg}");
            }
            other => panic!("expected post-crash error, got {other:?}"),
        }
        // shutdown on a dead writer still returns a sensible error.
        let _ = wal.shutdown();
    }

    #[test]
    fn fsync_after_error_propagates_to_submitter() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        faults.install(FaultPoint::WalFsyncAfter, 1, FaultAction::Error);
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults).unwrap();
        assert!(wal.submit(b"doomed".to_vec()).is_err());
        let _ = wal.shutdown();
    }

    #[test]
    fn start_lsn_above_one_is_honored() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 100, faults).unwrap();
        let a = wal.submit(b"a".to_vec()).unwrap();
        let b = wal.submit(b"b".to_vec()).unwrap();
        assert_eq!(a, 100);
        assert_eq!(b, 101);
        wal.shutdown().unwrap();
        let segs = list_segments(dir.path()).unwrap();
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].0, 100);
    }

    #[test]
    fn shutdown_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults).unwrap();
        wal.submit(b"x".to_vec()).unwrap();
        wal.shutdown().unwrap();
        // Second shutdown is a no-op.
        wal.shutdown().unwrap();
    }

    #[test]
    fn submit_after_shutdown_fails() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults).unwrap();
        wal.shutdown().unwrap();
        assert!(wal.submit(b"after".to_vec()).is_err());
    }

    #[test]
    fn group_commit_batches_many_records() {
        // Verify that a burst of submits from many threads results in
        // far fewer fsyncs than records: we count fsync hits via the
        // fault controller (with no trigger that fires).
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        faults.install(FaultPoint::WalFsyncBefore, u64::MAX, FaultAction::Error);
        let wal = Arc::new(Wal::create(dir.path(), &cfg_fast_batch(), 1, faults.clone()).unwrap());

        let n_threads = 8usize;
        let per_thread = 100usize;
        let threads: Vec<_> = (0..n_threads)
            .map(|_| {
                let wal = wal.clone();
                std::thread::spawn(move || {
                    for _ in 0..per_thread {
                        wal.submit(vec![0u8; 128]).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        Arc::try_unwrap(wal).ok().unwrap().shutdown().unwrap();

        let fsyncs = faults.hits(FaultPoint::WalFsyncBefore);
        let records = (n_threads * per_thread) as u64;
        assert!(
            fsyncs < records,
            "expected batching: {fsyncs} fsyncs for {records} records",
        );
    }

    #[test]
    fn drop_without_shutdown_still_flushes() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        let last_lsn = AtomicU64::new(0);
        {
            let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults).unwrap();
            for i in 0..10u64 {
                let lsn = wal.submit(format!("{i}").into_bytes()).unwrap();
                last_lsn.store(lsn, Ordering::Relaxed);
            }
            // No explicit shutdown; Drop does it.
        }
        let all = read_all(dir.path());
        assert_eq!(all.len(), 10);
        assert_eq!(last_lsn.load(Ordering::Relaxed), 10);
    }

    /// Phase 3: a batch made entirely of `synchronous=false` submits
    /// must NOT trigger the per-batch fsync. We count fsync calls via
    /// the `WalFsyncBefore` fault hit counter (no trigger that fires).
    #[test]
    fn async_only_batch_skips_fsync() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        faults.install(FaultPoint::WalFsyncBefore, u64::MAX, FaultAction::Error);
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults.clone()).unwrap();

        for i in 1..=20u64 {
            wal.submit_assigned_async_with_options(i, format!("a{i}").into_bytes(), false)
                .unwrap()
                .wait()
                .unwrap();
        }

        // No fsyncs yet. Pre-shutdown read avoids the shutdown-time
        // sync_all dirtying the counter.
        assert_eq!(faults.hits(FaultPoint::WalFsyncBefore), 0);

        // FsyncAll drains the writer and forces one fsync.
        wal.fsync_pending().unwrap();
        assert_eq!(faults.hits(FaultPoint::WalFsyncBefore), 1);

        wal.shutdown().unwrap();
        let on_disk = read_all(dir.path());
        assert_eq!(on_disk.len(), 20);
    }

    /// Phase 3: a mixed batch (any submit with `synchronous=true`)
    /// must fsync, so the async submits in that batch ride along
    /// behind the sync one. Verified via the fsync hit counter.
    #[test]
    fn mixed_batch_fsyncs_once() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        faults.install(FaultPoint::WalFsyncBefore, u64::MAX, FaultAction::Error);
        // Lengthen the deadline so the burst lands as one batch.
        let mut cfg = cfg_fast_batch();
        cfg.wal_async_group_commit_window_us = 5_000;
        let wal = Arc::new(Wal::create(dir.path(), &cfg, 1, faults.clone()).unwrap());

        // Queue an async submit + a sync submit from two threads. The
        // sync submit forces the batch to fsync.
        let w1 = Arc::clone(&wal);
        let t_async = std::thread::spawn(move || {
            w1.submit_assigned_async_with_options(1, b"a".to_vec(), false)
                .unwrap()
                .wait()
                .unwrap();
        });
        // Small stagger to keep the async submit at the head of the batch.
        std::thread::sleep(Duration::from_millis(1));
        let w2 = Arc::clone(&wal);
        let t_sync = std::thread::spawn(move || {
            w2.submit_assigned_async_with_options(2, b"b".to_vec(), true)
                .unwrap()
                .wait()
                .unwrap();
        });
        t_async.join().unwrap();
        t_sync.join().unwrap();

        assert_eq!(
            faults.hits(FaultPoint::WalFsyncBefore),
            1,
            "mixed batch should fsync exactly once"
        );

        // shutdown() triggers a metadata sync_all but that doesn't go
        // through WalFsyncBefore.
        Arc::try_unwrap(wal).ok().unwrap().shutdown().unwrap();
    }

    /// Phase 3: `Op::FsyncAll` at the head of the queue (no pending
    /// submits) still drives one fsync of the current segment.
    #[test]
    fn fsync_pending_idle_writer_syncs_current_segment() {
        let dir = TempDir::new().unwrap();
        let faults = FaultController::new();
        faults.install(FaultPoint::WalFsyncBefore, u64::MAX, FaultAction::Error);
        let wal = Wal::create(dir.path(), &cfg_fast_batch(), 1, faults.clone()).unwrap();

        // One async submit so there's something to fsync.
        wal.submit_assigned_async_with_options(1, b"only".to_vec(), false)
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(faults.hits(FaultPoint::WalFsyncBefore), 0);

        // FsyncAll arrives with the writer idle — drives one fsync.
        wal.fsync_pending().unwrap();
        assert_eq!(faults.hits(FaultPoint::WalFsyncBefore), 1);

        // A second FsyncAll with no new submits in between still fsyncs
        // (Phase 3 doesn't track durable_lsn — it's an unconditional
        // segment data-sync). Cheap on an empty segment.
        wal.fsync_pending().unwrap();
        assert_eq!(faults.hits(FaultPoint::WalFsyncBefore), 2);

        wal.shutdown().unwrap();
    }
}
