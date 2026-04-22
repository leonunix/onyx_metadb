//! Minimal recovery skeleton.
//!
//! Recovery's job in a full database is to consume every WAL record
//! after `manifest.checkpoint_lsn` and apply it to the in-memory /
//! on-disk index state. Phase 1 has no indexes yet, so this module
//! stops at "find the durable tail": walk the segments, decode records
//! forward, and tell the caller what the last good LSN was and whether
//! the final segment ended with a torn tail.
//!
//! Later phases will grow this into the real replay path that rebuilds
//! B+tree and LSM state. The invariants checked here — monotonic LSN,
//! torn tail only at the final segment — will remain.

use std::path::{Path, PathBuf};

use crate::error::{MetaDbError, Result};
use crate::tx::ApplyOutcome;
use crate::types::Lsn;
use crate::wal::WalOp;
use crate::wal::op::decode_body;
use crate::wal::record::{DecodeError, WalRecordIter};
use crate::wal::segment::{SegmentFile, list_segments, read_segment};

/// Result of a successful replay. On IO or structural error we return
/// [`MetaDbError`] instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// LSN of the first record whose `lsn >= from_lsn`, if any.
    pub first_lsn: Option<Lsn>,
    /// LSN of the last cleanly-decoded record.
    pub last_lsn: Option<Lsn>,
    /// Total number of records replayed (LSN >= `from_lsn`).
    pub record_count: u64,
    /// How the final segment ended. `None` means the final segment
    /// decoded cleanly to EOF; `Some` means the tail was truncated or
    /// CRC-bad and the caller should consider truncating the segment
    /// file at [`ReplayOutcome::tail_offset_bytes`].
    pub torn_tail: Option<DecodeError>,
    /// Byte offset within the final segment at which the last cleanly-
    /// decoded record ended. Suggested truncation point.
    pub tail_offset_bytes: u64,
    /// Path of the final segment (used by [`truncate_torn_tail`]).
    pub final_segment: Option<PathBuf>,
}

/// Walk every WAL segment in `dir` in LSN order, decoding records. Any
/// record with `lsn < from_lsn` is skipped silently (already applied).
/// A torn tail in the final segment is recorded in
/// [`ReplayOutcome::torn_tail`]; a torn record in a *non-final* segment
/// or any non-monotonic LSN is a hard corruption error.
pub fn replay(dir: &Path, from_lsn: Lsn) -> Result<ReplayOutcome> {
    replay_impl(dir, from_lsn, |_, _| Ok(()))
}

/// Replay WAL records forward and invoke `apply_op(lsn, op)` for each
/// decoded op. The `lsn` argument is the LSN of the enclosing record —
/// every op in a batch shares the record's LSN. Stops at the same
/// points [`replay`] does (torn final tail, mid-log corruption). Used
/// at `Db::open` to replay all committed ops onto in-memory state
/// before resuming writes.
pub fn replay_into<F>(dir: &Path, from_lsn: Lsn, mut apply_op: F) -> Result<ReplayOutcome>
where
    F: FnMut(Lsn, &WalOp) -> Result<ApplyOutcome>,
{
    replay_impl(dir, from_lsn, |lsn, body| {
        for op in decode_body(body)? {
            apply_op(lsn, &op)?;
        }
        Ok(())
    })
}

fn replay_impl<F>(dir: &Path, from_lsn: Lsn, mut per_record: F) -> Result<ReplayOutcome>
where
    F: FnMut(Lsn, &[u8]) -> Result<()>,
{
    let segments = list_segments(dir)?;
    let segment_count = segments.len();

    let mut outcome = ReplayOutcome {
        first_lsn: None,
        last_lsn: None,
        record_count: 0,
        torn_tail: None,
        tail_offset_bytes: 0,
        final_segment: None,
    };

    for (idx, (_, path)) in segments.into_iter().enumerate() {
        let is_final_segment = idx + 1 == segment_count;
        let buf = read_segment(&path)?;
        let mut iter = WalRecordIter::new(&buf);
        for rec in iter.by_ref() {
            if rec.lsn < from_lsn {
                continue;
            }
            if let Some(prev) = outcome.last_lsn {
                if rec.lsn <= prev {
                    return Err(MetaDbError::Corruption(format!(
                        "wal lsn non-monotonic: saw {} after {}",
                        rec.lsn, prev,
                    )));
                }
            }
            per_record(rec.lsn, rec.body)?;
            if outcome.first_lsn.is_none() {
                outcome.first_lsn = Some(rec.lsn);
            }
            outcome.last_lsn = Some(rec.lsn);
            outcome.record_count += 1;
        }
        if is_final_segment {
            outcome.tail_offset_bytes = iter.consumed() as u64;
            outcome.final_segment = Some(path.clone());
        }
        if let Some(err) = iter.stopped() {
            if is_final_segment {
                outcome.torn_tail = Some(err);
            } else {
                return Err(MetaDbError::Corruption(format!(
                    "wal mid-log corruption in segment {} at offset {}: {err}",
                    path.display(),
                    iter.consumed(),
                )));
            }
        }
    }

    Ok(outcome)
}

/// If the replay outcome reports a torn tail in the final segment,
/// truncate that segment to the offset of the last cleanly-decoded
/// record. No-op otherwise.
///
/// Must be called BEFORE constructing a new `Wal` on the same
/// directory: the writer appends at the file's current length, so a
/// torn tail left in place would leave unreachable garbage ahead of
/// every new record.
pub fn truncate_torn_tail(_dir: &Path, outcome: &ReplayOutcome) -> Result<()> {
    if outcome.torn_tail.is_none() {
        return Ok(());
    }
    let Some(path) = outcome.final_segment.as_ref() else {
        return Ok(());
    };
    SegmentFile::truncate_to(path, outcome.tail_offset_bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::testing::faults::FaultController;
    use crate::wal::Wal;
    use crate::wal::record::{WAL_HEADER_SIZE, encode};
    use crate::wal::segment::{SegmentFile, list_segments, segment_filename};
    use tempfile::TempDir;

    fn cfg() -> Config {
        let mut c = Config::new("unused");
        c.group_commit_timeout_us = 50;
        c
    }

    #[test]
    fn empty_dir_is_clean() {
        let dir = TempDir::new().unwrap();
        let out = replay(dir.path(), 1).unwrap();
        assert_eq!(out.record_count, 0);
        assert_eq!(out.first_lsn, None);
        assert_eq!(out.last_lsn, None);
        assert_eq!(out.torn_tail, None);
    }

    #[test]
    fn replay_finds_every_record() {
        let dir = TempDir::new().unwrap();
        {
            let wal = Wal::create(dir.path(), &cfg(), 1, FaultController::new()).unwrap();
            for i in 0..50u64 {
                wal.submit(format!("r{i}").into_bytes()).unwrap();
            }
            wal.shutdown().unwrap();
        }
        let out = replay(dir.path(), 1).unwrap();
        assert_eq!(out.record_count, 50);
        assert_eq!(out.first_lsn, Some(1));
        assert_eq!(out.last_lsn, Some(50));
        assert_eq!(out.torn_tail, None);
    }

    #[test]
    fn replay_skips_records_before_from_lsn() {
        let dir = TempDir::new().unwrap();
        {
            let wal = Wal::create(dir.path(), &cfg(), 1, FaultController::new()).unwrap();
            for _ in 0..20u64 {
                wal.submit(b"x".to_vec()).unwrap();
            }
            wal.shutdown().unwrap();
        }
        let out = replay(dir.path(), 15).unwrap();
        assert_eq!(out.record_count, 6); // LSNs 15..=20
        assert_eq!(out.first_lsn, Some(15));
        assert_eq!(out.last_lsn, Some(20));
    }

    #[test]
    fn replay_from_beyond_tail_is_empty() {
        let dir = TempDir::new().unwrap();
        {
            let wal = Wal::create(dir.path(), &cfg(), 1, FaultController::new()).unwrap();
            for _ in 0..5u64 {
                wal.submit(b"x".to_vec()).unwrap();
            }
            wal.shutdown().unwrap();
        }
        let out = replay(dir.path(), 999).unwrap();
        assert_eq!(out.record_count, 0);
        assert_eq!(out.first_lsn, None);
    }

    #[test]
    fn torn_tail_is_surfaced_for_final_segment() {
        let dir = TempDir::new().unwrap();
        {
            let wal = Wal::create(dir.path(), &cfg(), 1, FaultController::new()).unwrap();
            for _ in 0..10u64 {
                wal.submit(b"body".to_vec()).unwrap();
            }
            wal.shutdown().unwrap();
        }
        // Append a partial record to the only segment on disk.
        let segs = list_segments(dir.path()).unwrap();
        assert_eq!(segs.len(), 1);
        let mut torn = Vec::new();
        encode(&mut torn, 11, b"partially-written-body");
        let truncated = &torn[..torn.len() / 2];
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&segs[0].1)
                .unwrap();
            let at = std::fs::metadata(&segs[0].1).unwrap().len();
            f.write_all_at(truncated, at).unwrap();
            f.sync_all().unwrap();
        }

        let out = replay(dir.path(), 1).unwrap();
        assert_eq!(out.record_count, 10);
        assert_eq!(out.last_lsn, Some(10));
        assert!(
            out.torn_tail.is_some(),
            "torn tail must be surfaced for the final segment",
        );
        // tail_offset_bytes points past the last clean record.
        assert_eq!(out.tail_offset_bytes, 10 * (WAL_HEADER_SIZE as u64 + 4));
    }

    #[test]
    fn torn_record_in_non_final_segment_is_fatal() {
        let dir = TempDir::new().unwrap();
        // Build two segments: seg1 with a good record + torn tail,
        // seg2 with a good record. This is a shape no sane writer ever
        // produces, but a stray truncation in a backup could.
        let mut s1 = SegmentFile::create(dir.path(), 1).unwrap();
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"good");
        s1.append(&buf).unwrap();
        // Append partial record body.
        buf.clear();
        encode(&mut buf, 2, b"torn-body");
        s1.append(&buf[..buf.len() / 2]).unwrap();
        s1.sync_all().unwrap();
        drop(s1);

        let mut s2 = SegmentFile::create(dir.path(), 10).unwrap();
        let mut buf = Vec::new();
        encode(&mut buf, 10, b"after");
        s2.append(&buf).unwrap();
        s2.sync_all().unwrap();
        drop(s2);

        // Sanity: the file we expected to create is there.
        let _ = std::fs::metadata(dir.path().join(segment_filename(1))).unwrap();
        let _ = std::fs::metadata(dir.path().join(segment_filename(10))).unwrap();

        let err = replay(dir.path(), 1).unwrap_err();
        match err {
            MetaDbError::Corruption(msg) => {
                assert!(msg.contains("mid-log"), "unexpected corruption text: {msg}");
            }
            other => panic!("expected corruption, got {other:?}"),
        }
    }

    #[test]
    fn replay_spans_multiple_segments() {
        let dir = TempDir::new().unwrap();
        let mut cfg = cfg();
        cfg.wal_segment_bytes = 256; // force rotation every few records
        {
            let wal = Wal::create(dir.path(), &cfg, 1, FaultController::new()).unwrap();
            for i in 0..30u64 {
                wal.submit(vec![i as u8; 64]).unwrap();
            }
            wal.shutdown().unwrap();
        }
        let segs = list_segments(dir.path()).unwrap();
        assert!(
            segs.len() > 1,
            "expected rotation, got {} segment(s)",
            segs.len()
        );

        let out = replay(dir.path(), 1).unwrap();
        assert_eq!(out.record_count, 30);
        assert_eq!(out.first_lsn, Some(1));
        assert_eq!(out.last_lsn, Some(30));
    }

    #[test]
    fn replay_into_invokes_callback_for_each_decoded_op() {
        use crate::paged::L2pValue;
        use crate::wal::{WalOp, encode_body};

        let dir = TempDir::new().unwrap();
        {
            let wal = Wal::create(dir.path(), &cfg(), 1, FaultController::new()).unwrap();
            // Record 1: two ops.
            let body1 = encode_body(&[
                WalOp::L2pPut {
                    vol_ord: 0,
                    lba: 1,
                    value: L2pValue([1; 28]),
                },
                WalOp::L2pPut {
                    vol_ord: 0,
                    lba: 2,
                    value: L2pValue([2; 28]),
                },
            ]);
            wal.submit(body1).unwrap();
            // Record 2: one op.
            let body2 = encode_body(&[WalOp::L2pDelete {
                vol_ord: 0,
                lba: 1,
            }]);
            wal.submit(body2).unwrap();
            wal.shutdown().unwrap();
        }

        let mut seen = Vec::new();
        let out = replay_into(dir.path(), 1, |_lsn, op| {
            seen.push(op.clone());
            Ok(ApplyOutcome::Dedup) // callback return is ignored
        })
        .unwrap();
        assert_eq!(out.record_count, 2);
        assert_eq!(out.last_lsn, Some(2));
        assert_eq!(seen.len(), 3);
        assert!(matches!(seen[0], WalOp::L2pPut { lba: 1, .. }));
        assert!(matches!(seen[1], WalOp::L2pPut { lba: 2, .. }));
        assert!(matches!(seen[2], WalOp::L2pDelete { lba: 1, .. }));
    }

    #[test]
    fn replay_into_stops_and_truncates_torn_tail() {
        use crate::paged::L2pValue;
        use crate::wal::{WalOp, encode_body};
        use std::os::unix::fs::FileExt;

        let dir = TempDir::new().unwrap();
        {
            let wal = Wal::create(dir.path(), &cfg(), 1, FaultController::new()).unwrap();
            for i in 0..5u64 {
                wal.submit(encode_body(&[WalOp::L2pPut {
                    vol_ord: 0,
                    lba: i,
                    value: L2pValue([i as u8; 28]),
                }]))
                .unwrap();
            }
            wal.shutdown().unwrap();
        }
        // Append garbage to the tail so the next record is torn.
        let segs = list_segments(dir.path()).unwrap();
        let (_, path) = &segs[0];
        let at = std::fs::metadata(path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_all_at(&[0xDE, 0xAD, 0xBE, 0xEF], at).unwrap();
        f.sync_all().unwrap();

        let mut applied = 0usize;
        let out = replay_into(dir.path(), 1, |_lsn, _op| {
            applied += 1;
            Ok(ApplyOutcome::Dedup)
        })
        .unwrap();
        assert_eq!(applied, 5);
        assert!(out.torn_tail.is_some());
        truncate_torn_tail(dir.path(), &out).unwrap();
        // After truncation, re-running replay finds no torn tail.
        let out2 =
            replay_into(dir.path(), 1, |_lsn, _op| Ok(ApplyOutcome::Dedup)).unwrap();
        assert_eq!(out2.record_count, 5);
        assert!(out2.torn_tail.is_none());
    }
}
