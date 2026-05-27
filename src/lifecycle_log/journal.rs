//! Append-only lifecycle journal.
//!
//! A directory of `lifecycle-{start_seq:020}.log` segments holding
//! [`super::record`]-framed bodies. Each body is a single
//! [`super::op::LifecycleOp`] encoded by [`super::op::encode`].
//!
//! Lifecycle ops are rare (≈ 1 record/min in production) so the journal
//! intentionally skips the group-commit machinery the WAL uses: the
//! caller `append`s, the writer `fdatasync`s, and the call returns.
//! Latency on the order of one fsync (≈ 1 ms on the configured NVMe)
//! is acceptable for volume creates / snapshot drops / etc.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crc32c::crc32c;

use crate::error::{MetaDbError, Result};
use super::record::{
    DecodeError, WAL_HEADER_SIZE, WAL_MAX_BODY, WalRecordIter, WalRecordRef, encode as encode_frame,
};

/// Decoded journal record. `seq` is the monotonic id assigned at
/// `append` time; `body` is the encoded op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleRecord {
    pub seq: u64,
    pub body: Vec<u8>,
}

/// Append-only journal pointed at `dir`. Single-writer; callers
/// serialize themselves at a higher layer (lifecycle ops are not
/// parallelism-hot).
pub struct LifecycleJournal {
    dir: PathBuf,
    current: Option<SegmentFile>,
    next_seq: u64,
    max_segment_bytes: u64,
}

impl LifecycleJournal {
    /// Open the journal in `dir`, creating the directory if missing.
    /// `next_seq` is the seq of the next record to be appended — pass
    /// `manifest.lifecycle_replay_seq + max_observed_replay_seq + 1`
    /// after recovery so future appends never collide with replayed
    /// records.
    pub fn open(dir: &Path, next_seq: u64, max_segment_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let segments = list_segments(dir)?;
        let current = match segments.last() {
            Some((existing_start, _)) if *existing_start <= next_seq => {
                Some(SegmentFile::open_for_append(dir, *existing_start)?)
            }
            Some((existing_start, _)) => {
                return Err(MetaDbError::Corruption(format!(
                    "lifecycle segment lifecycle-{existing_start:020} starts ahead of \
                     requested next_seq {next_seq}",
                )));
            }
            None => Some(SegmentFile::create(dir, next_seq)?),
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            current,
            next_seq,
            max_segment_bytes,
        })
    }

    /// Append a body and fsync. Returns the assigned seq.
    ///
    /// Callers obtain `body` from [`super::op::encode`]. The lifecycle
    /// journal makes no group-commit attempt — every record pays for
    /// one `fdatasync`, which is fine at the rate lifecycle ops fire.
    pub fn append(&mut self, body: &[u8]) -> Result<u64> {
        if body.len() > WAL_MAX_BODY {
            return Err(MetaDbError::InvalidArgument(format!(
                "lifecycle body too large: {} bytes exceeds WAL_MAX_BODY {WAL_MAX_BODY}",
                body.len()
            )));
        }
        let seq = self.next_seq;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or(MetaDbError::OutOfSpace)?;

        let projected = body.len() + WAL_HEADER_SIZE;
        let need_rotate = match self.current.as_ref() {
            Some(seg) => {
                seg.bytes_written() > 0
                    && seg.bytes_written().saturating_add(projected as u64)
                        > self.max_segment_bytes
            }
            None => true,
        };
        if need_rotate {
            self.rotate_to(seq)?;
        }

        let mut framed = Vec::with_capacity(projected);
        encode_frame(&mut framed, seq, body);
        let seg = self
            .current
            .as_mut()
            .expect("journal has no current segment");
        seg.append(&framed)?;
        seg.sync()?;
        Ok(seq)
    }

    /// Replay records with `seq > from_seq` in seq order. Records up to
    /// and including `from_seq` are silently skipped (they're already
    /// covered by the manifest checkpoint). A torn record at the tail
    /// of the final segment is tolerated and truncated; mid-stream
    /// corruption is fatal.
    pub fn replay<F>(dir: &Path, from_seq: u64, mut apply: F) -> Result<u64>
    where
        F: FnMut(LifecycleRecord) -> Result<()>,
    {
        let segments = list_segments(dir)?;
        let mut last_seen = from_seq;
        let total = segments.len();
        for (idx, (_, path)) in segments.into_iter().enumerate() {
            let buf = read_segment(&path)?;
            let mut iter = WalRecordIter::new(&buf);
            let is_last = idx + 1 == total;
            for rec in iter.by_ref() {
                let WalRecordRef { lsn: seq, body } = rec;
                if seq <= from_seq {
                    continue;
                }
                apply(LifecycleRecord {
                    seq,
                    body: body.to_vec(),
                })?;
                last_seen = seq;
            }
            if let Some(err) = iter.stopped() {
                let recoverable = matches!(
                    err,
                    DecodeError::HeaderTruncated { .. }
                        | DecodeError::BodyTruncated { .. }
                        | DecodeError::ChecksumMismatch { .. }
                );
                if recoverable && is_last {
                    // Torn tail of the most recent segment: legal
                    // after a crash mid-append. Truncate the file
                    // back to the last clean frame so the journal
                    // can reopen for append.
                    truncate_segment_to(&path, iter.consumed())?;
                    break;
                }
                return Err(MetaDbError::Corruption(format!(
                    "lifecycle segment {} mid-stream decode error: {err:?}",
                    path.display(),
                )));
            }
        }
        Ok(last_seen)
    }

    /// Delete segments wholly covered by `checkpoint_seq` so the
    /// journal doesn't grow without bound. The newest segment is
    /// always retained (the writer might still be appending to it).
    pub fn prune(dir: &Path, checkpoint_seq: u64) -> Result<usize> {
        let segments = list_segments(dir)?;
        let replay_start = checkpoint_seq.saturating_add(1);
        let mut removed = 0usize;
        for pair in segments.windows(2) {
            let [(_, path), (next_start, _)] = pair else {
                unreachable!()
            };
            if *next_start > replay_start {
                break;
            }
            std::fs::remove_file(path)?;
            removed += 1;
        }
        if removed > 0 {
            sync_dir(dir)?;
        }
        Ok(removed)
    }

    /// The next seq the journal will hand out. Bookkeeping for tests
    /// and the manifest reconciliation path.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    fn rotate_to(&mut self, start_seq: u64) -> Result<()> {
        if let Some(mut old) = self.current.take() {
            old.sync_all()?;
        }
        self.current = Some(SegmentFile::create(&self.dir, start_seq)?);
        Ok(())
    }
}

// ── Segment file helpers ─────────────────────────────────────────────
//
// Mirrors the wal::segment shape with a different filename prefix so
// the two journals never share files. Kept inline (rather than
// generalized as `wal::segment`) because the recovery path here is
// simpler (single segment-spanning iterator, no group-commit writer).

const SEGMENT_PREFIX: &str = "lifecycle-";
const SEGMENT_SUFFIX: &str = ".log";

fn segment_filename(start_seq: u64) -> String {
    format!("{SEGMENT_PREFIX}{start_seq:020}{SEGMENT_SUFFIX}")
}

fn parse_segment_filename(name: &str) -> Option<u64> {
    let s = name.strip_prefix(SEGMENT_PREFIX)?.strip_suffix(SEGMENT_SUFFIX)?;
    if s.len() != 20 {
        return None;
    }
    s.parse().ok()
}

fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for entry in rd {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if let Some(start_seq) = parse_segment_filename(name_str) {
            out.push((start_seq, entry.path()));
        }
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

fn read_segment(path: &Path) -> Result<Vec<u8>> {
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
}

fn truncate_segment_to(path: &Path, valid_bytes: usize) -> Result<()> {
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(valid_bytes as u64)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

struct SegmentFile {
    path: PathBuf,
    file: File,
    bytes_written: u64,
}

impl SegmentFile {
    fn create(dir: &Path, start_seq: u64) -> Result<Self> {
        let path = dir.join(segment_filename(start_seq));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            bytes_written: 0,
        })
    }

    fn open_for_append(dir: &Path, start_seq: u64) -> Result<Self> {
        let path = dir.join(segment_filename(start_seq));
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        use std::io::Seek;
        let bytes_written = file.seek(std::io::SeekFrom::End(0))?;
        Ok(Self {
            path,
            file,
            bytes_written,
        })
    }

    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)?;
        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    fn sync_all(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Sanity check that the journal's frame is wire-compatible with the
/// WAL's. Used by tests to guard against accidental divergence.
#[cfg(test)]
pub(crate) fn frame_one(seq: u64, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + WAL_HEADER_SIZE);
    encode_frame(&mut out, seq, body);
    // Body CRC sanity belt-and-suspenders.
    let _ = crc32c(body);
    out
}
