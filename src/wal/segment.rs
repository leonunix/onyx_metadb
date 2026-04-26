//! Single WAL segment file.
//!
//! A segment is a flat, append-only file of back-to-back WAL records (see
//! [`super::record`]). The filename is `wal-{start_lsn:020}.log`, so a
//! lexicographic sort of the directory listing matches LSN order exactly.
//!
//! This module knows how to create, open, append to, and enumerate
//! segments. It does not know about LSN assignment, group commit, or
//! recovery — those live in [`super::writer`] and (later) a recovery
//! module.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::types::Lsn;

/// Canonical segment filename for the given starting LSN. 20-digit
/// zero-padded decimal so `u64::MAX` still fits in the fixed width.
pub fn segment_filename(start_lsn: Lsn) -> String {
    format!("wal-{start_lsn:020}.log")
}

/// Inverse of [`segment_filename`]. Returns `None` for names that don't
/// match the pattern.
pub fn parse_segment_filename(name: &str) -> Option<Lsn> {
    let s = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if s.len() != 20 {
        return None;
    }
    s.parse().ok()
}

/// Enumerate WAL segments in `dir`, sorted by starting LSN ascending.
/// Non-segment files (including `.tmp`, `.lock`, etc.) are ignored.
pub fn list_segments(dir: &Path) -> Result<Vec<(Lsn, PathBuf)>> {
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
        if let Some(start_lsn) = parse_segment_filename(name_str) {
            out.push((start_lsn, entry.path()));
        }
    }
    out.sort_by_key(|(lsn, _)| *lsn);
    Ok(out)
}

/// Delete WAL segments that are wholly covered by `checkpoint_lsn`.
///
/// Recovery starts at `checkpoint_lsn + 1`, and segment filenames give us the
/// first LSN in each segment. A segment can be deleted only when the next
/// segment starts no later than that replay start, proving this whole segment
/// contains checkpointed records. The newest segment is always retained because
/// it may still be open by the writer.
pub fn prune_segments(dir: &Path, checkpoint_lsn: Lsn) -> Result<usize> {
    let segments = list_segments(dir)?;
    let replay_start = checkpoint_lsn.saturating_add(1);
    let mut removed = 0usize;

    for pair in segments.windows(2) {
        let [(_, path), (next_start, _)] = pair else {
            unreachable!("windows(2) always yields pairs");
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

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

/// Append-only WAL segment owned by the writer thread.
pub struct SegmentFile {
    path: PathBuf,
    file: File,
    start_lsn: Lsn,
    bytes_written: u64,
}

impl SegmentFile {
    /// Create a new segment file. The parent directory must exist. Fails
    /// if the target file already exists.
    pub fn create(dir: &Path, start_lsn: Lsn) -> Result<Self> {
        let path = dir.join(segment_filename(start_lsn));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            start_lsn,
            bytes_written: 0,
        })
    }

    /// Open an existing segment for further append. Seeks to the end of
    /// the file; subsequent `append` calls continue past the existing
    /// content.
    pub fn open_for_append(dir: &Path, start_lsn: Lsn) -> Result<Self> {
        let path = dir.join(segment_filename(start_lsn));
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        use std::io::Seek;
        let bytes_written = file.seek(std::io::SeekFrom::End(0))?;
        Ok(Self {
            path,
            file,
            start_lsn,
            bytes_written,
        })
    }

    /// Append raw bytes (already-encoded WAL records) to the tail.
    pub fn append(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)?;
        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    /// `fdatasync` the segment.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// `fsync` the segment (content + metadata).
    pub fn sync_all(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// LSN stamped in the segment's filename — the LSN of the first
    /// record this segment holds (once a record is written).
    pub fn start_lsn(&self) -> Lsn {
        self.start_lsn
    }

    /// Total bytes this process has appended since the segment was
    /// opened, plus any pre-existing bytes when opened for append.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Path on disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the segment file at `path` to `len` bytes. Used by
    /// recovery to discard a torn tail before the next writer resumes.
    pub fn truncate_to(path: &Path, len: u64) -> Result<()> {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(len)?;
        file.sync_all()?;
        Ok(())
    }
}

/// Read a whole segment file into memory. For small-to-medium segments
/// this is the simplest way to hand the buffer to
/// [`super::record::WalRecordIter`]; larger segments may want a streaming
/// reader in a future iteration.
pub fn read_segment(path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let meta = file.metadata()?;
    let mut buf = Vec::with_capacity(meta.len() as usize);
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn filename_round_trip() {
        assert_eq!(segment_filename(1), "wal-00000000000000000001.log");
        assert_eq!(segment_filename(u64::MAX), "wal-18446744073709551615.log",);
        assert_eq!(
            parse_segment_filename("wal-00000000000000000001.log"),
            Some(1)
        );
        assert_eq!(
            parse_segment_filename("wal-18446744073709551615.log"),
            Some(u64::MAX),
        );
        assert_eq!(parse_segment_filename("wal-001.log"), None); // too short
        assert_eq!(parse_segment_filename("nope.log"), None);
        assert_eq!(parse_segment_filename("wal-00000000000000000001"), None);
    }

    #[test]
    fn create_then_open_preserves_bytes() {
        let dir = TempDir::new().unwrap();
        let mut s = SegmentFile::create(dir.path(), 1).unwrap();
        s.append(b"hello ").unwrap();
        s.append(b"world").unwrap();
        s.sync_all().unwrap();
        assert_eq!(s.bytes_written(), 11);
        drop(s);

        let mut s = SegmentFile::open_for_append(dir.path(), 1).unwrap();
        assert_eq!(s.bytes_written(), 11);
        s.append(b"!").unwrap();
        s.sync_all().unwrap();
        drop(s);

        let path = dir.path().join(segment_filename(1));
        let content = read_segment(&path).unwrap();
        assert_eq!(content, b"hello world!");
    }

    #[test]
    fn create_fails_if_exists() {
        let dir = TempDir::new().unwrap();
        SegmentFile::create(dir.path(), 1).unwrap();
        assert!(SegmentFile::create(dir.path(), 1).is_err());
    }

    #[test]
    fn list_segments_sorted() {
        let dir = TempDir::new().unwrap();
        for lsn in [7, 1, 42, 3] {
            SegmentFile::create(dir.path(), lsn).unwrap();
        }
        // Unrelated files are ignored.
        std::fs::write(dir.path().join("readme.txt"), b"ignore me").unwrap();
        std::fs::write(dir.path().join("wal-bad.log"), b"not a segment").unwrap();

        let segs = list_segments(dir.path()).unwrap();
        let lsns: Vec<_> = segs.iter().map(|(l, _)| *l).collect();
        assert_eq!(lsns, vec![1, 3, 7, 42]);
    }

    #[test]
    fn list_segments_missing_dir_is_empty() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("does_not_exist");
        let segs = list_segments(&missing).unwrap();
        assert!(segs.is_empty());
    }

    #[test]
    fn prune_segments_keeps_segment_that_may_contain_replay_start() {
        let dir = TempDir::new().unwrap();
        for lsn in [1, 10, 20, 30] {
            SegmentFile::create(dir.path(), lsn).unwrap();
        }

        let removed = prune_segments(dir.path(), 18).unwrap();
        assert_eq!(removed, 1);
        let lsns: Vec<_> = list_segments(dir.path())
            .unwrap()
            .into_iter()
            .map(|(lsn, _)| lsn)
            .collect();
        assert_eq!(lsns, vec![10, 20, 30]);

        let removed = prune_segments(dir.path(), 29).unwrap();
        assert_eq!(removed, 2);
        let lsns: Vec<_> = list_segments(dir.path())
            .unwrap()
            .into_iter()
            .map(|(lsn, _)| lsn)
            .collect();
        assert_eq!(lsns, vec![30]);
    }

    #[test]
    fn bytes_written_advances() {
        let dir = TempDir::new().unwrap();
        let mut s = SegmentFile::create(dir.path(), 1).unwrap();
        assert_eq!(s.bytes_written(), 0);
        s.append(&[0xAA; 100]).unwrap();
        assert_eq!(s.bytes_written(), 100);
        s.append(&[0xBB; 200]).unwrap();
        assert_eq!(s.bytes_written(), 300);
    }
}
