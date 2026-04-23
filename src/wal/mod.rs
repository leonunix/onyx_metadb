//! Write-ahead log.
//!
//! Layers from innermost to outermost:
//!
//! 1. [`record`]: encoding and decoding of one WAL record — header + CRC
//!    + body. Knows nothing about files or fsync.
//! 2. [`segment`]: append-only segment files, filename <-> LSN mapping,
//!    directory enumeration.
//! 3. [`writer`]: [`Wal`] facade with group commit, LSN assignment,
//!    rotation, and fault-injection hooks.
//!
//! Recovery (replay WAL forward from a checkpoint) lands in a later
//! phase and will live alongside these layers rather than being
//! embedded inside any of them.

pub mod op;
pub mod record;
pub mod segment;
pub mod writer;

pub use op::{
    TAG_DECREF, TAG_DEDUP_DELETE, TAG_DEDUP_PUT, TAG_INCREF, TAG_L2P_DELETE, TAG_L2P_PUT,
    WAL_BODY_SCHEMA_VERSION, WalOp, decode_body, encode_body,
};
pub use record::{
    DecodeError, WAL_HEADER_SIZE, WAL_MAX_BODY, WalRecordIter, WalRecordRef, decode, encode,
};
pub use segment::{
    SegmentFile, list_segments, parse_segment_filename, read_segment, segment_filename,
};
pub use writer::Wal;

use crate::error::Result;
use crate::types::Lsn;
use std::path::Path;

/// Convenience: drain every WAL record from every segment in `dir`,
/// ascending by LSN. Stops at the first decode failure and returns the
/// records up to that point (a torn tail in the final segment is normal
/// post-crash; a decode failure in a non-final segment indicates
/// corruption and will be surfaced as [`WalReplay::mid_log_error`] in a
/// later iteration).
///
/// For now this is a blunt test helper; recovery will replace it with a
/// version that reports torn-tail vs. mid-log corruption to the caller.
pub fn read_all(dir: &Path) -> Result<Vec<(Lsn, Vec<u8>)>> {
    let mut out = Vec::new();
    for (_, path) in list_segments(dir)? {
        let buf = read_segment(&path)?;
        let mut iter = WalRecordIter::new(&buf);
        for rec in iter.by_ref() {
            out.push((rec.lsn, rec.body.to_vec()));
        }
        // Ignore `iter.stopped()` for now; recovery will distinguish
        // "torn tail in last segment" from "mid-log corruption".
    }
    Ok(out)
}
