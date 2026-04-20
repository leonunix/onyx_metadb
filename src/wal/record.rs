//! WAL record framing.
//!
//! # Record layout
//!
//! ```text
//! offset  size  field
//!   0     8     lsn
//!   8     4     length   (byte length of body)
//!  12     4     crc32c   (of body bytes [0..length])
//!  16     N     body
//! ```
//!
//! Records are packed back-to-back with no alignment padding — segment
//! rotation, fsync, and group commit live a layer above. This module only
//! knows how to encode one record onto a byte sink and how to iterate
//! records out of a byte buffer.
//!
//! # Torn-write handling
//!
//! On decode, a short buffer returns [`DecodeError::HeaderTruncated`] or
//! [`DecodeError::BodyTruncated`], and a CRC mismatch returns
//! [`DecodeError::ChecksumMismatch`]. These are not automatically fatal:
//! the recovery layer uses them to distinguish a torn tail (normal after
//! crash) from mid-log corruption (fatal). [`WalRecordIter`] stops at the
//! first error and exposes it via [`WalRecordIter::stopped`].

use crate::types::Lsn;

/// Total size of the per-record header.
pub const WAL_HEADER_SIZE: usize = 16;

/// Upper bound on body length we're willing to decode. Records beyond
/// this are treated as obviously-bad headers — recovery stops rather
/// than attempting to read garbage bytes. Comfortably above the default
/// `group_commit_max_batch_bytes` of 4 MiB.
pub const WAL_MAX_BODY: usize = 16 * 1024 * 1024;

const OFF_LSN: usize = 0;
const OFF_LENGTH: usize = 8;
const OFF_CRC: usize = 12;

const _: () = {
    assert!(OFF_LSN + 8 == OFF_LENGTH);
    assert!(OFF_LENGTH + 4 == OFF_CRC);
    assert!(OFF_CRC + 4 == WAL_HEADER_SIZE);
};

/// Decoded record, borrowing its body from the underlying buffer.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WalRecordRef<'a> {
    pub lsn: Lsn,
    pub body: &'a [u8],
}

/// Reasons a decode attempt can fail. None of these are necessarily
/// fatal at the WAL level; recovery interprets them in context.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Fewer than [`WAL_HEADER_SIZE`] bytes remaining — cannot even read
    /// the header.
    HeaderTruncated { available: usize },
    /// Body field declares `expected` bytes but only `available` are left
    /// in the buffer.
    BodyTruncated { expected: u32, available: u32 },
    /// Body length exceeds [`WAL_MAX_BODY`]; header is presumed corrupt.
    LengthOverflow { length: u32 },
    /// CRC of body does not match the header's CRC field.
    ChecksumMismatch { expected: u32, actual: u32 },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeaderTruncated { available } => {
                write!(
                    f,
                    "header truncated ({available}B available, need {WAL_HEADER_SIZE}B)"
                )
            }
            Self::BodyTruncated {
                expected,
                available,
            } => {
                write!(
                    f,
                    "body truncated (expected {expected}B, have {available}B)"
                )
            }
            Self::LengthOverflow { length } => {
                write!(f, "length {length} exceeds WAL_MAX_BODY {WAL_MAX_BODY}")
            }
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "crc mismatch (expected {expected:#010x}, got {actual:#010x})"
                )
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Append a record to `out` and return the number of bytes appended
/// (always `WAL_HEADER_SIZE + body.len()`).
///
/// Panics if `body.len() > WAL_MAX_BODY`; use [`try_encode`] for a
/// non-panicking variant that surfaces the error to the caller.
pub fn encode(out: &mut Vec<u8>, lsn: Lsn, body: &[u8]) -> usize {
    try_encode(out, lsn, body).expect("wal record body exceeds WAL_MAX_BODY")
}

/// Non-panicking variant of [`encode`]. Returns `Err(length)` if the body
/// is too long.
pub fn try_encode(out: &mut Vec<u8>, lsn: Lsn, body: &[u8]) -> Result<usize, u32> {
    if body.len() > WAL_MAX_BODY {
        return Err(body.len() as u32);
    }
    let len = body.len() as u32;
    let crc = crc32c::crc32c(body);
    out.reserve(WAL_HEADER_SIZE + body.len());
    out.extend_from_slice(&lsn.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(body);
    Ok(WAL_HEADER_SIZE + body.len())
}

/// Decode a single record at the head of `buf`. On success returns the
/// decoded record and the number of bytes consumed; on failure, returns
/// the reason without advancing.
pub fn decode(buf: &[u8]) -> Result<(WalRecordRef<'_>, usize), DecodeError> {
    if buf.len() < WAL_HEADER_SIZE {
        return Err(DecodeError::HeaderTruncated {
            available: buf.len(),
        });
    }
    let lsn = u64::from_le_bytes(buf[OFF_LSN..OFF_LSN + 8].try_into().unwrap());
    let length = u32::from_le_bytes(buf[OFF_LENGTH..OFF_LENGTH + 4].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(buf[OFF_CRC..OFF_CRC + 4].try_into().unwrap());
    if length as usize > WAL_MAX_BODY {
        return Err(DecodeError::LengthOverflow { length });
    }
    let body_end = WAL_HEADER_SIZE + length as usize;
    if buf.len() < body_end {
        return Err(DecodeError::BodyTruncated {
            expected: length,
            available: (buf.len() - WAL_HEADER_SIZE) as u32,
        });
    }
    let body = &buf[WAL_HEADER_SIZE..body_end];
    let actual = crc32c::crc32c(body);
    if stored_crc != actual {
        return Err(DecodeError::ChecksumMismatch {
            expected: stored_crc,
            actual,
        });
    }
    Ok((WalRecordRef { lsn, body }, body_end))
}

/// Forward-only iterator that decodes back-to-back records from a
/// buffer. Stops at the first decode error; the reason is available via
/// [`stopped`](Self::stopped) and the offset at which it stopped via
/// [`consumed`](Self::consumed).
pub struct WalRecordIter<'a> {
    buf: &'a [u8],
    pos: usize,
    stopped: Option<DecodeError>,
}

impl<'a> WalRecordIter<'a> {
    /// Wrap a buffer for iteration. Does not allocate.
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            stopped: None,
        }
    }

    /// Bytes consumed so far. After the iterator stops, this is the offset
    /// of the first un-decodable byte; the caller should treat the buffer
    /// up to this offset as the authoritative durable tail.
    pub fn consumed(&self) -> usize {
        self.pos
    }

    /// Reason the iterator stopped, if any. `None` means end of buffer was
    /// reached cleanly.
    pub fn stopped(&self) -> Option<DecodeError> {
        self.stopped
    }
}

impl<'a> Iterator for WalRecordIter<'a> {
    type Item = WalRecordRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped.is_some() {
            return None;
        }
        if self.pos >= self.buf.len() {
            return None;
        }
        match decode(&self.buf[self.pos..]) {
            Ok((rec, consumed)) => {
                self.pos += consumed;
                Some(rec)
            }
            Err(e) => {
                self.stopped = Some(e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_body_round_trip() {
        let mut buf = Vec::new();
        let n = encode(&mut buf, 7, &[]);
        assert_eq!(n, WAL_HEADER_SIZE);
        assert_eq!(buf.len(), WAL_HEADER_SIZE);

        let (rec, consumed) = decode(&buf).unwrap();
        assert_eq!(rec.lsn, 7);
        assert_eq!(rec.body.len(), 0);
        assert_eq!(consumed, WAL_HEADER_SIZE);
    }

    #[test]
    fn single_record_round_trip() {
        let mut buf = Vec::new();
        encode(&mut buf, 42, b"hello world");
        let (rec, consumed) = decode(&buf).unwrap();
        assert_eq!(rec.lsn, 42);
        assert_eq!(rec.body, b"hello world");
        assert_eq!(consumed, WAL_HEADER_SIZE + 11);
    }

    #[test]
    fn many_records_iteration() {
        let mut buf = Vec::new();
        for i in 0..64u64 {
            let body = format!("record body {i}");
            encode(&mut buf, i + 1, body.as_bytes());
        }
        let mut iter = WalRecordIter::new(&buf);
        for i in 0..64u64 {
            let rec = iter.next().unwrap();
            assert_eq!(rec.lsn, i + 1);
            assert_eq!(rec.body, format!("record body {i}").as_bytes());
        }
        assert!(iter.next().is_none());
        assert_eq!(iter.stopped(), None);
        assert_eq!(iter.consumed(), buf.len());
    }

    #[test]
    fn decode_fails_on_short_header() {
        let err = decode(&[0u8; 10]).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::HeaderTruncated { available: 10 }
        ));
    }

    #[test]
    fn decode_fails_on_short_body() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"0123456789");
        // Drop the last 3 body bytes.
        let short = &buf[..buf.len() - 3];
        match decode(short).unwrap_err() {
            DecodeError::BodyTruncated {
                expected,
                available,
            } => {
                assert_eq!(expected, 10);
                assert_eq!(available, 7);
            }
            e => panic!("{e}"),
        }
    }

    #[test]
    fn decode_fails_on_crc_mismatch() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"abcdef");
        // Corrupt a body byte without updating the CRC header.
        buf[WAL_HEADER_SIZE] ^= 0x01;
        match decode(&buf).unwrap_err() {
            DecodeError::ChecksumMismatch { .. } => {}
            e => panic!("{e}"),
        }
    }

    #[test]
    fn decode_fails_on_length_overflow() {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        // Write an insane length field.
        buf[OFF_LENGTH..OFF_LENGTH + 4].copy_from_slice(&((WAL_MAX_BODY as u32) + 1).to_le_bytes());
        match decode(&buf).unwrap_err() {
            DecodeError::LengthOverflow { length } => {
                assert_eq!(length as usize, WAL_MAX_BODY + 1);
            }
            e => panic!("{e}"),
        }
    }

    #[test]
    fn iterator_stops_on_torn_tail() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"first");
        encode(&mut buf, 2, b"second");
        let good_bytes = buf.len();
        // Append a partial (torn) third record.
        let mut torn = Vec::new();
        encode(&mut torn, 3, b"third-record-payload-here");
        buf.extend_from_slice(&torn[..torn.len() / 2]); // truncate body

        let mut iter = WalRecordIter::new(&buf);
        let r1 = iter.next().unwrap();
        assert_eq!(r1.lsn, 1);
        let r2 = iter.next().unwrap();
        assert_eq!(r2.lsn, 2);
        assert!(iter.next().is_none());
        assert!(matches!(
            iter.stopped().unwrap(),
            DecodeError::BodyTruncated { .. }
        ));
        assert_eq!(iter.consumed(), good_bytes);
    }

    #[test]
    fn iterator_stops_on_single_bit_flip_in_middle() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"first");
        let after_first = buf.len();
        encode(&mut buf, 2, b"second-corrupt-me");
        encode(&mut buf, 3, b"third");
        // Flip a bit inside the second record's body.
        let bit_offset = after_first + WAL_HEADER_SIZE + 2;
        buf[bit_offset] ^= 0x01;

        let mut iter = WalRecordIter::new(&buf);
        let r1 = iter.next().unwrap();
        assert_eq!(r1.lsn, 1);
        assert!(iter.next().is_none());
        assert!(matches!(
            iter.stopped().unwrap(),
            DecodeError::ChecksumMismatch { .. }
        ));
        // Third record is unreachable after the torn second.
        assert_eq!(iter.consumed(), after_first);
    }

    #[test]
    fn try_encode_rejects_oversized() {
        let mut buf = Vec::new();
        let huge = vec![0u8; WAL_MAX_BODY + 1];
        let err = try_encode(&mut buf, 1, &huge).unwrap_err();
        assert_eq!(err as usize, WAL_MAX_BODY + 1);
        assert!(buf.is_empty(), "must not write partial record");
    }

    #[test]
    fn encode_returns_bytes_written() {
        let mut buf = Vec::new();
        let n = encode(&mut buf, 1, b"abcdef");
        assert_eq!(n, WAL_HEADER_SIZE + 6);
        assert_eq!(buf.len(), n);
    }

    #[test]
    fn crc_is_computed_over_body_only() {
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        encode(&mut buf1, 111, b"payload");
        encode(&mut buf2, 222, b"payload");
        // Same body -> same CRC regardless of LSN.
        let crc1 = &buf1[OFF_CRC..OFF_CRC + 4];
        let crc2 = &buf2[OFF_CRC..OFF_CRC + 4];
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn empty_buffer_iterator_is_clean() {
        let mut iter = WalRecordIter::new(&[]);
        assert!(iter.next().is_none());
        assert_eq!(iter.stopped(), None);
        assert_eq!(iter.consumed(), 0);
    }

    #[test]
    fn header_size_matches_constant() {
        assert_eq!(WAL_HEADER_SIZE, 16);
    }

    #[test]
    fn decode_error_display_is_informative() {
        let e = DecodeError::ChecksumMismatch {
            expected: 0xDEAD_BEEF,
            actual: 0xCAFE_BABE,
        };
        let s = format!("{e}");
        assert!(s.contains("crc"));
        assert!(s.contains("deadbeef"));
    }
}
