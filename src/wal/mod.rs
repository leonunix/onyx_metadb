//! Write-ahead log.
//!
//! Responsibilities, in layers from innermost to outermost:
//! 1. [`record`]: framing of one WAL record — encode, decode, CRC. Knows
//!    nothing about files, fsync, or LSN allocation.
//! 2. (phase 1 slice 3) `segment`: append-only segment files, rotation,
//!    torn-tail detection on open.
//! 3. (phase 1 slice 4) `writer`: group commit, LSN issuance, fsync.
//!
//! This file currently only re-exports the record codec. The higher
//! layers land in subsequent commits as per
//! [`docs/ROADMAP.md`](../../docs/ROADMAP.md).

pub mod record;

pub use record::{
    DecodeError, WAL_HEADER_SIZE, WAL_MAX_BODY, WalRecordIter, WalRecordRef, decode, encode,
};
