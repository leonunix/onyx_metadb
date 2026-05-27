//! Lifecycle journal.
//!
//! The buffer-as-sole-journal plan moves most metadb mutations to the
//! LV2 buffer (which onyx replays through the flusher on recovery). A
//! few ops cannot be derived from buffer entries — they have no user
//! IO behind them or they need a deterministic frozen plan list at
//! apply time:
//!
//! - `TakeSnapshot` (new snapshot id + L2P root incref plan)
//! - `DropSnapshot` (page-decref list computed at plan time)
//! - `CreateVolume` (new volume entry)
//! - `DropVolume` (page-decref list across the whole volume tree)
//! - `CloneVolume` (frozen source shard roots so the clone survives a
//!   concurrent `DropSnapshot`)
//! - `PromotionChunk` + `PromotionComplete` (per-chunk incref cursor)
//! - `Discard` (range-delete intent that bypasses the buffer today)
//!
//! These live in a single append-only journal file under
//! `lifecycle_log/lifecycle-{start_seq:020}.log`. Each record carries
//! its monotonic seq, a CRC32C-protected body, and is followed by a
//! per-record fsync — group commit doesn't pay off when lifecycle ops
//! are this rare (~1 record/min in production).
//!
//! # Recovery
//!
//! `manifest.lifecycle_replay_seq` is the highest seq whose effects
//! are covered by the last manifest commit. On open, the recovery
//! driver scans the journal in seq order, replays every record with
//! `seq > manifest.lifecycle_replay_seq`, and tolerates a torn record
//! only as the last entry of the last segment (mirrors the WAL
//! convention in [`crate::wal::recovery`]).
//!
//! # Reuse
//!
//! Record framing is identical to the WAL ([`crate::wal::record`]); we
//! re-export `encode` / `decode` so both journals share one frame
//! format and one CRC32C path. Segment filename + listing helpers are
//! delegated to [`crate::wal::segment`] via a thin wrapper that swaps
//! the `wal-` prefix for `lifecycle-`.
//!
//! # Phase A status
//!
//! This module is dead code outside its own tests. Phase A pins down
//! the framing, op enum, and journal/replay surface so Phase B can
//! shadow-validate against the existing WAL path without further
//! shape changes.

pub mod journal;
pub mod op;

#[cfg(test)]
mod tests;

pub use journal::{LifecycleJournal, LifecycleRecord};
pub use op::LifecycleOp;
