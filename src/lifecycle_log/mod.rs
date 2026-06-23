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
//! only as the last entry of the last segment.
//!
//! # Record framing
//!
//! Frame layout (LSN slot reused as monotonic seq, body + crc32c)
//! lives in [`record`]. Segment filenames use a `lifecycle-` prefix
//! and a 20-digit start seq; listing/parsing is inlined in
//! [`journal`].

pub mod journal;
pub mod op;
pub mod record;

#[cfg(test)]
mod tests;

pub use journal::{LifecycleJournal, LifecycleRecord};
pub use op::{DropMergeTarget, LifecycleOp};
