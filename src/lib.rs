//! onyx-metadb: embedded metadata engine for Onyx Storage.
//!
//! Two workload-specialized indexes in a single engine, sharing one WAL:
//! - Sharded COW B+tree for L2P (per-volume, fixed 8B key → 28B value)
//! - Fixed-record LSM for global dedup (32B hash → 27B entry)
//!
//! Public API, recovery semantics, and snapshot model are documented in
//! `docs/DESIGN.md`. Implementation phases are in `docs/ROADMAP.md`.

#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, rust_2018_idioms, unreachable_pub)]

// Stub — real modules land in Phase 1. See docs/ROADMAP.md.
