//! Paged-array dedup-reverse store.
//!
//! Replaces the LSM-backed dedup_reverse. The reverse
//! index has exactly one access pattern in onyx — `cleanup_dead_pba`
//! looking up every hash registered for a given PBA — so a sparse
//! paged array indexed by PBA collapses the per-call cost from "scan
//! every dedup_reverse SST whose key range intersects the PBA prefix"
//! to a single page lookup.
//!
//! # v1 simplification: single hash per PBA
//!
//! dedup-miss registration (the only writer) writes a fresh PBA each
//! time, so the steady-state mapping is one hash per PBA. The current
//! schema does support multiple `(pba, hash)` rows for the same PBA in
//! theory (hash-collision after PBA reuse) but the metadb / onyx test
//! suite does not exercise that case. v1 stores a single inline hash
//! per PBA slot and **returns `Corruption` on a put for a PBA that
//! already holds a different non-zero hash**. Stage 2.x adds overflow
//! pages so the structure can carry an unbounded list per PBA.
//!
//! # Concurrency
//!
//! One `PagedReverse` per database (no sharding — `cleanup_dead_pba`
//! is far off the hot path, fan-out parallelism is not worth the
//! per-shard accounting overhead). Internal `Mutex` covers the
//! page-table allocation; data-page reads + writes go through the
//! shared `PageCache`.

pub mod array;
pub use array::{ENTRIES_PER_PAGE, PagedReverse};
