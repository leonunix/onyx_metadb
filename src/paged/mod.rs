//! Paged L2P radix tree.
//!
//! See [`format`] for the on-disk layout and [`tree`] for the algorithm.
//! The paged tree replaces the B+tree L2P indexes for Onyx's 4 KiB LBA
//! mapping: keys are dense u64 LBAs so we use them as radix-tree indices
//! directly, saving the key-storage and key-comparison cost of a B+tree.

pub mod cache;
pub mod format;
pub mod invariants;
pub mod leaf_compact;
pub mod tree;

pub use format::{INDEX_FANOUT, L2pValue, LEAF_ENTRY_COUNT, LEAF_VALUE_SIZE, MAX_INDEX_LEVEL};
pub use tree::{DeleteOutcome, DiffEntry, InsertOutcome, PagedL2p, PagedRangeIter};
