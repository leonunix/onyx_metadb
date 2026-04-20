//! L2P B+tree.
//!
//! Each partition owns one B+tree whose keys are 8-byte big-endian LBAs
//! and whose values are 28-byte opaque blobs (Onyx's `BlockmapValue`,
//! but metadb does not interpret it). Pages are 4 KiB and live in the
//! shared page store.
//!
//! This module lands in slices:
//! 1. [`format`]: on-disk layout for leaf and internal pages; pure
//!    accessors (no algorithm, no IO).
//! 2. (future) `ops`: traversal, lookup, insert, delete, split, merge.
//! 3. (future) `scan`: range iterator over a subtree.

pub mod cache;
pub mod format;

pub use cache::PageBuf;
pub use format::{
    L2P_KEY_SIZE, L2P_VALUE_SIZE, L2pValue, LEAF_ENTRY_SIZE, MAX_INTERNAL_CHILDREN,
    MAX_INTERNAL_KEYS, MAX_LEAF_ENTRIES, init_internal, init_leaf, internal_child_at,
    internal_insert, internal_key_at, internal_key_count, internal_remove, internal_search,
    leaf_insert, leaf_key_at, leaf_key_count, leaf_remove, leaf_search, leaf_set_entry,
    leaf_value_at,
};
