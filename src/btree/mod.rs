//! Refcount B+tree (phase 6.5b).
//!
//! Single-writer, in-place, no snapshots. Each shard owns one B+tree
//! keyed by 8-byte big-endian PBAs with 4-byte big-endian `u32`
//! refcounts. L2P moved to [`crate::paged`] in phase 6.5a; `DiffEntry`
//! and `L2pValue` moved with it.
//!
//! Pages are 4 KiB and live in the shared page store. 336 entries per
//! leaf, 251 separator keys per internal node (same internal layout as
//! before the narrow-value specialization).

pub mod cache;
pub mod format;
pub mod invariants;
pub mod tree;

pub use cache::PageBuf;
pub use format::{
    L2P_KEY_SIZE, L2P_VALUE_SIZE, LEAF_ENTRY_SIZE, MAX_INTERNAL_CHILDREN, MAX_INTERNAL_KEYS,
    MAX_LEAF_ENTRIES, init_internal, init_leaf, internal_child_at, internal_insert,
    internal_key_at, internal_key_count, internal_remove, internal_search,
    internal_set_child, internal_set_first_child, leaf_insert, leaf_key_at, leaf_key_count,
    leaf_remove, leaf_search, leaf_set_entry, leaf_value_at,
};
pub use tree::{BTree, RangeIter};
