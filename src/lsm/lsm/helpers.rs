use super::*;

// Silence an otherwise-useful but currently-unused warning on the
// DedupOp import path; `use` is needed to keep the `_` pattern matching
// from turning into a dead-code warning if this file ever lands without
// `flush_memtable` being exercised.
#[allow(dead_code)]
pub(super) fn key_has_prefix(key: &Hash32, prefix: &[u8]) -> bool {
    key.len() >= prefix.len() && &key[..prefix.len()] == prefix
}

/// Whether keys starting with `prefix` could fall within `[min, max]`.
/// Conservative: returns true unless we can prove no match exists.
pub(super) fn prefix_might_match_range(min: &Hash32, max: &Hash32, prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return true;
    }
    // Any key starting with `prefix` lies in `[prefix || 0x00..,
    // prefix || 0xFF..]` inclusive. Overlap with `[min, max]` means
    // `prefix_upper >= min && prefix_lower <= max`.
    let mut lower = [0u8; 32];
    lower[..prefix.len()].copy_from_slice(prefix);
    let mut upper = [0xFFu8; 32];
    upper[..prefix.len()].copy_from_slice(prefix);
    upper.as_slice() >= min.as_slice() && lower.as_slice() <= max.as_slice()
}

#[allow(dead_code)]
pub(super) fn _dedup_op_import_anchor(op: DedupOp) -> DedupOp {
    op
}

// Convert a private `MetaDbError::InvalidArgument` into a more specific
// message when a caller misuses `Lsm` APIs. Kept as a helper to centralise
// the wording.
#[allow(dead_code)]
pub(super) fn invalid(msg: &str) -> MetaDbError {
    MetaDbError::InvalidArgument(msg.to_string())
}
