//! Refcount B+tree page formats: leaf + internal.
//!
//! Value type is [`RcEntry`] = `(rc: u32, birth_lsn: u64)` = 12 B. The
//! `birth_lsn` lets [`crate::db::apply_l2p_remap`] decide whether a
//! still-live snapshot might pin the pba (birth/death LSN suppression,
//! replacing the legacy "leaf-rc-suppress" rule). L2P lives in
//! [`crate::paged::PagedL2p`] and never touches this tree.
//!
//! # Leaf layout (within the 4032 B payload)
//!
//! ```text
//! entry 0:   [key:8B][rc:4B][birth_lsn:8B]
//! entry 1:   [key:8B][rc:4B][birth_lsn:8B]
//! ...
//! entry N-1: [key:8B][rc:4B][birth_lsn:8B]
//! ```
//!
//! Each entry is 20 B; the payload fits [`MAX_LEAF_ENTRIES`] = 201
//! entries with 12 B slack at the tail.
//!
//! Keys are stored big-endian so a byte-wise `memcmp` over raw payload
//! bytes would still match numeric order — useful for the verifier
//! tool and for any future zero-copy reader that wants to binary-search
//! without decoding each key.
//!
//! # Internal layout (within the 4032 B payload)
//!
//! Fixed-offset split: all keys up front, all child pointers behind
//! them.
//!
//! ```text
//!     0:                 key[0]     (8B)
//!     8:                 key[1]     (8B)
//!    ...
//!  8*K:                  key[K-1]   (8B)  where K = MAX_INTERNAL_KEYS = 251
//!  KEYS_REGION (2008):   child[0]   (8B)
//!  +8:                   child[1]   (8B)
//!  ...
//!  +8*(MAX_CHILDREN-1):  child[MAX-1] (8B)
//! ```
//!
//! An internal page with `key_count = N` uses the first `N` keys
//! (offsets 0..8N) and the first `N+1` children (from
//! `KEYS_REGION_SIZE` onwards). The remaining slots are unused and
//! zero-filled — CRC still covers them so any accidental write to an
//! unused slot is caught.
//!
//! # Separator-key semantics
//!
//! Following the standard B+tree convention, `keys[i]` is the *first
//! key present in the subtree rooted at `children[i+1]`*:
//!
//! - `children[0]` holds keys strictly less than `keys[0]`.
//! - `children[i+1]` holds keys `k` with `keys[i] <= k < keys[i+1]`.
//! - `children[key_count]` holds keys `>= keys[key_count-1]`.
//!
//! This is what [`internal_search`] returns: the index of the child to
//! descend into.

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::types::{Lsn, PageId};

/// Size of a key on disk (u64 PBA stored big-endian).
pub const L2P_KEY_SIZE: usize = 8;

/// Size of the leaf value on disk. 4-byte big-endian `u32` refcount
/// followed by 8-byte big-endian `Lsn` birth_lsn — see [`RcEntry`].
pub const L2P_VALUE_SIZE: usize = 12;

/// Byte offset of the rc field inside a leaf value.
const RC_OFFSET: usize = 0;
/// Byte offset of the birth_lsn field inside a leaf value.
const BIRTH_LSN_OFFSET: usize = 4;

/// Bytes per leaf entry (key + value).
pub const LEAF_ENTRY_SIZE: usize = L2P_KEY_SIZE + L2P_VALUE_SIZE;

/// Maximum entries that fit in one leaf page. 201 at 4 KiB page size
/// with a 64 B header and 20 B entries (12 B slack at tail).
pub const MAX_LEAF_ENTRIES: usize = PAGE_PAYLOAD_SIZE / LEAF_ENTRY_SIZE;

/// Value stored at each refcount-tree leaf slot.
///
/// `rc` is the running tally of distinct (vol, lba, value) tuples that
/// reference this pba (audit semantics). `birth_lsn` is the LSN at which
/// the entry transitioned from rc=0 to rc=1 — i.e. when the current
/// content of this pba was first incref'd. Birth/death LSN suppression
/// in [`crate::db::apply_l2p_remap`] uses this to decide whether a
/// concurrent snapshot might still pin the pba.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RcEntry {
    pub rc: u32,
    pub birth_lsn: Lsn,
}

impl RcEntry {
    /// Sentinel for "no entry": rc=0, birth_lsn=0. Returned by callers
    /// that fold a missing-key lookup into the same arithmetic path.
    pub const ZERO: Self = Self {
        rc: 0,
        birth_lsn: 0,
    };
}

impl std::fmt::Display for RcEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(rc={}, birth_lsn={})", self.rc, self.birth_lsn)
    }
}

/// Maximum separator keys per internal page. 4032 B payload fits 251
/// keys and 252 children (16 B per (key, child) pair plus 8 B for
/// child[0], with 16 B slack).
pub const MAX_INTERNAL_KEYS: usize = 251;

/// Maximum child pointers per internal page.
pub const MAX_INTERNAL_CHILDREN: usize = MAX_INTERNAL_KEYS + 1;

/// Byte span reserved for the keys region inside an internal page's
/// payload. Starts at payload offset 0.
const KEYS_REGION_SIZE: usize = MAX_INTERNAL_KEYS * L2P_KEY_SIZE;

/// Byte offset at which the children region begins inside the internal
/// payload.
const CHILDREN_REGION_OFFSET: usize = KEYS_REGION_SIZE;

const _: () = {
    assert!(PAGE_PAYLOAD_SIZE == 4032);
    assert!(LEAF_ENTRY_SIZE == 20);
    assert!(MAX_LEAF_ENTRIES == 201);
    assert!(LEAF_ENTRY_SIZE * MAX_LEAF_ENTRIES <= PAGE_PAYLOAD_SIZE);
    assert!(MAX_INTERNAL_KEYS == 251);
    assert!(MAX_INTERNAL_CHILDREN == 252);
    assert!(MAX_INTERNAL_KEYS * 8 + MAX_INTERNAL_CHILDREN * 8 <= PAGE_PAYLOAD_SIZE);
    // Sanity: entries region must fit in the payload.
    assert!(CHILDREN_REGION_OFFSET + MAX_INTERNAL_CHILDREN * 8 <= PAGE_PAYLOAD_SIZE);
    // Layout assumption: PAGE_SIZE = PAGE_HEADER_SIZE + PAGE_PAYLOAD_SIZE.
    assert!(PAGE_SIZE == PAGE_HEADER_SIZE + PAGE_PAYLOAD_SIZE);
};

// -------- Leaf accessors ---------------------------------------------------

/// Initialize a fresh empty leaf. `generation` is stamped on the
/// header; caller must still `seal()` before writing to disk.
pub fn init_leaf(page: &mut Page, generation: Lsn) {
    page.bytes_mut().fill(0);
    page.write_header(&PageHeader::new(PageType::L2pLeaf, generation));
}

/// Number of entries currently in the leaf.
pub fn leaf_key_count(page: &Page) -> usize {
    page.key_count() as usize
}

/// Key at leaf index `i`.
pub fn leaf_key_at(page: &Page, i: usize) -> u64 {
    debug_assert!(i < leaf_key_count(page), "leaf_key_at: out of range");
    let off = i * LEAF_ENTRY_SIZE;
    let p = page.payload();
    u64::from_be_bytes(p[off..off + L2P_KEY_SIZE].try_into().unwrap())
}

/// Value at leaf index `i`. Each field stored big-endian so a byte-wise
/// memcmp over the value bytes mirrors numeric order on the rc field
/// (useful for the verifier).
pub fn leaf_value_at(page: &Page, i: usize) -> RcEntry {
    debug_assert!(i < leaf_key_count(page), "leaf_value_at: out of range");
    let off = i * LEAF_ENTRY_SIZE + L2P_KEY_SIZE;
    let p = page.payload();
    let rc = u32::from_be_bytes(p[off + RC_OFFSET..off + RC_OFFSET + 4].try_into().unwrap());
    let birth_lsn = u64::from_be_bytes(
        p[off + BIRTH_LSN_OFFSET..off + BIRTH_LSN_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    RcEntry { rc, birth_lsn }
}

/// Overwrite the entry at index `i` without checking or bumping
/// `key_count`. Use [`leaf_insert`] if you want shifting plus count
/// bump; use this for in-place value updates on an existing key.
pub fn leaf_set_entry(page: &mut Page, i: usize, key: u64, value: RcEntry) {
    debug_assert!(i < MAX_LEAF_ENTRIES, "leaf_set_entry: index over capacity");
    let off = i * LEAF_ENTRY_SIZE;
    let p = page.payload_mut();
    p[off..off + L2P_KEY_SIZE].copy_from_slice(&key.to_be_bytes());
    let v_off = off + L2P_KEY_SIZE;
    p[v_off + RC_OFFSET..v_off + RC_OFFSET + 4].copy_from_slice(&value.rc.to_be_bytes());
    p[v_off + BIRTH_LSN_OFFSET..v_off + BIRTH_LSN_OFFSET + 8]
        .copy_from_slice(&value.birth_lsn.to_be_bytes());
}

/// Binary search for `key` in the leaf. Returns `Ok(i)` on a hit,
/// `Err(i)` on a miss where `i` is the insertion position.
pub fn leaf_search(page: &Page, key: u64) -> std::result::Result<usize, usize> {
    let n = leaf_key_count(page);
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let k = leaf_key_at(page, mid);
        match k.cmp(&key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(lo)
}

/// Insert `(key, value)` at position `pos`, shifting entries at
/// `pos..count` one slot to the right. Bumps `key_count` by one.
/// Returns [`MetaDbError::InvalidArgument`] if the leaf is full or
/// `pos > count`.
pub fn leaf_insert(page: &mut Page, pos: usize, key: u64, value: RcEntry) -> Result<()> {
    let n = leaf_key_count(page);
    if n >= MAX_LEAF_ENTRIES {
        return Err(MetaDbError::InvalidArgument(
            "leaf_insert: leaf is full".into(),
        ));
    }
    if pos > n {
        return Err(MetaDbError::InvalidArgument(format!(
            "leaf_insert: pos {pos} > count {n}",
        )));
    }
    if pos < n {
        let start = pos * LEAF_ENTRY_SIZE;
        let end = n * LEAF_ENTRY_SIZE;
        page.payload_mut()
            .copy_within(start..end, start + LEAF_ENTRY_SIZE);
    }
    leaf_set_entry(page, pos, key, value);
    page.set_key_count((n + 1) as u16);
    Ok(())
}

/// Remove the entry at position `pos`, shifting entries at
/// `pos+1..count` one slot to the left. Decrements `key_count`.
pub fn leaf_remove(page: &mut Page, pos: usize) -> Result<()> {
    let n = leaf_key_count(page);
    if pos >= n {
        return Err(MetaDbError::InvalidArgument(format!(
            "leaf_remove: pos {pos} >= count {n}",
        )));
    }
    if pos + 1 < n {
        let start = (pos + 1) * LEAF_ENTRY_SIZE;
        let end = n * LEAF_ENTRY_SIZE;
        page.payload_mut()
            .copy_within(start..end, pos * LEAF_ENTRY_SIZE);
    }
    // Zero the vacated tail slot so verify is deterministic and the
    // page's CRC reflects the logical state, not leftover bytes.
    let vacated = (n - 1) * LEAF_ENTRY_SIZE;
    for b in &mut page.payload_mut()[vacated..vacated + LEAF_ENTRY_SIZE] {
        *b = 0;
    }
    page.set_key_count((n - 1) as u16);
    Ok(())
}

// -------- Internal accessors -----------------------------------------------

/// Initialize an empty internal page holding exactly one child and no
/// separator keys. Further children are added via [`internal_insert`].
pub fn init_internal(page: &mut Page, generation: Lsn, first_child: PageId) {
    page.bytes_mut().fill(0);
    page.write_header(&PageHeader::new(PageType::L2pInternal, generation));
    internal_set_child_raw(page, 0, first_child);
}

/// Number of separator keys currently in the internal page. The number
/// of children is always one more than this (or exactly 1 when
/// `key_count == 0`).
pub fn internal_key_count(page: &Page) -> usize {
    page.key_count() as usize
}

/// Separator key at index `i`.
pub fn internal_key_at(page: &Page, i: usize) -> u64 {
    debug_assert!(
        i < internal_key_count(page),
        "internal_key_at: out of range"
    );
    let off = i * L2P_KEY_SIZE;
    let p = page.payload();
    u64::from_be_bytes(p[off..off + L2P_KEY_SIZE].try_into().unwrap())
}

/// Child page id at index `i` (0..=`key_count`).
pub fn internal_child_at(page: &Page, i: usize) -> PageId {
    debug_assert!(
        i <= internal_key_count(page),
        "internal_child_at: i > count"
    );
    let off = CHILDREN_REGION_OFFSET + i * 8;
    let p = page.payload();
    u64::from_be_bytes(p[off..off + 8].try_into().unwrap())
}

fn internal_set_key_raw(page: &mut Page, i: usize, key: u64) {
    let off = i * L2P_KEY_SIZE;
    let p = page.payload_mut();
    p[off..off + L2P_KEY_SIZE].copy_from_slice(&key.to_be_bytes());
}

fn internal_set_child_raw(page: &mut Page, i: usize, child: PageId) {
    let off = CHILDREN_REGION_OFFSET + i * 8;
    let p = page.payload_mut();
    p[off..off + 8].copy_from_slice(&child.to_be_bytes());
}

/// Overwrite `children[0]` without touching key_count. Used by the
/// split path to re-seat the leftmost child after a rewrite.
pub fn internal_set_first_child(page: &mut Page, child: PageId) {
    internal_set_child_raw(page, 0, child);
}

/// Overwrite `children[i]` without touching key_count. Used by the
/// CoW descent to rewire a parent to its freshly-CoW'd child.
pub fn internal_set_child(page: &mut Page, i: usize, child: PageId) {
    debug_assert!(
        i <= internal_key_count(page),
        "internal_set_child: i > count"
    );
    internal_set_child_raw(page, i, child);
}

/// Overwrite an existing separator key at position `i` in-place. Used
/// by the delete path when a sibling borrow or merge changes the
/// parent's pivot without changing `key_count`.
pub fn internal_set_key_at(page: &mut Page, i: usize, key: u64) {
    debug_assert!(
        i < internal_key_count(page),
        "internal_set_key_at: out of range",
    );
    internal_set_key_raw(page, i, key);
}

/// Prepend `(new_key, new_child)` at the front: `keys[0] = new_key`,
/// `children[0] = new_child`, shifting everything else right by one.
pub fn internal_push_front(page: &mut Page, new_key: u64, new_child: PageId) -> Result<()> {
    let n = internal_key_count(page);
    if n >= MAX_INTERNAL_KEYS {
        return Err(MetaDbError::InvalidArgument(
            "internal_push_front: internal is full".into(),
        ));
    }
    if n > 0 {
        let start = 0;
        let end = n * L2P_KEY_SIZE;
        page.payload_mut().copy_within(start..end, L2P_KEY_SIZE);
    }
    {
        let start = CHILDREN_REGION_OFFSET;
        let end = CHILDREN_REGION_OFFSET + (n + 1) * 8;
        page.payload_mut().copy_within(start..end, start + 8);
    }
    internal_set_key_raw(page, 0, new_key);
    internal_set_child_raw(page, 0, new_child);
    page.set_key_count((n + 1) as u16);
    Ok(())
}

/// Remove `keys[0]` and `children[0]`, shifting everything else left
/// by one slot. Inverse of [`internal_push_front`].
pub fn internal_pop_front(page: &mut Page) -> Result<()> {
    let n = internal_key_count(page);
    if n == 0 {
        return Err(MetaDbError::InvalidArgument(
            "internal_pop_front: page has no entries".into(),
        ));
    }
    {
        let start = L2P_KEY_SIZE;
        let end = n * L2P_KEY_SIZE;
        page.payload_mut().copy_within(start..end, 0);
    }
    {
        let start = CHILDREN_REGION_OFFSET + 8;
        let end = CHILDREN_REGION_OFFSET + (n + 1) * 8;
        page.payload_mut()
            .copy_within(start..end, CHILDREN_REGION_OFFSET);
    }
    // Zero vacated tail.
    let key_off = (n - 1) * L2P_KEY_SIZE;
    for b in &mut page.payload_mut()[key_off..key_off + L2P_KEY_SIZE] {
        *b = 0;
    }
    let child_off = CHILDREN_REGION_OFFSET + n * 8;
    for b in &mut page.payload_mut()[child_off..child_off + 8] {
        *b = 0;
    }
    page.set_key_count((n - 1) as u16);
    Ok(())
}

/// Descent index for `key`: the position in `children[..]` that
/// contains keys whose values are in the range covered by `key`.
/// Returns a value in `[0, key_count]`. Pure binary search; O(log N).
pub fn internal_search(page: &Page, key: u64) -> usize {
    let n = internal_key_count(page);
    // We want the smallest i such that keys[i] > key; equivalently, the
    // count of keys <= `key`.
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let k = internal_key_at(page, mid);
        if k <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Insert a `(separator_key, right_child)` pair such that `keys[pos]`
/// becomes `key` and `children[pos+1]` becomes `right_child`. Existing
/// keys at `pos..count` and children at `pos+1..count+1` shift right by
/// one slot. `key_count` is bumped.
pub fn internal_insert(page: &mut Page, pos: usize, key: u64, right_child: PageId) -> Result<()> {
    let n = internal_key_count(page);
    if n >= MAX_INTERNAL_KEYS {
        return Err(MetaDbError::InvalidArgument(
            "internal_insert: internal is full".into(),
        ));
    }
    if pos > n {
        return Err(MetaDbError::InvalidArgument(format!(
            "internal_insert: pos {pos} > count {n}",
        )));
    }
    // Shift keys[pos..n] right by 1 (8 B each).
    if pos < n {
        let start = pos * L2P_KEY_SIZE;
        let end = n * L2P_KEY_SIZE;
        page.payload_mut()
            .copy_within(start..end, start + L2P_KEY_SIZE);
        // Shift children[pos+1..n+1] right by 1 (8 B each).
        let start = CHILDREN_REGION_OFFSET + (pos + 1) * 8;
        let end = CHILDREN_REGION_OFFSET + (n + 1) * 8;
        page.payload_mut().copy_within(start..end, start + 8);
    }
    internal_set_key_raw(page, pos, key);
    internal_set_child_raw(page, pos + 1, right_child);
    page.set_key_count((n + 1) as u16);
    Ok(())
}

/// Remove the separator key at `pos` and its right neighbour child
/// (`children[pos+1]`). Shifts trailing keys and children left by one.
///
/// This is the "merge right sibling into left" primitive: after the
/// caller has emptied `children[pos+1]` into `children[pos]`, the
/// vacated slot is removed from the index.
pub fn internal_remove(page: &mut Page, pos: usize) -> Result<()> {
    let n = internal_key_count(page);
    if pos >= n {
        return Err(MetaDbError::InvalidArgument(format!(
            "internal_remove: pos {pos} >= count {n}",
        )));
    }
    // Shift keys[pos+1..n] left by 1.
    if pos + 1 < n {
        let start = (pos + 1) * L2P_KEY_SIZE;
        let end = n * L2P_KEY_SIZE;
        page.payload_mut()
            .copy_within(start..end, pos * L2P_KEY_SIZE);
    }
    // Shift children[pos+2..n+1] left by 1.
    if pos + 2 <= n {
        let start = CHILDREN_REGION_OFFSET + (pos + 2) * 8;
        let end = CHILDREN_REGION_OFFSET + (n + 1) * 8;
        page.payload_mut().copy_within(start..end, start - 8);
    }
    // Zero the vacated tail slots so the CRC reflects the logical
    // state.
    let key_off = (n - 1) * L2P_KEY_SIZE;
    for b in &mut page.payload_mut()[key_off..key_off + L2P_KEY_SIZE] {
        *b = 0;
    }
    let child_off = CHILDREN_REGION_OFFSET + n * 8;
    for b in &mut page.payload_mut()[child_off..child_off + 8] {
        *b = 0;
    }
    page.set_key_count((n - 1) as u16);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::Page;

    fn fresh_leaf() -> Page {
        let mut p = Page::zeroed();
        init_leaf(&mut p, 1);
        p
    }

    fn fresh_internal(first_child: PageId) -> Page {
        let mut p = Page::zeroed();
        init_internal(&mut p, 1, first_child);
        p
    }

    fn v(i: u8) -> RcEntry {
        RcEntry {
            rc: i as u32,
            birth_lsn: 1,
        }
    }

    // --- capacity sanity ---

    #[test]
    fn layout_constants() {
        assert_eq!(LEAF_ENTRY_SIZE, 20);
        assert_eq!(MAX_LEAF_ENTRIES, 201);
        assert_eq!(MAX_INTERNAL_KEYS, 251);
        assert_eq!(MAX_INTERNAL_CHILDREN, 252);
    }

    // --- leaf ---

    #[test]
    fn leaf_init_is_empty() {
        let p = fresh_leaf();
        assert_eq!(leaf_key_count(&p), 0);
        assert_eq!(p.header().unwrap().page_type, PageType::L2pLeaf);
    }

    #[test]
    fn leaf_insert_sorted_round_trip() {
        let mut p = fresh_leaf();
        let keys: Vec<u64> = (0..20u64).map(|k| k * 7).collect();
        for (i, k) in keys.iter().enumerate() {
            leaf_insert(&mut p, i, *k, v(i as u8)).unwrap();
        }
        assert_eq!(leaf_key_count(&p), 20);
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(leaf_key_at(&p, i), *k);
            assert_eq!(leaf_value_at(&p, i), v(i as u8));
        }
    }

    #[test]
    fn leaf_insert_shifts_existing_entries() {
        let mut p = fresh_leaf();
        // Seed [10, 30]
        leaf_insert(&mut p, 0, 10, v(10)).unwrap();
        leaf_insert(&mut p, 1, 30, v(30)).unwrap();
        // Insert 20 between them.
        leaf_insert(&mut p, 1, 20, v(20)).unwrap();
        assert_eq!(leaf_key_count(&p), 3);
        assert_eq!(leaf_key_at(&p, 0), 10);
        assert_eq!(leaf_key_at(&p, 1), 20);
        assert_eq!(leaf_key_at(&p, 2), 30);
        assert_eq!(leaf_value_at(&p, 2), v(30));
    }

    #[test]
    fn leaf_search_hit_and_miss() {
        let mut p = fresh_leaf();
        for (i, k) in [1u64, 5, 9, 13].iter().enumerate() {
            leaf_insert(&mut p, i, *k, v(i as u8)).unwrap();
        }
        assert_eq!(leaf_search(&p, 1), Ok(0));
        assert_eq!(leaf_search(&p, 5), Ok(1));
        assert_eq!(leaf_search(&p, 9), Ok(2));
        assert_eq!(leaf_search(&p, 13), Ok(3));
        assert_eq!(leaf_search(&p, 0), Err(0));
        assert_eq!(leaf_search(&p, 3), Err(1));
        assert_eq!(leaf_search(&p, 7), Err(2));
        assert_eq!(leaf_search(&p, 100), Err(4));
    }

    #[test]
    fn leaf_remove_shifts_tail_left() {
        let mut p = fresh_leaf();
        for (i, k) in [1u64, 2, 3, 4, 5].iter().enumerate() {
            leaf_insert(&mut p, i, *k, v(*k as u8)).unwrap();
        }
        leaf_remove(&mut p, 2).unwrap(); // drop key 3
        assert_eq!(leaf_key_count(&p), 4);
        let ks: Vec<u64> = (0..4).map(|i| leaf_key_at(&p, i)).collect();
        assert_eq!(ks, vec![1, 2, 4, 5]);
        // Vacated tail is zeroed.
        let vacated_off = 4 * LEAF_ENTRY_SIZE;
        assert!(
            p.payload()[vacated_off..vacated_off + LEAF_ENTRY_SIZE]
                .iter()
                .all(|&b| b == 0)
        );
    }

    #[test]
    fn leaf_capacity_is_enforced() {
        let mut p = fresh_leaf();
        for i in 0..MAX_LEAF_ENTRIES {
            leaf_insert(&mut p, i, i as u64, v(0)).unwrap();
        }
        assert!(
            leaf_insert(&mut p, MAX_LEAF_ENTRIES, 999, v(0)).is_err(),
            "full leaf must reject insert",
        );
    }

    #[test]
    fn leaf_insert_out_of_range_rejected() {
        let mut p = fresh_leaf();
        leaf_insert(&mut p, 0, 1, v(0)).unwrap();
        assert!(leaf_insert(&mut p, 5, 2, v(0)).is_err());
    }

    #[test]
    fn leaf_set_entry_updates_in_place() {
        let mut p = fresh_leaf();
        leaf_insert(&mut p, 0, 42, v(1)).unwrap();
        leaf_set_entry(&mut p, 0, 42, v(99));
        assert_eq!(leaf_key_count(&p), 1);
        assert_eq!(leaf_key_at(&p, 0), 42);
        assert_eq!(leaf_value_at(&p, 0), v(99));
    }

    #[test]
    fn leaf_page_seals_and_verifies() {
        let mut p = fresh_leaf();
        for i in 0..50u64 {
            leaf_insert(&mut p, i as usize, i, v(i as u8)).unwrap();
        }
        p.seal();
        p.verify(1).unwrap();
    }

    #[test]
    fn leaf_remove_entire_content() {
        let mut p = fresh_leaf();
        for i in 0..10u64 {
            leaf_insert(&mut p, i as usize, i, v(0)).unwrap();
        }
        while leaf_key_count(&p) > 0 {
            leaf_remove(&mut p, 0).unwrap();
        }
        assert_eq!(leaf_key_count(&p), 0);
        // Entire payload now zero.
        assert!(p.payload().iter().all(|&b| b == 0));
    }

    // --- internal ---

    #[test]
    fn internal_init_has_one_child_no_keys() {
        let p = fresh_internal(42);
        assert_eq!(internal_key_count(&p), 0);
        assert_eq!(internal_child_at(&p, 0), 42);
    }

    #[test]
    fn internal_insert_builds_sorted_separators() {
        let mut p = fresh_internal(100);
        // After init: child[0]=100
        // Insert (10, child=110) at pos 0 → keys=[10], children=[100,110]
        internal_insert(&mut p, 0, 10, 110).unwrap();
        // Insert (20, child=120) at pos 1 → keys=[10,20], children=[100,110,120]
        internal_insert(&mut p, 1, 20, 120).unwrap();
        // Insert (15, child=115) at pos 1 → keys=[10,15,20], children=[100,110,115,120]
        internal_insert(&mut p, 1, 15, 115).unwrap();

        assert_eq!(internal_key_count(&p), 3);
        assert_eq!(internal_key_at(&p, 0), 10);
        assert_eq!(internal_key_at(&p, 1), 15);
        assert_eq!(internal_key_at(&p, 2), 20);
        assert_eq!(internal_child_at(&p, 0), 100);
        assert_eq!(internal_child_at(&p, 1), 110);
        assert_eq!(internal_child_at(&p, 2), 115);
        assert_eq!(internal_child_at(&p, 3), 120);
    }

    #[test]
    fn internal_search_routes_to_correct_child() {
        let mut p = fresh_internal(100);
        internal_insert(&mut p, 0, 5, 110).unwrap();
        internal_insert(&mut p, 1, 10, 120).unwrap();
        internal_insert(&mut p, 2, 15, 130).unwrap();
        // keys=[5,10,15], children=[100,110,120,130]
        assert_eq!(internal_search(&p, 0), 0); // <5 → child[0]
        assert_eq!(internal_search(&p, 4), 0);
        assert_eq!(internal_search(&p, 5), 1); // 5 → child[1]
        assert_eq!(internal_search(&p, 7), 1);
        assert_eq!(internal_search(&p, 10), 2);
        assert_eq!(internal_search(&p, 14), 2);
        assert_eq!(internal_search(&p, 15), 3);
        assert_eq!(internal_search(&p, 999), 3);
    }

    #[test]
    fn internal_remove_pulls_right_sibling_out() {
        let mut p = fresh_internal(100);
        internal_insert(&mut p, 0, 5, 110).unwrap();
        internal_insert(&mut p, 1, 10, 120).unwrap();
        internal_insert(&mut p, 2, 15, 130).unwrap();
        // Remove key 10 (pos 1): drops separator 10 and child[2]=120.
        internal_remove(&mut p, 1).unwrap();
        assert_eq!(internal_key_count(&p), 2);
        assert_eq!(internal_key_at(&p, 0), 5);
        assert_eq!(internal_key_at(&p, 1), 15);
        assert_eq!(internal_child_at(&p, 0), 100);
        assert_eq!(internal_child_at(&p, 1), 110);
        assert_eq!(internal_child_at(&p, 2), 130);
    }

    #[test]
    fn internal_remove_last_entry() {
        let mut p = fresh_internal(100);
        internal_insert(&mut p, 0, 5, 110).unwrap();
        internal_insert(&mut p, 1, 10, 120).unwrap();
        internal_remove(&mut p, 1).unwrap();
        assert_eq!(internal_key_count(&p), 1);
        assert_eq!(internal_key_at(&p, 0), 5);
        assert_eq!(internal_child_at(&p, 0), 100);
        assert_eq!(internal_child_at(&p, 1), 110);
        // Vacated slots must be zero so CRC reflects logical state.
        let key_tail_off = L2P_KEY_SIZE;
        assert!(
            p.payload()[key_tail_off..key_tail_off + L2P_KEY_SIZE]
                .iter()
                .all(|&b| b == 0)
        );
    }

    #[test]
    fn internal_capacity_is_enforced() {
        let mut p = fresh_internal(0);
        for i in 0..MAX_INTERNAL_KEYS {
            internal_insert(&mut p, i, (i + 1) as u64, (i + 1) as PageId).unwrap();
        }
        assert!(internal_insert(&mut p, MAX_INTERNAL_KEYS, 999, 999).is_err());
    }

    #[test]
    fn internal_page_seals_and_verifies() {
        let mut p = fresh_internal(1);
        for i in 0..100 {
            internal_insert(&mut p, i, (i + 1) as u64, (i + 2) as PageId).unwrap();
        }
        p.seal();
        p.verify(7).unwrap();
    }

    #[test]
    fn internal_search_empty_returns_zero() {
        let p = fresh_internal(42);
        assert_eq!(internal_search(&p, 0), 0);
        assert_eq!(internal_search(&p, u64::MAX), 0);
    }

    #[test]
    fn internal_push_front_shifts_right() {
        let mut p = fresh_internal(100);
        internal_insert(&mut p, 0, 10, 110).unwrap();
        internal_insert(&mut p, 1, 20, 120).unwrap();
        // Before: keys=[10,20], children=[100,110,120]
        internal_push_front(&mut p, 5, 50).unwrap();
        // After:  keys=[5,10,20], children=[50,100,110,120]
        assert_eq!(internal_key_count(&p), 3);
        assert_eq!(internal_key_at(&p, 0), 5);
        assert_eq!(internal_key_at(&p, 1), 10);
        assert_eq!(internal_key_at(&p, 2), 20);
        assert_eq!(internal_child_at(&p, 0), 50);
        assert_eq!(internal_child_at(&p, 1), 100);
        assert_eq!(internal_child_at(&p, 2), 110);
        assert_eq!(internal_child_at(&p, 3), 120);
    }

    #[test]
    fn internal_pop_front_shifts_left() {
        let mut p = fresh_internal(100);
        internal_insert(&mut p, 0, 10, 110).unwrap();
        internal_insert(&mut p, 1, 20, 120).unwrap();
        internal_insert(&mut p, 2, 30, 130).unwrap();
        // Before: keys=[10,20,30], children=[100,110,120,130]
        internal_pop_front(&mut p).unwrap();
        // After:  keys=[20,30], children=[110,120,130]
        assert_eq!(internal_key_count(&p), 2);
        assert_eq!(internal_key_at(&p, 0), 20);
        assert_eq!(internal_key_at(&p, 1), 30);
        assert_eq!(internal_child_at(&p, 0), 110);
        assert_eq!(internal_child_at(&p, 1), 120);
        assert_eq!(internal_child_at(&p, 2), 130);
    }

    #[test]
    fn internal_push_pop_front_round_trip() {
        let mut p = fresh_internal(1);
        internal_insert(&mut p, 0, 10, 2).unwrap();
        internal_insert(&mut p, 1, 20, 3).unwrap();
        internal_push_front(&mut p, 5, 9).unwrap();
        internal_pop_front(&mut p).unwrap();
        // Back to original.
        assert_eq!(internal_key_count(&p), 2);
        assert_eq!(internal_key_at(&p, 0), 10);
        assert_eq!(internal_key_at(&p, 1), 20);
        assert_eq!(internal_child_at(&p, 0), 1);
        assert_eq!(internal_child_at(&p, 1), 2);
        assert_eq!(internal_child_at(&p, 2), 3);
    }

    #[test]
    fn internal_set_key_at_updates_in_place() {
        let mut p = fresh_internal(1);
        internal_insert(&mut p, 0, 10, 2).unwrap();
        internal_set_key_at(&mut p, 0, 99);
        assert_eq!(internal_key_at(&p, 0), 99);
        assert_eq!(internal_key_count(&p), 1);
    }

    #[test]
    fn internal_round_trip_through_page_store() {
        use crate::page_store::PageStore;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let ps = PageStore::create(dir.path().join("p")).unwrap();
        let pid = ps.allocate().unwrap();

        let mut p = Page::zeroed();
        init_internal(&mut p, 1, 10);
        internal_insert(&mut p, 0, 42, 20).unwrap();
        p.seal();
        ps.write_page(pid, &p).unwrap();
        ps.sync().unwrap();

        let r = ps.read_page(pid).unwrap();
        assert_eq!(internal_key_count(&r), 1);
        assert_eq!(internal_key_at(&r, 0), 42);
        assert_eq!(internal_child_at(&r, 0), 10);
        assert_eq!(internal_child_at(&r, 1), 20);
    }
}
