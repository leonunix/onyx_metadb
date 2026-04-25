use super::*;

pub(super) fn slot_span_for_level(level: u8) -> u64 {
    debug_assert!((1..=MAX_INDEX_LEVEL).contains(&level));
    // Each level-L index slot covers INDEX_FANOUT^(L-1) leaves, each
    // covering LEAF_ENTRY_COUNT LBAs. Exponents are safe up to level 4
    // (= 2^39 LBAs).
    1u64 << (LEAF_SHIFT + INDEX_SHIFT * (level as u32 - 1))
}

/// Merge two ascending `(key, value)` streams into `DiffEntry` items in
/// ascending key order. Same shape as `btree::merge_diff_into` so tests
/// that assert diff ordering don't need special casing.
pub(super) fn merge_diff_into(
    a: &[(u64, L2pValue)],
    b: &[(u64, L2pValue)],
    out: &mut Vec<DiffEntry>,
) {
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => {
                out.push(DiffEntry::RemovedInB {
                    key: a[i].0,
                    old: a[i].1,
                });
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(DiffEntry::AddedInB {
                    key: b[j].0,
                    new: b[j].1,
                });
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                if a[i].1 != b[j].1 {
                    out.push(DiffEntry::Changed {
                        key: a[i].0,
                        old: a[i].1,
                        new: b[j].1,
                    });
                }
                i += 1;
                j += 1;
            }
        }
    }
    while i < a.len() {
        out.push(DiffEntry::RemovedInB {
            key: a[i].0,
            old: a[i].1,
        });
        i += 1;
    }
    while j < b.len() {
        out.push(DiffEntry::AddedInB {
            key: b[j].0,
            new: b[j].1,
        });
        j += 1;
    }
}

pub(super) fn overlaps(range: &OwnedRange, lo: u64, hi: u64) -> bool {
    let lo_ok = match range.end_bound() {
        Bound::Included(&end) => lo <= end,
        Bound::Excluded(&end) => lo < end,
        Bound::Unbounded => true,
    };
    let hi_ok = match range.start_bound() {
        Bound::Included(&start) => hi >= start,
        Bound::Excluded(&start) => hi > start,
        Bound::Unbounded => true,
    };
    lo_ok && hi_ok
}
