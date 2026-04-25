use super::*;

/// Ascending iterator returned by [`BTree::range`]. Entries are
/// collected eagerly; the caller can call `.collect()` without
/// worrying about partial iteration.
pub struct RangeIter {
    pub(super) inner: std::vec::IntoIter<(u64, RcEntry)>,
}

impl Iterator for RangeIter {
    type Item = Result<(u64, RcEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

pub(super) fn bound_lower(b: Bound<&u64>) -> Bound<u64> {
    match b {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(super) fn bound_upper(b: Bound<&u64>) -> Bound<u64> {
    match b {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(super) fn lower_ok(lo: Bound<u64>, k: u64) -> bool {
    match lo {
        Bound::Included(v) => k >= v,
        Bound::Excluded(v) => k > v,
        Bound::Unbounded => true,
    }
}

pub(super) fn upper_ok(hi: Bound<u64>, k: u64) -> bool {
    match hi {
        Bound::Included(v) => k <= v,
        Bound::Excluded(v) => k < v,
        Bound::Unbounded => true,
    }
}

pub(super) fn range_overlaps(lo: Bound<u64>, hi: Bound<u64>, child_lo: u64, child_hi: u64) -> bool {
    // child range is [child_lo, child_hi]. Check for non-empty
    // intersection with [lo, hi] (bounds).
    let lo_ok = match hi {
        Bound::Included(v) => child_lo <= v,
        Bound::Excluded(v) => child_lo < v,
        Bound::Unbounded => true,
    };
    let hi_ok = match lo {
        Bound::Included(v) => child_hi >= v,
        Bound::Excluded(v) => child_hi > v,
        Bound::Unbounded => true,
    };
    lo_ok && hi_ok
}
