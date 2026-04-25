use super::*;

/// Counters returned by [`PagedL2p::warmup_index_pages`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct WarmupStats {
    /// Index pages successfully pinned. Leaf pages are never pinned.
    pub pages_pinned: u64,
    /// True if the walk was cut short by the cache's pin budget.
    pub skipped_budget: bool,
}

/// Owned range for multi-root scans. Mirrors `db::OwnedRange` (kept
/// crate-private there) so we can accept it in `range_at` without
/// reaching into `Db`'s private types.
#[derive(Clone, Debug)]
pub(super) struct OwnedRange {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl OwnedRange {
    pub(super) fn from_bounds<R: RangeBounds<u64>>(range: R) -> Self {
        Self {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        }
    }
}

impl RangeBounds<u64> for OwnedRange {
    fn start_bound(&self) -> Bound<&u64> {
        ref_bound(&self.start)
    }
    fn end_bound(&self) -> Bound<&u64> {
        ref_bound(&self.end)
    }
}

fn clone_bound(b: Bound<&u64>) -> Bound<u64> {
    match b {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn ref_bound(b: &Bound<u64>) -> Bound<&u64> {
    match b {
        Bound::Included(v) => Bound::Included(v),
        Bound::Excluded(v) => Bound::Excluded(v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Rich outcome returned by [`PagedL2p::insert_at_lsn_with_share_info`]
/// and [`PagedL2p::delete_at_lsn_with_share_info`]. The onyx-adapter
/// apply path (`WalOp::L2pRemap`, `WalOp::L2pRangeDelete`) uses
/// `leaf_was_shared` together with `prev` to decide whether to
/// suppress the paired refcount decref (see SPEC §3.1 decision
/// table): if the leaf that held the old mapping was shared with a
/// live snapshot, the snapshot's tree still references `prev.pba` via
/// the old leaf bytes, and decrementing `prev.pba`'s refcount here
/// would double-free it later when the snapshot drops.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct InsertOutcome {
    /// Previous mapping at the key, or `None` if the slot was unset.
    pub prev: Option<L2pValue>,
    /// `true` iff the leaf page that contained this key was shared
    /// with another tree (snapshot / clone) at the moment this op
    /// reached it — i.e. the pre-COW effective refcount was `> 1`.
    ///
    /// For a freshly-allocated leaf (no prior mapping existed at all)
    /// the value is `false`, matching the "not shared" semantics of
    /// an exclusive page.
    pub leaf_was_shared: bool,
}

/// Delete-path analogue of [`InsertOutcome`]. Returned by
/// [`PagedL2p::delete_at_lsn_with_share_info`]; consumed by the
/// onyx-adapter `WalOp::L2pRangeDelete` apply path. `prev` is `None`
/// when the lba was unmapped — the apply path short-circuits in that
/// case and treats the op as a no-op for refcount accounting.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct DeleteOutcome {
    /// Previous mapping at the key, or `None` if the slot was unset
    /// and this call was a no-op.
    pub prev: Option<L2pValue>,
    /// `true` iff the leaf page that contained this key was shared
    /// with another tree in the pre-op state — same semantics as
    /// [`InsertOutcome::leaf_was_shared`].
    pub leaf_was_shared: bool,
}

/// One entry in the delta between two subtrees. Emitted by
/// [`PagedL2p::diff_subtrees`] and surfaced via `Db::diff`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DiffEntry {
    /// Key exists in B but not in A.
    AddedInB { key: u64, new: L2pValue },
    /// Key exists in A but not in B.
    RemovedInB { key: u64, old: L2pValue },
    /// Key exists in both; value changed.
    Changed {
        key: u64,
        old: L2pValue,
        new: L2pValue,
    },
}

impl DiffEntry {
    /// Key this diff entry concerns.
    pub fn key(&self) -> u64 {
        match self {
            Self::AddedInB { key, .. }
            | Self::RemovedInB { key, .. }
            | Self::Changed { key, .. } => *key,
        }
    }
}

/// Iterator returned by [`PagedL2p::range`]. All items are materialised
/// up-front — range scans on Onyx's L2P are rare (snapshot exports,
/// debug tools) so eager collection keeps the implementation simple.
pub struct PagedRangeIter {
    inner: std::vec::IntoIter<(u64, L2pValue)>,
}

impl PagedRangeIter {
    pub(super) fn new(mut items: Vec<(u64, L2pValue)>) -> Self {
        items.sort_unstable_by_key(|(k, _)| *k);
        Self {
            inner: items.into_iter(),
        }
    }
}

impl Iterator for PagedRangeIter {
    type Item = Result<(u64, L2pValue)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}
