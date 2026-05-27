//! In-memory data-plane op enum.
//!
//! `WalOp` is the dispatch type the commit path uses to route mutations
//! through `apply_op_bare` and the per-shard apply lanes. It is **not**
//! serialised any more — the LV2 buffer is the durable record of every
//! data-plane mutation (recovery replays it through the flusher), and
//! the lifecycle journal owns its own codec for the few non-data-plane
//! ops (see [`crate::lifecycle_log::op::LifecycleOp`]).
//!
//! Variants here therefore cover only what the live commit path
//! constructs and dispatches: L2P puts/deletes/remaps (single and
//! range-shaped) plus the dedup index ops. Lifecycle ops have their
//! own enum and never reach `commit_ops`.

use crate::dedup_types::{DedupValue, Hash8};
use crate::paged::L2pValue;
use crate::types::{Lba, Pba, VolumeOrdinal};

/// Maximum LBAs in a single [`WalOp::L2pRemapRange`] dispatch. The
/// passthrough writer is bounded by `coalesce_max_lbas = 32`; 4096
/// leaves a comfortable defensive ceiling. The constant is kept here
/// so onyx-side callers (see `meta::backend::metadb::values::emit_l2p_remap_runs`)
/// can split runs before constructing the op.
pub const MAX_REMAP_RANGE_LBAS: usize = 4096;

/// One data-plane mutation as dispatched through `apply_op_bare`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalOp {
    L2pPut {
        vol_ord: VolumeOrdinal,
        lba: Lba,
        value: L2pValue,
    },
    L2pDelete {
        vol_ord: VolumeOrdinal,
        lba: Lba,
    },
    /// Fused L2P put + refcount decref(old) + refcount incref(new).
    ///
    /// `new_value`'s head 8 B is the target PBA per the `BlockmapValue`
    /// contract; apply reads it via
    /// [`L2pValue::head_pba`](crate::paged::L2pValue::head_pba).
    ///
    /// `guard = Some((pba, min_rc))` makes apply read `refcount(pba)`
    /// first and return an early no-op outcome if the value is `< min_rc`
    /// — used by the dedup hit path so a target that was concurrently
    /// freed cannot be re-linked.
    L2pRemap {
        vol_ord: VolumeOrdinal,
        lba: Lba,
        new_value: L2pValue,
        guard: Option<(Pba, u32)>,
    },
    /// Range-shaped variant of [`L2pRemap`](Self::L2pRemap): apply the
    /// same per-LBA remap semantics to `[start_lba .. start_lba + values.len())`
    /// of one volume in a single dispatch. Always unguarded.
    L2pRemapRange {
        vol_ord: VolumeOrdinal,
        start_lba: Lba,
        values: Box<[L2pValue]>,
    },
    /// Insert or overwrite `hash → value` in the dedup_index. `old_pba`
    /// is captured at `Transaction::commit` time so apply can stage the
    /// rc decref/incref pair without re-reading the on-disk dedup_index.
    DedupPut {
        hash: Hash8,
        value: DedupValue,
        old_pba: Option<Pba>,
    },
    DedupPutGuarded {
        hash: Hash8,
        value: DedupValue,
        pba_guard: Pba,
        min_rc: u32,
        old_pba: Option<Pba>,
    },
    /// Tombstone `hash`. `old_pba = Some(p)` stages decref(p); `None`
    /// is a no-op delete (no prior entry).
    DedupDelete {
        hash: Hash8,
        old_pba: Option<Pba>,
    },
    /// Tombstone `hash` only if the current forward index value is
    /// exactly `old_value`.
    DedupCompareDelete {
        hash: Hash8,
        old_value: DedupValue,
    },
    /// Replace `hash` with `new_value` only if the current forward
    /// index value is exactly `old_value`.
    DedupComparePut {
        hash: Hash8,
        old_value: DedupValue,
        new_value: DedupValue,
    },
}
