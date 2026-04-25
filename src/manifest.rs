//! Double-buffered manifest living in pages 0 and 1 of the page store.
//!
//! Phase 7 commit 6 steps the wire format from v5 to v6. v6 drops the
//! global L2P `shard_roots` vector — those roots now live per volume
//! inside `volumes`, encoded via [`encode_volume_entry_inline`]. Refcount
//! is a running tally, not per-volume, so `refcount_shard_roots` stays at
//! the manifest top level.
//!
//! v6 body layout:
//! - current refcount shard roots (inline)
//! - dedup_index + dedup_reverse level-chain heads
//! - snapshot table (one 32-byte `SnapshotEntry` per row; `vol_ord`
//!   reserved — commit 9 picks it up for per-volume snapshots)
//! - volume table (inline-encoded [`VolumeEntry`] rows)
//!
//! Older v3 / v4 / v5 manifests are not readable — Phase 7 is fresh-
//! install only. A v5 body on disk is reported as `Corruption`.

use std::mem::size_of;
use std::sync::Arc;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::{
    Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, SnapshotId, VolumeOrdinal,
};

/// Version of the current manifest body layout.
pub const MANIFEST_BODY_VERSION: u32 = 7;

// v6 body layout. The variable region begins at `OFF_V6_VARIABLE` and
// holds, in order:
//   refcount_shard_roots [shard_count × 8]
//   dedup_level_heads    [dedup_level_count × 8]
//   dedup_reverse_heads  [dedup_reverse_level_count × 8]
//   snapshots            [snapshot_count × SNAPSHOT_ENTRY_SIZE]
//   volumes              [per-entry inline, see encode_volume_entry_inline]
const OFF_BODY_VERSION: usize = 0;
const OFF_CHECKPOINT_LSN: usize = 4;
const OFF_FREE_LIST_HEAD: usize = 12;
const OFF_SHARD_COUNT: usize = 20;
const OFF_DEDUP_LEVEL_COUNT: usize = 24;
const OFF_DEDUP_REVERSE_LEVEL_COUNT: usize = 28;
const OFF_NEXT_SNAPSHOT_ID: usize = 32;
const OFF_NEXT_VOLUME_ORD: usize = 40;
// 42..44 reserved for alignment / future flags
const OFF_SNAPSHOT_COUNT: usize = 44;
const OFF_VOLUME_COUNT: usize = 48;
const OFF_V6_VARIABLE: usize = 52;

/// Per-snapshot row size on disk. v6 packs: id(8) + vol_ord(2) + 6 pad +
/// l2p_roots_page(8) + created_lsn(8). We keep the row at 32 bytes so
/// `max_snapshots_for_layout` math stays identical to v5 for callers
/// that just want "how many fit" — v6 drops the dead `refcount_roots_page`
/// slot, and the freed 6 bytes are reserved for future per-snapshot flags
/// without forcing another format bump.
const SNAPSHOT_ENTRY_SIZE: usize = 32;

const _: () = {
    assert!(OFF_BODY_VERSION + 4 == OFF_CHECKPOINT_LSN);
    assert!(OFF_CHECKPOINT_LSN + 8 == OFF_FREE_LIST_HEAD);
    assert!(OFF_FREE_LIST_HEAD + 8 == OFF_SHARD_COUNT);
    assert!(OFF_SHARD_COUNT + 4 == OFF_DEDUP_LEVEL_COUNT);
    assert!(OFF_DEDUP_LEVEL_COUNT + 4 == OFF_DEDUP_REVERSE_LEVEL_COUNT);
    assert!(OFF_DEDUP_REVERSE_LEVEL_COUNT + 4 == OFF_NEXT_SNAPSHOT_ID);
    assert!(OFF_NEXT_SNAPSHOT_ID + 8 == OFF_NEXT_VOLUME_ORD);
    assert!(OFF_NEXT_VOLUME_ORD + 4 == OFF_SNAPSHOT_COUNT);
    assert!(OFF_SNAPSHOT_COUNT + 4 == OFF_VOLUME_COUNT);
    assert!(OFF_VOLUME_COUNT + 4 == OFF_V6_VARIABLE);
    assert!(SNAPSHOT_ENTRY_SIZE == 32);
};

/// Maximum number of shard roots that fit in one snapshot-roots page.
pub const MAX_SHARD_ROOTS_PER_PAGE: usize = PAGE_PAYLOAD_SIZE / size_of::<PageId>();

/// Maximum number of snapshot entries that fit in a v6 manifest with the
/// given refcount shard count, ignoring volume and dedup level usage.
/// Upper bound; real capacity is lower once the volume table / dedup
/// levels eat into the variable region.
pub fn max_snapshots_for_shards(refcount_shard_count: usize) -> usize {
    max_snapshots_for_layout(refcount_shard_count, 0, 0)
}

/// Snapshot-table capacity given the full manifest layout. All inputs
/// are lengths / byte counts that end up in the variable region ahead
/// of the snapshot table.
pub fn max_snapshots_for_layout(
    refcount_shard_count: usize,
    total_dedup_level_count: usize,
    volumes_budget_bytes: usize,
) -> usize {
    let refcount_bytes = match refcount_shard_count.checked_mul(size_of::<PageId>()) {
        Some(v) => v,
        None => return 0,
    };
    let dedup_bytes = match total_dedup_level_count.checked_mul(size_of::<PageId>()) {
        Some(v) => v,
        None => return 0,
    };
    let used = match OFF_V6_VARIABLE
        .checked_add(refcount_bytes)
        .and_then(|v| v.checked_add(dedup_bytes))
        .and_then(|v| v.checked_add(volumes_budget_bytes))
    {
        Some(v) => v,
        None => return 0,
    };
    if used > PAGE_PAYLOAD_SIZE {
        return 0;
    }
    (PAGE_PAYLOAD_SIZE - used) / SNAPSHOT_ENTRY_SIZE
}

/// One snapshot's manifest entry. v6 tracks the owning volume's
/// ordinal and the snapshot's L2P shard-root vector (materialised via
/// a [`PageType::SnapshotRoots`] page). Refcount state is global and
/// never snapshotted — Phase 6.5b retired it. Commit 6 always stamps
/// `vol_ord = 0`; commit 9 picks it up for per-volume snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub id: SnapshotId,
    pub vol_ord: VolumeOrdinal,
    pub l2p_roots_page: PageId,
    pub created_lsn: Lsn,
    pub l2p_shard_roots: Box<[PageId]>,
}

impl SnapshotEntry {
    fn needs_l2p_roots_page(&self) -> bool {
        self.l2p_roots_page == NULL_PAGE
    }
}

// ---- Phase 7 / manifest v6 building blocks -------------------------------
//
// Not plugged into the live encode/decode path yet — the write path still
// emits v5. These types + codecs land now so Phase B can flip the wire
// format in one atomic change. Keeping them as standalone additive code
// during Phase A means the 8a soak can keep running against v5 without
// drift.

mod snapshot_roots;
mod store;
mod volume;

pub(crate) use snapshot_roots::{load_snapshot_roots, write_snapshot_roots_page};
pub use store::{LoadedManifest, ManifestStore};
pub use volume::{
    VOLUME_ENTRY_FIXED_SIZE, VOLUME_FLAG_DROP_PENDING, VolumeEntry, decode_volume_entry_inline,
    encode_volume_entry_inline, volume_entry_inline_size,
};

/// Decoded manifest body (v6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub body_version: u32,
    /// Greatest LSN whose WAL record has been applied to durable state.
    pub checkpoint_lsn: Lsn,
    /// Head of the persisted free-list page chain, or [`NULL_PAGE`].
    pub free_list_head: PageId,
    /// Current per-shard PBA-refcount B+tree roots. Refcount is a global
    /// running tally — not per-volume — so its roots stay at the
    /// manifest top level. Per-volume L2P roots live inside
    /// [`volumes`](Self::volumes) instead.
    pub refcount_shard_roots: Box<[PageId]>,
    /// Head page id of each dedup_index LSM level's chain. One per
    /// level; `NULL_PAGE` for an empty level.
    pub dedup_level_heads: Box<[PageId]>,
    /// Head page id of each dedup_reverse LSM level's chain. Same
    /// per-level shape as [`dedup_level_heads`](Self::dedup_level_heads).
    pub dedup_reverse_level_heads: Box<[PageId]>,
    /// Monotonic counter: next snapshot id to hand out.
    pub next_snapshot_id: u64,
    /// Monotonic counter: next volume ordinal to hand out. Initialised
    /// to `1` because `0` is reserved for the bootstrap volume that
    /// every database creates on `open` / `create`.
    pub next_volume_ord: VolumeOrdinal,
    /// Registered snapshots, in order of creation.
    pub snapshots: Vec<SnapshotEntry>,
    /// Registered volumes, in ordinal order.
    pub volumes: Vec<VolumeEntry>,
}

impl Manifest {
    /// Freshly-created empty manifest for a brand-new database. Caller
    /// fills in refcount roots and the bootstrap volume entry before
    /// committing.
    pub fn empty() -> Self {
        Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0,
            free_list_head: NULL_PAGE,
            refcount_shard_roots: Vec::new().into_boxed_slice(),
            dedup_level_heads: Vec::new().into_boxed_slice(),
            dedup_reverse_level_heads: Vec::new().into_boxed_slice(),
            next_snapshot_id: 1,
            next_volume_ord: 1,
            snapshots: Vec::new(),
            volumes: Vec::new(),
        }
    }

    /// Number of refcount shards tracked at the top level. Per-volume
    /// L2P shard counts live on each [`VolumeEntry`].
    pub fn shard_count(&self) -> usize {
        self.refcount_shard_roots.len()
    }

    /// Number of dedup_index levels currently tracked.
    pub fn dedup_level_count(&self) -> usize {
        self.dedup_level_heads.len()
    }

    /// Number of dedup_reverse levels currently tracked.
    pub fn dedup_reverse_level_count(&self) -> usize {
        self.dedup_reverse_level_heads.len()
    }

    /// Find a snapshot by id.
    pub fn find_snapshot(&self, id: SnapshotId) -> Option<&SnapshotEntry> {
        self.snapshots.iter().find(|e| e.id == id)
    }

    /// Dry-run [`encode`](Self::encode) against a scratch page. Returns
    /// `Err` with the same diagnostic `encode` would have produced if
    /// this manifest does not fit in a single page. Callers use this
    /// before applying irreversible side effects (refcount bumps, page
    /// writes) so a doomed commit surfaces as `Err` *before* leaking
    /// refcount state on disk.
    pub(crate) fn check_encodable(&self) -> Result<()> {
        let mut probe = Page::new(PageHeader::new(PageType::Manifest, 0));
        self.encode(&mut probe)
    }

    fn encode(&self, page: &mut Page) -> Result<()> {
        let refcount_shard_count = self.refcount_shard_roots.len();
        if refcount_shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest refcount shard count {} exceeds page capacity {}",
                refcount_shard_count, MAX_SHARD_ROOTS_PER_PAGE,
            )));
        }
        let dedup_level_count = self.dedup_level_heads.len();
        let dedup_reverse_level_count = self.dedup_reverse_level_heads.len();
        let total_dedup_levels = dedup_level_count + dedup_reverse_level_count;

        let volumes_budget_bytes: usize = self
            .volumes
            .iter()
            .map(|v| volume_entry_inline_size(v.shard_count as usize))
            .sum();

        let max_snapshots = max_snapshots_for_layout(
            refcount_shard_count,
            total_dedup_levels,
            volumes_budget_bytes,
        );
        if self.snapshots.len() > max_snapshots {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest snapshot count {} exceeds capacity {max_snapshots}",
                self.snapshots.len(),
            )));
        }

        for entry in &self.snapshots {
            if entry.needs_l2p_roots_page() {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} is missing l2p_roots_page",
                    entry.id,
                )));
            }
        }

        let p = page.payload_mut();
        p.fill(0);
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
            .copy_from_slice(&MANIFEST_BODY_VERSION.to_le_bytes());
        p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
            .copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
            .copy_from_slice(&self.free_list_head.to_le_bytes());
        p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4]
            .copy_from_slice(&(refcount_shard_count as u32).to_le_bytes());
        p[OFF_DEDUP_LEVEL_COUNT..OFF_DEDUP_LEVEL_COUNT + 4]
            .copy_from_slice(&(dedup_level_count as u32).to_le_bytes());
        p[OFF_DEDUP_REVERSE_LEVEL_COUNT..OFF_DEDUP_REVERSE_LEVEL_COUNT + 4]
            .copy_from_slice(&(dedup_reverse_level_count as u32).to_le_bytes());
        p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
            .copy_from_slice(&self.next_snapshot_id.to_le_bytes());
        p[OFF_NEXT_VOLUME_ORD..OFF_NEXT_VOLUME_ORD + 2]
            .copy_from_slice(&self.next_volume_ord.to_le_bytes());
        // OFF_NEXT_VOLUME_ORD + 2 .. OFF_SNAPSHOT_COUNT = 2 bytes reserved (already zero-filled).
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
            .copy_from_slice(&(self.snapshots.len() as u32).to_le_bytes());
        p[OFF_VOLUME_COUNT..OFF_VOLUME_COUNT + 4]
            .copy_from_slice(&(self.volumes.len() as u32).to_le_bytes());

        let mut off = OFF_V6_VARIABLE;
        for root in self.refcount_shard_roots.iter().copied() {
            p[off..off + 8].copy_from_slice(&root.to_le_bytes());
            off += 8;
        }
        for head in self.dedup_level_heads.iter().copied() {
            p[off..off + 8].copy_from_slice(&head.to_le_bytes());
            off += 8;
        }
        for head in self.dedup_reverse_level_heads.iter().copied() {
            p[off..off + 8].copy_from_slice(&head.to_le_bytes());
            off += 8;
        }
        for entry in &self.snapshots {
            p[off..off + 8].copy_from_slice(&entry.id.to_le_bytes());
            p[off + 8..off + 10].copy_from_slice(&entry.vol_ord.to_le_bytes());
            // p[off + 10..off + 16] = reserved / zero
            p[off + 16..off + 24].copy_from_slice(&entry.l2p_roots_page.to_le_bytes());
            p[off + 24..off + 32].copy_from_slice(&entry.created_lsn.to_le_bytes());
            off += SNAPSHOT_ENTRY_SIZE;
        }
        for entry in &self.volumes {
            encode_volume_entry_inline(entry, p, &mut off)?;
        }
        Ok(())
    }

    fn decode(page: &Page, page_store: &PageStore) -> Result<Self> {
        let p = page.payload();
        let body_version = u32::from_le_bytes(
            p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
                .try_into()
                .unwrap(),
        );
        if body_version != MANIFEST_BODY_VERSION {
            return Err(MetaDbError::Corruption(format!(
                "unsupported manifest body version {body_version}; only v{} is readable — \
                 Phase 7 is fresh-install only, rebuild the database",
                MANIFEST_BODY_VERSION,
            )));
        }
        Self::decode_v6(page, page_store)
    }

    fn decode_v6(page: &Page, page_store: &PageStore) -> Result<Self> {
        let p = page.payload();
        let checkpoint_lsn = u64::from_le_bytes(
            p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
                .try_into()
                .unwrap(),
        );
        let free_list_head = u64::from_le_bytes(
            p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
                .try_into()
                .unwrap(),
        );
        let refcount_shard_count =
            u32::from_le_bytes(p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4].try_into().unwrap())
                as usize;
        if refcount_shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::Corruption(format!(
                "manifest refcount shard_count {refcount_shard_count} exceeds page capacity {MAX_SHARD_ROOTS_PER_PAGE}",
            )));
        }
        let dedup_level_count = u32::from_le_bytes(
            p[OFF_DEDUP_LEVEL_COUNT..OFF_DEDUP_LEVEL_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let dedup_reverse_level_count = u32::from_le_bytes(
            p[OFF_DEDUP_REVERSE_LEVEL_COUNT..OFF_DEDUP_REVERSE_LEVEL_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let next_snapshot_id = u64::from_le_bytes(
            p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
                .try_into()
                .unwrap(),
        );
        let next_volume_ord = u16::from_le_bytes(
            p[OFF_NEXT_VOLUME_ORD..OFF_NEXT_VOLUME_ORD + 2]
                .try_into()
                .unwrap(),
        );
        let snapshot_count = u32::from_le_bytes(
            p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let volume_count = u32::from_le_bytes(
            p[OFF_VOLUME_COUNT..OFF_VOLUME_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut off = OFF_V6_VARIABLE;
        let refcount_shard_roots = read_u64_vec(p, &mut off, refcount_shard_count);
        let dedup_level_heads = read_u64_vec(p, &mut off, dedup_level_count);
        let dedup_reverse_level_heads = read_u64_vec(p, &mut off, dedup_reverse_level_count);

        let total_dedup_levels = dedup_level_count + dedup_reverse_level_count;
        // Snapshot count capacity depends on how much budget the volume
        // table ends up eating below — we don't know it precisely yet,
        // so cap against the all-zero-volumes upper bound. The decode
        // will error out naturally if the subsequent volume region
        // cannot fit, via `decode_volume_entry_inline`.
        let max_snapshots = max_snapshots_for_layout(refcount_shard_count, total_dedup_levels, 0);
        if snapshot_count > max_snapshots {
            return Err(MetaDbError::Corruption(format!(
                "manifest snapshot_count {snapshot_count} exceeds capacity {max_snapshots}",
            )));
        }

        let mut snapshots = Vec::with_capacity(snapshot_count);
        for _ in 0..snapshot_count {
            let id = u64::from_le_bytes(p[off..off + 8].try_into().unwrap());
            let vol_ord = u16::from_le_bytes(p[off + 8..off + 10].try_into().unwrap());
            // bytes [off + 10, off + 16) are reserved and ignored.
            let l2p_roots_page = u64::from_le_bytes(p[off + 16..off + 24].try_into().unwrap());
            let created_lsn = u64::from_le_bytes(p[off + 24..off + 32].try_into().unwrap());
            let l2p_shard_roots = load_snapshot_roots(page_store, l2p_roots_page)?;
            snapshots.push(SnapshotEntry {
                id,
                vol_ord,
                l2p_roots_page,
                created_lsn,
                l2p_shard_roots,
            });
            off += SNAPSHOT_ENTRY_SIZE;
        }

        let mut volumes = Vec::with_capacity(volume_count);
        for _ in 0..volume_count {
            let entry = decode_volume_entry_inline(p, &mut off)?;
            volumes.push(entry);
        }

        Ok(Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn,
            free_list_head,
            refcount_shard_roots,
            dedup_level_heads,
            dedup_reverse_level_heads,
            next_snapshot_id,
            next_volume_ord,
            snapshots,
            volumes,
        })
    }
}

fn read_u64_vec(p: &[u8], off: &mut usize, count: usize) -> Box<[PageId]> {
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(u64::from_le_bytes(p[*off..*off + 8].try_into().unwrap()));
        *off += 8;
    }
    out.into_boxed_slice()
}

#[doc(hidden)]
pub fn decode_page_for_fuzz(page: &Page, page_store: &PageStore) -> Result<Manifest> {
    Manifest::decode(page, page_store)
}

#[cfg(test)]
mod tests;
