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
use crate::types::{Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, SnapshotId, VolumeOrdinal};

/// Version of the current manifest body layout.
pub const MANIFEST_BODY_VERSION: u32 = 6;

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

/// Flag bits on a [`VolumeEntry`].
pub const VOLUME_FLAG_DROP_PENDING: u8 = 0x01;

/// One entry in the v6 manifest `volumes` table.
///
/// `l2p_shard_roots` is stored inline when it fits in the residual page
/// budget; v6 will spill to an external `SnapshotRoots` page (reusing the
/// existing page type + codec) past the threshold. This struct is the
/// in-memory representation; see [`encode_volume_entry_inline`] /
/// [`decode_volume_entry_inline`] for the on-disk form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeEntry {
    pub ord: VolumeOrdinal,
    pub shard_count: u32,
    pub l2p_shard_roots: Box<[PageId]>,
    pub created_lsn: Lsn,
    pub flags: u8,
}

/// Size of a [`VolumeEntry`]'s fixed header when encoded inline. The
/// variable `l2p_shard_roots` tail follows immediately.
pub const VOLUME_ENTRY_FIXED_SIZE: usize = 2 /* ord */
    + 4 /* shard_count */
    + 8 /* created_lsn */
    + 1 /* flags */
    + 1 /* reserved / alignment */;

/// Inline-encoded byte length of a volume entry with the given shard count.
pub fn volume_entry_inline_size(shard_count: usize) -> usize {
    VOLUME_ENTRY_FIXED_SIZE + shard_count * size_of::<PageId>()
}

/// Encode `entry` inline into `buf[off..off+len]` and advance `off`.
///
/// Fails with [`MetaDbError::InvalidArgument`] if the buffer does not have
/// enough residual bytes or if `entry.shard_count` doesn't match the
/// length of `entry.l2p_shard_roots`.
pub fn encode_volume_entry_inline(
    entry: &VolumeEntry,
    buf: &mut [u8],
    off: &mut usize,
) -> Result<()> {
    if entry.l2p_shard_roots.len() != entry.shard_count as usize {
        return Err(MetaDbError::InvalidArgument(format!(
            "volume {} has {} roots, declared shard_count {}",
            entry.ord,
            entry.l2p_shard_roots.len(),
            entry.shard_count,
        )));
    }
    let needed = volume_entry_inline_size(entry.shard_count as usize);
    if buf.len() < *off + needed {
        return Err(MetaDbError::InvalidArgument(format!(
            "volume entry requires {needed} bytes, only {} available",
            buf.len().saturating_sub(*off),
        )));
    }
    buf[*off..*off + 2].copy_from_slice(&entry.ord.to_le_bytes());
    buf[*off + 2..*off + 6].copy_from_slice(&entry.shard_count.to_le_bytes());
    buf[*off + 6..*off + 14].copy_from_slice(&entry.created_lsn.to_le_bytes());
    buf[*off + 14] = entry.flags;
    buf[*off + 15] = 0; // reserved
    *off += VOLUME_ENTRY_FIXED_SIZE;
    for root in entry.l2p_shard_roots.iter().copied() {
        buf[*off..*off + 8].copy_from_slice(&root.to_le_bytes());
        *off += 8;
    }
    Ok(())
}

/// Decode one volume entry inline from `buf[off..]` and advance `off`.
pub fn decode_volume_entry_inline(buf: &[u8], off: &mut usize) -> Result<VolumeEntry> {
    if buf.len() < *off + VOLUME_ENTRY_FIXED_SIZE {
        return Err(MetaDbError::Corruption(format!(
            "volume entry truncated: expected {VOLUME_ENTRY_FIXED_SIZE} header bytes, {} remain",
            buf.len().saturating_sub(*off),
        )));
    }
    let ord = u16::from_le_bytes(buf[*off..*off + 2].try_into().unwrap());
    let shard_count = u32::from_le_bytes(buf[*off + 2..*off + 6].try_into().unwrap());
    let created_lsn = u64::from_le_bytes(buf[*off + 6..*off + 14].try_into().unwrap());
    let flags = buf[*off + 14];
    // buf[*off + 15] reserved
    *off += VOLUME_ENTRY_FIXED_SIZE;
    let needed_roots = shard_count as usize * size_of::<PageId>();
    if buf.len() < *off + needed_roots {
        return Err(MetaDbError::Corruption(format!(
            "volume {ord} roots truncated: need {needed_roots}, {} remain",
            buf.len().saturating_sub(*off),
        )));
    }
    let mut roots = Vec::with_capacity(shard_count as usize);
    for _ in 0..shard_count {
        roots.push(u64::from_le_bytes(buf[*off..*off + 8].try_into().unwrap()));
        *off += 8;
    }
    Ok(VolumeEntry {
        ord,
        shard_count,
        l2p_shard_roots: roots.into_boxed_slice(),
        created_lsn,
        flags,
    })
}

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

        let max_snapshots =
            max_snapshots_for_layout(refcount_shard_count, total_dedup_levels, volumes_budget_bytes);
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

/// Ensure a loaded manifest carries v4 metadata: any snapshot missing a
/// Allocate and write one snapshot-roots page.
pub(crate) fn write_snapshot_roots_page(
    page_store: &PageStore,
    roots: &[PageId],
    generation: Lsn,
) -> Result<PageId> {
    if roots.is_empty() {
        return Err(MetaDbError::InvalidArgument(
            "snapshot root vector cannot be empty".into(),
        ));
    }
    if roots.len() > MAX_SHARD_ROOTS_PER_PAGE {
        return Err(MetaDbError::InvalidArgument(format!(
            "snapshot root count {} exceeds page capacity {}",
            roots.len(),
            MAX_SHARD_ROOTS_PER_PAGE,
        )));
    }
    let pid = page_store.allocate()?;
    let mut page = Page::new(PageHeader::new(PageType::SnapshotRoots, generation));
    page.set_key_count(roots.len() as u16);
    let p = page.payload_mut();
    for (i, root) in roots.iter().copied().enumerate() {
        let off = i * 8;
        p[off..off + 8].copy_from_slice(&root.to_le_bytes());
    }
    page.seal();
    page_store.write_page(pid, &page)?;
    Ok(pid)
}

/// Load the shard-root vector stored in one snapshot-roots page.
pub(crate) fn load_snapshot_roots(page_store: &PageStore, pid: PageId) -> Result<Box<[PageId]>> {
    if pid == NULL_PAGE {
        return Err(MetaDbError::Corruption(
            "snapshot roots_page cannot be NULL_PAGE".into(),
        ));
    }
    let page = page_store.read_page(pid)?;
    let header = page.header()?;
    if header.page_type != PageType::SnapshotRoots {
        return Err(MetaDbError::Corruption(format!(
            "page {pid} is not a SnapshotRoots page: {:?}",
            header.page_type,
        )));
    }
    let count = header.key_count as usize;
    if count > MAX_SHARD_ROOTS_PER_PAGE {
        return Err(MetaDbError::Corruption(format!(
            "snapshot roots page {pid} declares {} roots, exceeds {}",
            count, MAX_SHARD_ROOTS_PER_PAGE,
        )));
    }
    let p = page.payload();
    let mut roots = Vec::with_capacity(count);
    for i in 0..count {
        let off = i * 8;
        roots.push(u64::from_le_bytes(p[off..off + 8].try_into().unwrap()));
    }
    Ok(roots.into_boxed_slice())
}

/// Owns the two manifest slots and orchestrates alternating commits.
pub struct ManifestStore {
    page_store: Arc<PageStore>,
    sequence: u64,
    /// Slot the next commit will write to. Toggled on every successful
    /// commit so the other slot always retains the previous durable
    /// generation.
    next_slot: PageId,
    faults: Arc<FaultController>,
}

/// Latest valid manifest slot loaded from disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedManifest {
    pub slot: PageId,
    pub sequence: u64,
    pub manifest: Manifest,
}

impl ManifestStore {
    /// Load the newest valid manifest slot from disk without mutating the
    /// page store. Returns `Ok(None)` if neither slot decodes.
    pub fn load_latest(page_store: &PageStore) -> Result<Option<LoadedManifest>> {
        let a = load_slot(page_store, MANIFEST_PAGE_A).map(|(sequence, manifest)| LoadedManifest {
            slot: MANIFEST_PAGE_A,
            sequence,
            manifest,
        });
        let b = load_slot(page_store, MANIFEST_PAGE_B).map(|(sequence, manifest)| LoadedManifest {
            slot: MANIFEST_PAGE_B,
            sequence,
            manifest,
        });
        Ok(match (a, b) {
            (Some(a), Some(b)) => Some(if a.sequence >= b.sequence { a } else { b }),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        })
    }

    /// Open an existing manifest. Unlike [`open_or_create`](Self::open_or_create),
    /// this never writes a fresh empty manifest when both slots are invalid.
    pub fn open_existing(
        page_store: Arc<PageStore>,
        faults: Arc<FaultController>,
    ) -> Result<(Self, Manifest)> {
        let Some(loaded) = Self::load_latest(&page_store)? else {
            return Err(MetaDbError::Corruption(
                "no valid manifest slot found in existing database".into(),
            ));
        };
        let next_slot = if loaded.slot == MANIFEST_PAGE_A {
            MANIFEST_PAGE_B
        } else {
            MANIFEST_PAGE_A
        };
        Ok((
            Self {
                page_store,
                sequence: loaded.sequence,
                next_slot,
                faults,
            },
            loaded.manifest,
        ))
    }

    /// Open the manifest for a page store, creating a fresh empty
    /// manifest on disk if neither slot is valid. Returns the loaded
    /// (or freshly-persisted) [`Manifest`] alongside the store.
    pub fn open_or_create(
        page_store: Arc<PageStore>,
        faults: Arc<FaultController>,
    ) -> Result<(Self, Manifest)> {
        if let Some(loaded) = Self::load_latest(&page_store)? {
            let next_slot = if loaded.slot == MANIFEST_PAGE_A {
                MANIFEST_PAGE_B
            } else {
                MANIFEST_PAGE_A
            };
            return Ok((
                Self {
                    page_store,
                    sequence: loaded.sequence,
                    next_slot,
                    faults,
                },
                loaded.manifest,
            ));
        }
        let mut store = Self {
            page_store,
            sequence: 0,
            next_slot: MANIFEST_PAGE_A,
            faults,
        };
        let empty = Manifest::empty();
        store.commit(&empty)?;
        Ok((store, empty))
    }

    /// Current in-memory sequence number; bumped by each successful
    /// [`commit`](Self::commit).
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Page id that the next commit will target.
    pub fn next_slot(&self) -> PageId {
        self.next_slot
    }

    /// Durably write a new manifest to the inactive slot and fsync.
    /// On failure, leaves the store's in-memory state untouched so the
    /// next attempt goes to the same slot.
    pub fn commit(&mut self, manifest: &Manifest) -> Result<()> {
        let new_sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption("manifest sequence overflow".into()))?;
        let target_slot = self.next_slot;

        let mut page = Page::new(PageHeader::new(PageType::Manifest, new_sequence));
        manifest.encode(&mut page)?;
        page.seal();

        self.page_store.write_page(target_slot, &page)?;
        self.faults.inject(FaultPoint::ManifestFsyncBefore)?;
        self.page_store.sync()?;
        self.faults.inject(FaultPoint::ManifestFsyncAfter)?;

        self.sequence = new_sequence;
        self.next_slot = if target_slot == MANIFEST_PAGE_A {
            MANIFEST_PAGE_B
        } else {
            MANIFEST_PAGE_A
        };
        Ok(())
    }
}

fn load_slot(page_store: &PageStore, slot: PageId) -> Option<(u64, Manifest)> {
    let page = page_store.read_page_unchecked(slot).ok()?;
    page.verify(slot).ok()?;
    let header = page.header().ok()?;
    if header.page_type != PageType::Manifest {
        return None;
    }
    let manifest = Manifest::decode(&page, page_store).ok()?;
    Some((header.generation, manifest))
}

#[doc(hidden)]
pub fn decode_page_for_fuzz(page: &Page, page_store: &PageStore) -> Result<Manifest> {
    Manifest::decode(page, page_store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PAGE_SIZE;
    use crate::page_store::PageStore;
    use crate::testing::faults::FaultAction;
    use tempfile::TempDir;

    fn mk_store(dir: &TempDir) -> Arc<PageStore> {
        Arc::new(PageStore::create(dir.path().join("pages.onyx_meta")).unwrap())
    }

    fn reopen(dir: &TempDir) -> Arc<PageStore> {
        Arc::new(PageStore::open(dir.path().join("pages.onyx_meta")).unwrap())
    }

    fn bx(v: &[PageId]) -> Box<[PageId]> {
        v.to_vec().into_boxed_slice()
    }

    fn snap(
        ps: &PageStore,
        id: SnapshotId,
        vol_ord: VolumeOrdinal,
        l2p: &[PageId],
        lsn: Lsn,
    ) -> SnapshotEntry {
        let l2p_page = write_snapshot_roots_page(ps, l2p, lsn).unwrap();
        SnapshotEntry {
            id,
            vol_ord,
            l2p_roots_page: l2p_page,
            created_lsn: lsn,
            l2p_shard_roots: bx(l2p),
        }
    }

    fn boot_vol(shard_count: u32, roots: &[PageId]) -> VolumeEntry {
        VolumeEntry {
            ord: 0,
            shard_count,
            l2p_shard_roots: bx(roots),
            created_lsn: 0,
            flags: 0,
        }
    }

    #[test]
    fn fresh_open_creates_empty_manifest_at_sequence_1() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let faults = FaultController::new();
        let (store, manifest) = ManifestStore::open_or_create(ps, faults).unwrap();
        assert_eq!(manifest, Manifest::empty());
        assert_eq!(store.sequence(), 1);
        assert_eq!(store.next_slot(), MANIFEST_PAGE_B);
    }

    #[test]
    fn commit_then_reopen_recovers_manifest() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let faults = FaultController::new();
        let (mut store, _) = ManifestStore::open_or_create(ps.clone(), faults).unwrap();

        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 1234,
            free_list_head: 99,
            refcount_shard_roots: bx(&[17, 18, 19, 20]),
            dedup_level_heads: bx(&[NULL_PAGE, NULL_PAGE]),
            dedup_reverse_level_heads: bx(&[NULL_PAGE]),
            next_snapshot_id: 5,
            next_volume_ord: 1,
            snapshots: vec![snap(&ps, 1, 0, &[11, 12, 13, 14], 100)],
            volumes: vec![boot_vol(4, &[7, 8, 9, 10])],
        };
        store.commit(&m).unwrap();
        drop(store);

        let ps2 = reopen(&dir);
        let (store2, loaded) = ManifestStore::open_or_create(ps2, FaultController::new()).unwrap();
        assert_eq!(loaded, m);
        assert_eq!(store2.sequence(), 2);
    }

    #[test]
    fn commits_alternate_slots() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let faults = FaultController::new();
        let (mut store, _) = ManifestStore::open_or_create(ps, faults).unwrap();
        let m = Manifest::empty();
        for expected_next in [
            MANIFEST_PAGE_A,
            MANIFEST_PAGE_B,
            MANIFEST_PAGE_A,
            MANIFEST_PAGE_B,
        ] {
            store.commit(&m).unwrap();
            assert_eq!(store.next_slot(), expected_next);
        }
        assert_eq!(store.sequence(), 5);
    }

    #[test]
    fn higher_sequence_wins_on_open() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let (mut store, _) = ManifestStore::open_or_create(ps, FaultController::new()).unwrap();

        for lsn in [1u64, 2, 3, 4, 5] {
            let mut m = Manifest::empty();
            m.checkpoint_lsn = lsn;
            store.commit(&m).unwrap();
        }
        drop(store);

        let (_, loaded) =
            ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
        assert_eq!(loaded.checkpoint_lsn, 5);
    }

    #[test]
    fn corrupt_slot_a_falls_back_to_slot_b() {
        let dir = TempDir::new().unwrap();
        let page_path = dir.path().join("pages.onyx_meta");
        let ps = mk_store(&dir);

        let faults = FaultController::new();
        let (mut store, _) = ManifestStore::open_or_create(ps, faults).unwrap();
        let mut target = Manifest::empty();
        target.checkpoint_lsn = 777;
        store.commit(&target).unwrap();
        drop(store);

        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&page_path)
                .unwrap();
            let off = MANIFEST_PAGE_B * PAGE_SIZE as u64 + 500;
            f.write_all_at(&[0xFF], off).unwrap();
            f.sync_all().unwrap();
        }

        let (store2, loaded) =
            ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
        assert_eq!(loaded, Manifest::empty());
        assert_eq!(store2.sequence(), 1);
        assert_eq!(store2.next_slot(), MANIFEST_PAGE_B);
    }

    #[test]
    fn both_slots_corrupt_rewrites_fresh_empty() {
        let dir = TempDir::new().unwrap();
        let page_path = dir.path().join("pages.onyx_meta");
        {
            let ps = mk_store(&dir);
            let (_, _) = ManifestStore::open_or_create(ps, FaultController::new()).unwrap();
        }
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&page_path)
                .unwrap();
            f.write_all_at(&[0xFFu8; 8192], 0).unwrap();
            f.sync_all().unwrap();
        }
        let (store, manifest) =
            ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
        assert_eq!(manifest, Manifest::empty());
        assert_eq!(store.sequence(), 1);
    }

    #[test]
    fn fsync_before_error_does_not_advance_state() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let faults = FaultController::new();
        let (mut store, _) = ManifestStore::open_or_create(ps, faults.clone()).unwrap();
        let start_seq = store.sequence();
        let start_slot = store.next_slot();

        faults.install(FaultPoint::ManifestFsyncBefore, 1, FaultAction::Error);
        let mut m = Manifest::empty();
        m.checkpoint_lsn = 42;
        assert!(store.commit(&m).is_err());
        assert_eq!(store.sequence(), start_seq);
        assert_eq!(store.next_slot(), start_slot);
    }

    #[test]
    fn fsync_after_error_durably_wrote_but_callers_sees_err() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let faults = FaultController::new();
        let (mut store, _) = ManifestStore::open_or_create(ps, faults.clone()).unwrap();

        faults.install(FaultPoint::ManifestFsyncAfter, 1, FaultAction::Error);
        let mut m = Manifest::empty();
        m.checkpoint_lsn = 42;
        assert!(store.commit(&m).is_err());
        drop(store);

        let (_, loaded) =
            ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
        assert_eq!(loaded, m);
    }

    #[test]
    fn body_decode_rejects_wrong_version() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        let p = page.payload_mut();
        p[0..4].copy_from_slice(&99u32.to_le_bytes());
        page.seal();
        assert!(matches!(
            Manifest::decode(&page, &ps).unwrap_err(),
            MetaDbError::Corruption(_)
        ));
    }

    #[test]
    fn encode_decode_round_trip_with_refcount_and_dedup() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0xDEAD_BEEF_CAFE,
            free_list_head: 1234,
            refcount_shard_roots: bx(&[142, 143, 144, 145]),
            dedup_level_heads: bx(&[NULL_PAGE, 200, 300]),
            dedup_reverse_level_heads: bx(&[NULL_PAGE, 400]),
            next_snapshot_id: 99,
            next_volume_ord: 1,
            snapshots: vec![
                snap(&ps, 1, 0, &[10, 11, 12, 13], 100),
                snap(&ps, 5, 0, &[20, 21, 22, 23], 500),
            ],
            volumes: vec![boot_vol(4, &[42, 43, 44, 45])],
        };
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 7));
        m.encode(&mut page).unwrap();
        page.seal();
        page.verify(MANIFEST_PAGE_A).unwrap();
        let decoded = Manifest::decode(&page, &ps).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn encode_rejects_oversized_snapshot_table() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut m = Manifest::empty();
        m.refcount_shard_roots = bx(&[1, 2, 3, 4]);
        let cap = max_snapshots_for_shards(m.shard_count());
        assert!(cap > 0);
        for i in 0..(cap + 1) as u64 {
            m.snapshots.push(snap(&ps, i, 0, &[10, 11, 12, 13], i));
        }
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        assert!(m.encode(&mut page).is_err());
    }

    #[test]
    fn find_snapshot_locates_by_id() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut m = Manifest::empty();
        m.refcount_shard_roots = bx(&[1, 2, 3, 4]);
        m.snapshots.push(snap(&ps, 7, 0, &[42, 43, 44, 45], 100));
        assert_eq!(m.find_snapshot(7).unwrap().id, 7);
        assert_eq!(
            m.find_snapshot(7).unwrap().l2p_shard_roots.as_ref(),
            &[42, 43, 44, 45]
        );
        assert!(m.find_snapshot(99).is_none());
    }

    #[test]
    fn decode_rejects_pre_v6_body_versions() {
        // v5 and v4 are no longer supported — Phase 7 is fresh-install
        // only. Any body_version other than v6 reports `Corruption`.
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        for bad_version in [3u32, 4, 5] {
            let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
            {
                let p = page.payload_mut();
                p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
                    .copy_from_slice(&bad_version.to_le_bytes());
            }
            page.seal();
            match Manifest::decode(&page, &ps).unwrap_err() {
                MetaDbError::Corruption(msg) => {
                    assert!(msg.contains("unsupported manifest body version"), "{msg}");
                }
                e => panic!("expected corruption, got {e}"),
            }
        }
    }

    #[test]
    fn v6_volumes_table_round_trip() {
        // Exercise multi-volume encode/decode with non-zero ords and a
        // drop-pending flag so any per-entry alignment slip would show.
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 10,
            free_list_head: NULL_PAGE,
            refcount_shard_roots: bx(&[50, 51]),
            dedup_level_heads: Vec::new().into_boxed_slice(),
            dedup_reverse_level_heads: Vec::new().into_boxed_slice(),
            next_snapshot_id: 2,
            next_volume_ord: 3,
            snapshots: vec![snap(&ps, 1, 2, &[100, 101], 10)],
            volumes: vec![
                boot_vol(2, &[200, 201]),
                VolumeEntry {
                    ord: 2,
                    shard_count: 2,
                    l2p_shard_roots: bx(&[300, 301]),
                    created_lsn: 7,
                    flags: VOLUME_FLAG_DROP_PENDING,
                },
            ],
        };
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        m.encode(&mut page).unwrap();
        page.seal();
        let decoded = Manifest::decode(&page, &ps).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn v6_rejects_volume_count_exceeding_capacity() {
        // Cram the volume table with entries until encode has to
        // complain. Uses tiny volumes (shard_count = 1) so the failure
        // comes from the snapshot-table capacity check, not from an
        // inline-volume-entry budget mismatch.
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut m = Manifest::empty();
        m.refcount_shard_roots = bx(&[1]);
        // Start with just the bootstrap — this sets the baseline volume budget.
        m.volumes.push(boot_vol(1, &[10]));
        let baseline_budget: usize = m
            .volumes
            .iter()
            .map(|v| volume_entry_inline_size(v.shard_count as usize))
            .sum();
        let cap = max_snapshots_for_layout(m.shard_count(), 0, baseline_budget);
        for i in 0..(cap + 1) as u64 {
            m.snapshots.push(snap(&ps, i, 0, &[10], i));
        }
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        assert!(m.encode(&mut page).is_err());
    }

    #[test]
    fn volume_entry_inline_round_trip() {
        let entry = VolumeEntry {
            ord: 42,
            shard_count: 4,
            l2p_shard_roots: bx(&[100, 101, 102, 103]),
            created_lsn: 0xABCD_1234,
            flags: VOLUME_FLAG_DROP_PENDING,
        };
        let mut buf = vec![0u8; volume_entry_inline_size(entry.shard_count as usize)];
        let mut off = 0;
        encode_volume_entry_inline(&entry, &mut buf, &mut off).unwrap();
        assert_eq!(off, buf.len());
        let mut off = 0;
        let decoded = decode_volume_entry_inline(&buf, &mut off).unwrap();
        assert_eq!(decoded, entry);
        assert_eq!(off, buf.len());
    }

    #[test]
    fn volume_entry_inline_rejects_shard_count_mismatch() {
        let entry = VolumeEntry {
            ord: 1,
            shard_count: 2,
            l2p_shard_roots: bx(&[7]), // length 1, but shard_count 2
            created_lsn: 10,
            flags: 0,
        };
        let mut buf = vec![0u8; 256];
        let mut off = 0;
        assert!(matches!(
            encode_volume_entry_inline(&entry, &mut buf, &mut off),
            Err(MetaDbError::InvalidArgument(_))
        ));
    }

    #[test]
    fn volume_entry_inline_rejects_buffer_too_small() {
        let entry = VolumeEntry {
            ord: 9,
            shard_count: 16,
            l2p_shard_roots: bx(&[1; 16]),
            created_lsn: 0,
            flags: 0,
        };
        let mut buf = vec![0u8; VOLUME_ENTRY_FIXED_SIZE + 8]; // one root worth
        let mut off = 0;
        assert!(matches!(
            encode_volume_entry_inline(&entry, &mut buf, &mut off),
            Err(MetaDbError::InvalidArgument(_))
        ));
    }

    #[test]
    fn volume_entry_decode_rejects_truncated_roots() {
        // Encode a legit entry, then lop the final root off the buffer.
        let entry = VolumeEntry {
            ord: 3,
            shard_count: 3,
            l2p_shard_roots: bx(&[11, 22, 33]),
            created_lsn: 7,
            flags: 0,
        };
        let mut buf = vec![0u8; volume_entry_inline_size(3)];
        let mut off = 0;
        encode_volume_entry_inline(&entry, &mut buf, &mut off).unwrap();
        buf.truncate(buf.len() - 8);
        let mut off = 0;
        assert!(matches!(
            decode_volume_entry_inline(&buf, &mut off),
            Err(MetaDbError::Corruption(_))
        ));
    }

    #[test]
    fn volume_entry_many_back_to_back() {
        // Write several entries contiguously into a single buffer, decode
        // them all, verify the sliding offset + round-trip equality.
        let entries = vec![
            VolumeEntry {
                ord: 0,
                shard_count: 2,
                l2p_shard_roots: bx(&[10, 11]),
                created_lsn: 100,
                flags: 0,
            },
            VolumeEntry {
                ord: 1,
                shard_count: 4,
                l2p_shard_roots: bx(&[20, 21, 22, 23]),
                created_lsn: 200,
                flags: VOLUME_FLAG_DROP_PENDING,
            },
            VolumeEntry {
                ord: 65534,
                shard_count: 1,
                l2p_shard_roots: bx(&[NULL_PAGE]),
                created_lsn: 300,
                flags: 0,
            },
        ];
        let total: usize = entries
            .iter()
            .map(|e| volume_entry_inline_size(e.shard_count as usize))
            .sum();
        let mut buf = vec![0u8; total];
        let mut off = 0;
        for entry in &entries {
            encode_volume_entry_inline(entry, &mut buf, &mut off).unwrap();
        }
        assert_eq!(off, total);
        let mut off = 0;
        for expected in &entries {
            let got = decode_volume_entry_inline(&buf, &mut off).unwrap();
            assert_eq!(&got, expected);
        }
        assert_eq!(off, total);
    }
}
