//! Double-buffered manifest living in pages 0 and 1 of the page store.
//!
//! Phase 5 extends the phase-4 single-index manifest to cover the
//! second index type (dedup LSM) and the PBA refcount B+tree:
//! - current L2P shard roots (inline)
//! - current PBA refcount shard roots (inline, same shard count as L2P)
//! - LSM level-chain head page ids (one per level)
//! - snapshots, each of which has its own pair of
//!   [`PageType::SnapshotRoots`] pages: one for L2P, one for refcount
//!
//! Keeping the two roots-per-snapshot pages separate means the existing
//! page codec doesn't need to grow, and snapshot delete can free each
//! pair independently. A v3 manifest (phase 4) upgrades in place on
//! open: we invent a fresh empty refcount tree plus an empty dedup
//! LSM and rewrite the body as v4.

use std::mem::size_of;
use std::sync::Arc;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::{Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, SnapshotId};

/// Version of the current manifest body layout.
pub const MANIFEST_BODY_VERSION: u32 = 4;

// v4 body layout.
const OFF_BODY_VERSION: usize = 0;
const OFF_CHECKPOINT_LSN: usize = 4;
const OFF_FREE_LIST_HEAD: usize = 12;
const OFF_SHARD_COUNT: usize = 20;
const OFF_DEDUP_LEVEL_COUNT: usize = 24;
const OFF_NEXT_SNAPSHOT_ID: usize = 28;
const OFF_SNAPSHOT_COUNT: usize = 36;
const OFF_V4_VARIABLE: usize = 40;
/// v4 per-snapshot entry: id, l2p_roots_page, refcount_roots_page,
/// created_lsn = 4 × u64 = 32 bytes.
const SNAPSHOT_ENTRY_SIZE: usize = 32;

// v3 compatibility layout (phase 4). Single index, roots inline,
// snapshot entries 24 bytes (id + roots_page + created_lsn).
const V3_BODY_VERSION: u32 = 3;
const V3_OFF_SHARD_COUNT: usize = 20;
const V3_OFF_NEXT_SNAPSHOT_ID: usize = 24;
const V3_OFF_SNAPSHOT_COUNT: usize = 32;
const V3_OFF_VARIABLE: usize = 36;
const V3_SNAPSHOT_ENTRY_SIZE: usize = 24;

const _: () = {
    assert!(OFF_BODY_VERSION + 4 == OFF_CHECKPOINT_LSN);
    assert!(OFF_CHECKPOINT_LSN + 8 == OFF_FREE_LIST_HEAD);
    assert!(OFF_FREE_LIST_HEAD + 8 == OFF_SHARD_COUNT);
    assert!(OFF_SHARD_COUNT + 4 == OFF_DEDUP_LEVEL_COUNT);
    assert!(OFF_DEDUP_LEVEL_COUNT + 4 == OFF_NEXT_SNAPSHOT_ID);
    assert!(OFF_NEXT_SNAPSHOT_ID + 8 == OFF_SNAPSHOT_COUNT);
    assert!(OFF_SNAPSHOT_COUNT + 4 == OFF_V4_VARIABLE);
    assert!(SNAPSHOT_ENTRY_SIZE == 32);
    assert!(V3_SNAPSHOT_ENTRY_SIZE == 24);
};

/// Maximum number of shard roots that fit in one snapshot-roots page.
pub const MAX_SHARD_ROOTS_PER_PAGE: usize = PAGE_PAYLOAD_SIZE / size_of::<PageId>();

/// Maximum number of snapshot entries that fit in a v4 manifest for the
/// given shard and dedup-level counts. The variable region stores
/// `2 × shard_count` (L2P + refcount) plus `dedup_level_count` page ids
/// before the snapshot table begins.
pub fn max_snapshots_for_shards(shard_count: usize) -> usize {
    max_snapshots_for_layout(shard_count, 0)
}

/// Same as [`max_snapshots_for_shards`] but accounts for the
/// dedup-level count explicitly.
pub fn max_snapshots_for_layout(shard_count: usize, dedup_level_count: usize) -> usize {
    let roots_bytes = match shard_count
        .checked_mul(2)
        .and_then(|v| v.checked_mul(size_of::<PageId>()))
    {
        Some(v) => v,
        None => return 0,
    };
    let dedup_bytes = match dedup_level_count.checked_mul(size_of::<PageId>()) {
        Some(v) => v,
        None => return 0,
    };
    let used = OFF_V4_VARIABLE + roots_bytes + dedup_bytes;
    if used > PAGE_PAYLOAD_SIZE {
        return 0;
    }
    (PAGE_PAYLOAD_SIZE - used) / SNAPSHOT_ENTRY_SIZE
}

/// One snapshot's manifest entry. Each snapshot owns two SnapshotRoots
/// pages: one for its L2P shard-root vector and one for its refcount
/// shard-root vector. `NULL_PAGE` indicates a legacy v3 entry that has
/// not been rewritten as v4 yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub id: SnapshotId,
    pub l2p_roots_page: PageId,
    pub refcount_roots_page: PageId,
    pub created_lsn: Lsn,
    pub l2p_shard_roots: Box<[PageId]>,
    pub refcount_shard_roots: Box<[PageId]>,
}

impl SnapshotEntry {
    fn needs_l2p_roots_page(&self) -> bool {
        self.l2p_roots_page == NULL_PAGE
    }

    fn needs_refcount_roots_page(&self) -> bool {
        self.refcount_roots_page == NULL_PAGE && !self.refcount_shard_roots.is_empty()
    }
}

/// Decoded manifest body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub body_version: u32,
    /// Greatest LSN whose WAL record has been applied to durable state.
    pub checkpoint_lsn: Lsn,
    /// Head of the persisted free-list page chain, or [`NULL_PAGE`].
    pub free_list_head: PageId,
    /// Current per-shard L2P B+tree roots.
    pub shard_roots: Box<[PageId]>,
    /// Current per-shard PBA-refcount B+tree roots. Same length as
    /// `shard_roots`. Empty if the manifest was loaded as v3 and has
    /// not yet been materialized.
    pub refcount_shard_roots: Box<[PageId]>,
    /// Head page id of each dedup LSM level's chain. One per level;
    /// `NULL_PAGE` for an empty level.
    pub dedup_level_heads: Box<[PageId]>,
    /// Monotonic counter: next snapshot id to hand out.
    pub next_snapshot_id: u64,
    /// Registered snapshots, in order of creation.
    pub snapshots: Vec<SnapshotEntry>,
}

impl Manifest {
    /// Freshly-created empty manifest for a brand-new database.
    pub fn empty() -> Self {
        Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0,
            free_list_head: NULL_PAGE,
            shard_roots: Vec::new().into_boxed_slice(),
            refcount_shard_roots: Vec::new().into_boxed_slice(),
            dedup_level_heads: Vec::new().into_boxed_slice(),
            next_snapshot_id: 1,
            snapshots: Vec::new(),
        }
    }

    /// Number of shards currently tracked by the manifest.
    pub fn shard_count(&self) -> usize {
        self.shard_roots.len()
    }

    /// Number of dedup levels currently tracked.
    pub fn dedup_level_count(&self) -> usize {
        self.dedup_level_heads.len()
    }

    /// Find a snapshot by id.
    pub fn find_snapshot(&self, id: SnapshotId) -> Option<&SnapshotEntry> {
        self.snapshots.iter().find(|e| e.id == id)
    }

    fn encode(&self, page: &mut Page) -> Result<()> {
        let shard_count = self.shard_roots.len();
        if shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest shard count {} exceeds page capacity {}",
                shard_count, MAX_SHARD_ROOTS_PER_PAGE,
            )));
        }
        if !self.refcount_shard_roots.is_empty() && self.refcount_shard_roots.len() != shard_count {
            return Err(MetaDbError::InvalidArgument(format!(
                "refcount shard count {} does not match L2P shard count {shard_count}",
                self.refcount_shard_roots.len(),
            )));
        }
        let dedup_level_count = self.dedup_level_heads.len();
        let max_snapshots = max_snapshots_for_layout(shard_count, dedup_level_count);
        if self.snapshots.len() > max_snapshots {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest snapshot count {} exceeds capacity {max_snapshots}",
                self.snapshots.len(),
            )));
        }
        for entry in &self.snapshots {
            if entry.l2p_shard_roots.len() != shard_count {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} has {} L2P roots, expected {shard_count}",
                    entry.id,
                    entry.l2p_shard_roots.len(),
                )));
            }
            if !self.refcount_shard_roots.is_empty()
                && entry.refcount_shard_roots.len() != shard_count
            {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} has {} refcount roots, expected {shard_count}",
                    entry.id,
                    entry.refcount_shard_roots.len(),
                )));
            }
            if entry.needs_l2p_roots_page() {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} is missing l2p_roots_page",
                    entry.id,
                )));
            }
            if entry.needs_refcount_roots_page() {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} is missing refcount_roots_page",
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
            .copy_from_slice(&(shard_count as u32).to_le_bytes());
        p[OFF_DEDUP_LEVEL_COUNT..OFF_DEDUP_LEVEL_COUNT + 4]
            .copy_from_slice(&(dedup_level_count as u32).to_le_bytes());
        p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
            .copy_from_slice(&self.next_snapshot_id.to_le_bytes());
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
            .copy_from_slice(&(self.snapshots.len() as u32).to_le_bytes());

        let mut off = OFF_V4_VARIABLE;
        for root in self.shard_roots.iter().copied() {
            p[off..off + 8].copy_from_slice(&root.to_le_bytes());
            off += 8;
        }
        // Refcount roots region is always sized to match L2P, zero-filled
        // if we haven't materialized refcount trees yet. That way the
        // layout is stable across upgrades.
        for i in 0..shard_count {
            let root = if self.refcount_shard_roots.is_empty() {
                NULL_PAGE
            } else {
                self.refcount_shard_roots[i]
            };
            p[off..off + 8].copy_from_slice(&root.to_le_bytes());
            off += 8;
        }
        for head in self.dedup_level_heads.iter().copied() {
            p[off..off + 8].copy_from_slice(&head.to_le_bytes());
            off += 8;
        }
        for entry in &self.snapshots {
            p[off..off + 8].copy_from_slice(&entry.id.to_le_bytes());
            p[off + 8..off + 16].copy_from_slice(&entry.l2p_roots_page.to_le_bytes());
            p[off + 16..off + 24].copy_from_slice(&entry.refcount_roots_page.to_le_bytes());
            p[off + 24..off + 32].copy_from_slice(&entry.created_lsn.to_le_bytes());
            off += SNAPSHOT_ENTRY_SIZE;
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
        match body_version {
            MANIFEST_BODY_VERSION => Self::decode_v4(page, page_store),
            V3_BODY_VERSION => Self::decode_v3(page, page_store),
            other => Err(MetaDbError::Corruption(format!(
                "unsupported manifest body version {other}",
            ))),
        }
    }

    fn decode_v4(page: &Page, page_store: &PageStore) -> Result<Self> {
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
        let shard_count =
            u32::from_le_bytes(p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4].try_into().unwrap())
                as usize;
        if shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::Corruption(format!(
                "manifest shard_count {shard_count} exceeds page capacity {MAX_SHARD_ROOTS_PER_PAGE}",
            )));
        }
        let dedup_level_count = u32::from_le_bytes(
            p[OFF_DEDUP_LEVEL_COUNT..OFF_DEDUP_LEVEL_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let next_snapshot_id = u64::from_le_bytes(
            p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
                .try_into()
                .unwrap(),
        );
        let snapshot_count = u32::from_le_bytes(
            p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let max_snapshots = max_snapshots_for_layout(shard_count, dedup_level_count);
        if snapshot_count > max_snapshots {
            return Err(MetaDbError::Corruption(format!(
                "manifest snapshot_count {snapshot_count} exceeds capacity {max_snapshots}",
            )));
        }

        let mut off = OFF_V4_VARIABLE;
        let shard_roots = read_u64_vec(p, &mut off, shard_count);
        let refcount_shard_roots_raw = read_u64_vec(p, &mut off, shard_count);
        let refcount_shard_roots = if refcount_shard_roots_raw.iter().all(|r| *r == NULL_PAGE) {
            Vec::new().into_boxed_slice()
        } else {
            refcount_shard_roots_raw
        };
        let dedup_level_heads = read_u64_vec(p, &mut off, dedup_level_count);

        let mut snapshots = Vec::with_capacity(snapshot_count);
        for _ in 0..snapshot_count {
            let id = u64::from_le_bytes(p[off..off + 8].try_into().unwrap());
            let l2p_roots_page = u64::from_le_bytes(p[off + 8..off + 16].try_into().unwrap());
            let refcount_roots_page = u64::from_le_bytes(p[off + 16..off + 24].try_into().unwrap());
            let created_lsn = u64::from_le_bytes(p[off + 24..off + 32].try_into().unwrap());
            let l2p_shard_roots = load_snapshot_roots(page_store, l2p_roots_page)?;
            if l2p_shard_roots.len() != shard_count {
                return Err(MetaDbError::Corruption(format!(
                    "snapshot {id} L2P roots page has {} entries, expected {shard_count}",
                    l2p_shard_roots.len(),
                )));
            }
            let refcount_shard_roots = if refcount_roots_page == NULL_PAGE {
                Vec::new().into_boxed_slice()
            } else {
                let rr = load_snapshot_roots(page_store, refcount_roots_page)?;
                if rr.len() != shard_count {
                    return Err(MetaDbError::Corruption(format!(
                        "snapshot {id} refcount roots page has {} entries, expected {shard_count}",
                        rr.len(),
                    )));
                }
                rr
            };
            snapshots.push(SnapshotEntry {
                id,
                l2p_roots_page,
                refcount_roots_page,
                created_lsn,
                l2p_shard_roots,
                refcount_shard_roots,
            });
            off += SNAPSHOT_ENTRY_SIZE;
        }

        Ok(Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn,
            free_list_head,
            shard_roots,
            refcount_shard_roots,
            dedup_level_heads,
            next_snapshot_id,
            snapshots,
        })
    }

    fn decode_v3(page: &Page, page_store: &PageStore) -> Result<Self> {
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
        let shard_count = u32::from_le_bytes(
            p[V3_OFF_SHARD_COUNT..V3_OFF_SHARD_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::Corruption(format!(
                "v3 manifest shard_count {shard_count} exceeds page capacity {MAX_SHARD_ROOTS_PER_PAGE}",
            )));
        }
        let next_snapshot_id = u64::from_le_bytes(
            p[V3_OFF_NEXT_SNAPSHOT_ID..V3_OFF_NEXT_SNAPSHOT_ID + 8]
                .try_into()
                .unwrap(),
        );
        let snapshot_count = u32::from_le_bytes(
            p[V3_OFF_SNAPSHOT_COUNT..V3_OFF_SNAPSHOT_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut off = V3_OFF_VARIABLE;
        let shard_roots = read_u64_vec(p, &mut off, shard_count);

        let mut snapshots = Vec::with_capacity(snapshot_count);
        for _ in 0..snapshot_count {
            let id = u64::from_le_bytes(p[off..off + 8].try_into().unwrap());
            let roots_page = u64::from_le_bytes(p[off + 8..off + 16].try_into().unwrap());
            let created_lsn = u64::from_le_bytes(p[off + 16..off + 24].try_into().unwrap());
            let l2p_shard_roots = load_snapshot_roots(page_store, roots_page)?;
            snapshots.push(SnapshotEntry {
                id,
                l2p_roots_page: roots_page,
                refcount_roots_page: NULL_PAGE,
                created_lsn,
                l2p_shard_roots,
                refcount_shard_roots: Vec::new().into_boxed_slice(),
            });
            off += V3_SNAPSHOT_ENTRY_SIZE;
        }

        Ok(Self {
            body_version: V3_BODY_VERSION,
            checkpoint_lsn,
            free_list_head,
            shard_roots,
            refcount_shard_roots: Vec::new().into_boxed_slice(),
            dedup_level_heads: Vec::new().into_boxed_slice(),
            next_snapshot_id,
            snapshots,
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
/// refcount roots page gets one allocated, and the body_version is
/// bumped to v4. Returns `true` if any pages were allocated OR the body
/// version was bumped.
pub(crate) fn materialize_snapshot_root_pages(
    page_store: &PageStore,
    manifest: &mut Manifest,
    generation: Lsn,
) -> Result<bool> {
    let mut changed = manifest.body_version != MANIFEST_BODY_VERSION;
    manifest.body_version = MANIFEST_BODY_VERSION;
    for entry in &mut manifest.snapshots {
        if entry.needs_l2p_roots_page() {
            entry.l2p_roots_page =
                write_snapshot_roots_page(page_store, &entry.l2p_shard_roots, generation)?;
            changed = true;
        }
        if entry.needs_refcount_roots_page() {
            entry.refcount_roots_page =
                write_snapshot_roots_page(page_store, &entry.refcount_shard_roots, generation)?;
            changed = true;
        }
    }
    Ok(changed)
}

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

impl ManifestStore {
    /// Open the manifest for a page store, creating a fresh empty
    /// manifest on disk if neither slot is valid. Returns the loaded
    /// (or freshly-persisted) [`Manifest`] alongside the store.
    pub fn open_or_create(
        page_store: Arc<PageStore>,
        faults: Arc<FaultController>,
    ) -> Result<(Self, Manifest)> {
        let a = load_slot(&page_store, MANIFEST_PAGE_A);
        let b = load_slot(&page_store, MANIFEST_PAGE_B);
        match (a, b) {
            (Some((ga, ma)), Some((gb, mb))) => {
                if ga >= gb {
                    Ok((
                        Self {
                            page_store,
                            sequence: ga,
                            next_slot: MANIFEST_PAGE_B,
                            faults,
                        },
                        ma,
                    ))
                } else {
                    Ok((
                        Self {
                            page_store,
                            sequence: gb,
                            next_slot: MANIFEST_PAGE_A,
                            faults,
                        },
                        mb,
                    ))
                }
            }
            (Some((g, m)), None) => Ok((
                Self {
                    page_store,
                    sequence: g,
                    next_slot: MANIFEST_PAGE_B,
                    faults,
                },
                m,
            )),
            (None, Some((g, m))) => Ok((
                Self {
                    page_store,
                    sequence: g,
                    next_slot: MANIFEST_PAGE_A,
                    faults,
                },
                m,
            )),
            (None, None) => {
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
        }
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
        l2p: &[PageId],
        refcount: &[PageId],
        lsn: Lsn,
    ) -> SnapshotEntry {
        let l2p_page = write_snapshot_roots_page(ps, l2p, lsn).unwrap();
        let refcount_page = if refcount.is_empty() {
            NULL_PAGE
        } else {
            write_snapshot_roots_page(ps, refcount, lsn).unwrap()
        };
        SnapshotEntry {
            id,
            l2p_roots_page: l2p_page,
            refcount_roots_page: refcount_page,
            created_lsn: lsn,
            l2p_shard_roots: bx(l2p),
            refcount_shard_roots: bx(refcount),
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
            shard_roots: bx(&[7, 8, 9, 10]),
            refcount_shard_roots: bx(&[17, 18, 19, 20]),
            dedup_level_heads: bx(&[NULL_PAGE, NULL_PAGE]),
            next_snapshot_id: 5,
            snapshots: vec![snap(&ps, 1, &[11, 12, 13, 14], &[21, 22, 23, 24], 100)],
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
            shard_roots: bx(&[42, 43, 44, 45]),
            refcount_shard_roots: bx(&[142, 143, 144, 145]),
            dedup_level_heads: bx(&[NULL_PAGE, 200, 300]),
            next_snapshot_id: 99,
            snapshots: vec![
                snap(&ps, 1, &[10, 11, 12, 13], &[110, 111, 112, 113], 100),
                snap(&ps, 5, &[20, 21, 22, 23], &[120, 121, 122, 123], 500),
            ],
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
        m.shard_roots = bx(&[1, 2, 3, 4]);
        for i in 0..(max_snapshots_for_shards(m.shard_count()) + 1) as u64 {
            m.snapshots.push(snap(&ps, i, &[10, 11, 12, 13], &[], i));
        }
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        assert!(m.encode(&mut page).is_err());
    }

    #[test]
    fn find_snapshot_locates_by_id() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut m = Manifest::empty();
        m.shard_roots = bx(&[1, 2, 3, 4]);
        m.snapshots.push(snap(&ps, 7, &[42, 43, 44, 45], &[], 100));
        assert_eq!(m.find_snapshot(7).unwrap().id, 7);
        assert_eq!(
            m.find_snapshot(7).unwrap().l2p_shard_roots.as_ref(),
            &[42, 43, 44, 45]
        );
        assert!(m.find_snapshot(99).is_none());
    }

    #[test]
    fn decode_v3_rolled_forward_to_v4() {
        // Build a v3 payload by hand and run it through decode.
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        {
            let p = page.payload_mut();
            p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
                .copy_from_slice(&V3_BODY_VERSION.to_le_bytes());
            p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8].copy_from_slice(&123u64.to_le_bytes());
            p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8].copy_from_slice(&55u64.to_le_bytes());
            p[V3_OFF_SHARD_COUNT..V3_OFF_SHARD_COUNT + 4].copy_from_slice(&4u32.to_le_bytes());
            p[V3_OFF_NEXT_SNAPSHOT_ID..V3_OFF_NEXT_SNAPSHOT_ID + 8]
                .copy_from_slice(&8u64.to_le_bytes());
            p[V3_OFF_SNAPSHOT_COUNT..V3_OFF_SNAPSHOT_COUNT + 4]
                .copy_from_slice(&0u32.to_le_bytes());
            let mut off = V3_OFF_VARIABLE;
            for root in [7u64, 8, 9, 10] {
                p[off..off + 8].copy_from_slice(&root.to_le_bytes());
                off += 8;
            }
        }
        page.seal();

        let decoded = Manifest::decode(&page, &ps).unwrap();
        assert_eq!(decoded.body_version, V3_BODY_VERSION);
        assert_eq!(decoded.shard_roots.as_ref(), &[7, 8, 9, 10]);
        assert!(decoded.refcount_shard_roots.is_empty());
        assert!(decoded.dedup_level_heads.is_empty());
        assert_eq!(decoded.next_snapshot_id, 8);
    }

    #[test]
    fn materialize_v3_snapshot_root_pages_upgrades_manifest() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        // Synthesize a v3-style loaded manifest.
        let l2p_page = write_snapshot_roots_page(&ps, &[11, 12, 13, 14], 10).unwrap();
        let mut manifest = Manifest {
            body_version: V3_BODY_VERSION,
            checkpoint_lsn: 0,
            free_list_head: NULL_PAGE,
            shard_roots: bx(&[7, 8, 9, 10]),
            refcount_shard_roots: Vec::new().into_boxed_slice(),
            dedup_level_heads: Vec::new().into_boxed_slice(),
            next_snapshot_id: 2,
            snapshots: vec![SnapshotEntry {
                id: 1,
                l2p_roots_page: l2p_page,
                refcount_roots_page: NULL_PAGE,
                created_lsn: 10,
                l2p_shard_roots: bx(&[11, 12, 13, 14]),
                refcount_shard_roots: Vec::new().into_boxed_slice(),
            }],
        };
        // With refcount state still empty, materialize should only bump
        // the body_version.
        assert!(materialize_snapshot_root_pages(&ps, &mut manifest, 77).unwrap());
        assert_eq!(manifest.body_version, MANIFEST_BODY_VERSION);
        assert_eq!(manifest.snapshots[0].refcount_roots_page, NULL_PAGE);
    }
}
