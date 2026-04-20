//! Double-buffered manifest living in pages 0 and 1 of the page store.
//!
//! Phase 4 extends the phase-3 single-root manifest into:
//! - current per-shard root vector, stored inline in the manifest body
//! - snapshot entries that point at dedicated [`PageType::SnapshotRoots`]
//!   pages holding that snapshot's pinned shard-root vector
//!
//! Keeping the current roots inline makes open cheap, while moving
//! snapshot roots out to separate pages keeps snapshot capacity high even
//! with 16 shards.

use std::mem::size_of;
use std::sync::Arc;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::{Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, SnapshotId};

/// Version of the current manifest body layout.
pub const MANIFEST_BODY_VERSION: u32 = 3;

// v3 body layout.
const OFF_BODY_VERSION: usize = 0;
const OFF_CHECKPOINT_LSN: usize = 4;
const OFF_FREE_LIST_HEAD: usize = 12;
const OFF_SHARD_COUNT: usize = 20;
const OFF_NEXT_SNAPSHOT_ID: usize = 24;
const OFF_SNAPSHOT_COUNT: usize = 32;
const OFF_SHARD_ROOTS: usize = 36;
const SNAPSHOT_ENTRY_SIZE: usize = 24;

// v2 compatibility layout.
const V2_BODY_VERSION: u32 = 2;
const OFF_V2_PARTITION_ROOT: usize = 20;
const OFF_V2_NEXT_SNAPSHOT_ID: usize = 28;
const OFF_V2_SNAPSHOT_COUNT: usize = 36;
const OFF_V2_SNAPSHOT_TABLE: usize = 40;
const V2_SNAPSHOT_ENTRY_SIZE: usize = 24;

const _: () = {
    assert!(OFF_BODY_VERSION + 4 == OFF_CHECKPOINT_LSN);
    assert!(OFF_CHECKPOINT_LSN + 8 == OFF_FREE_LIST_HEAD);
    assert!(OFF_FREE_LIST_HEAD + 8 == OFF_SHARD_COUNT);
    assert!(OFF_SHARD_COUNT + 4 == OFF_NEXT_SNAPSHOT_ID);
    assert!(OFF_NEXT_SNAPSHOT_ID + 8 == OFF_SNAPSHOT_COUNT);
    assert!(OFF_SNAPSHOT_COUNT + 4 == OFF_SHARD_ROOTS);
    assert!(SNAPSHOT_ENTRY_SIZE == 24);
    assert!(V2_SNAPSHOT_ENTRY_SIZE == 24);
};

/// Maximum number of shard roots that fit in one snapshot-roots page.
pub const MAX_SHARD_ROOTS_PER_PAGE: usize = PAGE_PAYLOAD_SIZE / size_of::<PageId>();

/// Maximum number of snapshot entries that fit in a v3 manifest for the
/// given shard count.
pub fn max_snapshots_for_shards(shard_count: usize) -> usize {
    let roots_bytes = match shard_count.checked_mul(size_of::<PageId>()) {
        Some(v) => v,
        None => return 0,
    };
    let used = match OFF_SHARD_ROOTS.checked_add(roots_bytes) {
        Some(v) => v,
        None => return 0,
    };
    if used > PAGE_PAYLOAD_SIZE {
        return 0;
    }
    (PAGE_PAYLOAD_SIZE - used) / SNAPSHOT_ENTRY_SIZE
}

/// One snapshot's manifest entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// Unique identifier.
    pub id: SnapshotId,
    /// Page id of the [`PageType::SnapshotRoots`] page that stores
    /// `shard_roots`. `NULL_PAGE` is used only for legacy v2 manifests
    /// loaded in memory before they are upgraded.
    pub roots_page: PageId,
    /// LSN at which the snapshot was taken.
    pub created_lsn: Lsn,
    /// Pinned per-shard roots for this snapshot.
    pub shard_roots: Box<[PageId]>,
}

impl SnapshotEntry {
    fn needs_roots_page(&self) -> bool {
        self.roots_page == NULL_PAGE
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
    /// Current per-shard BTree roots.
    pub shard_roots: Box<[PageId]>,
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
            next_snapshot_id: 1,
            snapshots: Vec::new(),
        }
    }

    /// Number of shards currently tracked by the manifest.
    pub fn shard_count(&self) -> usize {
        self.shard_roots.len()
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
        let max_snapshots = max_snapshots_for_shards(shard_count);
        if self.snapshots.len() > max_snapshots {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest snapshot count {} exceeds capacity {} for {} shards",
                self.snapshots.len(),
                max_snapshots,
                shard_count,
            )));
        }
        for entry in &self.snapshots {
            if entry.shard_roots.len() != shard_count {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} has {} shard roots, expected {}",
                    entry.id,
                    entry.shard_roots.len(),
                    shard_count,
                )));
            }
            if entry.needs_roots_page() {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} is missing a roots_page",
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
        p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
            .copy_from_slice(&self.next_snapshot_id.to_le_bytes());
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
            .copy_from_slice(&(self.snapshots.len() as u32).to_le_bytes());

        let mut off = OFF_SHARD_ROOTS;
        for root in self.shard_roots.iter().copied() {
            p[off..off + 8].copy_from_slice(&root.to_le_bytes());
            off += 8;
        }
        for entry in &self.snapshots {
            p[off..off + 8].copy_from_slice(&entry.id.to_le_bytes());
            p[off + 8..off + 16].copy_from_slice(&entry.roots_page.to_le_bytes());
            p[off + 16..off + 24].copy_from_slice(&entry.created_lsn.to_le_bytes());
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
            MANIFEST_BODY_VERSION => Self::decode_v3(page, page_store),
            V2_BODY_VERSION => Self::decode_v2(page),
            other => Err(MetaDbError::Corruption(format!(
                "unsupported manifest body version {other}",
            ))),
        }
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
        let shard_count =
            u32::from_le_bytes(p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4].try_into().unwrap())
                as usize;
        if shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::Corruption(format!(
                "manifest shard_count {shard_count} exceeds page capacity {MAX_SHARD_ROOTS_PER_PAGE}",
            )));
        }
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
        let max_snapshots = max_snapshots_for_shards(shard_count);
        if snapshot_count > max_snapshots {
            return Err(MetaDbError::Corruption(format!(
                "manifest snapshot_count {snapshot_count} exceeds capacity {max_snapshots} for {shard_count} shards",
            )));
        }

        let mut off = OFF_SHARD_ROOTS;
        let mut shard_roots = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shard_roots.push(u64::from_le_bytes(p[off..off + 8].try_into().unwrap()));
            off += 8;
        }

        let mut snapshots = Vec::with_capacity(snapshot_count);
        for _ in 0..snapshot_count {
            let id = u64::from_le_bytes(p[off..off + 8].try_into().unwrap());
            let roots_page = u64::from_le_bytes(p[off + 8..off + 16].try_into().unwrap());
            let created_lsn = u64::from_le_bytes(p[off + 16..off + 24].try_into().unwrap());
            let roots = load_snapshot_roots(page_store, roots_page)?;
            if roots.len() != shard_count {
                return Err(MetaDbError::Corruption(format!(
                    "snapshot {id} roots page {roots_page} has {} roots, expected {shard_count}",
                    roots.len(),
                )));
            }
            snapshots.push(SnapshotEntry {
                id,
                roots_page,
                created_lsn,
                shard_roots: roots,
            });
            off += SNAPSHOT_ENTRY_SIZE;
        }

        Ok(Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn,
            free_list_head,
            shard_roots: shard_roots.into_boxed_slice(),
            next_snapshot_id,
            snapshots,
        })
    }

    fn decode_v2(page: &Page) -> Result<Self> {
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
        let partition_root = u64::from_le_bytes(
            p[OFF_V2_PARTITION_ROOT..OFF_V2_PARTITION_ROOT + 8]
                .try_into()
                .unwrap(),
        );
        let next_snapshot_id = u64::from_le_bytes(
            p[OFF_V2_NEXT_SNAPSHOT_ID..OFF_V2_NEXT_SNAPSHOT_ID + 8]
                .try_into()
                .unwrap(),
        );
        let snapshot_count = u32::from_le_bytes(
            p[OFF_V2_SNAPSHOT_COUNT..OFF_V2_SNAPSHOT_COUNT + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let max_v2_snapshots = (PAGE_PAYLOAD_SIZE - OFF_V2_SNAPSHOT_TABLE) / V2_SNAPSHOT_ENTRY_SIZE;
        if snapshot_count > max_v2_snapshots {
            return Err(MetaDbError::Corruption(format!(
                "legacy manifest snapshot_count {snapshot_count} exceeds {max_v2_snapshots}",
            )));
        }

        let mut snapshots = Vec::with_capacity(snapshot_count);
        for i in 0..snapshot_count {
            let off = OFF_V2_SNAPSHOT_TABLE + i * V2_SNAPSHOT_ENTRY_SIZE;
            let id = u64::from_le_bytes(p[off..off + 8].try_into().unwrap());
            let root = u64::from_le_bytes(p[off + 8..off + 16].try_into().unwrap());
            let created_lsn = u64::from_le_bytes(p[off + 16..off + 24].try_into().unwrap());
            snapshots.push(SnapshotEntry {
                id,
                roots_page: NULL_PAGE,
                created_lsn,
                shard_roots: vec![root].into_boxed_slice(),
            });
        }

        Ok(Self {
            body_version: V2_BODY_VERSION,
            checkpoint_lsn,
            free_list_head,
            shard_roots: vec![partition_root].into_boxed_slice(),
            next_snapshot_id,
            snapshots,
        })
    }
}

/// Materialize missing snapshot-root pages for legacy v2 manifests loaded
/// in memory. Returns `true` if any pages were allocated.
pub(crate) fn materialize_snapshot_root_pages(
    page_store: &PageStore,
    manifest: &mut Manifest,
    generation: Lsn,
) -> Result<bool> {
    let mut changed = manifest.body_version != MANIFEST_BODY_VERSION;
    manifest.body_version = MANIFEST_BODY_VERSION;
    for entry in &mut manifest.snapshots {
        if entry.needs_roots_page() {
            entry.roots_page =
                write_snapshot_roots_page(page_store, &entry.shard_roots, generation)?;
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
            "snapshot roots_page cannot be NULL_PAGE in v3 manifest".into(),
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

    fn roots(roots: &[PageId]) -> Box<[PageId]> {
        roots.to_vec().into_boxed_slice()
    }

    fn snapshot(
        ps: &PageStore,
        id: SnapshotId,
        shard_roots: &[PageId],
        created_lsn: Lsn,
    ) -> SnapshotEntry {
        let roots_page = write_snapshot_roots_page(ps, shard_roots, created_lsn).unwrap();
        SnapshotEntry {
            id,
            roots_page,
            created_lsn,
            shard_roots: roots(shard_roots),
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
            shard_roots: roots(&[7, 8, 9, 10]),
            next_snapshot_id: 5,
            snapshots: vec![snapshot(&ps, 1, &[11, 12, 13, 14], 100)],
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
    fn body_decode_rejects_overflowing_snapshot_count() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        let mut m = Manifest::empty();
        m.shard_roots = roots(&[1, 2, 3, 4]);
        m.encode(&mut page).unwrap();
        let p = page.payload_mut();
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4].copy_from_slice(&9999u32.to_le_bytes());
        page.seal();
        assert!(matches!(
            Manifest::decode(&page, &ps).unwrap_err(),
            MetaDbError::Corruption(_)
        ));
    }

    #[test]
    fn encode_decode_round_trip() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0xDEAD_BEEF_CAFE,
            free_list_head: 1234,
            shard_roots: roots(&[42, 43, 44, 45]),
            next_snapshot_id: 99,
            snapshots: vec![
                snapshot(&ps, 1, &[10, 11, 12, 13], 100),
                snapshot(&ps, 5, &[20, 21, 22, 23], 500),
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
        m.shard_roots = roots(&[1, 2, 3, 4]);
        for i in 0..(max_snapshots_for_shards(m.shard_count()) + 1) as u64 {
            m.snapshots.push(snapshot(&ps, i, &[10, 11, 12, 13], i));
        }
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        assert!(m.encode(&mut page).is_err());
    }

    #[test]
    fn find_snapshot_locates_by_id() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut m = Manifest::empty();
        m.shard_roots = roots(&[1, 2, 3, 4]);
        m.snapshots.push(snapshot(&ps, 7, &[42, 43, 44, 45], 100));
        assert_eq!(m.find_snapshot(7).unwrap().id, 7);
        assert_eq!(
            m.find_snapshot(7).unwrap().shard_roots.as_ref(),
            &[42, 43, 44, 45]
        );
        assert!(m.find_snapshot(99).is_none());
    }

    #[test]
    fn decode_v2_manifest_into_single_shard_layout() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        let p = page.payload_mut();
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&V2_BODY_VERSION.to_le_bytes());
        p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8].copy_from_slice(&123u64.to_le_bytes());
        p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8].copy_from_slice(&55u64.to_le_bytes());
        p[OFF_V2_PARTITION_ROOT..OFF_V2_PARTITION_ROOT + 8].copy_from_slice(&7u64.to_le_bytes());
        p[OFF_V2_NEXT_SNAPSHOT_ID..OFF_V2_NEXT_SNAPSHOT_ID + 8]
            .copy_from_slice(&3u64.to_le_bytes());
        p[OFF_V2_SNAPSHOT_COUNT..OFF_V2_SNAPSHOT_COUNT + 4].copy_from_slice(&1u32.to_le_bytes());
        let off = OFF_V2_SNAPSHOT_TABLE;
        p[off..off + 8].copy_from_slice(&1u64.to_le_bytes());
        p[off + 8..off + 16].copy_from_slice(&11u64.to_le_bytes());
        p[off + 16..off + 24].copy_from_slice(&100u64.to_le_bytes());
        page.seal();

        let decoded = Manifest::decode(&page, &ps).unwrap();
        assert_eq!(decoded.body_version, V2_BODY_VERSION);
        assert_eq!(decoded.shard_roots.as_ref(), &[7]);
        assert_eq!(decoded.next_snapshot_id, 3);
        assert_eq!(decoded.snapshots.len(), 1);
        assert_eq!(decoded.snapshots[0].roots_page, NULL_PAGE);
        assert_eq!(decoded.snapshots[0].shard_roots.as_ref(), &[11]);
    }

    #[test]
    fn materialize_legacy_snapshot_root_pages_upgrades_manifest() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let mut manifest = Manifest {
            body_version: V2_BODY_VERSION,
            checkpoint_lsn: 0,
            free_list_head: NULL_PAGE,
            shard_roots: roots(&[7]),
            next_snapshot_id: 2,
            snapshots: vec![SnapshotEntry {
                id: 1,
                roots_page: NULL_PAGE,
                created_lsn: 10,
                shard_roots: roots(&[11]),
            }],
        };
        assert!(materialize_snapshot_root_pages(&ps, &mut manifest, 77).unwrap());
        assert_eq!(manifest.body_version, MANIFEST_BODY_VERSION);
        assert_ne!(manifest.snapshots[0].roots_page, NULL_PAGE);
        let loaded = load_snapshot_roots(&ps, manifest.snapshots[0].roots_page).unwrap();
        assert_eq!(loaded.as_ref(), &[11]);
    }
}
