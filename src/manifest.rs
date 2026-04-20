//! Double-buffered manifest living in pages 0 and 1 of the page store.
//!
//! # Model
//!
//! The manifest is the authoritative record of "where is everything":
//! which LSN the WAL can be truncated up to, where the persisted free
//! list starts, and (from later phases) every partition's root page, the
//! snapshot table, and the LSM level manifest.
//!
//! There are always exactly two manifest slots: [`MANIFEST_PAGE_A`] and
//! [`MANIFEST_PAGE_B`]. Writes alternate between them so one slot always
//! holds a durable snapshot of the previous state. On open, the slot
//! with the higher generation that also passes CRC wins.
//!
//! # Torn-write semantics
//!
//! - A crash in the middle of writing slot X leaves X torn (CRC fails);
//!   recovery picks slot Y with its older but intact contents.
//! - Because we write a whole 4 KiB page atomically from the kernel's
//!   point of view, "torn" here means "user-space buffer didn't make it
//!   to disk" rather than "garbage interleaved with valid content".
//! - Both slots invalid is indistinguishable from "freshly created
//!   empty database". Callers that need to distinguish them must use
//!   outside-of-the-manifest signals (e.g., presence of a page file of
//!   non-minimal size with mysterious contents).
//!
//! # Layout (v2)
//!
//! Each slot is a [`Page`] of type [`PageType::Manifest`]. The payload
//! holds the manifest body:
//!
//! ```text
//! off  size  field
//!   0    4   body_version (= 2)
//!   4    8   checkpoint_lsn     (durable WAL tail)
//!  12    8   free_list_head     (NULL_PAGE if no persisted free list)
//!  20    8   partition_root     (PageId of the current BTree root)
//!  28    8   next_snapshot_id   (monotonic counter for new snapshots)
//!  36    4   snapshot_count     (N)
//!  40  24*N  snapshot entries:
//!              offset 40 + i*24: [8B snap_id][8B root][8B created_lsn]
//! ```
//!
//! The body fits in the 4032 B page payload up to
//! [`MAX_SNAPSHOTS_PER_MANIFEST`] entries. Growing beyond that would
//! require a chained manifest, which is out of scope for phase 3.
//!
//! The page header's `generation` field is used as the monotonic slot
//! sequence. It is not a WAL LSN; it's an opaque counter that only
//! compares against the other slot's generation.

use std::sync::Arc;

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::page::{Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::{Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, SnapshotId};

/// Version of the manifest body layout.
pub const MANIFEST_BODY_VERSION: u32 = 2;

// Offsets inside `page.payload()`.
const OFF_BODY_VERSION: usize = 0;
const OFF_CHECKPOINT_LSN: usize = 4;
const OFF_FREE_LIST_HEAD: usize = 12;
const OFF_PARTITION_ROOT: usize = 20;
const OFF_NEXT_SNAPSHOT_ID: usize = 28;
const OFF_SNAPSHOT_COUNT: usize = 36;
const OFF_SNAPSHOT_TABLE: usize = 40;
const SNAPSHOT_ENTRY_SIZE: usize = 24;

/// Upper bound on snapshots per manifest slot. Chained manifests
/// (future phase) will lift this cap.
pub const MAX_SNAPSHOTS_PER_MANIFEST: usize =
    (PAGE_SIZE - 64 - OFF_SNAPSHOT_TABLE) / SNAPSHOT_ENTRY_SIZE;

const _: () = {
    assert!(OFF_BODY_VERSION + 4 == OFF_CHECKPOINT_LSN);
    assert!(OFF_CHECKPOINT_LSN + 8 == OFF_FREE_LIST_HEAD);
    assert!(OFF_FREE_LIST_HEAD + 8 == OFF_PARTITION_ROOT);
    assert!(OFF_PARTITION_ROOT + 8 == OFF_NEXT_SNAPSHOT_ID);
    assert!(OFF_NEXT_SNAPSHOT_ID + 8 == OFF_SNAPSHOT_COUNT);
    assert!(OFF_SNAPSHOT_COUNT + 4 == OFF_SNAPSHOT_TABLE);
    assert!(SNAPSHOT_ENTRY_SIZE == 24);
    assert!(
        OFF_SNAPSHOT_TABLE + SNAPSHOT_ENTRY_SIZE * MAX_SNAPSHOTS_PER_MANIFEST <= PAGE_SIZE - 64
    );
};

/// One snapshot's entry in the manifest table.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// Unique identifier.
    pub id: SnapshotId,
    /// Page id that this snapshot pins as the tree root.
    pub root: PageId,
    /// LSN at which the snapshot was taken.
    pub created_lsn: Lsn,
}

/// Decoded manifest body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub body_version: u32,
    /// Greatest LSN whose WAL record has been applied to durable
    /// index state. WAL records with `lsn <= checkpoint_lsn` can be
    /// truncated.
    pub checkpoint_lsn: Lsn,
    /// Head of the persisted free-list page chain, or [`NULL_PAGE`].
    pub free_list_head: PageId,
    /// Current BTree root page id.
    pub partition_root: PageId,
    /// Monotonic counter: next snapshot id to hand out.
    pub next_snapshot_id: u64,
    /// Registered snapshots, in order of creation.
    pub snapshots: Vec<SnapshotEntry>,
}

impl Manifest {
    /// Freshly-created empty manifest for a brand-new database. The
    /// caller must set `partition_root` before this is meaningful —
    /// [`Db::create`] does exactly that after allocating the root
    /// leaf.
    pub fn empty() -> Self {
        Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0,
            free_list_head: NULL_PAGE,
            partition_root: NULL_PAGE,
            next_snapshot_id: 1,
            snapshots: Vec::new(),
        }
    }

    /// Find a snapshot by id.
    pub fn find_snapshot(&self, id: SnapshotId) -> Option<&SnapshotEntry> {
        self.snapshots.iter().find(|e| e.id == id)
    }

    fn encode(&self, page: &mut Page) -> Result<()> {
        if self.snapshots.len() > MAX_SNAPSHOTS_PER_MANIFEST {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest snapshot count {} exceeds MAX_SNAPSHOTS_PER_MANIFEST {}",
                self.snapshots.len(),
                MAX_SNAPSHOTS_PER_MANIFEST,
            )));
        }
        let p = page.payload_mut();
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&self.body_version.to_le_bytes());
        p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
            .copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
            .copy_from_slice(&self.free_list_head.to_le_bytes());
        p[OFF_PARTITION_ROOT..OFF_PARTITION_ROOT + 8]
            .copy_from_slice(&self.partition_root.to_le_bytes());
        p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
            .copy_from_slice(&self.next_snapshot_id.to_le_bytes());
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
            .copy_from_slice(&(self.snapshots.len() as u32).to_le_bytes());
        for (i, e) in self.snapshots.iter().enumerate() {
            let off = OFF_SNAPSHOT_TABLE + i * SNAPSHOT_ENTRY_SIZE;
            p[off..off + 8].copy_from_slice(&e.id.to_le_bytes());
            p[off + 8..off + 16].copy_from_slice(&e.root.to_le_bytes());
            p[off + 16..off + 24].copy_from_slice(&e.created_lsn.to_le_bytes());
        }
        Ok(())
    }

    fn decode(page: &Page) -> Result<Self> {
        let p = page.payload();
        let body_version = u32::from_le_bytes(
            p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
                .try_into()
                .unwrap(),
        );
        if body_version != MANIFEST_BODY_VERSION {
            return Err(MetaDbError::Corruption(format!(
                "unsupported manifest body version {body_version}",
            )));
        }
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
            p[OFF_PARTITION_ROOT..OFF_PARTITION_ROOT + 8]
                .try_into()
                .unwrap(),
        );
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
        if snapshot_count > MAX_SNAPSHOTS_PER_MANIFEST {
            return Err(MetaDbError::Corruption(format!(
                "manifest snapshot_count {snapshot_count} exceeds {MAX_SNAPSHOTS_PER_MANIFEST}",
            )));
        }
        let mut snapshots = Vec::with_capacity(snapshot_count);
        for i in 0..snapshot_count {
            let off = OFF_SNAPSHOT_TABLE + i * SNAPSHOT_ENTRY_SIZE;
            let id = u64::from_le_bytes(p[off..off + 8].try_into().unwrap());
            let root = u64::from_le_bytes(p[off + 8..off + 16].try_into().unwrap());
            let created_lsn = u64::from_le_bytes(p[off + 16..off + 24].try_into().unwrap());
            snapshots.push(SnapshotEntry {
                id,
                root,
                created_lsn,
            });
        }
        Ok(Self {
            body_version,
            checkpoint_lsn,
            free_list_head,
            partition_root,
            next_snapshot_id,
            snapshots,
        })
    }
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
                // Fresh (or fully-corrupt) DB: materialize an empty
                // manifest so subsequent commits always have a valid
                // prior slot to alternate against.
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

        // Only now do we advance in-memory state. If any of the IO
        // steps above returned an error, the caller sees the error and
        // the store still thinks the next commit should target the same
        // slot. (The other slot retains its previous durable contents.)
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
    let manifest = Manifest::decode(&page).ok()?;
    Some((header.generation, manifest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::PageStore;
    use crate::testing::faults::FaultAction;
    use tempfile::TempDir;

    fn mk_store(dir: &TempDir) -> Arc<PageStore> {
        Arc::new(PageStore::create(dir.path().join("pages.onyx_meta")).unwrap())
    }

    fn reopen(dir: &TempDir) -> Arc<PageStore> {
        Arc::new(PageStore::open(dir.path().join("pages.onyx_meta")).unwrap())
    }

    #[test]
    fn fresh_open_creates_empty_manifest_at_sequence_1() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let faults = FaultController::new();
        let (store, manifest) = ManifestStore::open_or_create(ps, faults).unwrap();
        assert_eq!(manifest, Manifest::empty());
        assert_eq!(store.sequence(), 1);
        // First write went to slot A so next goes to B.
        assert_eq!(store.next_slot(), MANIFEST_PAGE_B);
    }

    #[test]
    fn commit_then_reopen_recovers_manifest() {
        let dir = TempDir::new().unwrap();
        let ps = mk_store(&dir);
        let faults = FaultController::new();
        let (mut store, _) = ManifestStore::open_or_create(ps, faults).unwrap();

        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 1234,
            free_list_head: 99,
            partition_root: 7,
            next_snapshot_id: 5,
            snapshots: vec![SnapshotEntry {
                id: 1,
                root: 11,
                created_lsn: 100,
            }],
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
        // After open_or_create: sequence=1, next_slot=B.
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

        // Commit twice so slot A (sequence=1 from fresh) and slot B
        // (sequence=2) are both populated. After the second commit,
        // slot A holds the older, slot B the newer.
        let faults = FaultController::new();
        let (mut store, _) = ManifestStore::open_or_create(ps, faults).unwrap();
        let mut target = Manifest::empty();
        target.checkpoint_lsn = 777;
        store.commit(&target).unwrap();
        drop(store);

        // Corrupt slot B (the winning one) on disk.
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
        // Slot B torn → falls back to slot A which is the empty initial
        // manifest at sequence=1. Next commit should therefore go to B.
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
        // Smash both slots.
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
        // State untouched; next commit targets the same slot with the
        // next sequence number.
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

        // The data *is* on disk and fsynced — the post-fsync fault
        // fired after durability was established. Reopen sees it.
        let (_, loaded) =
            ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
        assert_eq!(loaded, m);
    }

    #[test]
    fn body_decode_rejects_wrong_version() {
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        let p = page.payload_mut();
        p[0..4].copy_from_slice(&99u32.to_le_bytes()); // bogus version
        page.seal();
        assert!(matches!(
            Manifest::decode(&page).unwrap_err(),
            MetaDbError::Corruption(_)
        ));
    }

    #[test]
    fn body_decode_rejects_overflowing_snapshot_count() {
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        let m = Manifest::empty();
        m.encode(&mut page).unwrap();
        // Poke an absurd snapshot_count.
        let p = page.payload_mut();
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4].copy_from_slice(&9999u32.to_le_bytes());
        page.seal();
        assert!(matches!(
            Manifest::decode(&page).unwrap_err(),
            MetaDbError::Corruption(_)
        ));
    }

    #[test]
    fn encode_decode_round_trip() {
        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0xDEAD_BEEF_CAFE,
            free_list_head: 1234,
            partition_root: 42,
            next_snapshot_id: 99,
            snapshots: vec![
                SnapshotEntry {
                    id: 1,
                    root: 10,
                    created_lsn: 100,
                },
                SnapshotEntry {
                    id: 5,
                    root: 20,
                    created_lsn: 500,
                },
            ],
        };
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 7));
        m.encode(&mut page).unwrap();
        page.seal();
        page.verify(MANIFEST_PAGE_A).unwrap();
        let decoded = Manifest::decode(&page).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn encode_rejects_oversized_snapshot_table() {
        let mut m = Manifest::empty();
        for i in 0..(MAX_SNAPSHOTS_PER_MANIFEST + 1) as u64 {
            m.snapshots.push(SnapshotEntry {
                id: i,
                root: 0,
                created_lsn: 0,
            });
        }
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        assert!(m.encode(&mut page).is_err());
    }

    #[test]
    fn find_snapshot_locates_by_id() {
        let mut m = Manifest::empty();
        m.snapshots.push(SnapshotEntry {
            id: 7,
            root: 42,
            created_lsn: 100,
        });
        assert_eq!(
            m.find_snapshot(7).unwrap(),
            &SnapshotEntry {
                id: 7,
                root: 42,
                created_lsn: 100,
            }
        );
        assert!(m.find_snapshot(99).is_none());
    }
}
