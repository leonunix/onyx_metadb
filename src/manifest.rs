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
//! # Layout
//!
//! Each slot is a [`Page`] of type [`PageType::Manifest`]. The payload
//! holds the manifest body:
//!
//! ```text
//! off  size  field
//!   0    4   body_version
//!   4    8   checkpoint_lsn     (durable WAL tail)
//!  12    8   free_list_head     (NULL_PAGE if no persisted free list)
//!  20    4   partition_count    (v1: always 0)
//!  24    4   snapshot_count     (v1: always 0)
//!  28  ...   reserved / tables (grows in later phases)
//! ```
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
use crate::types::{Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId};

/// Version of the manifest body layout.
pub const MANIFEST_BODY_VERSION: u32 = 1;

// Offsets inside `page.payload()`.
const OFF_BODY_VERSION: usize = 0;
const OFF_CHECKPOINT_LSN: usize = 4;
const OFF_FREE_LIST_HEAD: usize = 12;
const OFF_PARTITION_COUNT: usize = 20;
const OFF_SNAPSHOT_COUNT: usize = 24;
const MANIFEST_BODY_END: usize = 28;

const _: () = {
    assert!(OFF_BODY_VERSION + 4 == OFF_CHECKPOINT_LSN);
    assert!(OFF_CHECKPOINT_LSN + 8 == OFF_FREE_LIST_HEAD);
    assert!(OFF_FREE_LIST_HEAD + 8 == OFF_PARTITION_COUNT);
    assert!(OFF_PARTITION_COUNT + 4 == OFF_SNAPSHOT_COUNT);
    assert!(OFF_SNAPSHOT_COUNT + 4 == MANIFEST_BODY_END);
    assert!(MANIFEST_BODY_END <= PAGE_SIZE - 64); // fits in page payload
};

/// Decoded manifest body.  v1 is fixed-layout; v2 will carry variable-
/// length tables for partitions, snapshots, and LSM levels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub body_version: u32,
    /// Greatest LSN whose WAL record has been applied to durable index
    /// state. WAL records with LSN <= checkpoint_lsn can be truncated.
    pub checkpoint_lsn: Lsn,
    /// Head of the persisted free-list page chain, or [`NULL_PAGE`].
    pub free_list_head: PageId,
}

impl Manifest {
    /// Freshly-created empty manifest suitable for a brand-new database.
    pub fn empty() -> Self {
        Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0,
            free_list_head: NULL_PAGE,
        }
    }

    fn encode(&self, page: &mut Page) {
        let p = page.payload_mut();
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&self.body_version.to_le_bytes());
        p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
            .copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
            .copy_from_slice(&self.free_list_head.to_le_bytes());
        p[OFF_PARTITION_COUNT..OFF_PARTITION_COUNT + 4].copy_from_slice(&0u32.to_le_bytes());
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4].copy_from_slice(&0u32.to_le_bytes());
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
        // v1 ignores partition / snapshot counts: always zero. We still
        // sanity-check them to catch a header decoded against the wrong
        // body version.
        let partition_count = u32::from_le_bytes(
            p[OFF_PARTITION_COUNT..OFF_PARTITION_COUNT + 4]
                .try_into()
                .unwrap(),
        );
        let snapshot_count = u32::from_le_bytes(
            p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
                .try_into()
                .unwrap(),
        );
        if partition_count != 0 || snapshot_count != 0 {
            return Err(MetaDbError::Corruption(format!(
                "manifest v1 expected 0 partitions/snapshots, got {partition_count}/{snapshot_count}",
            )));
        }
        Ok(Self {
            body_version,
            checkpoint_lsn,
            free_list_head,
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
        manifest.encode(&mut page);
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
        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0,
            free_list_head: NULL_PAGE,
        };
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
            let m = Manifest {
                body_version: MANIFEST_BODY_VERSION,
                checkpoint_lsn: lsn,
                free_list_head: NULL_PAGE,
            };
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
        let target = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 777,
            free_list_head: NULL_PAGE,
        };
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
        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 42,
            free_list_head: NULL_PAGE,
        };
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
        let m = Manifest {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 42,
            free_list_head: NULL_PAGE,
        };
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
    fn body_decode_rejects_nonzero_counts_for_v1() {
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        let m = Manifest::empty();
        m.encode(&mut page);
        // Poke a nonzero partition_count.
        let p = page.payload_mut();
        p[OFF_PARTITION_COUNT..OFF_PARTITION_COUNT + 4].copy_from_slice(&3u32.to_le_bytes());
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
        };
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 7));
        m.encode(&mut page);
        page.seal();
        page.verify(MANIFEST_PAGE_A).unwrap();
        let decoded = Manifest::decode(&page).unwrap();
        assert_eq!(decoded, m);
    }
}
