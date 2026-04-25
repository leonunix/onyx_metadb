use super::*;

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
