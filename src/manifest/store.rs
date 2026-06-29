use crate::cache::PageCache;

use super::catalog::{self, CatalogKind};
use super::*;

/// Owns the two manifest slots and orchestrates alternating commits.
///
/// v23: also owns the two catalog chains (volume catalog + snapshot table) of
/// EACH slot. The chains are double-buffered alongside the manifest slots — a
/// commit rewrites only the target slot's chains in place (reusing their pids,
/// growing/shrinking continuations) and the target slot's body. The other
/// slot's body + chains are never touched, so the previous generation stays
/// fully intact. See [`ManifestStore::commit`].
pub struct ManifestStore {
    page_store: Arc<PageStore>,
    /// Invalidated for a trailing catalog page before it is handed back to the
    /// allocator (on shrink), so a recycled pid can't surface a stale cached
    /// page.
    page_cache: Arc<PageCache>,
    sequence: u64,
    /// Slot the next commit will write to. Toggled on every successful
    /// commit so the other slot always retains the previous durable
    /// generation.
    next_slot: PageId,
    /// Per-slot volume-catalog chain pids (head first). Index 0 =
    /// [`MANIFEST_PAGE_A`], 1 = [`MANIFEST_PAGE_B`]. Reused in place when that
    /// slot is committed; seeded from disk for both slots at open.
    slot_volume_chain: [Vec<PageId>; 2],
    /// Per-slot snapshot-table chain pids (head first); mirror of
    /// [`slot_volume_chain`](Self::slot_volume_chain).
    slot_snapshot_chain: [Vec<PageId>; 2],
    faults: Arc<FaultController>,
}

/// Latest valid manifest slot loaded from disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedManifest {
    pub slot: PageId,
    pub sequence: u64,
    pub manifest: Manifest,
}

fn slot_index(slot: PageId) -> usize {
    if slot == MANIFEST_PAGE_A { 0 } else { 1 }
}

fn other_slot(slot: PageId) -> PageId {
    if slot == MANIFEST_PAGE_A {
        MANIFEST_PAGE_B
    } else {
        MANIFEST_PAGE_A
    }
}

/// Read the two catalog chain-head pids out of a slot's fixed header WITHOUT a
/// full [`Manifest::decode`]. Header-only on purpose: the OTHER (non-winning)
/// generation's body may reference `SnapshotRoots` / L2P pages the engine has
/// since reclaimed — a full decode would then fail and we'd lose track of that
/// slot's (still-intact) catalog chains, leaking + mis-flagging them. The
/// catalog chains themselves are never freed except on shrink, so they stay
/// walkable regardless of the rest of the body's validity.
fn slot_catalog_heads(page_store: &PageStore, slot: PageId) -> Option<(PageId, PageId)> {
    let page = page_store.read_page_unchecked(slot).ok()?;
    page.verify(slot).ok()?;
    let header = page.header().ok()?;
    if header.page_type != PageType::Manifest {
        return None;
    }
    let p = page.payload();
    let body_version = u32::from_le_bytes(p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].try_into().ok()?);
    if body_version != MANIFEST_BODY_VERSION {
        return None;
    }
    let vol = u64::from_le_bytes(
        p[OFF_VOLUME_CATALOG_HEAD..OFF_VOLUME_CATALOG_HEAD + 8]
            .try_into()
            .ok()?,
    );
    let snap = u64::from_le_bytes(
        p[OFF_SNAPSHOT_CATALOG_HEAD..OFF_SNAPSHOT_CATALOG_HEAD + 8]
            .try_into()
            .ok()?,
    );
    Some((vol, snap))
}

/// Enumerate the catalog chain pids referenced by BOTH manifest slots. Both
/// generations' chains are legitimately live (double-buffer), so the verifier +
/// orphan-reclaim must keep every one of them — walking only the winning slot
/// would flag the other slot's chains as leaked. Header-only head reads (see
/// [`slot_catalog_heads`]) so a slot whose body references reclaimed pages still
/// contributes its catalog chains.
pub(crate) fn catalog_chain_pids_all_slots(page_store: &PageStore) -> Vec<PageId> {
    let mut pids = Vec::new();
    for slot in [MANIFEST_PAGE_A, MANIFEST_PAGE_B] {
        let Some((vol_head, snap_head)) = slot_catalog_heads(page_store, slot) else {
            continue;
        };
        for (head, kind) in [(vol_head, CatalogKind::Volumes), (snap_head, CatalogKind::Snapshots)] {
            if let Ok(p) = catalog::chain_pids(page_store, head, kind) {
                pids.extend(p);
            }
        }
    }
    pids
}

impl ManifestStore {
    /// Load the newest valid manifest slot from disk without mutating the
    /// page store. Returns `Ok(None)` if neither slot carries a decodable
    /// manifest page.
    pub fn load_latest(page_store: &PageStore) -> Result<Option<LoadedManifest>> {
        let mut candidates = [MANIFEST_PAGE_A, MANIFEST_PAGE_B]
            .into_iter()
            .filter_map(|slot| load_slot_header(page_store, slot).map(|sequence| (slot, sequence)))
            .collect::<Vec<_>>();
        candidates.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        for (slot, sequence) in candidates {
            match load_slot_body(page_store, slot, sequence) {
                Ok(loaded) => return Ok(Some(loaded)),
                Err(err) => {
                    tracing::warn!(
                        slot,
                        sequence,
                        error = %err,
                        "metadb manifest slot body failed to decode"
                    );
                }
            }
        }
        Ok(None)
    }

    fn new_seeded(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        faults: Arc<FaultController>,
        loaded: &LoadedManifest,
    ) -> Self {
        let mut store = Self {
            page_store,
            page_cache,
            sequence: loaded.sequence,
            next_slot: other_slot(loaded.slot),
            slot_volume_chain: [Vec::new(), Vec::new()],
            slot_snapshot_chain: [Vec::new(), Vec::new()],
            faults,
        };
        // Seed BOTH slots' chains so the next commit (to the OTHER slot) reuses
        // that slot's existing pids in place instead of leaking them. The
        // winning slot's heads come from its decoded manifest; the other slot's
        // come from a header-only read (its body may reference reclaimed pages,
        // but its catalog chains are intact — see [`slot_catalog_heads`]).
        store.seed_slot_chains(
            loaded.slot,
            loaded.manifest.volume_catalog_head_pid,
            loaded.manifest.snapshot_catalog_head_pid,
        );
        let other = other_slot(loaded.slot);
        if let Some((vol_head, snap_head)) = slot_catalog_heads(&store.page_store, other) {
            store.seed_slot_chains(other, vol_head, snap_head);
        }
        store
    }

    /// Open an existing manifest. Unlike [`open_or_create`](Self::open_or_create),
    /// this never writes a fresh empty manifest when both slots are invalid.
    pub fn open_existing(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        faults: Arc<FaultController>,
    ) -> Result<(Self, Manifest)> {
        let Some(loaded) = Self::load_latest(&page_store)? else {
            return Err(MetaDbError::Corruption(
                "no valid manifest slot found in existing database".into(),
            ));
        };
        let store = Self::new_seeded(page_store, page_cache, faults, &loaded);
        Ok((store, loaded.manifest))
    }

    /// Open the manifest for a page store, creating a fresh empty
    /// manifest on disk if neither slot is valid. Returns the loaded
    /// (or freshly-persisted) [`Manifest`] alongside the store.
    pub fn open_or_create(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        faults: Arc<FaultController>,
    ) -> Result<(Self, Manifest)> {
        if let Some(loaded) = Self::load_latest(&page_store)? {
            let store = Self::new_seeded(page_store, page_cache, faults, &loaded);
            return Ok((store, loaded.manifest));
        }
        let mut store = Self {
            page_store,
            page_cache,
            sequence: 0,
            next_slot: MANIFEST_PAGE_A,
            slot_volume_chain: [Vec::new(), Vec::new()],
            slot_snapshot_chain: [Vec::new(), Vec::new()],
            faults,
        };
        let mut empty = Manifest::empty();
        store.commit(&mut empty)?;
        Ok((store, empty))
    }

    /// Record the chain pids a slot references (walked from its head anchors).
    /// Used to seed both slots at open so subsequent commits reuse pids in
    /// place rather than leaking the slot's existing chain.
    fn seed_slot_chains(&mut self, slot: PageId, vol_head: PageId, snap_head: PageId) {
        let idx = slot_index(slot);
        self.slot_volume_chain[idx] =
            catalog::chain_pids(&self.page_store, vol_head, CatalogKind::Volumes).unwrap_or_default();
        self.slot_snapshot_chain[idx] =
            catalog::chain_pids(&self.page_store, snap_head, CatalogKind::Snapshots)
                .unwrap_or_default();
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

    /// Durably commit `manifest`. v23 writes, in order:
    ///
    /// 1. the target slot's volume + snapshot catalog chains (reusing their pids
    ///    in place, growing/shrinking continuations), then fsync;
    /// 2. the manifest slot page (pointing at the chain heads), then fsync.
    ///
    /// Only the target slot's chains + body are touched, so the other slot's
    /// generation stays fully intact — a torn new slot falls back to it. Pages
    /// the chains no longer need (a shrink) are freed only after the slot is
    /// durable.
    ///
    /// On failure before the manifest fsync, in-memory state is untouched (the
    /// retry targets the same slot).
    pub fn commit(&mut self, manifest: &mut Manifest) -> Result<()> {
        let new_sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption("manifest sequence overflow".into()))?;
        let target_slot = self.next_slot;
        let idx = slot_index(target_slot);
        let generation = manifest.checkpoint_lsn;

        // Fail fast (region fit + durable_seq invariant) before any allocation.
        manifest.check_encodable()?;

        // 1. Serialise both tables, then lay them over the target slot's chains
        //    (reusing pids in place). The byte encoders run before any
        //    allocate() so a malformed entry leaks nothing.
        let volume_bytes = manifest.encode_volume_catalog_bytes()?;
        let snapshot_bytes = manifest.encode_snapshot_catalog_bytes()?;
        let (vol_chain, vol_sealed, vol_free) = catalog::build_catalog_chain(
            &self.page_store,
            CatalogKind::Volumes,
            &volume_bytes,
            &self.slot_volume_chain[idx],
            generation,
        )?;
        let (snap_chain, snap_sealed, snap_free) = catalog::build_catalog_chain(
            &self.page_store,
            CatalogKind::Snapshots,
            &snapshot_bytes,
            &self.slot_snapshot_chain[idx],
            generation,
        )?;

        // 2. Chain pages durable BEFORE the manifest slot references them (same
        //    "external pages first" discipline as L2P / RC / deadlist).
        let mut sealed = vol_sealed;
        sealed.extend(snap_sealed);
        self.page_store.write_sealed_page_runs(sealed)?;
        self.page_store.sync()?;

        // 3. Manifest slot, pointing at the chain heads.
        manifest.volume_catalog_head_pid = vol_chain[0];
        manifest.snapshot_catalog_head_pid = snap_chain[0];
        let mut page = Page::new(PageHeader::new(PageType::Manifest, new_sequence));
        manifest.encode(&mut page, vol_chain[0], snap_chain[0])?;
        page.seal();

        self.page_store.write_page(target_slot, &page)?;
        self.faults.inject(FaultPoint::ManifestFsyncBefore)?;
        self.page_store.sync()?;
        self.faults.inject(FaultPoint::ManifestFsyncAfter)?;

        // 4. Durable — point of no return. Advance bookkeeping.
        self.sequence = new_sequence;
        self.next_slot = other_slot(target_slot);
        self.slot_volume_chain[idx] = vol_chain;
        self.slot_snapshot_chain[idx] = snap_chain;

        // 5. Free trailing continuation pages a shrink dropped (this slot's own
        //    former pages, now referenced by nobody). Best-effort: a free error
        //    only leaks pages (reclaimed at next open). Invalidate the cache
        //    before releasing each pid so a recycled page can't be read stale.
        for pid in vol_free.into_iter().chain(snap_free) {
            self.page_cache.invalidate(pid);
            if let Err(err) = self.page_store.free(pid, generation) {
                tracing::warn!(
                    page_id = pid,
                    error = %err,
                    "metadb: failed to free shrunken manifest catalog chain page"
                );
            }
        }
        Ok(())
    }
}

fn load_slot_header(page_store: &PageStore, slot: PageId) -> Option<u64> {
    let page = page_store.read_page_unchecked(slot).ok()?;
    page.verify(slot).ok()?;
    let header = page.header().ok()?;
    if header.page_type != PageType::Manifest {
        return None;
    }
    Some(header.generation)
}

fn load_slot_body(page_store: &PageStore, slot: PageId, sequence: u64) -> Result<LoadedManifest> {
    let page = page_store.read_page_unchecked(slot)?;
    page.verify(slot)?;
    let header = page.header()?;
    if header.page_type != PageType::Manifest {
        return Err(MetaDbError::Corruption(format!(
            "manifest slot {slot} is {:?}, not Manifest",
            header.page_type,
        )));
    }
    if header.generation != sequence {
        return Err(MetaDbError::Corruption(format!(
            "manifest slot {slot} sequence changed while loading: header={} candidate={sequence}",
            header.generation,
        )));
    }
    Ok(LoadedManifest {
        slot,
        sequence,
        manifest: Manifest::decode(&page, page_store)?,
    })
}
