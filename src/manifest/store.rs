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
    /// Per-slot persisted free-list bitmap RUN pids (contiguous, `[start,
    /// start+capacity)`, including the growth reserve). Reused in place across
    /// commits; relocated (geometric grow) only when the bitmap outgrows the
    /// reserve. Empty on the file path (the bitmap is device-only) and until the
    /// first device commit.
    slot_free_list_run: [Vec<PageId>; 2],
    faults: Arc<FaultController>,
}

/// Latest valid manifest slot loaded from disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedManifest {
    pub slot: PageId,
    pub sequence: u64,
    pub manifest: Manifest,
}

/// Immutable output of the expensive half of a manifest commit.
///
/// Catalog and free-list pages are already durable. Publishing this generation
/// only writes and syncs the single manifest slot page, then advances the
/// in-memory double-buffer bookkeeping.
pub(crate) struct PreparedManifestCommit {
    new_sequence: u64,
    target_slot: PageId,
    generation: Lsn,
    manifest_page: Page,
    volume_chain: Vec<PageId>,
    snapshot_chain: Vec<PageId>,
    free_list_run: Vec<PageId>,
    volume_free: Vec<PageId>,
    snapshot_free: Vec<PageId>,
    free_list_free: Vec<PageId>,
    is_device: bool,
}

/// Post-durable garbage left by a manifest catalog/free-bitmap chain shrink.
/// The referenced pages belong to the just-overwritten (older) slot and can be
/// queued for epoch reclaim after the apply gate is released.
pub(crate) struct PublishedManifestCleanup {
    generation: Lsn,
    pages: Vec<PageId>,
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

/// Read the catalog + free-list chain-head pids out of a slot's fixed header
/// WITHOUT a full [`Manifest::decode`]. Header-only on purpose: the OTHER
/// (non-winning) generation's body may reference `SnapshotRoots` / L2P pages the
/// engine has since reclaimed — a full decode would then fail and we'd lose
/// track of that slot's (still-intact) chains, leaking + mis-flagging them. The
/// chains themselves are never freed except on shrink, so they stay walkable
/// regardless of the rest of the body's validity. Returns `(volume_head,
/// snapshot_head, free_list_head)`; `free_list_head` is [`NULL_PAGE`] on a slot
/// written by the file path (no persisted bitmap).
fn slot_catalog_heads(page_store: &PageStore, slot: PageId) -> Option<(PageId, PageId, PageId)> {
    let page = page_store.read_page_unchecked(slot).ok()?;
    page.verify(slot).ok()?;
    let header = page.header().ok()?;
    if header.page_type != PageType::Manifest {
        return None;
    }
    let p = page.payload();
    let body_version =
        u32::from_le_bytes(p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].try_into().ok()?);
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
    let free_list = u64::from_le_bytes(
        p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
            .try_into()
            .ok()?,
    );
    Some((vol, snap, free_list))
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
        let Some((vol_head, snap_head, free_list_head)) = slot_catalog_heads(page_store, slot)
        else {
            continue;
        };
        for (head, kind) in [
            (vol_head, CatalogKind::Volumes),
            (snap_head, CatalogKind::Snapshots),
        ] {
            if let Ok(p) = catalog::chain_pids(page_store, head, kind) {
                pids.extend(p);
            }
        }
        // The persisted free-list bitmap run (incl. its growth reserve) is a live
        // per-slot anchor too: orphan-reclaim must not free it. NULL_PAGE = file
        // path / not yet persisted → no run.
        if let Ok(p) = catalog::free_list_run_pids(page_store, free_list_head) {
            pids.extend(p);
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
            slot_free_list_run: [Vec::new(), Vec::new()],
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
            loaded.manifest.free_list_head,
        );
        let other = other_slot(loaded.slot);
        if let Some((vol_head, snap_head, free_list_head)) =
            slot_catalog_heads(&store.page_store, other)
        {
            store.seed_slot_chains(other, vol_head, snap_head, free_list_head);
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
            slot_free_list_run: [Vec::new(), Vec::new()],
            faults,
        };
        let mut empty = Manifest::empty();
        store.commit(&mut empty)?;
        Ok((store, empty))
    }

    /// Record the chain pids a slot references (walked from its head anchors).
    /// Used to seed both slots at open so subsequent commits reuse pids in
    /// place rather than leaking the slot's existing chain.
    fn seed_slot_chains(
        &mut self,
        slot: PageId,
        vol_head: PageId,
        snap_head: PageId,
        free_list_head: PageId,
    ) {
        let idx = slot_index(slot);
        self.slot_volume_chain[idx] =
            catalog::chain_pids(&self.page_store, vol_head, CatalogKind::Volumes)
                .unwrap_or_default();
        self.slot_snapshot_chain[idx] =
            catalog::chain_pids(&self.page_store, snap_head, CatalogKind::Snapshots)
                .unwrap_or_default();
        self.slot_free_list_run[idx] =
            catalog::free_list_run_pids(&self.page_store, free_list_head).unwrap_or_default();
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

    /// Durably commit `manifest`. Writes, in order:
    ///
    /// 1. the target slot's volume + snapshot catalog chains (reusing their pids
    ///    in place, growing/shrinking continuations), plus — on a fixed-capacity
    ///    device — the persisted free-list bitmap chain (`free_list_head`), then
    ///    fsync;
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
        let prepared = self.prepare_commit(manifest)?;
        self.publish_prepared(prepared)
    }

    /// Encode and persist every page except the manifest slot itself.
    ///
    /// The target slot is the sacrificial (older) half of the double buffer, so
    /// its catalog/free-list chains may be rewritten before publication while
    /// the other slot remains the crash fallback. Callers may run this phase
    /// outside their global apply barrier once the manifest generation is
    /// frozen.
    pub(crate) fn prepare_commit(
        &mut self,
        manifest: &mut Manifest,
    ) -> Result<PreparedManifestCommit> {
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

        // 1b. [device path only] Build the persisted free-list bitmap chain so
        //     the next open loads the free list in O(bitmap pages) instead of a
        //     ~O(meta size) bounded page scan (~75 s → <5 s on a large meta LD).
        //     Its own chain pages are frontier-appended (see
        //     `alloc_free_list_chain`): they MUST NOT perturb the free list they
        //     encode, so this is the LAST allocation of the commit and the
        //     `(high_water, free_list)` pair snapshotted right after is what the
        //     bitmap + `page_high_water` are built from. The file path keeps its
        //     EOF-scan open and never persists a bitmap (`free_list_head` stays
        //     NULL) — zero regression.
        let is_device = self.page_store.capacity_pages().is_some();
        let (fl_run, fl_sealed, fl_free, device_high_water) = if is_device {
            let existing = self.slot_free_list_run[idx].clone();
            // Pages the bitmap needs to cover [FIRST_DATA_PAGE, high_water); +1
            // slack so the frontier-append below (when relocating) can't push the
            // bitmap past the run it's sized for.
            let needed = catalog::free_list_run_data_pages(catalog::free_list_bitmap_len(
                self.page_store.high_water(),
            )) + 1;
            let (fl_run, fl_free) = if existing.len() >= needed {
                // Reuse the whole run in place (steady state — no allocation, so
                // the free-list snapshot below is untouched).
                (existing, Vec::new())
            } else {
                // Relocate: allocate a fresh CONTIGUOUS run with geometric reserve
                // (bounds lifetime relocation churn to O(N)), free the old run.
                // Frontier-append never pops the interior free list, so the
                // snapshot stays consistent.
                let new_cap = needed.max(existing.len().saturating_mul(2));
                let fresh = self.page_store.allocate_frontier_pages(new_cap)?;
                (fresh, existing)
            };
            // Consistent snapshot of the free list as of this commit. Pages the
            // flush reclaims AFTER commit (the post-commit inline/async reclaim
            // pass, which cannot run before the manifest is durable) are not in
            // this bitmap — they stay in the running free list and are captured
            // by the next commit's bitmap. The bitmap is therefore a SAFE
            // (never marks a used page free) but possibly-conservative snapshot;
            // a crash in that narrow window Free-stamps those pages on disk but
            // leaves them unlisted until a future flush re-persists them.
            let (h2, bitmap) = self.page_store.snapshot_free_bitmap_and_high_water();
            debug_assert_eq!(bitmap.len(), catalog::free_list_bitmap_len(h2));
            let fl_sealed = catalog::seal_free_list_run(&fl_run, &bitmap, generation)?;
            (fl_run, fl_sealed, fl_free, h2)
        } else {
            (Vec::new(), Vec::new(), Vec::new(), 0)
        };

        // 2. Chain pages durable BEFORE the manifest slot references them (same
        //    "external pages first" discipline as L2P / RC / deadlist).
        let mut sealed = vol_sealed;
        sealed.extend(snap_sealed);
        sealed.extend(fl_sealed);
        self.page_store.write_sealed_page_runs(sealed)?;
        self.page_store.sync()?;

        // 3. Manifest slot, pointing at the chain heads.
        manifest.volume_catalog_head_pid = vol_chain[0];
        manifest.snapshot_catalog_head_pid = snap_chain[0];
        if is_device {
            // Authoritative pair from the post-allocation snapshot above; the
            // bitmap covers `[FIRST_DATA_PAGE, page_high_water)`.
            manifest.free_list_head = fl_run[0];
            manifest.page_high_water = device_high_water;
        } else {
            // v24: sample the page high-water AFTER every allocation this commit
            // made (checkpoint roots + the catalog chains just built above), so
            // it is a strict upper bound on every page id these roots reach. The
            // file open scans to EOF and doesn't depend on it.
            manifest.page_high_water = self.page_store.high_water();
        }
        let mut page = Page::new(PageHeader::new(PageType::Manifest, new_sequence));
        manifest.encode(&mut page, vol_chain[0], snap_chain[0])?;
        page.seal();

        Ok(PreparedManifestCommit {
            new_sequence,
            target_slot,
            generation,
            manifest_page: page,
            volume_chain: vol_chain,
            snapshot_chain: snap_chain,
            free_list_run: fl_run,
            volume_free: vol_free,
            snapshot_free: snap_free,
            free_list_free: fl_free,
            is_device,
        })
    }

    /// Publish a prepared generation by writing only its manifest slot page.
    pub(crate) fn publish_prepared(&mut self, prepared: PreparedManifestCommit) -> Result<()> {
        let cleanup = self.publish_prepared_deferred_cleanup(prepared)?;
        self.cleanup_published(cleanup);
        Ok(())
    }

    /// Publish without enqueueing obsolete catalog pages for reclaim. The
    /// checkpoint path uses this form so only the 4 KiB slot write, sync, and
    /// in-memory generation flip run under its apply gate.
    pub(crate) fn publish_prepared_deferred_cleanup(
        &mut self,
        prepared: PreparedManifestCommit,
    ) -> Result<PublishedManifestCleanup> {
        let PreparedManifestCommit {
            new_sequence,
            target_slot,
            generation,
            manifest_page,
            volume_chain,
            snapshot_chain,
            free_list_run,
            volume_free,
            snapshot_free,
            free_list_free,
            is_device,
        } = prepared;
        let idx = slot_index(target_slot);

        debug_assert_eq!(self.next_slot, target_slot);
        debug_assert_eq!(self.sequence.saturating_add(1), new_sequence);
        self.page_store.write_page(target_slot, &manifest_page)?;
        self.faults.inject(FaultPoint::ManifestFsyncBefore)?;
        self.page_store.sync()?;
        self.faults.inject(FaultPoint::ManifestFsyncAfter)?;

        // 4. Durable — point of no return. Advance bookkeeping.
        self.sequence = new_sequence;
        self.next_slot = other_slot(target_slot);
        self.slot_volume_chain[idx] = volume_chain;
        self.slot_snapshot_chain[idx] = snapshot_chain;
        if is_device {
            self.slot_free_list_run[idx] = free_list_run;
        }

        let mut pages = volume_free;
        pages.extend(snapshot_free);
        pages.extend(free_list_free);
        Ok(PublishedManifestCleanup { generation, pages })
    }

    /// Enqueue post-publish catalog garbage in one deferred-free batch. Errors
    /// are space leaks only: the new manifest slot is already durable and open
    /// recovery can reclaim the orphaned pages.
    pub(crate) fn cleanup_published(&self, cleanup: PublishedManifestCleanup) {
        for &pid in &cleanup.pages {
            self.page_cache.invalidate(pid);
        }
        if let Err(err) = self
            .page_store
            .free_many(&cleanup.pages, cleanup.generation)
        {
            tracing::warn!(
                pages = cleanup.pages.len(),
                error = %err,
                "metadb: failed to free shrunken manifest catalog chain pages"
            );
        }
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
