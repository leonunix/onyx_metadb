//! Offline consistency verifier plus helpers reused by recovery/open.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::btree::format::{internal_child_at, internal_key_count, leaf_key_count};
use crate::cache::PageCache;
use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::lsm::persist::decode_level_page;
use crate::lsm::{SstHandle, SstReader};
use crate::manifest::{LoadedManifest, Manifest, ManifestStore, load_snapshot_roots};
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::paged::format::{INDEX_FANOUT, index_child_at};
use crate::types::{FIRST_DATA_PAGE, Lsn, NULL_PAGE, PageId};
use crate::{BTree, PagedL2p};

#[derive(Clone, Debug, Default)]
pub struct VerifyOptions {
    /// Escalate orphaned allocated pages from warnings to hard failures.
    pub strict: bool,
}

#[derive(Clone, Debug, Default)]
pub struct VerifyReport {
    pub path: PathBuf,
    pub manifest_slot: Option<PageId>,
    pub manifest_sequence: Option<u64>,
    pub checkpoint_lsn: Option<Lsn>,
    pub high_water: u64,
    pub scanned_pages: u64,
    pub live_pages: usize,
    pub free_pages: usize,
    pub orphan_pages: Vec<PageId>,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
}

impl VerifyReport {
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}

#[derive(Default)]
struct LivePages {
    refs: BTreeMap<PageId, u32>,
}

impl LivePages {
    fn mark(&mut self, pid: PageId) {
        *self.refs.entry(pid).or_insert(0) += 1;
    }

    fn contains(&self, pid: PageId) -> bool {
        self.refs.contains_key(&pid)
    }
}

pub fn verify_path(path: impl AsRef<Path>, options: VerifyOptions) -> Result<VerifyReport> {
    let path = path.as_ref();
    let page_store = Arc::new(PageStore::open(path.join("pages.onyx_meta"))?);
    let mut report = VerifyReport {
        path: path.to_path_buf(),
        high_water: page_store.high_water(),
        ..VerifyReport::default()
    };

    let manifest = match ManifestStore::load_latest(&page_store)? {
        Some(loaded) => {
            report.manifest_slot = Some(loaded.slot);
            report.manifest_sequence = Some(loaded.sequence);
            report.checkpoint_lsn = Some(loaded.manifest.checkpoint_lsn);
            loaded
        }
        None => {
            report
                .issues
                .push("no valid manifest slot could be decoded".into());
            return Ok(report);
        }
    };

    let mut free_pages = BTreeSet::new();
    let mut verified_headers = HashMap::new();
    for pid in FIRST_DATA_PAGE..page_store.high_water() {
        report.scanned_pages += 1;
        let raw = match page_store.read_page_unchecked(pid) {
            Ok(page) => page,
            Err(err) => {
                report.issues.push(format!("page {pid} unreadable: {err}"));
                continue;
            }
        };
        if let Err(err) = raw.verify(pid) {
            report
                .issues
                .push(format!("page {pid} failed verify: {err}"));
            continue;
        }
        match raw.header() {
            Ok(header) => {
                if header.page_type == PageType::Free {
                    free_pages.insert(pid);
                }
                verified_headers.insert(pid, header.refcount);
            }
            Err(err) => report
                .issues
                .push(format!("page {pid} header decode failed: {err}")),
        }
    }
    report.free_pages = free_pages.len();

    match collect_live_pages(&page_store, &manifest) {
        Ok(live) => {
            report.live_pages = live.refs.len();
            for (pid, expected) in &live.refs {
                if free_pages.contains(pid) {
                    report
                        .issues
                        .push(format!("page {pid} is both live and on the free list"));
                }
                match verified_headers.get(pid).copied() {
                    Some(actual) if actual == *expected => {}
                    Some(actual) => report.issues.push(format!(
                        "page {pid} refcount mismatch: header={actual}, expected={expected}"
                    )),
                    None => report
                        .issues
                        .push(format!("live page {pid} did not pass the page scan")),
                }
            }

            for pid in FIRST_DATA_PAGE..page_store.high_water() {
                if free_pages.contains(&pid) || live.contains(pid) {
                    continue;
                }
                report.orphan_pages.push(pid);
            }
        }
        Err(err) => report.issues.push(format!("live-page walk failed: {err}")),
    }

    if !report.orphan_pages.is_empty() {
        let msg = format!("orphan allocated pages: {:?}", report.orphan_pages);
        if options.strict {
            report.issues.push(msg);
        } else {
            report.warnings.push(msg);
        }
    }

    Ok(report)
}

pub(crate) fn reclaim_orphan_pages(
    page_store: &Arc<PageStore>,
    manifest: &Manifest,
    generation: Lsn,
) -> Result<usize> {
    let live = collect_live_pages(
        page_store,
        &LoadedManifest {
            slot: NULL_PAGE,
            sequence: 0,
            manifest: manifest.clone(),
        },
    )?;
    let mut reclaimed = 0usize;
    for pid in FIRST_DATA_PAGE..page_store.high_water() {
        if live.contains(pid) {
            continue;
        }
        let is_free = match page_store.read_page_unchecked(pid) {
            Ok(page) => matches!(page.header(), Ok(header) if header.page_type == PageType::Free),
            Err(_) => false,
        };
        if is_free {
            continue;
        }
        page_store.free(pid, generation)?;
        reclaimed += 1;
    }
    Ok(reclaimed)
}

fn collect_live_pages(page_store: &Arc<PageStore>, loaded: &LoadedManifest) -> Result<LivePages> {
    let manifest = &loaded.manifest;
    let mut live = LivePages::default();
    let mut seen_paged = HashSet::new();
    let mut seen_btree = HashSet::new();
    let mut seen_level_pages = HashSet::new();
    let mut seen_ssts = HashSet::new();
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * PAGE_SIZE as u64));

    for &root in manifest.shard_roots.iter() {
        if root == NULL_PAGE {
            continue;
        }
        live.mark(root);
        walk_paged_tree(page_store, root, &mut live, &mut seen_paged)?;
        let tree = PagedL2p::open(page_store.clone(), root, 1)?;
        tree.check_invariants()?;
    }
    for &root in manifest.refcount_shard_roots.iter() {
        if root == NULL_PAGE {
            continue;
        }
        live.mark(root);
        walk_btree(page_store, root, &mut live, &mut seen_btree)?;
        let mut tree = BTree::open(page_store.clone(), root, 1)?;
        tree.check_invariants()?;
    }

    for snapshot in &manifest.snapshots {
        let l2p_roots = snapshot_roots(
            page_store,
            snapshot.l2p_roots_page,
            &snapshot.l2p_shard_roots,
        )?;
        if snapshot.l2p_roots_page != NULL_PAGE {
            live.mark(snapshot.l2p_roots_page);
        }
        for &root in l2p_roots.iter() {
            if root == NULL_PAGE {
                continue;
            }
            live.mark(root);
            walk_paged_tree(page_store, root, &mut live, &mut seen_paged)?;
            let tree = PagedL2p::open(page_store.clone(), root, 1)?;
            tree.check_invariants()?;
        }

        let refcount_roots = snapshot_roots(
            page_store,
            snapshot.refcount_roots_page,
            &snapshot.refcount_shard_roots,
        )?;
        if snapshot.refcount_roots_page != NULL_PAGE {
            live.mark(snapshot.refcount_roots_page);
        }
        for &root in refcount_roots.iter() {
            if root == NULL_PAGE {
                continue;
            }
            live.mark(root);
            walk_btree(page_store, root, &mut live, &mut seen_btree)?;
            let mut tree = BTree::open(page_store.clone(), root, 1)?;
            tree.check_invariants()?;
        }
    }

    walk_lsm_heads(
        page_store,
        &page_cache,
        &manifest.dedup_level_heads,
        &mut live,
        &mut seen_level_pages,
        &mut seen_ssts,
    )?;
    walk_lsm_heads(
        page_store,
        &page_cache,
        &manifest.dedup_reverse_level_heads,
        &mut live,
        &mut seen_level_pages,
        &mut seen_ssts,
    )?;

    Ok(live)
}

fn snapshot_roots<'a>(
    page_store: &PageStore,
    roots_page: PageId,
    inline_roots: &'a [PageId],
) -> Result<Box<[PageId]>> {
    if roots_page == NULL_PAGE {
        return Ok(inline_roots.to_vec().into_boxed_slice());
    }
    let loaded = load_snapshot_roots(page_store, roots_page)?;
    if !inline_roots.is_empty() && loaded.as_ref() != inline_roots {
        return Err(MetaDbError::Corruption(format!(
            "snapshot roots page {roots_page} disagrees with inline manifest roots"
        )));
    }
    Ok(loaded)
}

fn walk_paged_tree(
    page_store: &PageStore,
    root: PageId,
    live: &mut LivePages,
    seen: &mut HashSet<PageId>,
) -> Result<()> {
    if !seen.insert(root) {
        return Ok(());
    }
    let page = page_store.read_page(root)?;
    match page.header()?.page_type {
        PageType::PagedLeaf => Ok(()),
        PageType::PagedIndex => {
            let mut non_null = 0usize;
            for slot in 0..INDEX_FANOUT {
                let child = index_child_at(&page, slot);
                if child == NULL_PAGE {
                    continue;
                }
                non_null += 1;
                live.mark(child);
                walk_paged_tree(page_store, child, live, seen)?;
            }
            if non_null != page.key_count() as usize {
                return Err(MetaDbError::Corruption(format!(
                    "paged index {root} child count {} disagrees with header {}",
                    non_null,
                    page.key_count(),
                )));
            }
            Ok(())
        }
        other => Err(MetaDbError::Corruption(format!(
            "page {root} has unexpected type {other:?} in paged tree walk"
        ))),
    }
}

fn walk_btree(
    page_store: &PageStore,
    root: PageId,
    live: &mut LivePages,
    seen: &mut HashSet<PageId>,
) -> Result<()> {
    if !seen.insert(root) {
        return Ok(());
    }
    let page = page_store.read_page(root)?;
    match page.header()?.page_type {
        PageType::L2pLeaf => {
            let count = leaf_key_count(&page);
            if count > crate::btree::format::MAX_LEAF_ENTRIES {
                return Err(MetaDbError::Corruption(format!(
                    "btree leaf {root} overflows with {count} entries"
                )));
            }
            Ok(())
        }
        PageType::L2pInternal => {
            let key_count = internal_key_count(&page);
            for idx in 0..=key_count {
                let child = internal_child_at(&page, idx);
                live.mark(child);
                walk_btree(page_store, child, live, seen)?;
            }
            Ok(())
        }
        other => Err(MetaDbError::Corruption(format!(
            "page {root} has unexpected type {other:?} in btree walk"
        ))),
    }
}

fn walk_lsm_heads(
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    heads: &[PageId],
    live: &mut LivePages,
    seen_level_pages: &mut HashSet<PageId>,
    seen_ssts: &mut HashSet<PageId>,
) -> Result<()> {
    for &head in heads {
        if head == NULL_PAGE {
            continue;
        }
        live.mark(head);
        let mut cursor = head;
        while cursor != NULL_PAGE {
            if !seen_level_pages.insert(cursor) {
                return Err(MetaDbError::Corruption(format!(
                    "LsmLevels chain loops back to page {cursor}"
                )));
            }
            let page = page_store.read_page(cursor)?;
            let (next, handles) = decode_level_page(cursor, &page)?;
            if next != NULL_PAGE {
                live.mark(next);
            }
            for handle in handles {
                mark_sst(handle, live);
                if seen_ssts.insert(handle.head_page) {
                    verify_sst(page_store, page_cache, handle)?;
                }
            }
            cursor = next;
        }
    }
    Ok(())
}

fn mark_sst(handle: SstHandle, live: &mut LivePages) {
    for offset in 0..handle.page_count() {
        live.mark(handle.head_page + offset as u64);
    }
}

fn verify_sst(page_store: &PageStore, page_cache: &PageCache, handle: SstHandle) -> Result<()> {
    let reader = SstReader::open(page_store, page_cache, handle)?;
    let mut prev = None;
    let mut count = 0u64;
    for rec in reader.scan() {
        let rec = rec?;
        if let Some(prev_hash) = prev {
            if rec.hash() <= &prev_hash {
                return Err(MetaDbError::Corruption(format!(
                    "SST {} records are not strictly ascending",
                    handle.head_page
                )));
            }
        }
        prev = Some(*rec.hash());
        count += 1;
    }
    if count != handle.record_count {
        return Err(MetaDbError::Corruption(format!(
            "SST {} scanned {} records but handle says {}",
            handle.head_page, count, handle.record_count
        )));
    }
    Ok(())
}
