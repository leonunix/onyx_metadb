//! Offline consistency verifier plus helpers reused by recovery/open.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::PagedL2p;
use crate::error::{MetaDbError, Result};
use crate::manifest::{LoadedManifest, Manifest, ManifestStore, load_snapshot_roots};
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::paged::format::{INDEX_FANOUT, index_child_at};
use crate::types::{FIRST_DATA_PAGE, Lsn, NULL_PAGE, PageId};

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
    // A3 cutover: page rc lives in the `L2pPageRc` array, not the page
    // header. The scan records which pids passed verify, and which are
    // L2P pages (`PagedLeaf` / `PagedIndex`) — only those carry a page-rc
    // array entry. Non-L2P live pages (refcount / page-rc / dedup meta
    // chains, dead-list segments) had a trivial header rc of 1 under the
    // old scheme and are NOT tracked by the array, so the rc comparison
    // below skips them.
    let mut scanned_pids: HashSet<PageId> = HashSet::new();
    let mut l2p_pids: HashSet<PageId> = HashSet::new();
    for pid in FIRST_DATA_PAGE..page_store.high_water() {
        report.scanned_pages += 1;
        let raw = match page_store.read_page_unchecked(pid) {
            Ok(page) => page,
            Err(err) => {
                report.issues.push(format!("page {pid} unreadable: {err}"));
                continue;
            }
        };
        if raw.bytes().iter().all(|b| *b == 0) {
            free_pages.insert(pid);
            continue;
        }
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
                if matches!(
                    header.page_type,
                    PageType::PagedLeaf | PageType::PagedIndex
                ) {
                    l2p_pids.insert(pid);
                }
                scanned_pids.insert(pid);
            }
            Err(err) => report
                .issues
                .push(format!("page {pid} header decode failed: {err}")),
        }
    }
    report.free_pages = free_pages.len();

    // Open the L2P-page-rc array from the manifest roots so the
    // parent-pointer counts can be checked against it (the old A4 step,
    // folded into A3 — verify would otherwise misfire on the now-dead
    // header rc field). Fold-consistent reads; the store is offline so
    // there is no concurrent fold, but `get_consistent` is the correct
    // "free decision"-grade read regardless.
    let verify_page_cache = Arc::new(crate::cache::PageCache::new(
        page_store.clone(),
        16 * 1024 * 1024,
    ));
    let page_rc = crate::l2p_page_rc::L2pPageRc::open(
        page_store.clone(),
        verify_page_cache,
        &manifest.manifest.l2p_page_rc_shard_roots,
        &manifest.manifest.l2p_page_rc_durable_seq,
    )?;

    match collect_live_pages(&page_store, &manifest) {
        Ok(live) => {
            report.live_pages = live.refs.len();
            for (pid, expected) in &live.refs {
                if free_pages.contains(pid) {
                    report
                        .issues
                        .push(format!("page {pid} is both live and on the free list"));
                }
                if !scanned_pids.contains(pid) {
                    report
                        .issues
                        .push(format!("live page {pid} did not pass the page scan"));
                    continue;
                }
                // Only L2P pages carry a page-rc array entry; the
                // refcount/page-rc/dedup meta chains + dead-list segments
                // that `collect_live_pages` also marks are not refcounted
                // there.
                if !l2p_pids.contains(pid) {
                    continue;
                }
                match page_rc.get_consistent(*pid) {
                    Ok(actual) if actual == *expected => {}
                    Ok(actual) => report.issues.push(format!(
                        "page {pid} page-rc mismatch: array={actual}, expected={expected}"
                    )),
                    Err(err) => report
                        .issues
                        .push(format!("page {pid} page-rc read failed: {err}")),
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
            Ok(page) => {
                page.bytes().iter().all(|b| *b == 0)
                    || matches!(page.header(), Ok(header) if header.page_type == PageType::Free)
            }
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
    let mut seen_paged: HashSet<PageId> = HashSet::new();
    let mut seen_btree: HashSet<PageId> = HashSet::new();

    for volume in &manifest.volumes {
        for &root in volume.l2p_shard_roots.iter() {
            if root == NULL_PAGE {
                continue;
            }
            live.mark(root);
            walk_paged_tree(page_store, root, &mut live, &mut seen_paged)?;
            let tree = PagedL2p::open(page_store.clone(), root, 1)?;
            tree.check_invariants()?;
        }
        walk_dead_list_chain(
            page_store,
            volume.dead_list_head_pid,
            volume.dead_list_tail_pid,
            &mut live,
        )?;
    }
    for &meta_pid in manifest.refcount_shard_roots.iter() {
        if meta_pid == NULL_PAGE {
            continue;
        }
        // Walker marks every meta page in the chain (head + continuations)
        // via its on_meta callback, matching walk_cuckoo / walk_dedup_reverse.
        walk_refcount_paged_array(page_store, meta_pid, &mut live, &mut seen_btree)?;
    }
    // v17 (snapshot-scaling Phase A2): the L2P-page-rc shard group is a
    // second `RcShard`/`PagedRefcountArray` group with the identical
    // paged-array layout, so its meta chains + data pages walk through
    // the same routine. Without this the orphan-reclaim-on-open pass
    // would treat the page-rc meta pages as unreachable and free them,
    // corrupting the store on the next reopen.
    for &meta_pid in manifest.l2p_page_rc_shard_roots.iter() {
        if meta_pid == NULL_PAGE {
            continue;
        }
        walk_refcount_paged_array(page_store, meta_pid, &mut live, &mut seen_btree)?;
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
        // v6 dropped per-snapshot refcount state; refcount tree is
        // walked once at the top level above.
    }

    // The legacy `dedup_index_shard_heads` manifest slot now carries
    // `[[meta_page_id]]` (single-element box) for the cuckoo. Walk
    // the cuckoo meta page + every data page it indexes.
    let dedup_index_meta_pid: PageId = manifest
        .dedup_index_shard_heads
        .first()
        .and_then(|s| s.first().copied())
        .unwrap_or(NULL_PAGE);
    if dedup_index_meta_pid != NULL_PAGE {
        walk_cuckoo_dedup_index(page_store, dedup_index_meta_pid, &mut live)?;
    }
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

/// Walk a volume's dead-list segment chain backward from `tail_pid`
/// through each segment's `prev_seg_pid`, marking every chain page as
/// live and validating segment-header invariants (magic, non-empty
/// record_count, monotonic LSN ranges across the chain, terminator).
/// No-op if `tail_pid == NULL_PAGE`.
fn walk_dead_list_chain(
    page_store: &PageStore,
    head_pid: PageId,
    tail_pid: PageId,
    live: &mut LivePages,
) -> Result<()> {
    if tail_pid == NULL_PAGE {
        if head_pid != NULL_PAGE {
            return Err(MetaDbError::Corruption(format!(
                "dead-list chain: tail_pid=NULL_PAGE but head_pid={head_pid} (only one anchor set)"
            )));
        }
        return Ok(());
    }
    if head_pid == NULL_PAGE {
        return Err(MetaDbError::Corruption(format!(
            "dead-list chain: head_pid=NULL_PAGE but tail_pid={tail_pid} (only one anchor set)"
        )));
    }
    let mut cur = tail_pid;
    let mut prev_min_lsn: Option<Lsn> = None;
    let mut seen_chain: HashSet<PageId> = HashSet::new();
    let mut reached_head = false;
    loop {
        if !seen_chain.insert(cur) {
            return Err(MetaDbError::Corruption(format!(
                "dead-list chain has a cycle at page {cur}"
            )));
        }
        let page = page_store.read_page(cur)?;
        let header = page.header()?;
        if header.page_type != PageType::DeadListSegment {
            return Err(MetaDbError::Corruption(format!(
                "dead-list chain page {cur} has wrong page_type {:?}",
                header.page_type
            )));
        }
        let seg = crate::deadlist::SegmentHeader::decode(page.payload())?;
        if let Some(prev_min) = prev_min_lsn {
            if seg.max_lsn >= prev_min {
                return Err(MetaDbError::Corruption(format!(
                    "dead-list segment at {cur} max_lsn={} >= next segment min_lsn={prev_min} (chain must be strictly older going backward)",
                    seg.max_lsn
                )));
            }
        }
        for i in 0..seg.seg_page_count as u64 {
            live.mark(cur + i);
        }
        if cur == head_pid {
            reached_head = true;
            if seg.prev_seg_pid != NULL_PAGE {
                return Err(MetaDbError::Corruption(format!(
                    "dead-list head segment at {cur} has non-NULL prev_seg_pid={}",
                    seg.prev_seg_pid
                )));
            }
            break;
        }
        if seg.prev_seg_pid == NULL_PAGE {
            return Err(MetaDbError::Corruption(format!(
                "dead-list segment at {cur} terminates chain (prev=NULL) before reaching head_pid={head_pid}"
            )));
        }
        prev_min_lsn = Some(seg.min_lsn);
        cur = seg.prev_seg_pid;
    }
    if !reached_head {
        return Err(MetaDbError::Corruption(format!(
            "dead-list chain walk did not reach head_pid={head_pid} (tail={tail_pid})"
        )));
    }
    Ok(())
}

/// Walk the dedup_index cuckoo meta chain + every allocated data
/// page, marking them as live. Cuckoo data pages have no further
/// outgoing references (no overflow chain like dedup_reverse).
fn walk_cuckoo_dedup_index(
    page_store: &PageStore,
    meta_pid: PageId,
    live: &mut LivePages,
) -> Result<()> {
    let page_table = crate::paged_meta::walk_chain(
        page_store,
        meta_pid,
        PageType::CuckooData,
        0xFFFF,
        24, // bucket_count + seed1 + seed2
        |pid| live.mark(pid),
    )?;
    for pid in page_table {
        // Cuckoo's page-table uses 0 (not NULL_PAGE) as the
        // "unallocated" sentinel — a hole in the bucket → page
        // mapping. Real data pages are always >= FIRST_DATA_PAGE.
        if pid == 0 || pid == NULL_PAGE {
            continue;
        }
        live.mark(pid);
        let data_page = page_store.read_page(pid)?;
        let dh = data_page.header()?;
        if dh.page_type != PageType::CuckooData {
            return Err(MetaDbError::Corruption(format!(
                "cuckoo data page {pid} has wrong type {:?}",
                dh.page_type
            )));
        }
    }
    Ok(())
}

/// Walk the paged-array refcount shard's meta chain and mark every
/// allocated data page as live. Replaces the legacy `walk_btree` for
/// refcount roots.
fn walk_refcount_paged_array(
    page_store: &PageStore,
    meta_pid: PageId,
    live: &mut LivePages,
    seen: &mut HashSet<PageId>,
) -> Result<()> {
    if !seen.insert(meta_pid) {
        return Ok(());
    }
    let page_table = crate::paged_meta::walk_chain(
        page_store,
        meta_pid,
        PageType::RefcountArray,
        0xFFFF,
        0,
        |pid| live.mark(pid),
    )?;
    for pid in page_table {
        // 0 = unallocated (paged-array hole sentinel).
        if pid == 0 || pid == NULL_PAGE {
            continue;
        }
        live.mark(pid);
        // Refcount data pages have no outgoing references; just verify
        // the type tag.
        let data_page = page_store.read_page(pid)?;
        let dh = data_page.header()?;
        if dh.page_type != PageType::RefcountArray {
            return Err(MetaDbError::Corruption(format!(
                "refcount data page {pid} has wrong type {:?}",
                dh.page_type
            )));
        }
    }
    Ok(())
}
