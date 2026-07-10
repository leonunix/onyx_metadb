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
use crate::types::{FIRST_DATA_PAGE, Lsn, NULL_PAGE, Pba, PageId};

#[derive(Clone, Debug, Default)]
pub struct VerifyOptions {
    /// Escalate orphaned allocated pages from warnings to hard failures.
    pub strict: bool,
    /// Run the birth-LSN shadow check:
    /// for every head-reachable L2P page P of each non-clone volume,
    /// assert `birth_lsn(P) <= youngest_snap(V)` ⟺ P is reachable from
    /// the youngest snapshot's tree. This proves `birth_lsn` is a reliable
    /// immutable birth metadata substrate (the COW kill decision will
    /// use) WITHOUT changing any behavior — page-rc stays authoritative.
    /// Off by default so existing verify callers are unaffected.
    pub check_birth_shadow: bool,
    /// Run the per-clone page-livelist audit (BFG): for every
    /// clone (volume with `VOLUME_FLAG_CLONE_LINEAGE`), reconstruct the
    /// live-ALLOC set from its livelist chain (ALLOC minus matched FREE) and
    /// assert it equals `reachable(clone) ∩ {birth > branched_at_lsn}` — the
    /// clone-private subtree the livelist must faithfully record before clone
    /// drop relies on it instead of page-rc. Off by default.
    pub check_clone_livelist: bool,
    /// Run the clone COW-kill pinner check as a hard gate: for every clone,
    /// assert every page reachable from a
    /// surviving pinner (other volume head / snapshot tree) satisfies
    /// `birth <= B_eff` where `B_eff = max({branched_at_lsn} ∪ {own snapshot
    /// created_lsn} ∪ {descendant branch points})` — i.e. the page-rc-independent
    /// clone COW-kill would preserve every shared page. The page-rc-independent
    /// tripwire for the page-rc-independent clone COW decision. Off by default
    /// so existing callers are unaffected.
    pub check_clone_birth_shadow: bool,
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
    /// `(page_id, page_type)` for every entry in `orphan_pages` whose header
    /// decoded cleanly — a bare page id isn't actionable; knowing it's e.g. a
    /// paged-tree leaf vs. a cuckoo bucket page tells us what is actually
    /// leaking. Parallel diagnostic to `orphan_pages`, not a replacement (an
    /// orphan with an undecodable header has no entry here).
    pub orphan_page_types: Vec<(PageId, PageType)>,
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
    let manifest = match ManifestStore::load_latest(&page_store)? {
        Some(loaded) => loaded,
        None => {
            let mut report = VerifyReport {
                path: path.to_path_buf(),
                high_water: page_store.high_water(),
                ..VerifyReport::default()
            };
            report
                .issues
                .push("no valid manifest slot could be decoded".into());
            return Ok(report);
        }
    };
    let mut report = verify_page_store(&page_store, &manifest, options)?;
    report.path = path.to_path_buf();
    Ok(report)
}

/// Device-generic counterpart of [`verify_path`]: audits an already-open
/// `page_store` against an already-loaded `manifest` instead of opening a
/// plain-file page store from a path. This is what lets a chunklet-backed (or
/// any other [`PageDevice`]-backed) metadb instance be audited — `PageStore`
/// and every check below only ever touch the device through the `PageDevice`
/// trait, never a file path.
pub fn verify_page_store(
    page_store: &Arc<PageStore>,
    manifest: &LoadedManifest,
    options: VerifyOptions,
) -> Result<VerifyReport> {
    let mut report = VerifyReport {
        high_water: page_store.high_water(),
        manifest_slot: Some(manifest.slot),
        manifest_sequence: Some(manifest.sequence),
        checkpoint_lsn: Some(manifest.manifest.checkpoint_lsn),
        ..VerifyReport::default()
    };

    let mut free_pages = BTreeSet::new();
    // Never-written pages (all-zero, no header ever stamped) — a SUBSET of
    // `free_pages`. Distinguished from explicitly `PageType::Free`-typed
    // pages because a live anchor's reserved-but-not-yet-grown-into headroom
    // (e.g. the device-path free-list bitmap's growth reserve, see
    // `manifest::catalog_chain_pids_all_slots` / `catalog::free_list_run_pids`)
    // is legitimately both "live" (anchored, must survive orphan reclaim) and
    // all-zero (never actually used yet) — that is not a conflict, unlike a
    // page that WAS written+freed and is somehow still reachable.
    let mut zero_never_written: BTreeSet<PageId> = BTreeSet::new();
    // BFG: per-L2P-page refcounting was DELETED, so verify no longer
    // cross-checks an array rc. The scan just records which pids passed verify
    // (used below to flag a live page that didn't survive the byte scan).
    let mut scanned_pids: HashSet<PageId> = HashSet::new();
    let mut typed_pages: BTreeMap<PageId, PageType> = BTreeMap::new();
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
            zero_never_written.insert(pid);
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
                typed_pages.insert(pid, header.page_type);
                scanned_pids.insert(pid);
            }
            Err(err) => report
                .issues
                .push(format!("page {pid} header decode failed: {err}")),
        }
    }
    report.free_pages = free_pages.len();

    // BFG: per-L2P-page refcounting was DELETED, so there is no
    // page-rc array to cross-check parent-pointer counts against. The
    // structural reachability walk (`collect_live_pages`) still runs to flag
    // live/free conflicts, live pages that didn't pass the scan, and orphans
    // (pages reachable from no live root). The page-rc-INDEPENDENT shadows
    // (`check_birth_shadow` / `check_page_deadlist_shadow` /
    // `check_clone_livelist_shadow`) are the structural-soundness oracles now.
    match collect_live_pages(page_store, manifest) {
        Ok(live) => {
            report.live_pages = live.refs.len();
            for (pid, _expected) in &live.refs {
                if zero_never_written.contains(pid) {
                    // Reserved-but-unused growth headroom of a live anchor
                    // (e.g. free-list bitmap reserve) — expected, not a
                    // conflict; nothing has ever been written here.
                    continue;
                }
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
            }

            for pid in FIRST_DATA_PAGE..page_store.high_water() {
                if free_pages.contains(&pid) || live.contains(pid) {
                    continue;
                }
                report.orphan_pages.push(pid);
                if let Some(page_type) = typed_pages.get(&pid) {
                    report.orphan_page_types.push((pid, *page_type));
                }
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

    if options.check_birth_shadow {
        if let Err(err) = check_birth_shadow(page_store, &manifest.manifest, &mut report) {
            report
                .issues
                .push(format!("birth-shadow check failed: {err}"));
        }
        if let Err(err) = check_page_deadlist(page_store, &manifest.manifest, &mut report) {
            report
                .issues
                .push(format!("page-deadlist check failed: {err}"));
        }
    }

    if options.check_clone_livelist
        && let Err(err) = check_clone_livelist(page_store, &manifest.manifest, &mut report)
    {
        report
            .issues
            .push(format!("clone-livelist check failed: {err}"));
    }

    if options.check_clone_birth_shadow
        && let Err(err) = check_clone_birth_shadow(page_store, &manifest.manifest, &mut report)
    {
        report
            .issues
            .push(format!("clone-birth-shadow check failed: {err}"));
    }

    Ok(report)
}

/// Per-clone page-livelist audit. For every clone (a
/// volume carrying `VOLUME_FLAG_CLONE_LINEAGE`, set at clone creation and
/// sticky past promotion), reconstruct the live-ALLOC set from its livelist
/// chain — ALLOC records not cancelled by a matching `(pid, birth)` FREE —
/// and prove it equals the clone's clone-private subtree:
///
///   live-ALLOC(C)  ==  reachable_l2p_pages(C roots) ∩ { birth_lsn > B }
///
/// where `B = branched_at_lsn`. This is the substrate invariant clone drop relies
/// on to free a clone's private pages from the livelist
/// instead of walking the tree / consulting page-rc. Both directions are
/// hard `issues`: a logged page no longer reachable (`extra`) is a stale /
/// over-logged record (a leak at the cutover); a reachable
/// clone-private page absent from the live-ALLOC set (`missing`) is an
/// under-log (a premature-free at the cutover). Structural checks: every
/// record has `birth > B`, and `live_allocs` rejects a FREE without a prior
/// matching ALLOC.
fn check_clone_livelist(
    page_store: &Arc<PageStore>,
    manifest: &Manifest,
    report: &mut VerifyReport,
) -> Result<()> {
    for volume in &manifest.volumes {
        if volume.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE == 0 {
            continue;
        }
        let vol = volume.ord;
        let b = volume.branched_at_lsn;

        let records = crate::livelist::read_chain_records(volume.page_live_list_tail_pid, |p| {
            page_store.read_page(p)
        })?;
        for r in &records {
            if r.birth_lsn <= b {
                report.issues.push(format!(
                    "clone-livelist vol {vol}: record pid={} has birth_lsn={} <= branched_at_lsn={b} \
                     (a livelist member must be clone-private, born after the branch)",
                    r.pid, r.birth_lsn
                ));
            }
        }
        let live = match crate::livelist::live_allocs(records) {
            Ok(live) => live,
            Err(err) => {
                report
                    .issues
                    .push(format!("clone-livelist vol {vol}: {err}"));
                continue;
            }
        };
        let live_set: HashSet<(PageId, Lsn)> =
            live.iter().map(|r| (r.pid, r.birth_lsn)).collect();

        // Ground truth: the clone's clone-private subtree — pages reachable
        // from its current roots whose immutable birth is after the branch.
        let reachable = reachable_l2p_pages(page_store, &volume.l2p_shard_roots)?;
        let mut clone_private: HashSet<(PageId, Lsn)> = HashSet::new();
        for pid in &reachable {
            let header = match page_store
                .read_page_unchecked(*pid)
                .and_then(|p| p.header())
            {
                Ok(h) => h,
                Err(_) => continue,
            };
            if matches!(
                header.page_type,
                PageType::PagedLeaf | PageType::PagedIndex
            ) && header.birth_lsn > b
            {
                clone_private.insert((*pid, header.birth_lsn));
            }
        }

        let mut extra: Vec<(PageId, Lsn)> =
            live_set.difference(&clone_private).copied().collect();
        if !extra.is_empty() {
            extra.sort_unstable();
            report.issues.push(format!(
                "clone-livelist vol {vol} (B={b}): {} live-ALLOC record(s) name pages that are NOT \
                 clone-private-reachable (stale / over-logged): {extra:?} \
                 (live={} clone_private={})",
                extra.len(),
                live_set.len(),
                clone_private.len(),
            ));
        }
        let mut missing: Vec<(PageId, Lsn)> =
            clone_private.difference(&live_set).copied().collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            report.issues.push(format!(
                "clone-livelist vol {vol} (B={b}): {} clone-private-reachable page(s) absent from the \
                 live-ALLOC set (under-logged): {missing:?} \
                 (live={} clone_private={})",
                missing.len(),
                live_set.len(),
                clone_private.len(),
            ));
        }
    }
    Ok(())
}

/// Birth-LSN shadow check. For each
/// non-clone volume, prove that the immutable `birth_lsn` faithfully
/// predicts snapshot-preservation — the exact comparison the COW kill
/// decision uses (`birth_lsn <= youngest_snap`):
///
///   for every head-reachable L2P page P:
///     birth_lsn(P) <= youngest_snap(V)  ⟺  P reachable from youngest snapshot
///
/// Rationale: a page still reachable from the head was not COW'd since the
/// youngest snapshot iff `birth_lsn <= youngest_snap`; in that case the
/// snapshot still pins that exact version (reachable from its tree).
/// Mismatches are hard `issues` — they pinpoint a wrong `birth_lsn` stamp
/// before any liveness decision rides on it (resolves birth-shadow equivalence empirically).
///
/// Clones are skipped: their pages form a cross-volume DAG (cross-volume clone sharing) whose
/// sharing a per-volume reachability walk cannot see — that is livelist
/// territory. Page-rc remains authoritative throughout.
fn check_birth_shadow(
    page_store: &Arc<PageStore>,
    manifest: &Manifest,
    report: &mut VerifyReport,
) -> Result<()> {
    for volume in &manifest.volumes {
        if volume.parent_vol_ord.is_some() {
            continue;
        }
        let vol = volume.ord;
        // The youngest snapshot pins the most recent point-in-time view; a
        // head page reachable from ANY live snapshot is reachable from this
        // one (it was not COW'd since), so the youngest suffices.
        let youngest = manifest
            .snapshots
            .iter()
            .filter(|s| s.vol_ord == vol)
            .max_by_key(|s| s.created_lsn);
        let youngest_snap = youngest.map_or(0, |s| s.created_lsn);

        let head_set = reachable_l2p_pages(page_store, &volume.l2p_shard_roots)?;
        let snap_set = match youngest {
            None => HashSet::new(),
            Some(snap) => {
                let roots =
                    snapshot_roots(page_store, snap.l2p_roots_page, &snap.l2p_shard_roots)?;
                reachable_l2p_pages(page_store, &roots)?
            }
        };

        for pid in &head_set {
            let header = match page_store
                .read_page_unchecked(*pid)
                .and_then(|p| p.header())
            {
                Ok(h) => h,
                Err(_) => continue,
            };
            if !matches!(
                header.page_type,
                PageType::PagedLeaf | PageType::PagedIndex
            ) {
                continue;
            }
            let birth_predicts_preserved = youngest_snap > 0 && header.birth_lsn <= youngest_snap;
            let reachable_from_youngest_snapshot = snap_set.contains(pid);
            if birth_predicts_preserved != reachable_from_youngest_snapshot {
                report.issues.push(format!(
                    "birth-shadow mismatch vol {vol} page {pid}: birth_lsn={} \
                     youngest_snap={youngest_snap} \
                     birth_predicts_preserved={birth_predicts_preserved} \
                     reachable_from_youngest_snapshot={reachable_from_youngest_snapshot}",
                    header.birth_lsn
                ));
            }
        }
    }
    Ok(())
}

/// Clone COW-kill birth-operand SHADOW (BFG —
/// READ-ONLY characterization, page-rc stays authoritative). `check_birth_shadow`
/// skips clones because a per-volume birth comparison cannot see cross-volume
/// DAG sharing; this audit measures exactly that gap for the candidate
/// post-page-rc clone COW-kill operand.
///
/// For each clone C (`VOLUME_FLAG_CLONE_LINEAGE`) the clone COW-kill operand is
/// `COW iff birth(P) <= B_eff`, where `B_eff` is the MAX of the page-rc-independent
/// pinner-LSN set the runtime `cow_for_write` actually uses (built by
/// `Db::clone_cow_pinners`):
///
/// ```text
/// B_eff = max( branched_at_lsn(C),
///              max{ snapshot.created_lsn : snapshot of C },
///              max{ branched_at_lsn(V)   : V another clone-lineage volume,
///                                          branched_at_lsn(V) > branched_at_lsn(C) } )
/// ```
///
/// The third term — descendant branch points (incl. PROMOTED ex-descendants whose
/// `parent_vol_ord` is cleared) — is what makes the operand sufficient for the
/// DAG: it pins a born>B page a surviving descendant still references after C's own
/// snapshot is dropped (the descendant-share premature-free the pure-birth `max(B, youngest_snap)`
/// operand misses). The page-rc-INDEPENDENT ground truth "P is pinned" = P
/// reachable from any OTHER live volume head or any snapshot tree (a pinned page
/// MUST be COW'd or a clone write clobbers a shared page).
///
/// Only the SAFETY direction is a finding: `pinned(P) && birth(P) > B_eff` — the
/// operand would write in place over a shared page (the parent/sibling corruption
/// hazard). The reverse (`birth(P) <= B_eff` but unpinned — e.g. a origin-fallthrough sole-owned
/// origin page) is a benign conservative over-COW and is NOT flagged.
///
/// An EMPTY result proves the clone COW-kill clone operand is safe for the audited manifest.
pub(crate) fn clone_birth_shadow_findings(
    page_store: &Arc<PageStore>,
    manifest: &Manifest,
) -> Result<Vec<String>> {
    let mut findings = Vec::new();
    for volume in &manifest.volumes {
        if volume.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE == 0 {
            continue;
        }
        let vol = volume.ord;
        let b = volume.branched_at_lsn;
        let youngest_snap_clone = manifest
            .snapshots
            .iter()
            .filter(|s| s.vol_ord == vol)
            .map(|s| s.created_lsn)
            .max()
            .unwrap_or(0);
        // Descendant branch points (the clone COW-kill completion): max over every OTHER
        // clone-lineage volume that branched after C — the conservative superset
        // of C's descendants that also covers promoted ex-descendants. Mirrors
        // `Db::clone_cow_pinners`'s third term.
        let max_descendant_branch = manifest
            .volumes
            .iter()
            .filter(|other| {
                other.ord != vol
                    && other.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE != 0
                    && other.branched_at_lsn > b
            })
            .map(|other| other.branched_at_lsn)
            .max()
            .unwrap_or(0);
        let b_eff = b.max(youngest_snap_clone).max(max_descendant_branch);

        let head_set = reachable_l2p_pages(page_store, &volume.l2p_shard_roots)?;

        // page-rc-INDEPENDENT pinner set: every OTHER live volume head + every
        // snapshot tree (incl. this clone's own snapshots and any descendant's).
        let mut pinner_roots: Vec<PageId> = Vec::new();
        for other in &manifest.volumes {
            if other.ord == vol {
                continue;
            }
            pinner_roots.extend(
                other
                    .l2p_shard_roots
                    .iter()
                    .copied()
                    .filter(|&r| r != NULL_PAGE),
            );
        }
        for snap in &manifest.snapshots {
            let roots = snapshot_roots(page_store, snap.l2p_roots_page, &snap.l2p_shard_roots)?;
            pinner_roots.extend(roots.into_iter().filter(|&r| r != NULL_PAGE));
        }
        let pinner_set = reachable_l2p_pages(page_store, &pinner_roots)?;

        for pid in &head_set {
            // Skip unreadable/undecodable pages (mirrors `check_birth_shadow`):
            // acceptable here because this is a read-only characterization, not
            // an `is_clean()` gate — a marginally-corrupt page is a verify-scan
            // concern, not a clone-COW-operand data point.
            let header = match page_store
                .read_page_unchecked(*pid)
                .and_then(|p| p.header())
            {
                Ok(h) => h,
                Err(_) => continue,
            };
            if !matches!(
                header.page_type,
                PageType::PagedLeaf | PageType::PagedIndex
            ) {
                continue;
            }
            let operand_cows = header.birth_lsn <= b_eff;
            let pinned = pinner_set.contains(pid);
            if pinned && !operand_cows {
                findings.push(format!(
                    "clone-birth-shadow vol {vol} (B={b} youngest_snap={youngest_snap_clone} \
                     max_descendant_branch={max_descendant_branch} B_eff={b_eff}) page {pid}: \
                     birth_lsn={} > B_eff but page is reachable from a surviving pinner (the \
                     clone COW-kill would clobber a shared page)",
                    header.birth_lsn
                ));
            }
        }
    }
    Ok(findings)
}

/// HARD `is_clean()` gate wrapping [`clone_birth_shadow_findings`] (BFG
/// ). Pushes every safety-direction finding into
/// `report.issues` so a manifest where the clone COW-kill clone COW-kill operand would
/// clobber a shared page fails verification. Sound to gate now that the operand
/// consults descendant branch points (the pure-birth gap is closed); page-rc is
/// kept as the inverted-shadow floor until page-rc removal, so this is the page-rc-INDEPENDENT
/// tripwire validating the operand across the soak.
fn check_clone_birth_shadow(
    page_store: &Arc<PageStore>,
    manifest: &Manifest,
    report: &mut VerifyReport,
) -> Result<()> {
    for finding in clone_birth_shadow_findings(page_store, manifest)? {
        report.issues.push(finding);
    }
    Ok(())
}

/// CLI/offline entry for [`clone_birth_shadow_findings`]: open the latest
/// manifest at `path` and return the safety-direction findings (empty == the
/// clone COW-kill clone COW-kill operand is safe for this metadb).
pub fn audit_clone_birth_shadow(path: impl AsRef<Path>) -> Result<Vec<String>> {
    let page_store = Arc::new(PageStore::open(path.as_ref().join("pages.onyx_meta"))?);
    match ManifestStore::load_latest(&page_store)? {
        Some(loaded) => clone_birth_shadow_findings(&page_store, &loaded.manifest),
        None => Err(MetaDbError::Corruption(
            "no valid manifest slot could be decoded".into(),
        )),
    }
}

/// Page-deadlist record audit. For each
/// non-clone volume, read every [`DeadRecord`] across its page-deadlist
/// chains — the live HEAD chain plus each snapshot's sealed chain — and
/// check the two model invariants the `drop_snapshot` free decision rides
/// on:
///
///   1. **birth < death** per record (a page is born strictly before the
///      COW that displaces it; `birth >= death` would corrupt the
///      `snap_pinned` / `birth > S_prev` partition).
///   2. **Disjointness**: each dying page version `(pid, birth, death)` is
///      recorded exactly once across all of the volume's chains. A death
///      flows into the head accumulator once and is sealed into exactly one
///      snapshot, so a duplicate means a double-record (which would
///      double-free at the 2b cutover).
///
/// Both are merge-independent (they hold before and after the not-yet-
/// implemented cross-drop deadlist MERGE). The completeness/coverage check
/// (every snapshot-pinned page lands in exactly one live chain) is deferred
/// until MERGE lands, since an un-merged chain is legitimately incomplete.
/// `walk_dead_list_chain` (in `collect_live_pages`) already covers head-chain
/// structural integrity; the snapshot chains are walked here.
fn check_page_deadlist(
    page_store: &Arc<PageStore>,
    manifest: &Manifest,
    report: &mut VerifyReport,
) -> Result<()> {
    for volume in &manifest.volumes {
        if volume.parent_vol_ord.is_some() {
            continue;
        }
        let vol = volume.ord;
        // (chain label, tail pid) for the HEAD chain + every snapshot chain.
        let mut chains: Vec<(String, PageId)> =
            vec![("head".to_string(), volume.page_dead_list_tail_pid)];
        for snap in manifest.snapshots.iter().filter(|s| s.vol_ord == vol) {
            chains.push((format!("snap#{}", snap.id), snap.page_dead_list_tail_pid));
        }
        // (pid, birth, death) -> chain that first recorded it.
        let mut seen: std::collections::HashMap<(PageId, Lsn, Lsn), String> =
            std::collections::HashMap::new();
        let mut chain_pids: HashSet<PageId> = HashSet::new();
        for (label, tail) in &chains {
            let records =
                crate::deadlist::read_chain_records(*tail, |p| page_store.read_page(p))?;
            for r in records {
                if r.birth_lsn >= r.death_lsn {
                    report.issues.push(format!(
                        "page-deadlist vol {vol} {label}: record pid={} has birth_lsn={} \
                         >= death_lsn={} (birth must precede death)",
                        r.pba, r.birth_lsn, r.death_lsn
                    ));
                }
                chain_pids.insert(r.pba);
                let key = (r.pba, r.birth_lsn, r.death_lsn);
                if let Some(first) = seen.insert(key, label.clone()) {
                    report.issues.push(format!(
                        "page-deadlist vol {vol}: page version pid={} (birth={} death={}) \
                         recorded in both {first} and {label} (double-record)",
                        r.pba, r.birth_lsn, r.death_lsn
                    ));
                }
            }
        }
        // COVERAGE: every L2P page a snapshot still pins but the head has
        // COW'd away (reachable from some snapshot tree, NOT from the head)
        // died off the head while that snapshot pinned it, so it MUST appear
        // in some live page-deadlist chain. A pinned-off-head page missing
        // from `chain_pids` is a completeness hole — exactly what the 2b
        // cutover (deadlist drives the free) would leak. Index + leaf pages
        // both COW, so both are in scope.
        let head_set = reachable_l2p_pages(page_store, &volume.l2p_shard_roots)?;
        let mut pinned: HashSet<PageId> = HashSet::new();
        for snap in manifest.snapshots.iter().filter(|s| s.vol_ord == vol) {
            let roots = snapshot_roots(page_store, snap.l2p_roots_page, &snap.l2p_shard_roots)?;
            pinned.extend(reachable_l2p_pages(page_store, &roots)?);
        }
        for pid in pinned.difference(&head_set) {
            if !chain_pids.contains(pid) {
                report.issues.push(format!(
                    "page-deadlist vol {vol}: page {pid} is snapshot-pinned and off the head \
                     but absent from every deadlist chain (completeness hole)"
                ));
            }
        }
    }
    Ok(())
}

/// Set of every L2P page reachable from `roots` (roots + all descendants),
/// reusing [`walk_paged_tree`]. Membership only — no multiplicity. Exposed
/// `pub(crate)` so the BFG clone-drop livelist shadow
/// (`Db::check_clone_livelist_shadow`) can build its independent
/// C-exclusive reachability oracle from the same walk verify uses.
pub(crate) fn reachable_l2p_pages(
    page_store: &Arc<PageStore>,
    roots: &[PageId],
) -> Result<HashSet<PageId>> {
    let mut live = LivePages::default();
    let mut seen: HashSet<PageId> = HashSet::new();
    for &root in roots {
        if root == NULL_PAGE {
            continue;
        }
        live.mark(root);
        walk_paged_tree(page_store, root, &mut live, &mut seen)?;
    }
    Ok(live.refs.into_keys().collect())
}

/// BFG: the set of data-PBA `head_pba`s mapped by any
/// of `roots` — each a per-shard L2P root, either a surviving volume's current
/// root or a snapshot root. `drop_volume`'s promoted-PBA decref uses this to
/// gate the surface decision: a promoted PBA still reachable from a surviving
/// root MUST NOT be surfaced as freed (a survivor still maps it → PBA-level
/// premature free). Walks the durable pages via `page_store` (callers flush the
/// trees and hold the drop/apply/snapshot_views gates first), so it is
/// consistent with the locked trees WITHOUT indexing a per-shard guard — every
/// `PagedL2p` reads the shared `page_store`, so any root walks correctly (the
/// same property `collect_range_for_roots` / `collect_live_pages` rely on).
/// Reuses the caller's shared `page_cache` (scan-resistant) across all roots so
/// the survivor scan does not allocate a fresh per-root cache.
pub(crate) fn reachable_l2p_head_pbas(
    page_store: &Arc<PageStore>,
    page_cache: &Arc<crate::cache::PageCache>,
    roots: &[PageId],
) -> Result<HashSet<Pba>> {
    let mut pbas: HashSet<Pba> = HashSet::new();
    for &root in roots {
        if root == NULL_PAGE {
            continue;
        }
        let mut tree = PagedL2p::open_with_cache(page_store.clone(), page_cache.clone(), root, 1)?;
        for item in tree.range_at(root, ..)? {
            let (_lba, value) = item?;
            pbas.insert(value.head_pba());
        }
    }
    Ok(pbas)
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

    // v23: the volume catalog + snapshot table live in per-slot chains anchored
    // in each manifest slot's body (not inline in the manifest page anymore).
    // BOTH slots' chains are live (double-buffer), so mark every chain page of
    // both generations — otherwise the non-winning slot's chain is mis-flagged
    // as orphaned and `reclaim_orphan_pages` (run on open) frees it out from
    // under that generation. Pure anchor pages, unreachable via any tree walk.
    for pid in crate::manifest::catalog_chain_pids_all_slots(page_store) {
        live.mark(pid);
    }

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
            true, // PBA chain: strict ordering (single wal_checkpoint bound).
        )?;
        // BFG: the volume's HEAD page-deadlist chain (L2P
        // metadata-page deaths) is a SECOND independent chain. Without
        // marking it, `reclaim_orphan_pages` (run on open) would free its
        // segments out from under the live manifest anchors → page-type
        // corruption on the next walk. Same `DeadListSegment` codec, so
        // the same chain walker applies.
        walk_dead_list_chain(
            page_store,
            volume.page_dead_list_head_pid,
            volume.page_dead_list_tail_pid,
            &mut live,
            false, // H1 page chain: per-shard seal bounds → ordering relaxed to warn.
        )?;
        // BFG: the per-clone page-livelist chain (clone-private
        // page ALLOC/FREE log) is a THIRD independent chain. Mark its segment
        // pages live so `reclaim_orphan_pages`-on-open does not free them out
        // from under the manifest anchors → page-type corruption. Walk by tail
        // (like the snapshot deadlist marking below); `LiveListSegment` codec.
        if volume.page_live_list_tail_pid != NULL_PAGE {
            for pid in crate::livelist::walk_chain_pages(volume.page_live_list_tail_pid, |p| {
                page_store.read_page(p)
            })? {
                live.mark(pid);
            }
        }
        // BFG: the promoted-PBA log is a FOURTH
        // independent per-volume chain (raw PBAs the promotion walker incref'd,
        // `LiveListSegment` codec). Mark its segment pages live so orphan-reclaim
        // does not free the chain on reopen.
        if volume.promoted_log_tail_pid != NULL_PAGE {
            for pid in crate::livelist::walk_chain_pages(volume.promoted_log_tail_pid, |p| {
                page_store.read_page(p)
            })? {
                live.mark(pid);
            }
        }
    }
    for &meta_pid in manifest.refcount_shard_roots.iter() {
        if meta_pid == NULL_PAGE {
            continue;
        }
        // Walker marks every meta page in the chain (head + continuations)
        // via its on_meta callback, matching walk_cuckoo / walk_dedup_reverse.
        walk_refcount_paged_array(page_store, meta_pid, &mut live, &mut seen_btree)?;
    }
    // BFG: the v17 L2P-page-rc shard group is deleted, so there are no
    // page-rc meta chains to mark live here.

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
        // BFG: a snapshot owns the sealed page-deadlist chain
        // it inherited from the head at take time. Only the tail anchor is
        // stored (the chain is immutable, head implicit at
        // `prev_seg_pid == NULL_PAGE`), so walk it to NULL and mark every
        // segment page live — else orphan reclaim frees it on reopen.
        if snapshot.page_dead_list_tail_pid != NULL_PAGE {
            for pid in crate::deadlist::walk_chain_pages(
                snapshot.page_dead_list_tail_pid,
                |p| page_store.read_page(p),
            )? {
                live.mark(pid);
            }
        }
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
    // v25 online resize: during a Growing phase the OLD (frozen) cuckoo table is
    // still live — reads fall through to it and the migration walker drains it —
    // but it is anchored by `dedup_migration_old_head`, NOT the primary head
    // above. Walk it too, or orphan reclaim on open would free the OLD table's
    // meta + data pages and the allocator would re-hand them out, clobbering
    // still-referenced dedup entries (silent data loss).
    if manifest.dedup_migration_old_head != NULL_PAGE {
        walk_cuckoo_dedup_index(page_store, manifest.dedup_migration_old_head, &mut live)?;
    }
    Ok(live)
}

pub(crate) fn snapshot_roots<'a>(
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
    // H1: `strict` enforces the "max_lsn strictly older going backward"
    // cross-segment ordering as a HARD Corruption. The PBA chain keeps it
    // (true); the page chain relaxes it to a warn (false) because per-shard
    // seal bounds (`root_birth_lsn(shard)`) make adjacent segments' LSN ranges
    // legitimately overlap across shards. The invariant is NON-load-bearing for
    // the free/merge logic (`read_chain_records` flattens; `dl_record_freed` is
    // a pure per-record predicate; the merge re-sorts) — kept only as a
    // tripwire that has historically caught orphaned-tail / broken-prev bugs.
    strict: bool,
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
                if strict {
                    return Err(MetaDbError::Corruption(format!(
                        "dead-list segment at {cur} max_lsn={} >= next segment min_lsn={prev_min} (chain must be strictly older going backward)",
                        seg.max_lsn
                    )));
                }
                // H1: page chain — per-shard seal bounds make this overlap
                // legitimate; warn (keep the tripwire visible) and continue.
                tracing::warn!(
                    page = cur,
                    max_lsn = seg.max_lsn,
                    next_min_lsn = prev_min,
                    "page-deadlist segment LSN range overlaps the next segment (expected under per-shard seal; not corruption)"
                );
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
