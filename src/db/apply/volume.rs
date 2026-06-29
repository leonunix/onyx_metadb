use super::*;

pub(in crate::db) fn collect_paged_refcounts_for_roots(
    page_store: &Arc<PageStore>,
    roots: &[PageId],
) -> Result<BTreeMap<PageId, u32>> {
    fn walk(
        page_store: &PageStore,
        pid: PageId,
        refs: &mut BTreeMap<PageId, u32>,
        seen: &mut HashSet<PageId>,
    ) -> Result<()> {
        if !seen.insert(pid) {
            return Ok(());
        }
        let page = page_store.read_page(pid)?;
        match page.header()?.page_type {
            PageType::PagedLeaf => Ok(()),
            PageType::PagedIndex => {
                for slot in 0..crate::paged::format::INDEX_FANOUT {
                    let child = crate::paged::format::index_child_at(&page, slot);
                    if child == crate::types::NULL_PAGE {
                        continue;
                    }
                    *refs.entry(child).or_insert(0) += 1;
                    walk(page_store, child, refs, seen)?;
                }
                Ok(())
            }
            other => Err(MetaDbError::Corruption(format!(
                "page {pid} has unexpected type {other:?} in paged refcount walk"
            ))),
        }
    }

    let mut refs = BTreeMap::new();
    let mut seen = HashSet::new();
    for &root in roots {
        if root == crate::types::NULL_PAGE {
            continue;
        }
        *refs.entry(root).or_insert(0) += 1;
        walk(page_store, root, &mut refs, &mut seen)?;
    }
    Ok(refs)
}

/// Allocate a fresh shard group for a `CreateVolume` apply. Delegates
/// to [`create_l2p_shards`]; kept separate so the Db public API and
/// the recovery replay closure share one call site.
pub(in crate::db) fn apply_create_volume(
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    shard_count: u32,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
    created_lsn: Lsn,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let n = validate_shard_count(shard_count)?;
    create_l2p_shards(
        page_store.clone(),
        page_cache.clone(),
        n,
        metrics,
        use_buffer,
        created_lsn,
    )
}

/// Apply a `DropVolume` op's page free. Reuses [`apply_drop_snapshot_pages`];
/// `DropVolume` has the same per-page semantics and just doesn't need the
/// freed-leaf-values vec the snapshot path surfaces in its report. `free_pages`
/// is the explicit reachability free-set (always `Some`; both CLONE_LINEAGE and
/// non-clone drops freeze it); see the core for the contract.
pub(in crate::db) fn apply_drop_volume(
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    pages: &[PageId],
    free_pages: Option<&[PageId]>,
) -> Result<usize> {
    let (_leaf_values, pages_freed) =
        apply_drop_snapshot_pages(page_store, lsn, pages, free_pages)?;
    Ok(pages_freed)
}

/// Build the new volume's shard group for a clone. Each source root
/// becomes the initial root of a fresh [`PagedL2p`]; empty source
/// shards (`NULL_PAGE` root) get a freshly-allocated empty leaf so the
/// tree is always operable.
pub(in crate::db) fn build_clone_volume_shards(
    src_shard_roots: &[PageId],
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    created_lsn: Lsn,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(src_shard_roots.len());
    let mut actual_roots = Vec::with_capacity(src_shard_roots.len());
    for (shard_idx, &root) in src_shard_roots.iter().enumerate() {
        let tree = if root == crate::types::NULL_PAGE {
            PagedL2p::create_with_cache(page_store.clone(), page_cache.clone(), created_lsn)?
        } else {
            PagedL2p::open_with_cache(
                page_store.clone(),
                page_cache.clone(),
                root,
                created_lsn + 1,
            )?
        };
        actual_roots.push(tree.root());
        shards.push(super::helpers::make_l2p_shard(
            tree,
            page_cache,
            shard_idx,
            metrics.clone(),
            // Cloned volume has no on-disk dirty pages of its own
            // yet; its shards' content is durable for every LSN at
            // or before `created_lsn` (because the volume didn't
            // exist before). Setting `last_flushed_lsn = created_lsn`
            // keeps `Db::compute_min_last_flushed_lsn` from being
            // dragged down to 0 by a fresh volume that hasn't been
            // flushed yet. Subsequent partial flushes bump it as
            // usual.
            created_lsn,
            use_buffer,
        ));
    }
    Ok((shards, actual_roots.into_boxed_slice()))
}

/// Read page `pid`, harvest its leaf values (if a `PagedLeaf`), and free
/// it idempotently. Shared by both `apply_drop_snapshot_pages` branches.
/// Crash/replay-safe: an already-freed page (truncated tail, all-zeroes,
/// or `PageType::Free` header) counts toward `pages_freed` without a
/// second free. `free_idempotent` stamps `generation = lsn`.
fn harvest_and_free(
    page_store: &Arc<PageStore>,
    pid: PageId,
    lsn: Lsn,
    freed_leaf_values: &mut Vec<L2pValue>,
    pages_freed: &mut usize,
) -> Result<()> {
    use crate::page::PageType;
    // The page bytes still carry the now-dead header rc — left untouched;
    // verify reads against the array.
    let page = match page_store.read_page_unchecked(pid) {
        Ok(page) => page,
        Err(MetaDbError::PageOutOfRange(out_of_range)) if out_of_range == pid => {
            // A prior replay may already have freed a tail page and
            // `PageStore::open` truncated the zero/Free suffix below this
            // pid; the page is gone because this drop already completed.
            *pages_freed += 1;
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    if page.bytes().iter().all(|b| *b == 0) {
        // `free_idempotent` punches freed pages; a page already freed by a
        // crashed prior apply reads back as all zeroes.
        *pages_freed += 1;
        return Ok(());
    }
    page.verify(pid)?;
    let header = page.header()?;
    if header.page_type == PageType::Free {
        // Already freed by a crashed prior apply of this op.
        *pages_freed += 1;
        return Ok(());
    }
    if matches!(header.page_type, PageType::PagedLeaf) {
        for i in 0..crate::paged::format::LEAF_ENTRY_COUNT {
            if crate::paged::format::leaf_bit_set(&page, i) {
                if let Some(value) = crate::paged::format::leaf_value_at(&page, i)? {
                    freed_leaf_values.push(value);
                }
            }
        }
    }
    // free_idempotent stamps generation=lsn via the Free header.
    if page_store.free_idempotent(pid, lsn)? {
        *pages_freed += 1;
    }
    Ok(())
}

/// Core of the `DropSnapshot` / `DropVolume` page free (BFG).
///
/// Frees exactly `free_pages` — the explicit, page-rc-INDEPENDENT free-set the
/// producer froze under the held gates and a HARD shadow validated before the
/// WAL submit (deadlist for non-clone snapshots; reachability `exclusive` for
/// CLONE_LINEAGE / non-clone volumes + clone-involved snapshots). `pages` (the
/// `collect_drop_pages_with_birth` cascade frontier) is retained only for the
/// `free_pages ⊆ pages` subset assertion. The legacy page-rc cascade + its R2
/// fold-consistent `get_consistent` guard are DELETED — page-rc no longer
/// exists, so the free decision can never read it.
///
/// Returns `(freed_leaf_values, pages_freed)`.
pub(in crate::db) fn apply_drop_snapshot_pages(
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    pages: &[PageId],
    free_pages: Option<&[PageId]>,
) -> Result<(Vec<L2pValue>, usize)> {
    let mut freed_leaf_values: Vec<L2pValue> = Vec::new();
    let mut pages_freed: usize = 0;

    // BFG: page-rc is deleted. Every drop op now freezes an
    // explicit, page-rc-INDEPENDENT free-set (deadlist for non-clone snapshots,
    // reachability `exclusive` for CLONE_LINEAGE / non-clone volumes + clone-
    // involved snapshots — all proven HARD by the page-rc-independent shadows
    // before the WAL submit). The legacy `None` page-rc cascade is gone; a None
    // here can only be a corrupt legacy op (impossible on a fresh-metadb v22
    // rebuild).
    let set = free_pages.ok_or_else(|| {
        MetaDbError::Corruption(
            "DropVolume/DropSnapshot must carry an explicit free_pages set \
             (page-rc cascade deleted)"
                .to_string(),
        )
    })?;
    // Invariant: the frozen free-set is a subset of the cascade-frontier
    // `pages` (`collect_drop_pages_with_birth`); the shadows prove
    // `set == structural_free`, and structural_free is filtered from `pages`.
    // Asserted at the apply boundary (which replay also re-enters) so a future
    // producer bug trips in test.
    debug_assert!(
        {
            let frontier: std::collections::HashSet<PageId> = pages.iter().copied().collect();
            set.iter().all(|p| frontier.contains(p))
        },
        "free_pages must be a subset of the cascade `pages`"
    );
    for &pid in set {
        harvest_and_free(
            page_store,
            pid,
            lsn,
            &mut freed_leaf_values,
            &mut pages_freed,
        )?;
    }

    page_store.sync()?;

    Ok((freed_leaf_values, pages_freed))
}

/// Apply a full `LifecycleOp::DropSnapshot`: free the op's explicit
/// `free_pages` set. ignores `pba_decrefs`: global PBA rc is not a
/// per-live-LBA counter, so dropping a snapshot must not subtract one PBA
/// rc entry per logical LBA in the snapshot diff.
pub(in crate::db) fn apply_drop_snapshot_pages_and_decrefs(
    page_store: &Arc<PageStore>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    pages: &[PageId],
    pba_decrefs: &[Pba],
    free_pages: Option<&[PageId]>,
) -> Result<ApplyOutcome> {
    let (freed_leaf_values, pages_freed) =
        apply_drop_snapshot_pages(page_store, lsn, pages, free_pages)?;
    let _ = refcount_shards;
    let _ = pba_decrefs;

    Ok(ApplyOutcome::DropSnapshot {
        freed_leaf_values,
        pages_freed,
        freed_pbas: Vec::new(),
    })
}
