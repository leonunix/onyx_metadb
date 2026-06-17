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
    page_rc: &Arc<crate::l2p_page_rc::L2pPageRc>,
    shard_count: u32,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
    // A3: the volume's `created_lsn` (the CreateVolume op's lsn, strictly
    // below the volume's first write op) — stamps each shard root's
    // page-rc +1 so the fold applies it without colliding a later op.
    created_lsn: Lsn,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let n = validate_shard_count(shard_count)?;
    create_l2p_shards(
        page_store.clone(),
        page_cache.clone(),
        page_rc.clone(),
        n,
        metrics,
        use_buffer,
        created_lsn,
    )
}

/// Apply a `DropVolume` op's page-decref cascade. Reuses
/// [`apply_drop_snapshot_pages`]; `DropVolume` has the same
/// per-page semantics (decref, free at rc=0, idempotent via
/// `page.generation >= lsn`) and just doesn't need the freed-leaf-values
/// vec the snapshot path surfaces in its report.
pub(in crate::db) fn apply_drop_volume(
    page_store: &Arc<PageStore>,
    page_rc: &Arc<crate::l2p_page_rc::L2pPageRc>,
    lsn: Lsn,
    txg: crate::types::Txg,
    pages: &[PageId],
) -> Result<usize> {
    let (_leaf_values, pages_freed) =
        apply_drop_snapshot_pages(page_store, page_rc, lsn, txg, pages)?;
    Ok(pages_freed)
}

/// Increment the on-disk refcount of each shard root that a cloned
/// volume pins. Idempotent across replays: pages already stamped with
/// `page.generation >= lsn` are skipped (same guard pattern
/// [`apply_drop_snapshot_pages`] uses). `NULL_PAGE` roots — empty
/// source shards — are ignored because the clone materialises fresh
/// empty trees for those shards (see [`build_clone_volume_shards`]).
pub(in crate::db) fn apply_clone_volume_incref(
    page_rc: &Arc<crate::l2p_page_rc::L2pPageRc>,
    faults: &FaultController,
    lsn: Lsn,
    txg: crate::types::Txg,
    src_shard_roots: &[PageId],
) -> Result<()> {
    for (idx, &pid) in src_shard_roots.iter().enumerate() {
        if pid == crate::types::NULL_PAGE {
            continue;
        }
        // A3 cutover: incref the page-rc array, not the page header. The
        // page bytes are untouched (no whole-page rc write to be clobbered
        // by a concurrent flush — which is the coupling this whole project
        // removes). `stage`'s `page_lsn >= lsn` replay-skip gives the same
        // idempotency the old `header.generation >= lsn` guard gave. The
        // staged delta rides the next checkpoint's TXG fold (clone_volume
        // commits no manifest itself) or a recovery replay.
        page_rc.stage(txg, pid, 1, lsn)?;
        // Fault injection window: fires after the first root's incref is
        // staged but before subsequent ones. Recovery's replay-skip guard
        // skips the pre-fault root and completes the rest.
        if idx == 0 {
            faults.inject(FaultPoint::CloneVolumeMidIncref)?;
        }
    }
    Ok(())
}

/// Build the new volume's shard group for a clone. Each source root
/// becomes the initial root of a fresh [`PagedL2p`]; empty source
/// shards (`NULL_PAGE` root) get a freshly-allocated empty leaf so the
/// tree is always operable. Caller must have already incref'd the
/// non-null roots via [`apply_clone_volume_incref`].
pub(in crate::db) fn build_clone_volume_shards(
    src_shard_roots: &[PageId],
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    page_rc: &Arc<crate::l2p_page_rc::L2pPageRc>,
    created_lsn: Lsn,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(src_shard_roots.len());
    let mut actual_roots = Vec::with_capacity(src_shard_roots.len());
    for (shard_idx, &root) in src_shard_roots.iter().enumerate() {
        // Clone shards MUST share the one global page-rc store (not a
        // private one), so the clone's COW page-rc deltas land in the
        // same array the source volume uses. Hence the `_rc` ctors.
        let tree = if root == crate::types::NULL_PAGE {
            PagedL2p::create_with_cache_rc(
                page_store.clone(),
                page_cache.clone(),
                page_rc.clone(),
                created_lsn,
            )?
        } else {
            PagedL2p::open_with_cache_rc(
                page_store.clone(),
                page_cache.clone(),
                page_rc.clone(),
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

/// Core of the `DropSnapshot` / `DropVolume` page-refcount cascade.
/// Iterates `pages`, decrements each page's refcount by 1, stamps
/// `generation = lsn`, and frees any page that hits rc=0. Idempotent
/// on replay via the generation check.
///
/// Returns `(freed_leaf_values, pages_freed)` rather than a full
/// `ApplyOutcome` so `DropVolume` (which doesn't care about leaf values)
/// can reuse the function without unpacking a misnamed variant.
pub(in crate::db) fn apply_drop_snapshot_pages(
    page_store: &Arc<PageStore>,
    page_rc: &Arc<crate::l2p_page_rc::L2pPageRc>,
    lsn: Lsn,
    txg: crate::types::Txg,
    pages: &[PageId],
) -> Result<(Vec<L2pValue>, usize)> {
    use crate::page::PageType;

    let mut freed_leaf_values: Vec<L2pValue> = Vec::new();
    let mut pages_freed: usize = 0;

    for &pid in pages {
        // A3 cutover: decref the page-rc array, not the page header.
        // Replay idempotency comes from `stage`'s `page_lsn >= lsn` skip
        // (the analogue of the old `header.generation >= lsn` guard): a
        // replayed decref whose array page is already folded at/after
        // `lsn` returns `(prev, prev)` with no change, so it neither
        // double-decrements nor re-frees.
        let (prev, new) = page_rc.stage(txg, pid, -1, lsn)?;
        if new != 0 || prev == 0 {
            // Either still referenced after the decref (new > 0), or the
            // replay-skip fired (prev == new == 0, already applied). The
            // page rc lives in the array now, so a non-freeing decref
            // writes nothing — there is no header to rewrite.
            continue;
        }
        // 1→0 transition. Confirm with a fold-consistent read before the
        // IRREVERSIBLE free (R2): the cheap staged `new` can straddle a
        // concurrent fold's [publish, clear] window and floor a live rc to
        // a spurious 0. L2P page frees are immediate (no GC gate re-check),
        // so this is the only guard.
        if page_rc.get_consistent(pid)? != 0 {
            continue;
        }
        // rc==0 confirmed: read the page to harvest leaf values, then
        // free it idempotently. The page bytes still carry the now-dead
        // header rc — left untouched; verify reads against the array.
        let page = match page_store.read_page_unchecked(pid) {
            Ok(page) => page,
            Err(MetaDbError::PageOutOfRange(out_of_range)) if out_of_range == pid => {
                // A prior replay may already have freed a tail page and
                // `PageStore::open` truncated the zero/Free suffix below
                // this pid; the page is gone because this cascade already
                // completed (the replay-skip above did not catch it only
                // because the array page was not yet folded pre-crash).
                pages_freed += 1;
                continue;
            }
            Err(err) => return Err(err),
        };
        if page.bytes().iter().all(|b| *b == 0) {
            // `free_idempotent` punches freed pages; a page already freed
            // by a crashed prior apply reads back as all zeroes.
            pages_freed += 1;
            continue;
        }
        page.verify(pid)?;
        let header = page.header()?;
        if header.page_type == PageType::Free {
            // Already freed by a crashed prior apply of this op.
            pages_freed += 1;
            continue;
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
        let pushed = page_store.free_idempotent(pid, lsn)?;
        if pushed {
            pages_freed += 1;
        }
    }

    page_store.sync()?;

    Ok((freed_leaf_values, pages_freed))
}

/// Apply a full `LifecycleOp::DropSnapshot`: the page-refcount cascade for
/// `pages`. Phase 5 ignores `pba_decrefs`: global PBA rc is not a
/// per-live-LBA counter, so dropping a snapshot must not subtract one PBA
/// rc entry per logical LBA in the snapshot diff.
pub(in crate::db) fn apply_drop_snapshot_pages_and_decrefs(
    page_store: &Arc<PageStore>,
    refcount_shards: &[Shard],
    page_rc: &Arc<crate::l2p_page_rc::L2pPageRc>,
    lsn: Lsn,
    txg: crate::types::Txg,
    pages: &[PageId],
    pba_decrefs: &[Pba],
) -> Result<ApplyOutcome> {
    let (freed_leaf_values, pages_freed) =
        apply_drop_snapshot_pages(page_store, page_rc, lsn, txg, pages)?;
    let _ = refcount_shards;
    let _ = pba_decrefs;

    Ok(ApplyOutcome::DropSnapshot {
        freed_leaf_values,
        pages_freed,
        freed_pbas: Vec::new(),
    })
}
