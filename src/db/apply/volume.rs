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
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let n = validate_shard_count(shard_count)?;
    create_l2p_shards(
        page_store.clone(),
        page_cache.clone(),
        n,
        metrics,
        use_buffer,
    )
}

/// Apply a `DropVolume` op's page-decref cascade. Reuses
/// [`apply_drop_snapshot_pages`]; `DropVolume` has the same
/// per-page semantics (decref, free at rc=0, idempotent via
/// `page.generation >= lsn`) and just doesn't need the freed-leaf-values
/// vec the snapshot path surfaces in its report.
pub(in crate::db) fn apply_drop_volume(
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    pages: &[PageId],
) -> Result<usize> {
    let (_leaf_values, pages_freed) = apply_drop_snapshot_pages(page_store, lsn, pages)?;
    Ok(pages_freed)
}

/// Increment the on-disk refcount of each shard root that a cloned
/// volume pins. Idempotent across replays: pages already stamped with
/// `page.generation >= lsn` are skipped (same guard pattern
/// [`apply_drop_snapshot_pages`] uses). `NULL_PAGE` roots — empty
/// source shards — are ignored because the clone materialises fresh
/// empty trees for those shards (see [`build_clone_volume_shards`]).
pub(in crate::db) fn apply_clone_volume_incref(
    page_store: &Arc<PageStore>,
    faults: &FaultController,
    lsn: Lsn,
    src_shard_roots: &[PageId],
) -> Result<()> {
    for (idx, &pid) in src_shard_roots.iter().enumerate() {
        if pid == crate::types::NULL_PAGE {
            continue;
        }
        let mut page = page_store.read_page_unchecked(pid)?;
        page.verify(pid)?;
        let header = page.header()?;
        if header.generation >= lsn {
            // Already incref'd by a prior apply of this same CloneVolume
            // op (replay-after-crash case); skip.
            continue;
        }
        let new_rc = header.refcount.checked_add(1).ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "clone_volume: refcount overflow on source root page {pid}"
            ))
        })?;
        page.set_refcount(new_rc);
        page.set_generation(lsn);
        page.seal();
        page_store.write_page(pid, &page)?;
        // Fault injection window: fires after the first root is durably
        // incref'd but before subsequent ones. Recovery's generation-stamp
        // guard skips the pre-fault root and completes the rest.
        if idx == 0 {
            faults.inject(FaultPoint::CloneVolumeMidIncref)?;
        }
    }
    page_store.sync()?;
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
    created_lsn: Lsn,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(src_shard_roots.len());
    let mut actual_roots = Vec::with_capacity(src_shard_roots.len());
    for (shard_idx, &root) in src_shard_roots.iter().enumerate() {
        let tree = if root == crate::types::NULL_PAGE {
            PagedL2p::create_with_cache(page_store.clone(), page_cache.clone())?
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
    lsn: Lsn,
    pages: &[PageId],
) -> Result<(Vec<L2pValue>, usize)> {
    use crate::page::PageType;

    let mut freed_leaf_values: Vec<L2pValue> = Vec::new();
    let mut pages_freed: usize = 0;

    for &pid in pages {
        let mut page = match page_store.read_page_unchecked(pid) {
            Ok(page) => page,
            Err(MetaDbError::PageOutOfRange(out_of_range)) if out_of_range == pid => {
                // A prior replay may already have freed a tail page and
                // `PageStore::open` may then have truncated that zero/Free
                // suffix below this pid. Replaying the same DropSnapshot /
                // DropVolume WAL record is still idempotent: the page is
                // gone because this very cascade already completed.
                pages_freed += 1;
                continue;
            }
            Err(err) => return Err(err),
        };
        if page.bytes().iter().all(|b| *b == 0) {
            // `free_idempotent` punches freed pages after writing the
            // Free header. On reopen, a page already processed by a
            // crashed prior apply can therefore read back as all zeroes
            // rather than as a decodable Free page.
            pages_freed += 1;
            continue;
        }
        // verify before we do anything; `read_page_unchecked` skipped it
        page.verify(pid)?;
        let header = page.header()?;
        if header.generation >= lsn {
            // already processed by a prior apply of this same DropSnapshot
            // (replay-after-crash case); skip to keep the overall apply
            // idempotent.
            if header.page_type == PageType::Free {
                // count it so the outcome is stable across replays — but
                // skip the write.
                pages_freed += 1;
            }
            continue;
        }
        let rc = header.refcount;
        if rc == 0 {
            return Err(MetaDbError::Corruption(format!(
                "DropSnapshot apply: page {pid} already at refcount 0"
            )));
        }
        let new_rc = rc - 1;
        if new_rc == 0 {
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
        } else {
            page.set_refcount(new_rc);
            page.set_generation(lsn);
            page.seal();
            page_store.write_page(pid, &page)?;
        }
    }

    page_store.sync()?;

    Ok((freed_leaf_values, pages_freed))
}

/// Apply a full `LifecycleOp::DropSnapshot`: the page-refcount cascade for
/// `pages` followed by the SPEC §3.3 leaf-rc-suppress compensation —
/// one `decref(pba, 1)` per entry in `pba_decrefs`, collected at plan
/// time via `diff_with_current`.
///
/// `pba_decrefs` is walked in shard-sorted order; each shard mutex is
/// taken once, mirroring `apply_l2p_range_delete`'s pattern. The
/// returned `freed_pbas` lists every pba whose refcount transitioned
/// from `>0` to `0` during this apply. Duplicates in `pba_decrefs`
/// are intentional (packed-slot many-LBA-share-one-pba case) and each
/// produces one decref; only the one that drives rc to 0 adds the pba
/// to `freed_pbas`.
///
/// No leaf-rc-suppress logic here: `drop_snapshot` holds
/// `drop_gate.write()` + `apply_gate.write()`, so there is no
/// concurrent mutation, and "suppress because a live snapshot still
/// pins it" cannot apply — we *are* dropping the snapshot.
pub(in crate::db) fn apply_drop_snapshot_pages_and_decrefs(
    page_store: &Arc<PageStore>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    pages: &[PageId],
    pba_decrefs: &[Pba],
) -> Result<ApplyOutcome> {
    let (freed_leaf_values, pages_freed) = apply_drop_snapshot_pages(page_store, lsn, pages)?;

    let mut rc_bucket: Vec<Vec<usize>> = vec![Vec::new(); refcount_shards.len()];
    for (idx, &pba) in pba_decrefs.iter().enumerate() {
        rc_bucket[shard_for_key(refcount_shards, pba)].push(idx);
    }

    let mut freed_pbas: Vec<Pba> = Vec::new();
    for (sid, indices) in rc_bucket.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let shard = &refcount_shards[sid];
        for &idx in indices {
            let pba = pba_decrefs[idx];
            let (pre, new) = shard.rc.stage(pba, -1, lsn)?;
            if new == 0 && pre > 0 {
                freed_pbas.push(pba);
            }
        }
    }

    Ok(ApplyOutcome::DropSnapshot {
        freed_leaf_values,
        pages_freed,
        freed_pbas,
    })
}
