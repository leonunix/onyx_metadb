use super::*;

/// Ensure a loaded manifest carries v4 metadata: any snapshot missing a
/// Allocate and write one snapshot-roots page.
pub(crate) fn write_snapshot_roots_page(
    page_store: &PageStore,
    roots: &[PageId],
    generation: Lsn,
) -> Result<PageId> {
    if roots.is_empty() {
        return Err(MetaDbError::InvalidArgument(
            "snapshot root vector cannot be empty".into(),
        ));
    }
    if roots.len() > MAX_SHARD_ROOTS_PER_PAGE {
        return Err(MetaDbError::InvalidArgument(format!(
            "snapshot root count {} exceeds page capacity {}",
            roots.len(),
            MAX_SHARD_ROOTS_PER_PAGE,
        )));
    }
    let pid = page_store.allocate()?;
    let mut page = Page::new(PageHeader::new(PageType::SnapshotRoots, generation));
    page.set_key_count(roots.len() as u16);
    let p = page.payload_mut();
    for (i, root) in roots.iter().copied().enumerate() {
        let off = i * 8;
        p[off..off + 8].copy_from_slice(&root.to_le_bytes());
    }
    page.seal();
    page_store.write_page(pid, &page)?;
    Ok(pid)
}

/// Load the shard-root vector stored in one snapshot-roots page.
pub(crate) fn load_snapshot_roots(page_store: &PageStore, pid: PageId) -> Result<Box<[PageId]>> {
    if pid == NULL_PAGE {
        return Err(MetaDbError::Corruption(
            "snapshot roots_page cannot be NULL_PAGE".into(),
        ));
    }
    let page = page_store.read_page(pid)?;
    let header = page.header()?;
    if header.page_type != PageType::SnapshotRoots {
        return Err(MetaDbError::Corruption(format!(
            "page {pid} is not a SnapshotRoots page: {:?}",
            header.page_type,
        )));
    }
    let count = header.key_count as usize;
    if count > MAX_SHARD_ROOTS_PER_PAGE {
        return Err(MetaDbError::Corruption(format!(
            "snapshot roots page {pid} declares {} roots, exceeds {}",
            count, MAX_SHARD_ROOTS_PER_PAGE,
        )));
    }
    let p = page.payload();
    let mut roots = Vec::with_capacity(count);
    for i in 0..count {
        let off = i * 8;
        roots.push(u64::from_le_bytes(p[off..off + 8].try_into().unwrap()));
    }
    Ok(roots.into_boxed_slice())
}
