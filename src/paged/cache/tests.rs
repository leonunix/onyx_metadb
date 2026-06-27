use super::*;
use crate::config::PAGE_SIZE;
use crate::paged::format::{
    L2pValue, index_set_child, leaf_bit_set, leaf_set, leaf_value_at,
};
use tempfile::TempDir;

fn mk_store() -> (TempDir, Arc<PageStore>) {
    let dir = TempDir::new().unwrap();
    let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
    (dir, ps)
}

fn v(byte: u8) -> L2pValue {
    // v3 compact encoder requires unit_original_size = lba_count * 4096
    // so it can drop on-disk lba_count and recover it on decode.
    // The per-LBA seq trailer, the per-PBA birth_lsn (v4), and the
    // per-PBA base_pba (v5) must fit a u32 delta off the leaf's
    // bases, so use `byte` as a small u64 for each.
    let mut x = [byte; crate::paged::format::LEAF_VALUE_SIZE];
    x[0..8].copy_from_slice(&(byte as u64).to_be_bytes());
    x[13..17].copy_from_slice(&4096u32.to_be_bytes());
    x[17..19].copy_from_slice(&1u16.to_be_bytes());
    x[28..36].copy_from_slice(&(byte as u64).to_be_bytes());
    x[36..44].copy_from_slice(&(byte as u64).to_be_bytes());
    L2pValue(x)
}

#[test]
fn alloc_leaf_and_index_tagged_correctly() {
    let (_d, ps) = mk_store();
    let mut buf = PageBuf::new(ps);
    let leaf = buf.alloc_leaf(1).unwrap();
    let idx = buf.alloc_index(1, 2).unwrap();
    assert_eq!(buf.read_level(leaf).unwrap(), 0);
    assert_eq!(buf.read_level(idx).unwrap(), 2);
}

#[test]
fn flush_persists_leaf_content() {
    let (_d, ps) = mk_store();
    let mut buf = PageBuf::new(ps.clone());
    let pid = buf.alloc_leaf(1).unwrap();
    let v = v(0x42);
    leaf_set(buf.modify(pid, 1).unwrap(), 5, &v).unwrap();
    buf.flush().unwrap();

    let mut buf2 = PageBuf::new(ps);
    let p = buf2.read(pid).unwrap();
    assert!(leaf_bit_set(p, 5));
}

#[test]
fn install_flushed_snapshot_page_detaches_dirty_copy() {
    let (_d, ps) = mk_store();
    let mut buf = PageBuf::new(ps.clone());
    let pid = buf.alloc_leaf(1).unwrap();
    let v = v(0x42);
    leaf_set(buf.modify(pid, 1).unwrap(), 5, &v).unwrap();

    let flushed = buf.dirty_snapshot().write().unwrap();
    let mut sealed_pages = Vec::new();
    flushed.append_sealed_pages(&mut sealed_pages);
    ps.write_sealed_page_runs(sealed_pages).unwrap();
    ps.sync().unwrap();
    assert_eq!(buf.dirty_count(), 1);
    assert_eq!(
        buf.install_flushed_snapshot_page(&flushed, 0),
        Some((pid, true))
    );

    assert!(!buf.contains(pid));
    assert_eq!(buf.dirty_count(), 0);
    let page = buf.read(pid).unwrap();
    assert!(leaf_bit_set(page, 5));
}

#[test]
fn install_flushed_snapshot_page_keeps_newer_dirty_copy() {
    let (_d, ps) = mk_store();
    let mut buf = PageBuf::new(ps);
    let pid = buf.alloc_leaf(1).unwrap();
    let first = v(0x42);
    let second = v(0x24);
    leaf_set(buf.modify(pid, 1).unwrap(), 5, &first).unwrap();

    let snapshot = buf.dirty_snapshot();
    leaf_set(buf.modify(pid, 2).unwrap(), 6, &second).unwrap();
    let flushed = snapshot.write().unwrap();

    assert_eq!(
        buf.install_flushed_snapshot_page(&flushed, 0),
        Some((pid, false))
    );
    assert!(buf.contains(pid));
    assert_eq!(buf.dirty_count(), 1);
    let page = buf.read(pid).unwrap();
    assert_eq!(leaf_value_at(page, 5).unwrap(), Some(first));
    assert_eq!(leaf_value_at(page, 6).unwrap(), Some(second));
}

#[test]
fn page_store_reclaim_requires_cache_invalidation_before_reuse() {
    let (_d, ps) = mk_store();
    let page_cache = Arc::new(PageCache::new(ps.clone(), DEFAULT_PAGE_CACHE_BYTES));
    let mut buf = PageBuf::with_cache(ps.clone(), page_cache.clone());
    let pid = buf.alloc_leaf(1).unwrap();
    let old = v(0x11);
    leaf_set(buf.modify(pid, 1).unwrap(), 7, &old).unwrap();
    buf.flush().unwrap();
    assert_eq!(leaf_value_at(buf.read(pid).unwrap(), 7).unwrap(), Some(old));

    ps.free(pid, 2).unwrap();
    ps.try_reclaim().unwrap();

    let reused = ps.allocate().unwrap();
    assert_eq!(reused, pid);
    let mut page = Page::zeroed();
    init_leaf(&mut page, 3);
    let new = v(0x22);
    leaf_set(&mut page, 7, &new).unwrap();
    page.seal();
    ps.write_page(reused, &page).unwrap();

    let mut stale_buf = PageBuf::with_cache(ps.clone(), page_cache.clone());
    assert_eq!(
        leaf_value_at(stale_buf.read(reused).unwrap(), 7).unwrap(),
        Some(old),
        "PageStore-only reclaim leaves the shared cache stale"
    );

    page_cache.invalidate(reused);
    let mut fresh_buf = PageBuf::with_cache(ps, page_cache);
    assert_eq!(
        leaf_value_at(fresh_buf.read(reused).unwrap(), 7).unwrap(),
        Some(new)
    );
}

#[test]
fn alloc_leaf_invalidates_reused_pinned_index_page() {
    let (_d, ps) = mk_store();
    let page_cache = Arc::new(PageCache::new_with_pin_budget(
        ps.clone(),
        DEFAULT_PAGE_CACHE_BYTES,
        PAGE_SIZE as u64,
    ));
    let mut buf = PageBuf::with_cache(ps.clone(), page_cache.clone());
    let idx = buf.alloc_index(1, 1).unwrap();
    buf.flush().unwrap();
    assert_eq!(buf.read_level(idx).unwrap(), 1);
    assert_eq!(page_cache.pinned_pages(), 1);

    ps.free(idx, 2).unwrap();
    ps.try_reclaim().unwrap();
    buf.alloc_pool.push(idx);

    let reused = buf.alloc_leaf(3).unwrap();
    assert_eq!(reused, idx);
    assert_eq!(
        page_cache.pinned_pages(),
        0,
        "fresh allocation must evict the stale pinned index incarnation"
    );
    assert_eq!(buf.read_level(reused).unwrap(), 0);
    let v = v(0x33);
    leaf_set(buf.modify(reused, 3).unwrap(), 7, &v).unwrap();
    buf.flush().unwrap();

    // A fresh PageBuf reopen reads the leaf straight from disk. (Per-L2P-page
    // refcounting was deleted, so there is no longer a private page-rc store
    // to clobber the leaf via the `alloc_pool.push` free-list hack.)
    let mut fresh = PageBuf::with_cache(ps, page_cache);
    assert_eq!(fresh.read_level(reused).unwrap(), 0);
    assert_eq!(
        leaf_value_at(fresh.read(reused).unwrap(), 7).unwrap(),
        Some(v)
    );
}

#[test]
fn checkpoint_install_invalidates_stale_leaf_cache_entry() {
    let (_d, ps) = mk_store();
    let page_cache = Arc::new(PageCache::new(ps.clone(), DEFAULT_PAGE_CACHE_BYTES));
    let mut old = Page::zeroed();
    init_index(&mut old, 1, 1);
    old.seal();
    ps.write_page(1, &old).unwrap();
    page_cache.insert(1, Arc::new(old));

    let mut buf = PageBuf::with_cache(ps, page_cache.clone());
    buf.alloc_pool.push(1);
    let leaf = buf.alloc_leaf(2).unwrap();
    assert_eq!(leaf, 1);
    let v = v(0x44);
    leaf_set(buf.modify(leaf, 2).unwrap(), 9, &v).unwrap();

    let flushed = buf.dirty_snapshot().write().unwrap();
    let mut sealed_pages = Vec::new();
    flushed.append_sealed_pages(&mut sealed_pages);
    buf.page_store.write_sealed_page_runs(sealed_pages).unwrap();
    buf.page_store.sync().unwrap();
    assert_eq!(
        buf.install_flushed_snapshot_page(&flushed, 0),
        Some((leaf, true))
    );

    let mut fresh = PageBuf::with_cache(buf.page_store.clone(), page_cache);
    assert_eq!(fresh.read_level(leaf).unwrap(), 0);
    assert_eq!(
        leaf_value_at(fresh.read(leaf).unwrap(), 9).unwrap(),
        Some(v)
    );
}

#[test]
fn cow_clone_invalidates_reused_pinned_index_page() {
    let (_d, ps) = mk_store();
    let page_cache = Arc::new(PageCache::new_with_pin_budget(
        ps.clone(),
        DEFAULT_PAGE_CACHE_BYTES,
        PAGE_SIZE as u64,
    ));
    let mut buf = PageBuf::with_cache(ps.clone(), page_cache.clone());
    let stale_idx = buf.alloc_index(1, 1).unwrap();
    let live_leaf = buf.alloc_leaf(1).unwrap();
    buf.flush().unwrap();
    assert_eq!(buf.read_level(stale_idx).unwrap(), 1);
    assert_eq!(page_cache.pinned_pages(), 1);
    buf.alloc_pool.clear();

    // `cow_for_write` always copies now (per-L2P-page refcounting deleted), so
    // no rc bump is needed to force the clone allocation.
    ps.free(stale_idx, 2).unwrap();
    ps.try_reclaim().unwrap();

    let new_leaf = buf.cow_for_write(live_leaf, 3).unwrap();
    assert_eq!(new_leaf, stale_idx);
    assert_eq!(
        page_cache.pinned_pages(),
        0,
        "COW allocation must evict the stale pinned index incarnation"
    );
    assert_eq!(buf.read_level(new_leaf).unwrap(), 0);
    buf.flush().unwrap();

    let mut fresh = PageBuf::with_cache(ps, page_cache);
    assert_eq!(fresh.read_level(new_leaf).unwrap(), 0);
}
