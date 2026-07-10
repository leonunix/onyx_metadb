use super::*;
use std::sync::atomic::Ordering;

impl PageStore {
    /// Allocate a fresh page id. If the free list has entries, one is
    /// popped and returned; otherwise `high_water` advances by one and
    /// the file is pre-extended in `grow_chunk` units so most calls
    /// avoid a `set_len` syscall. The on-disk content is not
    /// initialized — the caller is expected to write a sealed page at
    /// the returned id.
    pub fn allocate(&self) -> Result<PageId> {
        let mut inner = self.inner.lock();
        if let Some(page_id) = inner.free_list.pop() {
            self.free_list_pages.fetch_sub(1, Ordering::Relaxed);
            return Ok(page_id);
        }
        let page_id = inner.high_water;
        let new_high = inner
            .high_water
            .checked_add(1)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.device.ensure_covers(new_high)?;
        inner.high_water = new_high;
        self.high_water_pages.store(new_high, Ordering::Relaxed);
        Ok(page_id)
    }

    /// Allocate a contiguous run of `count` page ids.
    ///
    /// Fast path reuses a contiguous run from the free list. Reclaim
    /// appends sorted page ids, so checkpoint-local allocation runs can
    /// recycle recently freed runs instead of monotonically pushing
    /// `high_water` forward. The common case is a contiguous LIFO
    /// suffix; if that suffix is fragmented, scan backward for an older
    /// contiguous run before extending the file.
    pub fn allocate_run(&self, count: usize) -> Result<PageId> {
        if count == 0 {
            return Err(MetaDbError::InvalidArgument(
                "allocate_run requires count > 0".into(),
            ));
        }
        let count_usize = count;
        let count = u64::try_from(count)
            .map_err(|_| MetaDbError::InvalidArgument("page run too large".into()))?;
        let mut inner = self.inner.lock();
        if let Some(start) = take_contiguous_free_run(&mut inner.free_list, count_usize) {
            self.free_list_pages
                .fetch_sub(count_usize, Ordering::Relaxed);
            return Ok(start);
        }
        let start = inner.high_water;
        let new_high = inner
            .high_water
            .checked_add(count)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.device.ensure_covers(new_high)?;
        inner.high_water = new_high;
        self.high_water_pages.store(new_high, Ordering::Relaxed);
        Ok(start)
    }

    /// Append `count` fresh page ids at the frontier, **bypassing the free
    /// list** (unlike [`allocate`](Self::allocate) / [`allocate_batch`], which
    /// pop reusable interior pages first). Used to reserve the persisted
    /// free-list bitmap chain: that chain's bytes are built from a free-list
    /// snapshot taken after this call, so the allocation must not itself
    /// add/remove any free-list entry. Returns the appended ids in ascending
    /// order. Bumps `high_water` by exactly `count`.
    pub fn allocate_frontier_pages(&self, count: usize) -> Result<Vec<PageId>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let count_u64 = u64::try_from(count)
            .map_err(|_| MetaDbError::InvalidArgument("page run too large".into()))?;
        let mut inner = self.inner.lock();
        let start = inner.high_water;
        let new_high = start
            .checked_add(count_u64)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.device.ensure_covers(new_high)?;
        inner.high_water = new_high;
        self.high_water_pages.store(new_high, Ordering::Relaxed);
        Ok((start..new_high).collect())
    }

    /// Allocate up to `count` page ids as a local scratch batch.
    ///
    /// Unlike [`allocate_run`](Self::allocate_run), this does not require
    /// reused pages to be contiguous. Hot COW writers only need a small pool
    /// of fresh page ids; forcing that pool to come from a 256-page contiguous
    /// free-list span strands fragmented reclaimed pages and keeps pushing the
    /// high-water mark forward under random-write workloads.
    ///
    /// The returned vector is ordered as a stack for callers that consume it
    /// with `pop()`: reclaimed free-list pages are returned before newly
    /// extended tail pages.
    pub fn allocate_batch(&self, count: usize) -> Result<Vec<PageId>> {
        if count == 0 {
            return Err(MetaDbError::InvalidArgument(
                "allocate_batch requires count > 0".into(),
            ));
        }
        let mut inner = self.inner.lock();
        let reuse = count.min(inner.free_list.len());
        let mut reused = Vec::with_capacity(reuse);
        for _ in 0..reuse {
            // LIFO keeps the free-list's existing cache-locality behaviour.
            if let Some(page_id) = inner.free_list.pop() {
                reused.push(page_id);
            }
        }
        if !reused.is_empty() {
            self.free_list_pages
                .fetch_sub(reused.len(), Ordering::Relaxed);
        }

        let missing = count - reused.len();
        let mut pages = Vec::with_capacity(count);
        if missing > 0 {
            let missing_u64 = u64::try_from(missing)
                .map_err(|_| MetaDbError::InvalidArgument("page batch too large".into()))?;
            let start = inner.high_water;
            let new_high = inner
                .high_water
                .checked_add(missing_u64)
                .ok_or(MetaDbError::OutOfSpace)?;
            self.device.ensure_covers(new_high)?;
            inner.high_water = new_high;
            self.high_water_pages.store(new_high, Ordering::Relaxed);
            // Store new tail pages in reverse so `pop()` yields ascending ids.
            pages.extend((start..new_high).rev());
        }
        // Appended last so `pop()` consumes reclaimed pages before growing
        // into the newly extended tail.
        pages.extend(reused);
        Ok(pages)
    }
}
