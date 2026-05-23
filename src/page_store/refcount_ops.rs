use super::*;

impl PageStore {
    /// Atomically mutate the refcount of `page_id` by `delta` (positive
    /// for incref, negative for decref). Returns the post-delta rc.
    ///
    /// Bypasses [`PageCache`] and [`PageBuf`]: reads the authoritative
    /// on-disk version inside a per-pid sharded mutex, mutates, writes
    /// back. Used by [`crate::paged::PageBuf::cow_for_write`] when the
    /// page is shared across multiple tree instances (post-`clone_volume`).
    /// Without this, two trees each holding a Clean copy would race:
    /// both read the same pre-decrement rc, both write rc-1 via flush,
    /// losing one decrement.
    ///
    /// Leaves `page.generation` unchanged — that field is reserved for
    /// WAL-apply idempotency markers and must not regress.
    ///
    /// The caller is responsible for invalidating any cached copies of
    /// `page_id` in `PageCache` / `PageBuf` after this call so a
    /// subsequent read observes the new rc.
    pub fn atomic_rc_delta(&self, page_id: PageId, delta: i32) -> Result<u32> {
        self.check_in_range(page_id)?;
        let shard = (page_id as usize) % RC_LOCK_SHARDS;
        let _guard = self.rc_locks[shard].lock();
        let mut page = read_page_raw(&self.file, page_id)?;
        page.verify(page_id)?;
        let cur = page.refcount();
        let new_rc = if delta >= 0 {
            cur.checked_add(delta as u32)
        } else {
            cur.checked_sub((-delta) as u32)
        }
        .ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "atomic_rc_delta: page {page_id} refcount {cur} + {delta} out of range"
            ))
        })?;
        page.set_refcount(new_rc);
        page.seal();
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        Ok(new_rc)
    }

    /// Same as [`atomic_rc_delta`] but with WAL-replay idempotency. The
    /// `(lsn, ordinal)` pair identifies one rc-delta application within
    /// a WAL record. If the page already carries a later marker, this
    /// delta is treated as already applied and skipped. On successful
    /// apply the page is stamped with `(lsn, ordinal)`.
    ///
    /// Used by [`crate::paged::PageBuf::cow_for_write`] so that a WAL
    /// op replayed after crash does not double-apply an already landed
    /// delta. A single WAL record can contain multiple L2P ops with the
    /// same LSN, so comparing only `generation >= lsn` is insufficient:
    /// distinct same-LSN deltas on the same page must not swallow each
    /// other. `ordinal` disambiguates those same-record applications.
    ///
    /// `lsn` must be strictly greater than zero — tree pages
    /// carry `generation = 0` for their entire unsnapped lifetime, so
    /// `lsn = 0` would spuriously skip on every call. The function
    /// rejects that case.
    ///
    /// The caller is responsible for invalidating any cached copies
    /// of `page_id` in `PageCache` / `PageBuf` after this call so a
    /// subsequent read observes the new rc + generation.
    pub fn atomic_rc_delta_with_gen(
        &self,
        page_id: PageId,
        delta: i32,
        lsn: Lsn,
        ordinal: u32,
    ) -> Result<u32> {
        if lsn == 0 {
            return Err(MetaDbError::InvalidArgument(
                "atomic_rc_delta_with_gen: lsn must be > 0".into(),
            ));
        }
        self.check_in_range(page_id)?;
        let shard = (page_id as usize) % RC_LOCK_SHARDS;
        let _guard = self.rc_locks[shard].lock();
        let mut page = read_page_raw(&self.file, page_id)?;
        page.verify(page_id)?;
        let cur_gen = page.generation();
        let cur_ordinal = page.flags();
        let cur_rc = page.refcount();
        if cur_gen > lsn || (cur_gen == lsn && cur_ordinal >= ordinal) {
            return Ok(cur_rc);
        }
        let new_rc = if delta >= 0 {
            cur_rc.checked_add(delta as u32)
        } else {
            cur_rc.checked_sub((-delta) as u32)
        }
        .ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "atomic_rc_delta_with_gen: page {page_id} refcount {cur_rc} + {delta} out of range"
            ))
        })?;
        page.set_refcount(new_rc);
        page.set_generation(lsn);
        page.set_flags(ordinal);
        page.seal();
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        Ok(new_rc)
    }

    /// Batch form of [`atomic_rc_delta_with_gen`]. All target pages are
    /// locked in shard order, read through the page-store batch path,
    /// mutated in memory, and written back through the sealed-page batch
    /// writer. This preserves the same per-page WAL idempotency markers
    /// while giving NVMe real queue depth for COW refcount commits.
    pub(crate) fn atomic_rc_delta_batch_with_gen(
        &self,
        deltas: &[RcDeltaWithGen],
    ) -> Result<Vec<u32>> {
        if deltas.is_empty() {
            return Ok(Vec::new());
        }
        {
            let inner = self.inner.lock();
            for delta in deltas {
                if delta.lsn == 0 {
                    return Err(MetaDbError::InvalidArgument(
                        "atomic_rc_delta_batch_with_gen: lsn must be > 0".into(),
                    ));
                }
                if delta.page_id >= inner.high_water {
                    return Err(MetaDbError::PageOutOfRange(delta.page_id));
                }
            }
        }

        let mut indexed: Vec<(usize, RcDeltaWithGen)> =
            deltas.iter().copied().enumerate().collect();
        indexed.sort_unstable_by_key(|(_, delta)| delta.page_id);
        if let Some(duplicate) = indexed.windows(2).find_map(|pair| {
            if pair[0].1.page_id == pair[1].1.page_id {
                Some(pair[0].1.page_id)
            } else {
                None
            }
        }) {
            return Err(MetaDbError::Corruption(format!(
                "atomic_rc_delta_batch_with_gen: duplicate page {duplicate} in one batch"
            )));
        }

        let mut shards: Vec<usize> = indexed
            .iter()
            .map(|(_, delta)| (delta.page_id as usize) % RC_LOCK_SHARDS)
            .collect();
        shards.sort_unstable();
        shards.dedup();
        let _guards: Vec<_> = shards
            .iter()
            .map(|&shard| self.rc_locks[shard].lock())
            .collect();

        let page_ids: Vec<PageId> = indexed.iter().map(|(_, delta)| delta.page_id).collect();
        let pages = self.read_pages(&page_ids)?;
        let mut results = vec![0u32; deltas.len()];
        let mut sealed_pages = Vec::new();

        for (((original_idx, delta), mut page), page_id) in
            indexed.into_iter().zip(pages).zip(page_ids)
        {
            let cur_gen = page.generation();
            let cur_ordinal = page.flags();
            let cur_rc = page.refcount();
            if cur_gen > delta.lsn || (cur_gen == delta.lsn && cur_ordinal >= delta.ordinal) {
                results[original_idx] = cur_rc;
                continue;
            }
            let new_rc = if delta.delta >= 0 {
                cur_rc.checked_add(delta.delta as u32)
            } else {
                cur_rc.checked_sub((-delta.delta) as u32)
            }
            .ok_or_else(|| {
                MetaDbError::Corruption(format!(
                    "atomic_rc_delta_batch_with_gen: page {} refcount {} + {} out of range",
                    delta.page_id, cur_rc, delta.delta
                ))
            })?;
            page.set_refcount(new_rc);
            page.set_generation(delta.lsn);
            page.set_flags(delta.ordinal);
            page.seal();
            results[original_idx] = new_rc;
            sealed_pages.push((page_id, Arc::new(page)));
        }

        self.write_sealed_page_runs(sealed_pages)?;
        Ok(results)
    }
}
