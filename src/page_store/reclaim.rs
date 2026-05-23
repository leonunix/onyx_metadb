use super::*;

impl PageStore {
    /// Free `count` pages starting at `start`, stamping each with
    /// `generation`. Pages rejoin the single-page free list individually.
    /// Convenience wrapper over [`free`]; fails as soon as any page id
    /// falls outside the allowed range.
    pub fn free_run(&self, start: PageId, count: u32, generation: Lsn) -> Result<()> {
        let page_ids: Vec<PageId> = (start..start + count as u64).collect();
        self.free_many(&page_ids, generation)
    }

    /// Mark `page_id` as free. The physical Free-stamp + hole-punch +
    /// free-list push is **deferred** until [`try_reclaim`] runs and
    /// observes that no live reader could still walk the page (see
    /// [`crate::epoch`] for the safety proof). The on-disk bytes stay
    /// the page's old (still-valid) content during the deferred window,
    /// so a stale L2P reader that falls through page-cache to disk
    /// keeps decoding correctly. `generation` is recorded with the
    /// deferred entry and stamped onto the Free page at reclaim time.
    ///
    /// Refuses to free reserved pages (manifest slots) or pages outside
    /// the current high-water range.
    pub fn free(&self, page_id: PageId, generation: Lsn) -> Result<()> {
        self.free_many(&[page_id], generation)
    }

    /// Batch form of [`free`]. All pages are tagged with one epoch and
    /// inserted under one deferred-free lock acquisition; this keeps
    /// checkpoint install from paying per-page lock/epoch overhead when a
    /// dirty shard retires tens of thousands of pages at once.
    pub fn free_many(&self, page_ids: &[PageId], generation: Lsn) -> Result<()> {
        if page_ids.is_empty() {
            return Ok(());
        }
        {
            let inner = self.inner.lock();
            for &page_id in page_ids {
                if page_id < FIRST_DATA_PAGE {
                    return Err(MetaDbError::InvalidArgument(format!(
                        "page {page_id} is reserved (manifest slot)",
                    )));
                }
                if page_id >= inner.high_water {
                    return Err(MetaDbError::PageOutOfRange(page_id));
                }
            }
        }

        let mut sorted = page_ids.to_vec();
        sorted.sort_unstable();
        if let Some(duplicate) = sorted.windows(2).find_map(|pair| {
            if pair[0] == pair[1] {
                Some(pair[0])
            } else {
                None
            }
        }) {
            return Err(MetaDbError::Corruption(format!(
                "page_store: duplicate free of page {duplicate} in one batch",
            )));
        }

        // Tag with the pre-bump epoch and bump global so any reader
        // pinning after this call observes G_pin > tag.
        let tag = self.epoch.advance();
        let mut deferred = self.deferred_free.lock();
        for &page_id in &sorted {
            if deferred.contains_key(&page_id) {
                return Err(MetaDbError::Corruption(format!(
                    "page_store: double free of page {page_id} (already pending reclaim)",
                )));
            }
        }
        for page_id in sorted {
            deferred.insert(
                page_id,
                DeferredFree {
                    epoch: tag,
                    generation,
                    idempotent: false,
                },
            );
        }
        Ok(())
    }

    /// Idempotent version of [`free`]. If `page_id` is already pending
    /// reclaim, or is already on disk as a `Free` / zero page, no work
    /// is queued and `Ok(false)` is returned. Otherwise the deferred
    /// entry is recorded and `Ok(true)` is returned.
    ///
    /// Used by WAL-replay paths (e.g. `DropSnapshot`) that may re-run
    /// against pages a crashed predecessor already freed. Cross-process
    /// correctness comes from [`open`] rebuilding `free_list` by scanning
    /// Free-typed pages, so each Free page ends up on the list exactly
    /// once regardless of how many times this was called before the
    /// crash.
    pub fn free_idempotent(&self, page_id: PageId, generation: Lsn) -> Result<bool> {
        if page_id < FIRST_DATA_PAGE {
            return Err(MetaDbError::InvalidArgument(format!(
                "page {page_id} is reserved (manifest slot)",
            )));
        }
        self.check_in_range(page_id)?;
        // Disk Free / zero check: a crash + replay path may already have
        // physically freed this pid in an earlier attempt.
        if let Ok(existing) = read_page_raw(&self.file, page_id) {
            if is_zero_page(&existing) {
                return Ok(false);
            }
            if let Ok(h) = existing.header() {
                if h.page_type == PageType::Free {
                    return Ok(false);
                }
            }
        }
        let tag = self.epoch.advance();
        let mut deferred = self.deferred_free.lock();
        if deferred.contains_key(&page_id) {
            return Ok(false);
        }
        deferred.insert(
            page_id,
            DeferredFree {
                epoch: tag,
                generation,
                idempotent: true,
            },
        );
        Ok(true)
    }

    /// Drain every deferred-free entry whose tag is below the smallest
    /// active reader pin, physically free those pids (Free-stamp +
    /// hole-punch + free-list push), and return the list of reclaimed
    /// pids so the caller can invalidate any stale page-cache entries.
    ///
    /// Idempotent and lock-free relative to readers: callers that hold
    /// no apply-side guard may invoke this from a background sweeper.
    pub fn try_reclaim(&self) -> Result<ReclaimOutcome> {
        self.try_reclaim_limit(usize::MAX)
    }

    /// Budgeted variant of [`try_reclaim`]. Reclaims at most `max_pages`
    /// safe entries so latency-sensitive callers can make progress
    /// without turning one checkpoint into an unbounded free storm.
    pub fn try_reclaim_limit(&self, max_pages: usize) -> Result<ReclaimOutcome> {
        if max_pages == 0 {
            return Ok(ReclaimOutcome::default());
        }
        let safe_below = self.epoch.min_active_pin();
        let mut deferred = self.deferred_free.lock();
        if deferred.is_empty() {
            return Ok(ReclaimOutcome {
                safe_below,
                ..ReclaimOutcome::default()
            });
        }
        let selected: Vec<(PageId, DeferredFree)> = deferred
            .iter()
            .filter_map(|(pid, entry)| {
                if entry.epoch < safe_below {
                    Some((*pid, *entry))
                } else {
                    None
                }
            })
            .take(max_pages)
            .collect();
        for (pid, _) in &selected {
            deferred.remove(pid);
        }
        drop(deferred);

        let reclaimed = self.reclaim_sorted_runs(&selected)?;
        Ok(ReclaimOutcome {
            safe_below,
            selected: selected.len(),
            reclaimed,
        })
    }

    fn reclaim_sorted_runs(&self, pages: &[(PageId, DeferredFree)]) -> Result<Vec<PageId>> {
        if pages.is_empty() {
            return Ok(Vec::new());
        }

        // Coalesce the pid-sorted selection into contiguous runs.
        // Each run becomes one `IORING_OP_WRITEV` SQE; punch_hole jobs
        // accumulate alongside so the post-write fallocate sweep can
        // chase them serially without re-walking the page list.
        let mut runs: Vec<(PageId, Vec<Arc<Page>>)> = Vec::new();
        let mut punch_jobs: Vec<(PageId, usize)> = Vec::new();
        let mut reclaimed: Vec<PageId> = Vec::with_capacity(pages.len());

        let mut idx = 0;
        while idx < pages.len() {
            let start = pages[idx].0;
            let mut end = idx + 1;
            while end < pages.len()
                && pages[end].0 == pages[end - 1].0 + 1
                && end - idx < MAX_RECLAIM_RUN_PAGES
            {
                end += 1;
            }

            let mut run_pages: Vec<Arc<Page>> = Vec::with_capacity(end - idx);
            for (_, entry) in &pages[idx..end] {
                let mut page = Page::new(PageHeader::new(PageType::Free, entry.generation));
                page.set_refcount(0);
                page.seal();
                run_pages.push(Arc::new(page));
            }
            let count = end - idx;
            runs.push((start, run_pages));
            if count >= MIN_PUNCH_HOLE_RUN_PAGES {
                punch_jobs.push((start, count));
            }
            reclaimed.extend(pages[idx..end].iter().map(|(pid, _)| *pid));
            idx = end;
        }

        // Phase B: parallel Free-stamp via IoSubmitter. Fan out every
        // run as one `IORING_OP_WRITEV` SQE and drain replies — the
        // old serial `write_page_run_bytes` loop was the dominant
        // cost (39 s `flush_reclaim_max_us` on nvme-box) when the
        // backlog exceeded ~1M pages.
        if !self.io_submitters.is_empty() {
            let mut receivers = Vec::with_capacity(runs.len());
            for (start, run_pages) in runs {
                let submitter = self
                    .io_submitter_for(start)
                    .expect("io_submitters non-empty above");
                receivers.push(submitter.submit_write_run_async_with_priority(
                    start,
                    run_pages,
                    crate::io_submitter::IoPriority::Sync,
                )?);
            }
            let mut first_err: Option<MetaDbError> = None;
            for rx in receivers {
                match rx.recv() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        if first_err.is_none() {
                            first_err = Some(err);
                        }
                    }
                    Err(_) => {
                        if first_err.is_none() {
                            first_err = Some(MetaDbError::Io(io::Error::other(
                                "io submitter dropped reply for reclaim run",
                            )));
                        }
                    }
                }
            }
            if let Some(err) = first_err {
                return Err(err);
            }
        } else {
            // Fallback path (io_uring unavailable / disabled by tests):
            // materialise each run as a contiguous byte buffer and
            // pwrite. Same wall-time profile as the original code.
            for (start, run_pages) in &runs {
                let mut bytes = Vec::with_capacity(run_pages.len() * PAGE_SIZE);
                for p in run_pages {
                    bytes.extend_from_slice(p.bytes());
                }
                self.write_page_run_bytes(*start, &bytes)?;
            }
        }

        // Phase C: punch_hole sweep. `fallocate` is a syscall (no
        // io_uring fast path in our build); the cost per call is
        // ~5 µs on NVMe so the serial loop is not the bottleneck.
        for (start, count) in punch_jobs {
            self.punch_free_run(start, count)?;
        }

        let mut inner = self.inner.lock();
        inner.free_list.extend(reclaimed.iter().copied());
        self.truncate_free_tail_locked(&mut inner)?;
        Ok(reclaimed)
    }

    fn truncate_free_tail_locked(&self, inner: &mut Inner) -> Result<()> {
        if inner.high_water <= FIRST_DATA_PAGE || inner.free_list.is_empty() {
            return Ok(());
        }
        let tail_page = inner.high_water - 1;
        if !inner.free_list.iter().any(|pid| *pid == tail_page) {
            return Ok(());
        }

        inner.free_list.sort_unstable();
        inner.free_list.dedup();
        let original_high_water = inner.high_water;
        while inner.high_water > FIRST_DATA_PAGE
            && inner
                .free_list
                .last()
                .is_some_and(|pid| *pid == inner.high_water - 1)
        {
            inner.free_list.pop();
            inner.high_water -= 1;
        }

        if inner.high_water < original_high_water {
            self.file.set_len(
                inner
                    .high_water
                    .checked_mul(PAGE_SIZE as u64)
                    .ok_or(MetaDbError::OutOfSpace)?,
            )?;
            inner.committed_file_pages = inner.high_water;
        }
        Ok(())
    }

    fn punch_free_run(&self, start_page: PageId, page_count: usize) -> Result<()> {
        punch_hole(
            &self.file,
            start_page * PAGE_SIZE as u64,
            (page_count * PAGE_SIZE) as u64,
        )
    }
}
