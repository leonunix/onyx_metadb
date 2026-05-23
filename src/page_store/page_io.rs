use super::*;

impl PageStore {
    /// Read page `page_id`. Performs full integrity verification before
    /// returning.
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.check_in_range(page_id)?;
        let started = Instant::now();
        let page = self.read_pool.read_page(page_id)?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_read_batch(1, PAGE_SIZE, started.elapsed());
        }
        Ok(page)
    }

    /// Read and verify several pages. On Linux this uses one io_uring submit
    /// per chunk, so callers with many cache misses can raise device queue
    /// depth instead of serialising `pread` calls.
    pub(crate) fn read_pages(&self, page_ids: &[PageId]) -> Result<Vec<Page>> {
        if page_ids.is_empty() {
            return Ok(Vec::new());
        }
        for &page_id in page_ids {
            self.check_in_range(page_id)?;
        }
        let started = Instant::now();
        let pages = read_pages_raw(&self.file, page_ids, self.read_uring())?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_read_batch(
                page_ids.len(),
                page_ids.len() * PAGE_SIZE,
                started.elapsed(),
            );
        }
        for (page_id, page) in page_ids.iter().copied().zip(&pages) {
            page.verify(page_id)?;
        }
        Ok(pages)
    }

    /// Read page `page_id` without running `verify`. Used by recovery and
    /// verifier tooling that want to inspect potentially-bad pages without
    /// erroring out.
    pub fn read_page_unchecked(&self, page_id: PageId) -> Result<Page> {
        self.check_in_range(page_id)?;
        read_page_raw(&self.file, page_id)
    }

    /// Write `page` at `page_id`. The caller is responsible for having
    /// called [`Page::seal`] first; `write_page` does not reseal.
    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        self.write_page_for_class(page_id, page, IoLaneClass::L2p)
    }

    /// Write `page` at `page_id`, routed through the [`IoLaneClass`]
    /// submitter so dedup / refcount bursts cannot saturate the L2p
    /// SQ. Caller must have sealed the page; this method does not
    /// reseal.
    pub fn write_page_for_class(
        &self,
        page_id: PageId,
        page: &Page,
        class: IoLaneClass,
    ) -> Result<()> {
        self.check_in_range(page_id)?;
        let started = Instant::now();
        if let Some(submitter) = self.io_submitter_for_class(class) {
            submitter.submit_write(page_id, Arc::new(page.clone()))?;
        } else if let Some(submitter) = self.io_submitter_for(page_id) {
            submitter.submit_write(page_id, Arc::new(page.clone()))?;
        } else {
            self.file
                .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(1, PAGE_SIZE, started.elapsed());
        }
        Ok(())
    }

    /// Write a contiguous run of already-sealed page bytes starting at
    /// `start_page`. `bytes.len()` must be a non-zero multiple of
    /// [`PAGE_SIZE`].
    pub fn write_page_run_bytes(&self, start_page: PageId, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() || bytes.len() % PAGE_SIZE != 0 {
            return Err(MetaDbError::InvalidArgument(format!(
                "page run write requires a non-empty multiple of {PAGE_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let pages = (bytes.len() / PAGE_SIZE) as u64;
        let last = start_page
            .checked_add(pages - 1)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.check_in_range(last)?;
        let started = Instant::now();
        self.file
            .write_all_at(bytes, start_page * PAGE_SIZE as u64)?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(pages as usize, bytes.len(), started.elapsed());
        }
        Ok(())
    }

    /// Write multiple already-sealed page runs, keeping the final
    /// durability boundary at the caller's later [`sync`](Self::sync).
    pub fn write_page_runs_parallel(&self, runs: Vec<(PageId, Vec<u8>)>) -> Result<()> {
        let (ops, bytes) = runs.iter().fold((0usize, 0usize), |(o, b), (_, run)| {
            (o + run.len() / PAGE_SIZE, b + run.len())
        });
        let started = Instant::now();
        write_page_runs_raw(&self.file, runs, self.write_uring())?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(ops, bytes, started.elapsed());
        }
        Ok(())
    }

    pub fn write_sealed_page_runs(&self, pages: Vec<(PageId, Arc<Page>)>) -> Result<()> {
        self.write_sealed_page_runs_for_class_and_priority(
            pages,
            IoLaneClass::L2p,
            crate::io_submitter::IoPriority::Sync,
        )
    }

    /// Background-priority variant routed for streaming-writeback. The
    /// submitter parks runs in its deferred queue once `inflight_bg`
    /// reaches the configured cap, so sustained writeback cannot
    /// starve commit-path writes of SQE slots.
    pub fn write_sealed_page_runs_background(&self, pages: Vec<(PageId, Arc<Page>)>) -> Result<()> {
        self.write_sealed_page_runs_for_class_and_priority(
            pages,
            IoLaneClass::L2p,
            crate::io_submitter::IoPriority::Background,
        )
    }

    pub fn write_sealed_page_runs_for_class(
        &self,
        pages: Vec<(PageId, Arc<Page>)>,
        class: IoLaneClass,
    ) -> Result<()> {
        self.write_sealed_page_runs_for_class_and_priority(
            pages,
            class,
            crate::io_submitter::IoPriority::Sync,
        )
    }

    pub fn write_sealed_page_runs_for_class_and_priority(
        &self,
        mut pages: Vec<(PageId, Arc<Page>)>,
        class: IoLaneClass,
        priority: crate::io_submitter::IoPriority,
    ) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let ops = pages.len();
        let bytes = ops * PAGE_SIZE;
        pages.sort_unstable_by_key(|(pid, _)| *pid);
        let started = Instant::now();
        // Route through the lane-class submitter so dedup / refcount /
        // L2p write streams cannot saturate one another's SQ. Falls
        // back to hash routing (legacy behaviour) when the pool is
        // smaller than expected, and to pwrite when io_uring is
        // unavailable.
        let class_submitter = self.io_submitter_for_class(class);
        if class_submitter.is_some() || !self.io_submitters.is_empty() {
            let runs = coalesce_sealed_runs(pages, MAX_SEALED_WRITE_RUN_PAGES);
            let receivers: Vec<_> = runs
                .into_iter()
                .map(|(start, run_pages)| {
                    let submitter = class_submitter
                        .or_else(|| self.io_submitter_for(start))
                        .expect("io_submitters non-empty above");
                    submitter.submit_write_run_async_with_priority(start, run_pages, priority)
                })
                .collect::<Result<Vec<_>>>()?;
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
                                "io submitter dropped reply for write run",
                            )));
                        }
                    }
                }
            }
            if let Some(err) = first_err {
                return Err(err);
            }
        } else {
            write_sealed_pages_raw(&self.file, pages, self.write_uring())?;
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(ops, bytes, started.elapsed());
        }
        Ok(())
    }

    /// Fallback writer for sealed pages. It keeps only one coalesced
    /// byte run in memory at a time, rather than materialising every
    /// dirty checkpoint page into a second full-size buffer.
    pub(super) fn write_sealed_page_runs_pwrite(
        file: &File,
        pages: Vec<(PageId, Arc<Page>)>,
    ) -> Result<()> {
        let mut run_start: Option<PageId> = None;
        let mut run_next = 0;
        let mut run_bytes = Vec::with_capacity(MAX_SEALED_WRITE_RUN_PAGES * PAGE_SIZE);

        fn flush_run(
            file: &File,
            run_start: &mut Option<PageId>,
            run_bytes: &mut Vec<u8>,
        ) -> Result<()> {
            if let Some(start) = run_start.take() {
                file.write_all_at(run_bytes, start * PAGE_SIZE as u64)?;
                run_bytes.clear();
            }
            Ok(())
        }

        for (pid, page) in pages {
            let run_pages = run_bytes.len() / PAGE_SIZE;
            if run_start.is_some() && (pid != run_next || run_pages >= MAX_SEALED_WRITE_RUN_PAGES) {
                flush_run(file, &mut run_start, &mut run_bytes)?;
            }
            if run_start.is_none() {
                run_start = Some(pid);
            }
            run_bytes.extend_from_slice(page.bytes());
            run_next = pid + 1;
        }
        flush_run(file, &mut run_start, &mut run_bytes)?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn read_uring(&self) -> Option<&Mutex<Option<IoUring>>> {
        Some(&self.read_uring)
    }

    #[cfg(target_os = "linux")]
    fn write_uring(&self) -> Option<&Mutex<Option<IoUring>>> {
        Some(&self.write_uring)
    }

    #[cfg(not(target_os = "linux"))]
    fn read_uring(&self) -> Option<&()> {
        None
    }

    #[cfg(not(target_os = "linux"))]
    fn write_uring(&self) -> Option<&()> {
        None
    }
}
