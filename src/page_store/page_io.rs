use super::*;

impl PageStore {
    /// Read page `page_id`. Performs full integrity verification before
    /// returning.
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.check_in_range(page_id)?;
        let started = Instant::now();
        let page = self.device.read_page(page_id)?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_read_batch(1, PAGE_SIZE, started.elapsed());
        }
        Ok(page)
    }

    /// Read and verify several pages. On Linux the file device uses one
    /// io_uring submit per chunk, so callers with many cache misses can
    /// raise device queue depth instead of serialising `pread` calls.
    pub(crate) fn read_pages(&self, page_ids: &[PageId]) -> Result<Vec<Page>> {
        if page_ids.is_empty() {
            return Ok(Vec::new());
        }
        for &page_id in page_ids {
            self.check_in_range(page_id)?;
        }
        let started = Instant::now();
        let pages = self.device.read_pages(page_ids)?;
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
        self.device.read_page_unchecked(page_id)
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
        self.device.write_page(page_id, page, class)?;
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
        self.device.write_page_run_bytes(start_page, bytes)?;
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
        self.device.write_page_runs_parallel(runs)?;
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
        pages: Vec<(PageId, Arc<Page>)>,
        class: IoLaneClass,
        priority: crate::io_submitter::IoPriority,
    ) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let ops = pages.len();
        let bytes = ops * PAGE_SIZE;
        let started = Instant::now();
        // The fixed-device PageBlockIo path cannot express io_uring priority
        // classes and used to discard `Background`. Preserve the semantic at
        // the PageStore boundary: manifest publication can drain and exclude
        // these writes before taking apply_gate, while ordinary foreground
        // page IO remains fully concurrent.
        let _background_guard = (priority == crate::io_submitter::IoPriority::Background)
            .then(|| self.publish_io_barrier.read());
        self.device.write_sealed_page_runs(pages, class, priority)?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(ops, bytes, started.elapsed());
        }
        Ok(())
    }
}
