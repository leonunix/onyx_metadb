use super::*;

impl PageStore {
    /// `fdatasync` the page file (content only).
    ///
    /// The file device routes this through its central [`IoSubmitter`]s so
    /// the fsync SQE serialises naturally behind every previously Ok'd
    /// write (each blocked on its own CQE before returning). A fixed
    /// device flushes the underlying block window.
    pub fn sync(&self) -> Result<()> {
        let started = Instant::now();
        self.device.sync()?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_fsync(started.elapsed());
        }
        Ok(())
    }

    /// `fsync` the page file (content + metadata).
    pub fn sync_all(&self) -> Result<()> {
        let started = Instant::now();
        self.device.sync_all()?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_fsync(started.elapsed());
        }
        Ok(())
    }
}
