use super::*;

impl PageStore {
    /// `fdatasync` the page file (content only).
    ///
    /// Routed through the central [`IoSubmitter`] so the fsync
    /// SQE serialises naturally behind every previously Ok'd
    /// `write_page` / `write_sealed_page_runs` op (each of those
    /// blocked on its own CQE before returning, so by the time the
    /// fsync op is dequeued the kernel has already received the
    /// bytes). When the submitter is unavailable, fall back to the
    /// `fdatasync(2)` syscall — which is per-fd, so it sees the same
    /// kernel state regardless of the path the writes took.
    pub fn sync(&self) -> Result<()> {
        let started = Instant::now();
        if !self.io_submitters.is_empty() {
            // Fan out `IORING_OP_FSYNC` to every submitter in parallel.
            // io_uring fsync only orders writes on the same ring, so a
            // single fsync via submitter[0] would NOT cover the writes
            // routed to submitter[1..]. Issuing in parallel + waiting
            // for all replies gives the same "every Ok'd write is
            // durable" guarantee the previous single-submitter path
            // gave, just spread across N rings.
            let receivers: Vec<_> = self
                .io_submitters
                .iter()
                .map(|sub| sub.submit_fsync_async())
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
                                "io submitter dropped fsync reply",
                            )));
                        }
                    }
                }
            }
            if let Some(err) = first_err {
                return Err(err);
            }
        } else {
            self.file.sync_data()?;
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_fsync(started.elapsed());
        }
        Ok(())
    }

    /// `fsync` the page file (content + metadata).
    pub fn sync_all(&self) -> Result<()> {
        let started = Instant::now();
        self.file.sync_all()?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_fsync(started.elapsed());
        }
        Ok(())
    }
}
