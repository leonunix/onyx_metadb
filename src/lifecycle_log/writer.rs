//! The lifecycle journal writer the live `Db` holds.
//!
//! A `Db` appends lifecycle records through exactly one of two backends —
//! a directory of segment files ([`FileJournal`](super::LifecycleJournal), the
//! default path) or a fixed block ring ([`RingJournal`], the device path over a
//! chunklet meta LogicalDisk). The append / next-seq / prune surface is
//! identical, so the enum keeps every call site backend-agnostic.
//!
//! Recovery (replay) is *not* routed through here: it runs before the writer is
//! constructed and the open path picks `FileJournal::replay` vs
//! `RingJournal::replay` directly.

use crate::error::Result;

use super::{LifecycleJournal, RingJournal};

/// Backend-agnostic lifecycle journal writer. Single-writer; the `Db` wraps it
/// in a `Mutex`, matching the discipline both backends assume.
pub enum LifecycleWriter {
    /// Segment-file journal (default / file path).
    File(LifecycleJournal),
    /// Fixed block ring (device path).
    Ring(RingJournal),
}

impl LifecycleWriter {
    /// Append `body`, fsync/flush, return the assigned monotonic seq.
    pub fn append(&mut self, body: &[u8]) -> Result<u64> {
        match self {
            LifecycleWriter::File(j) => j.append(body),
            LifecycleWriter::Ring(r) => r.append(body),
        }
    }

    /// Next seq the journal will hand out.
    pub fn next_seq(&self) -> u64 {
        match self {
            LifecycleWriter::File(j) => j.next_seq(),
            LifecycleWriter::Ring(r) => r.next_seq(),
        }
    }

    /// Non-mutating prune preview. `Ring` → `Some(target_head)` to stamp into
    /// `manifest.journal_ring_head` *before* the manifest commit; `File` →
    /// `None` (segment journals persist no ring head). See
    /// [`RingJournal::prune_target`] for why the head must be made durable
    /// before [`prune`](Self::prune) frees the blocks.
    pub fn prune_target(&self, checkpoint_seq: u64) -> Result<Option<u64>> {
        match self {
            LifecycleWriter::File(_) => Ok(None),
            LifecycleWriter::Ring(r) => Ok(Some(r.prune_target(checkpoint_seq)?)),
        }
    }

    /// Advance the prune boundary past records `<= checkpoint_seq`. Call only
    /// after a manifest commit carrying [`prune_target`](Self::prune_target)'s
    /// head is durable. `Ring` → frees blocks, returns `Some(new_head)`; `File`
    /// → deletes covered segments, returns `None`.
    pub fn prune(&mut self, checkpoint_seq: u64) -> Result<Option<u64>> {
        match self {
            LifecycleWriter::File(j) => {
                j.prune_covered(checkpoint_seq)?;
                Ok(None)
            }
            LifecycleWriter::Ring(r) => Ok(Some(r.prune(checkpoint_seq)?)),
        }
    }

    /// Current ring head (device path bookkeeping / tests). `File` → `None`.
    pub fn ring_head(&self) -> Option<u64> {
        match self {
            LifecycleWriter::File(_) => None,
            LifecycleWriter::Ring(r) => Some(r.ring_head()),
        }
    }
}
