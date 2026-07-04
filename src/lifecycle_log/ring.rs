//! Block-granular ring lifecycle journal (device-backed).
//!
//! The [file journal](super::journal) uses a directory of append-only segment
//! files: `readdir` enumeration, `create_new` segments, byte `append`, torn-tail
//! `set_len` truncation. None of that exists on a raw block window (onyx over a
//! chunklet LogicalDisk), so this module reconstructs the same semantics as a
//! fixed ring of 4 KiB blocks.
//!
//! # Layout
//!
//! The ring is `block_count` contiguous 4 KiB blocks. Each record occupies a
//! run of `ceil((WAL_HEADER_SIZE + body) / BLOCK_SIZE)` consecutive blocks
//! (wrapping the ring boundary), holding the same `seq(8)|len(4)|crc32c(4)|body`
//! frame as the file journal, zero-padded to the block boundary. Records are
//! written at strictly increasing `seq` (globally monotonic, starting at 1);
//! `seq == 0` marks a never-written block.
//!
//! # The one semantic difference from the file journal
//!
//! **Append never rewrites an already-published block.** The file version can
//! lean on the filesystem journal to make an in-place tail rewrite crash-safe;
//! a raw block has no such backstop, so a torn rewrite of a durable block would
//! lose an already-`ack`ed record. Here every append writes only *free* blocks
//! `[tail, tail+run)` and then flushes; live blocks `[head, tail)` are never
//! touched. A crash mid-append leaves a torn run whose CRC (or missing
//! continuation) fails on replay — and that record was never acked, so losing
//! it is correct.
//!
//! # Recovery
//!
//! `head` (the prune boundary, persisted as `manifest.journal_ring_head`) is
//! where the oldest un-pruned record starts. Replay walks records from `head`
//! with strictly-`+1` `seq` contiguity; the first block that is zero
//! (`seq == 0`), fails CRC, or breaks contiguity is the tail. `tail` itself is
//! never persisted — it is rediscovered by this scan.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};

use super::journal::LifecycleRecord;
use super::record::{WAL_HEADER_SIZE, WAL_MAX_BODY, encode as encode_frame};

/// Ring block size. Matches the page size so the underlying device (a chunklet
/// LogicalDisk window, min IO 4 KiB) sees aligned block IO.
pub const BLOCK_SIZE: usize = PAGE_SIZE;

/// A fixed window of contiguous 4 KiB blocks backing a [`RingJournal`].
/// Offsets are block indices within the window; the onyx adapter adds the
/// journal region's base offset within the meta LogicalDisk.
pub trait JournalDevice: Send + Sync {
    /// Number of 4 KiB blocks in the window.
    fn block_count(&self) -> u64;
    /// Read block `idx` into `buf` (exactly [`BLOCK_SIZE`] bytes).
    fn read_block(&self, idx: u64, buf: &mut [u8]) -> Result<()>;
    /// Write `buf` (exactly [`BLOCK_SIZE`] bytes) to block `idx`.
    fn write_block(&self, idx: u64, buf: &[u8]) -> Result<()>;
    /// Durability fence — every prior `write_block` is durable on return.
    fn flush(&self) -> Result<()>;
}

/// In-memory [`JournalDevice`] for tests / embedded use.
pub struct MemJournalDevice {
    blocks: Mutex<Vec<u8>>,
    block_count: u64,
}

impl MemJournalDevice {
    pub fn new(block_count: u64) -> Self {
        Self {
            blocks: Mutex::new(vec![0u8; (block_count as usize) * BLOCK_SIZE]),
            block_count,
        }
    }
}

impl JournalDevice for MemJournalDevice {
    fn block_count(&self) -> u64 {
        self.block_count
    }

    fn read_block(&self, idx: u64, buf: &mut [u8]) -> Result<()> {
        debug_assert_eq!(buf.len(), BLOCK_SIZE);
        let off = (idx as usize) * BLOCK_SIZE;
        let blocks = self.blocks.lock();
        if off + BLOCK_SIZE > blocks.len() {
            return Err(MetaDbError::InvalidArgument(format!(
                "ring block {idx} out of range ({} blocks)",
                self.block_count
            )));
        }
        buf.copy_from_slice(&blocks[off..off + BLOCK_SIZE]);
        Ok(())
    }

    fn write_block(&self, idx: u64, buf: &[u8]) -> Result<()> {
        debug_assert_eq!(buf.len(), BLOCK_SIZE);
        let off = (idx as usize) * BLOCK_SIZE;
        let mut blocks = self.blocks.lock();
        if off + BLOCK_SIZE > blocks.len() {
            return Err(MetaDbError::InvalidArgument(format!(
                "ring block {idx} out of range ({} blocks)",
                self.block_count
            )));
        }
        blocks[off..off + BLOCK_SIZE].copy_from_slice(buf);
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Number of blocks a record with `body_len` bytes occupies.
fn run_blocks(body_len: usize) -> u64 {
    let total = WAL_HEADER_SIZE + body_len;
    total.div_ceil(BLOCK_SIZE) as u64
}

/// Parse a frame header out of the first block of a record run.
/// Returns `(seq, body_len, crc)`. `seq == 0` signals a never-written block.
fn parse_header(block: &[u8]) -> (u64, u32, u32) {
    let seq = u64::from_le_bytes(block[0..8].try_into().unwrap());
    let len = u32::from_le_bytes(block[8..12].try_into().unwrap());
    let crc = u32::from_le_bytes(block[12..16].try_into().unwrap());
    (seq, len, crc)
}

/// Read `run` blocks starting at `start` (wrapping) into a fresh buffer.
fn read_run(device: &dyn JournalDevice, block_count: u64, start: u64, run: u64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; (run as usize) * BLOCK_SIZE];
    for i in 0..run {
        let idx = (start + i) % block_count;
        device.read_block(idx, &mut buf[(i as usize) * BLOCK_SIZE..(i as usize + 1) * BLOCK_SIZE])?;
    }
    Ok(buf)
}

/// Walk records from `head` with `+1` seq contiguity, invoking `on_record` for
/// each valid record. Stops at the tail (zero/torn/discontiguous block) or after
/// a full lap. Returns `(records_end_block, max_seq_seen, live_blocks)`.
fn scan_from<F>(
    device: &dyn JournalDevice,
    block_count: u64,
    head: u64,
    mut on_record: F,
) -> Result<(u64, u64, u64)>
where
    F: FnMut(u64, &[u8]) -> Result<()>,
{
    let mut pos = head;
    let mut expected: Option<u64> = None;
    let mut max_seq = 0u64;
    let mut walked = 0u64;
    let mut block = vec![0u8; BLOCK_SIZE];
    loop {
        if walked >= block_count {
            break;
        }
        device.read_block(pos, &mut block)?;
        let (seq, len, crc) = parse_header(&block);
        if seq == 0 {
            break; // never-written block: the tail.
        }
        if len as usize > WAL_MAX_BODY {
            break;
        }
        let run = run_blocks(len as usize);
        if run > block_count || walked + run > block_count {
            break;
        }
        if let Some(exp) = expected {
            if seq != exp {
                break; // stale record from a prior lap: the tail.
            }
        }
        let run_buf = if run == 1 {
            block.clone()
        } else {
            read_run(device, block_count, pos, run)?
        };
        let body = &run_buf[WAL_HEADER_SIZE..WAL_HEADER_SIZE + len as usize];
        if crc32c::crc32c(body) != crc {
            break; // torn write: the tail.
        }
        on_record(seq, body)?;
        max_seq = max_seq.max(seq);
        expected = Some(seq + 1);
        pos = (pos + run) % block_count;
        walked += run;
    }
    Ok((pos, max_seq, walked))
}

/// A fixed-ring lifecycle journal over a [`JournalDevice`]. Single-writer,
/// serialized by the caller's `Mutex<..>` (the same discipline the file journal
/// relies on).
pub struct RingJournal {
    device: Arc<dyn JournalDevice>,
    block_count: u64,
    /// Oldest live block (prune boundary); persisted as `journal_ring_head`.
    head: u64,
    /// Next free block (rediscovered on open, never persisted).
    tail: u64,
    /// Live blocks in `[head, tail)`.
    used: u64,
    next_seq: u64,
}

impl RingJournal {
    /// Open the ring for append. `ring_head` is the persisted prune boundary
    /// (`manifest.journal_ring_head`); `next_seq` is the caller's authoritative
    /// next seq (post-recovery), mirroring [`super::journal::LifecycleJournal::open`].
    /// The tail + live-block count are rediscovered by scanning from `ring_head`.
    pub fn open(device: Arc<dyn JournalDevice>, ring_head: u64, next_seq: u64) -> Result<Self> {
        let block_count = device.block_count();
        if block_count == 0 {
            return Err(MetaDbError::InvalidArgument(
                "ring journal device has zero blocks".into(),
            ));
        }
        if ring_head >= block_count {
            return Err(MetaDbError::Corruption(format!(
                "ring journal head {ring_head} >= block_count {block_count}",
            )));
        }
        let (tail, max_seq, used) = scan_from(device.as_ref(), block_count, ring_head, |_, _| Ok(()))?;
        Ok(Self {
            device,
            block_count,
            head: ring_head,
            tail,
            used,
            next_seq: next_seq.max(max_seq.saturating_add(1)),
        })
    }

    /// Replay records with `seq > from_seq` from `ring_head`, in seq order.
    /// Mirrors [`super::journal::LifecycleJournal::replay`] (static, pre-open).
    pub fn replay<F>(
        device: &Arc<dyn JournalDevice>,
        ring_head: u64,
        from_seq: u64,
        mut apply: F,
    ) -> Result<u64>
    where
        F: FnMut(LifecycleRecord) -> Result<()>,
    {
        let block_count = device.block_count();
        if block_count == 0 || ring_head >= block_count {
            return Ok(from_seq);
        }
        let mut last_seen = from_seq;
        scan_from(device.as_ref(), block_count, ring_head, |seq, body| {
            if seq > from_seq {
                apply(LifecycleRecord {
                    seq,
                    body: body.to_vec(),
                })?;
            }
            last_seen = last_seen.max(seq);
            Ok(())
        })?;
        Ok(last_seen)
    }

    /// Append `body`, flush, return the assigned seq. Fails with a clean
    /// [`MetaDbError::OutOfSpace`] when the record would overwrite live blocks
    /// (`JournalFull`) — the caller (range_delete / snapshot / etc.) propagates it.
    pub fn append(&mut self, body: &[u8]) -> Result<u64> {
        if body.len() > WAL_MAX_BODY {
            return Err(MetaDbError::InvalidArgument(format!(
                "lifecycle body too large: {} bytes exceeds WAL_MAX_BODY {WAL_MAX_BODY}",
                body.len()
            )));
        }
        let run = run_blocks(body.len());
        if run > self.block_count {
            return Err(MetaDbError::InvalidArgument(format!(
                "lifecycle record needs {run} blocks but ring holds only {}",
                self.block_count
            )));
        }
        let free = self.block_count - self.used;
        if run > free {
            return Err(MetaDbError::OutOfSpace); // ring full — see doc; caller propagates.
        }
        let seq = self.next_seq;

        // Frame + zero-pad to a whole number of blocks, then write only free
        // blocks [tail, tail+run) (never a live block), and flush.
        let mut framed = Vec::with_capacity((run as usize) * BLOCK_SIZE);
        encode_frame(&mut framed, seq, body);
        framed.resize((run as usize) * BLOCK_SIZE, 0);
        for i in 0..run {
            let idx = (self.tail + i) % self.block_count;
            let start = (i as usize) * BLOCK_SIZE;
            self.device
                .write_block(idx, &framed[start..start + BLOCK_SIZE])?;
        }
        self.device.flush()?;

        self.next_seq = self.next_seq.checked_add(1).ok_or(MetaDbError::OutOfSpace)?;
        self.tail = (self.tail + run) % self.block_count;
        self.used += run;
        Ok(seq)
    }

    /// Non-mutating counterpart of [`prune`](Self::prune): the `ring_head` this
    /// journal *would* advance to if pruned at `checkpoint_seq`, without freeing
    /// any block.
    ///
    /// This is the load-bearing half of the crash-safe prune ordering. The
    /// checkpoint path must: (1) `prune_target` → stamp the result into
    /// `manifest.journal_ring_head`; (2) commit the manifest durably; (3) only
    /// **then** call [`prune`](Self::prune) to free the blocks in memory. If it
    /// freed blocks before the head was durable, a wrapped append could reuse a
    /// block below the *persisted* head and a crash would leave replay scanning
    /// from a head that no longer starts a valid record. Deferring the in-memory
    /// free until the target head is durable keeps the persisted head pointing at
    /// a block that is never reused out from under it.
    pub fn prune_target(&self, checkpoint_seq: u64) -> Result<u64> {
        Ok(self.scan_prune(checkpoint_seq)?.0)
    }

    /// Advance the prune boundary past every record with `seq <= checkpoint_seq`,
    /// freeing their blocks for reuse. Returns the new `ring_head`. Rewrites
    /// nothing. Call ONLY after a manifest commit carrying
    /// [`prune_target`](Self::prune_target)'s head is durable (see that method).
    pub fn prune(&mut self, checkpoint_seq: u64) -> Result<u64> {
        let (target, freed) = self.scan_prune(checkpoint_seq)?;
        self.head = target;
        self.used -= freed;
        Ok(self.head)
    }

    /// Walk covered records from `head` at `checkpoint_seq`, returning
    /// `(target_head, freed_blocks)` without mutating. Appends only touch the
    /// tail, so this is stable against concurrent appends and identical whether
    /// called before or after the intervening manifest commit.
    fn scan_prune(&self, checkpoint_seq: u64) -> Result<(u64, u64)> {
        let mut pos = self.head;
        let mut remaining = self.used;
        let mut freed = 0u64;
        let mut block = vec![0u8; BLOCK_SIZE];
        loop {
            if remaining == 0 {
                break;
            }
            self.device.read_block(pos, &mut block)?;
            let (seq, len, _crc) = parse_header(&block);
            if seq == 0 || seq > checkpoint_seq {
                break;
            }
            let run = run_blocks(len as usize);
            if run > remaining {
                break; // defensive: never advance past the tail.
            }
            pos = (pos + run) % self.block_count;
            remaining -= run;
            freed += run;
        }
        Ok((pos, freed))
    }

    /// Next seq the ring will hand out.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Current prune boundary (persist as `manifest.journal_ring_head`).
    pub fn ring_head(&self) -> u64 {
        self.head
    }

    /// Live blocks currently held (diagnostics / tests).
    pub fn used_blocks(&self) -> u64 {
        self.used
    }
}

#[cfg(test)]
mod tests;
