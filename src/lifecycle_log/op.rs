//! Lifecycle op enum and wire format.
//!
//! The buffer cannot derive these ops on replay (they have no user IO
//! behind them, or they need a deterministic frozen plan that must
//! match across crashes). Each op carries everything `apply_op` would
//! need to redo it idempotently — frozen lists, page generations, etc.
//!
//! # Wire format
//!
//! Each record body starts with one tag byte; the rest is op-specific.
//! Records are framed with [`super::record`].
//!
//! Numeric encoding is little-endian (matches manifest convention).
//! Variable-length lists are prefixed by a `u32` count.
//!
//! # Phase A
//!
//! The op enum and codec are pinned in Phase A so Phase B can write
//! the shadow-validation path without further shape churn. Apply
//! functions live in `crate::db::apply_lifecycle` (added in Phase B).

use crate::error::{MetaDbError, Result};
use crate::types::{Lba, PageId, Pba, SnapshotId, VolumeOrdinal};

// Lba / Pba / PageId / SnapshotId are u64 aliases; VolumeOrdinal is u16.
// We keep the named imports for variant signatures so reader intent is clear.
const _: fn() = || {
    let _: Lba = 0;
    let _: Pba = 0;
    let _: PageId = 0;
    let _: SnapshotId = 0;
    let _: VolumeOrdinal = 0;
};

/// Tag byte that prefixes each lifecycle record body.
pub const TAG_TAKE_SNAPSHOT: u8 = 0x01;
pub const TAG_DROP_SNAPSHOT: u8 = 0x30;
pub const TAG_CREATE_VOLUME: u8 = 0x40;
pub const TAG_DROP_VOLUME: u8 = 0x41;
pub const TAG_CLONE_VOLUME: u8 = 0x42;
pub const TAG_PROMOTION_CHUNK: u8 = 0x44;
pub const TAG_PROMOTION_COMPLETE: u8 = 0x45;
pub const TAG_DISCARD: u8 = 0x50;

/// Schema version byte stamped at the head of every lifecycle segment
/// (file offset 0 of the segment, separate from the per-record frame).
/// Bumped on incompatible body changes the same way the WAL does it.
pub const LIFECYCLE_BODY_SCHEMA_VERSION: u8 = 0xC0;

/// All op variants the lifecycle journal carries. Each variant
/// captures everything `apply` needs to redo the op idempotently —
/// frozen plan lists, page generations, etc.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleOp {
    /// Pre-allocate snapshot id `id` for volume `vol_ord`. The
    /// per-shard L2P root incref runs in apply; the manifest record is
    /// inserted by the caller after apply succeeds.
    TakeSnapshot {
        id: SnapshotId,
        vol_ord: VolumeOrdinal,
        l2p_shard_roots: Vec<PageId>,
    },
    /// Decref every page in `pages` (already frozen at plan time) and
    /// retire the snapshot entry. Replay-idempotent via the
    /// `page.generation >= lsn` check in `apply_op`.
    DropSnapshot {
        id: SnapshotId,
        pages: Vec<PageId>,
        pba_decrefs: Vec<Pba>,
    },
    /// Register a fresh volume with `shard_count` empty L2P shard
    /// roots. Apply allocates per-shard paged-tree roots; the
    /// manifest-level VolumeEntry insertion is the caller's job.
    CreateVolume {
        ord: VolumeOrdinal,
        shard_count: u32,
    },
    /// Drop volume `ord`, decref every page in `pages`. Same
    /// idempotency protocol as `DropSnapshot`.
    DropVolume {
        ord: VolumeOrdinal,
        pages: Vec<PageId>,
    },
    /// VDO-style writable clone: create `new_ord` whose initial shard
    /// roots are the inlined `src_shard_roots`. Inlining keeps replay
    /// oblivious to later `DropSnapshot` records that race against
    /// this clone.
    CloneVolume {
        src_ord: VolumeOrdinal,
        new_ord: VolumeOrdinal,
        src_snap_id: SnapshotId,
        src_shard_roots: Vec<PageId>,
    },
    /// One chunk of the background clone-promotion walker (
    /// [[no-refcount-hot-path-design]] Phase 4 Step 5). Apply increfs
    /// every PBA in `pba_increfs` and bumps `promotion_cursor` to
    /// `next_cursor` (`None` ⇒ walker reached end of clone).
    /// Idempotency rides on the cursor: a replay where the cursor is
    /// already past `next_cursor` skips the incref pass.
    PromotionChunk {
        vol_ord: VolumeOrdinal,
        pba_increfs: Vec<Pba>,
        next_cursor: Option<Lba>,
    },
    /// Walker's final emission — clears `parent_vol_ord` and
    /// `promotion_cursor` on `vol_ord`. Idempotent.
    PromotionComplete { vol_ord: VolumeOrdinal },
    /// User-side TRIM / range-delete over `[start_lba, start_lba + count)`.
    /// Apply clears the range and decrefs freed PBAs; the captured per-LBA
    /// list is rebuilt from the current L2P at apply time — replay
    /// after crash is naturally idempotent because the same LBAs are
    /// already empty.
    Discard {
        vol_ord: VolumeOrdinal,
        start_lba: Lba,
        count: u32,
    },
}

impl LifecycleOp {
    /// Tag byte at offset 0 of the encoded body.
    pub fn tag(&self) -> u8 {
        match self {
            LifecycleOp::TakeSnapshot { .. } => TAG_TAKE_SNAPSHOT,
            LifecycleOp::DropSnapshot { .. } => TAG_DROP_SNAPSHOT,
            LifecycleOp::CreateVolume { .. } => TAG_CREATE_VOLUME,
            LifecycleOp::DropVolume { .. } => TAG_DROP_VOLUME,
            LifecycleOp::CloneVolume { .. } => TAG_CLONE_VOLUME,
            LifecycleOp::PromotionChunk { .. } => TAG_PROMOTION_CHUNK,
            LifecycleOp::PromotionComplete { .. } => TAG_PROMOTION_COMPLETE,
            LifecycleOp::Discard { .. } => TAG_DISCARD,
        }
    }
}

/// Encode a single op into a fresh `Vec`. Used by the journal writer.
pub fn encode(op: &LifecycleOp) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(op.tag());
    match op {
        LifecycleOp::TakeSnapshot {
            id,
            vol_ord,
            l2p_shard_roots,
        } => {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&vol_ord.to_le_bytes());
            put_u32(&mut out, l2p_shard_roots.len() as u32);
            for pid in l2p_shard_roots {
                out.extend_from_slice(&pid.to_le_bytes());
            }
        }
        LifecycleOp::DropSnapshot {
            id,
            pages,
            pba_decrefs,
        } => {
            out.extend_from_slice(&id.to_le_bytes());
            put_u32(&mut out, pages.len() as u32);
            for pid in pages {
                out.extend_from_slice(&pid.to_le_bytes());
            }
            put_u32(&mut out, pba_decrefs.len() as u32);
            for pba in pba_decrefs {
                out.extend_from_slice(&pba.to_le_bytes());
            }
        }
        LifecycleOp::CreateVolume { ord, shard_count } => {
            out.extend_from_slice(&ord.to_le_bytes());
            out.extend_from_slice(&shard_count.to_le_bytes());
        }
        LifecycleOp::DropVolume { ord, pages } => {
            out.extend_from_slice(&ord.to_le_bytes());
            put_u32(&mut out, pages.len() as u32);
            for pid in pages {
                out.extend_from_slice(&pid.to_le_bytes());
            }
        }
        LifecycleOp::CloneVolume {
            src_ord,
            new_ord,
            src_snap_id,
            src_shard_roots,
        } => {
            out.extend_from_slice(&src_ord.to_le_bytes());
            out.extend_from_slice(&new_ord.to_le_bytes());
            out.extend_from_slice(&src_snap_id.to_le_bytes());
            put_u32(&mut out, src_shard_roots.len() as u32);
            for pid in src_shard_roots {
                out.extend_from_slice(&pid.to_le_bytes());
            }
        }
        LifecycleOp::PromotionChunk {
            vol_ord,
            pba_increfs,
            next_cursor,
        } => {
            out.extend_from_slice(&vol_ord.to_le_bytes());
            put_u32(&mut out, pba_increfs.len() as u32);
            for pba in pba_increfs {
                out.extend_from_slice(&pba.to_le_bytes());
            }
            match next_cursor {
                Some(lba) => {
                    out.push(1);
                    out.extend_from_slice(&lba.to_le_bytes());
                }
                None => out.push(0),
            }
        }
        LifecycleOp::PromotionComplete { vol_ord } => {
            out.extend_from_slice(&vol_ord.to_le_bytes());
        }
        LifecycleOp::Discard {
            vol_ord,
            start_lba,
            count,
        } => {
            out.extend_from_slice(&vol_ord.to_le_bytes());
            out.extend_from_slice(&start_lba.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
        }
    }
    out
}

/// Decode a single op from a complete record body. Returns
/// `Err(MetaDbError::Corruption)` for short / unknown tag / truncated
/// bodies. Recovery surfaces these as fatal if they appear before the
/// final segment's last record, or as a torn-tail truncation if at the
/// final position (mirrors the WAL convention).
pub fn decode(body: &[u8]) -> Result<LifecycleOp> {
    let mut c = Cursor::new(body);
    let tag = c.u8()?;
    let op = match tag {
        TAG_TAKE_SNAPSHOT => LifecycleOp::TakeSnapshot {
            id: c.u64()?,
            vol_ord: c.u16()?,
            l2p_shard_roots: c.u64_vec()?,
        },
        TAG_DROP_SNAPSHOT => LifecycleOp::DropSnapshot {
            id: c.u64()?,
            pages: c.u64_vec()?,
            pba_decrefs: c.u64_vec()?,
        },
        TAG_CREATE_VOLUME => LifecycleOp::CreateVolume {
            ord: c.u16()?,
            shard_count: c.u32()?,
        },
        TAG_DROP_VOLUME => LifecycleOp::DropVolume {
            ord: c.u16()?,
            pages: c.u64_vec()?,
        },
        TAG_CLONE_VOLUME => LifecycleOp::CloneVolume {
            src_ord: c.u16()?,
            new_ord: c.u16()?,
            src_snap_id: c.u64()?,
            src_shard_roots: c.u64_vec()?,
        },
        TAG_PROMOTION_CHUNK => {
            let vol_ord = c.u16()?;
            let pba_increfs = c.u64_vec()?;
            let next_cursor = match c.u8()? {
                0 => None,
                1 => Some(c.u64()?),
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "lifecycle PromotionChunk: bad next_cursor discriminator {other}"
                    )));
                }
            };
            LifecycleOp::PromotionChunk {
                vol_ord,
                pba_increfs,
                next_cursor,
            }
        }
        TAG_PROMOTION_COMPLETE => LifecycleOp::PromotionComplete {
            vol_ord: c.u16()?,
        },
        TAG_DISCARD => LifecycleOp::Discard {
            vol_ord: c.u16()?,
            start_lba: c.u64()?,
            count: c.u32()?,
        },
        other => {
            return Err(MetaDbError::Corruption(format!(
                "lifecycle op: unknown tag 0x{other:02x}"
            )));
        }
    };
    if !c.is_at_end() {
        return Err(MetaDbError::Corruption(format!(
            "lifecycle op: {} trailing bytes after tag 0x{tag:02x}",
            c.remaining(),
        )));
    }
    Ok(op)
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

struct Cursor<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, off: 0 }
    }
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.off)
    }
    fn is_at_end(&self) -> bool {
        self.off >= self.buf.len()
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .off
            .checked_add(n)
            .ok_or_else(|| MetaDbError::Corruption("lifecycle op: length overflow".into()))?;
        if end > self.buf.len() {
            return Err(MetaDbError::Corruption(format!(
                "lifecycle op: short read ({} bytes left, wanted {n})",
                self.buf.len() - self.off,
            )));
        }
        let s = &self.buf[self.off..end];
        self.off = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u64_vec(&mut self) -> Result<Vec<u64>> {
        let n = self.u32()? as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.u64()?);
        }
        Ok(out)
    }
}
