//! WAL operation record codec.
//!
//! One WAL record body is a concatenation of tagged ops. Most ops have
//! a fixed payload size decided by the tag, so no length prefix is
//! needed. `DROP_SNAPSHOT` is variable-length; its payload begins with
//! a 4-byte count that tells the decoder how many page-ids follow.
//!
//! `TAKE_SNAPSHOT` still lives entirely in the manifest — a snapshot
//! that didn't make it to a durable manifest commit simply never
//! existed, so recovery has nothing to replay. `DROP_SNAPSHOT` is the
//! exception: it mutates page refcounts *and* the snapshot list, and
//! those two effects aren't atomic against a single manifest commit,
//! so the drop is logged to the WAL and re-driven on recovery.
//!
//! # Body layout
//!
//! ```text
//! [schema_version: 1B][tag: 1B][payload: per tag] × N
//! ```
//!
//! The first byte of every body is [`WAL_BODY_SCHEMA_VERSION`]. It is
//! distinct from every op tag (all tags have the high bit clear; the
//! schema byte has the high bit set), so a body written by an older
//! metadb binary — which started directly with a tag byte — is rejected
//! with [`MetaDbError::Corruption`] at decode time. The version is
//! covered by the WAL record CRC like every other body byte, so a flip
//! on the version byte is caught before recovery interprets it.
//!
//! Bump the version whenever the on-disk body shape changes (new op,
//! changed field widths, etc.). There is no backward-compatible
//! decoder — per SPEC §7 "不做 WAL 兼容", pre-bump WAL is a hard stop
//! and must be drained via the prior binary before upgrading.
//!
//! Tag table:
//!
//! | tag | mnemonic            | payload                                                                             |   size   |
//! |-----|---------------------|-------------------------------------------------------------------------------------|----------|
//! | 01  | `L2P_PUT`           | vol_ord (2 B BE) + lba (8 B BE) + value (36 B)                                      |    47    |
//! | 02  | `L2P_DELETE`        | vol_ord (2 B BE) + lba (8 B BE)                                                     |    10    |
//! | 03  | `L2P_REMAP`         | vol_ord (2 B BE) + lba (8 B BE) + new_value (36 B) + guard_tag (1 B) + [guard]      | 48 / 60  |
//! | 04  | `L2P_RANGE_DELETE`  | vol_ord (2 B BE) + start (8 B BE) + end (8 B BE) + count (4 B BE) + (lba,value)×count | 22+44n |
//! | 05  | `L2P_REMAP_RANGE`   | vol_ord (2 B BE) + start_lba (8 B BE) + count (4 B BE) + value×count (36 B each)    | 15+36n   |
//! | 10  | `DEDUP_PUT`         | hash (32 B) + value (28 B)                                                          |    60    |
//! | 11  | `DEDUP_DEL`         | hash (32 B)                                                                         |    32    |
//! | 12  | `DEDUP_REVERSE_PUT` | pba (8 B BE) + hash (32 B)                                                          |    40    |
//! | 13  | `DEDUP_REVERSE_DEL` | pba (8 B BE) + hash (32 B)                                                          |    40    |
//! | 14  | `DEDUP_PUT_GUARDED` | hash (32 B) + value (28 B) + pba_guard (8 B BE) + min_rc (4 B BE)                  |    72    |
//! | 15  | `DEDUP_COMPARE_DELETE` | hash (32 B) + old_value (28 B)                                                   |    60    |
//! | 16  | `DEDUP_COMPARE_PUT` | hash (32 B) + old_value (28 B) + new_value (28 B)                                  |    88    |
//! | 20  | `INCREF`            | pba (8 B BE) + delta (4 B BE)                                                       |    12    |
//! | 21  | `DECREF`            | pba (8 B BE) + delta (4 B BE)                                                       |    12    |
//! | 30  | `DROP_SNAPSHOT`     | id (8 B BE) + page_count (4 B BE) + pid×page_count + decref_count (4 B BE) + pba×decref_count | 16+8(n+m) |
//! | 40  | `CREATE_VOLUME`     | ord (2 B BE) + shard_count (4 B BE)                                                 |     6    |
//! | 41  | `DROP_VOLUME`       | ord (2 B BE) + count (4 B BE) + pid×count                                           |   6+8n   |
//! | 42  | `CLONE_VOLUME`      | src_ord (2 B BE) + new_ord (2 B BE) + snap_id (8 B BE) + shard_count (4 B BE) + pid×shard_count | 16+8n |
//!
//! `L2P_REMAP` guard: tag `0x00` = no guard (payload ends); tag `0x01`
//! = guarded, followed by `pba (8 B BE) + min_rc (4 B BE)` — 12 more
//! bytes. Apply reads `refcount(pba)` and skips the whole op if the
//! value is `< min_rc` (SPEC §3.1). In v2 the `new_value` field is 36 B
//! (28 B BlockmapValue + 8 B seq_guard), so total record sizes including
//! the tag byte are 48 B unguarded and 60 B guarded.
//!
//! `L2P_*` value fields are 36 B in schema 0xB2+ (was 28 B in 0xB1). The
//! trailing 8 B is a big-endian commit seq used by the apply path's
//! seq_guard CAS check; see [`paged::format::L2P_SEQ_OFFSET`]. Schema
//! bumps 0xB1 → 0xB2 → 0xB3 are flag days — old WAL bodies are rejected
//! on recovery and must be drained with the prior binary before upgrading.
//!
//! v3 (0xB3): retired the legacy `DedupReversePut` / `DedupReverseDelete`
//! op codes (tags 0x12 / 0x13) along with the `paged_reverse` module.
//! Onyx switched to promote-on-verified-hit; the persistent reverse index
//! has been inert in production since then, and refcount→0 cleanup uses
//! old-mapping read-back to recompute the hash instead.
//!
//! Phase 7 commit 6 put `vol_ord` on L2P ops so apply can route them to
//! the right per-volume shard group. `vol_ord = 0` is the bootstrap
//! volume; until commit 8 introduces real volume creation the live
//! `commit_ops` path only ever emits 0. Tags 0x40+ are per-volume
//! lifecycle ops whose apply semantics land with commit 8 / 9; their
//! encode/decode was wired up in Phase A, and the apply path currently
//! returns `Corruption` for them because a Phase B binary should never
//! see a record it didn't emit.
//!
//! Keys use big-endian so byte order matches numeric order; that's
//! consistent with the rest of metadb.

use crate::dedup_types::{DedupValue, Hash8};
use crate::error::{MetaDbError, Result};
use crate::paged::L2pValue;
use crate::types::{Lba, PageId, Pba, SnapshotId, VolumeOrdinal};

/// On-disk body schema version. Written as the first byte of every
/// WAL record body; decoders reject any other value.
///
/// Value `0xB1` was chosen for two properties:
/// 1. The high bit is set, distinguishing it from every op tag (all
///    tags are `≤ 0x42`). A legacy body that starts with a tag byte
///    fails the version check deterministically rather than being
///    silently reinterpreted.
/// 2. The low nibble (`1`) encodes the human-readable version number,
///    so a future bump becomes `0xB2`, `0xB3`, … and keeps the property
///    above.
///
/// Phase A bumped the implicit "no prefix" format to v1 (0xB1).
/// v2 (0xB2): L2pValue grew 28 → 36 B with a per-LBA seq trailer.
/// v3 (0xB3): retired `DedupReversePut` / `DedupReverseDelete` (tags
/// 0x12 / 0x13) along with the legacy `paged_reverse` module.
/// v4 (0xB4): added `L2pRemapRange` (tag 0x05) — range-shaped variant
/// of `L2pRemap` that carries N contiguous LBAs in one record so the
/// commit-side bucket assembly and WAL bytes amortize across the range.
/// Stage 1 emits it only from the passthrough writer path; packed and
/// dedup-hit paths keep using per-LBA `L2pRemap`.
pub const WAL_BODY_SCHEMA_VERSION: u8 = 0xB4;

pub const TAG_L2P_PUT: u8 = 0x01;
pub const TAG_L2P_DELETE: u8 = 0x02;
pub const TAG_L2P_REMAP: u8 = 0x03;
pub const TAG_L2P_RANGE_DELETE: u8 = 0x04;
pub const TAG_L2P_REMAP_RANGE: u8 = 0x05;

/// Maximum `captured` entries in a single `L2pRangeDelete` WAL record.
/// Larger ranges are auto-split by [`Db::range_delete`] so the WAL
/// body stays bounded (SPEC §3.2). The limit comes from the 4-byte
/// count field in the on-disk encoding; 65536 is well below `u32::MAX`
/// and keeps the largest body under ~1 MiB (`22 + 16*65536`).
pub const MAX_RANGE_DELETE_CAPTURED: usize = 65536;

/// Maximum LBAs in a single `L2pRemapRange` WAL record. Onyx's passthrough
/// writer is bounded by `coalesce_max_lbas = 32`; 4096 leaves a comfortable
/// defensive ceiling without bloating the worst-case body
/// (`15 + 36*4096 ≈ 144 KiB`). Decoders reject larger counts so a corrupt
/// length prefix can't drive an allocation explosion.
pub const MAX_REMAP_RANGE_LBAS: usize = 4096;

/// `L2P_REMAP` guard discriminator: no guard — apply runs
/// unconditionally, matching `L2pPut + Incref + Decref` fused into one
/// record.
pub const L2P_REMAP_GUARD_NONE: u8 = 0x00;
/// `L2P_REMAP` guard discriminator: guarded — apply reads
/// `refcount(pba)` first and skips the op if `< min_rc`. Used by
/// onyx's dedup hit path so a dedup target that was already freed
/// between plan and apply cannot be re-linked.
pub const L2P_REMAP_GUARD_SOME: u8 = 0x01;
pub const TAG_DEDUP_PUT: u8 = 0x10;
pub const TAG_DEDUP_DELETE: u8 = 0x11;
// Tags 0x12 / 0x13 were `DEDUP_REVERSE_PUT` / `DEDUP_REVERSE_DELETE` and
// retired in schema 0xB3. Do not reuse — a 0xB2 segment that survives
// past the WAL_BODY_SCHEMA_VERSION reject would otherwise decode as a
// different op.
pub const TAG_DEDUP_PUT_GUARDED: u8 = 0x14;
pub const TAG_DEDUP_COMPARE_DELETE: u8 = 0x15;
pub const TAG_DEDUP_COMPARE_PUT: u8 = 0x16;
pub const TAG_INCREF: u8 = 0x20;
pub const TAG_DECREF: u8 = 0x21;
pub const TAG_DROP_SNAPSHOT: u8 = 0x30;
pub const TAG_CREATE_VOLUME: u8 = 0x40;
pub const TAG_DROP_VOLUME: u8 = 0x41;
pub const TAG_CLONE_VOLUME: u8 = 0x42;

/// One mutation op as stored in a WAL record body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalOp {
    /// L2P put targeted at `vol_ord`. Phase B commit 6 always sets
    /// `vol_ord = 0` (bootstrap volume); commit 7 exposes per-volume
    /// write APIs that can emit non-zero ordinals.
    L2pPut {
        vol_ord: VolumeOrdinal,
        lba: Lba,
        value: L2pValue,
    },
    /// L2P delete, same `vol_ord` story as [`L2pPut`](Self::L2pPut).
    L2pDelete {
        vol_ord: VolumeOrdinal,
        lba: Lba,
    },
    /// Onyx-adapter hot path: fuse L2P put + refcount decref(old) +
    /// refcount incref(new) into a single WAL record. Replaces the
    /// `L2pPut + Incref + Decref` triple that the pre-metadb onyx
    /// writer would emit for every remap.
    ///
    /// `new_value`'s head 8 bytes are the target PBA (BE) by the
    /// `BlockmapValue` contract (SPEC §3.1); the apply path uses
    /// [`L2pValue::head_pba`](crate::paged::L2pValue::head_pba) on it
    /// and on the previous value to drive the decref/incref decision
    /// table.
    ///
    /// `guard`: `Some((pba, min_rc))` = apply reads `refcount(pba)`
    /// first and returns an early `ApplyOutcome::L2pRemap { applied:
    /// false, .. }` if the value is strictly less than `min_rc`.
    /// `None` = unconditional apply. Used by dedup hit to refuse the
    /// remap when the intended target was concurrently freed.
    L2pRemap {
        vol_ord: VolumeOrdinal,
        lba: Lba,
        new_value: L2pValue,
        guard: Option<(Pba, u32)>,
    },
    /// Onyx-adapter bulk delete over `[start, end)` for one volume.
    /// The `captured: Vec<(Lba, L2pValue)>` is the `(lba, full_value)`
    /// list scanned at plan time — apply emits one refcount decref per
    /// entry, suppressed when any live snap pins the *exact* full
    /// 28-byte value at this lba (birth/death LSN + precise compare).
    /// Carrying the full value (vs. just `head_pba`) lets the snap-pin
    /// check match audit semantics: distinct `(V, lba, value_28B)`
    /// tuples count separately even when they share `head_pba` (e.g.
    /// salt differs).
    ///
    /// The plan-time capture makes replay deterministic: on restart
    /// the same decrefs fire regardless of what the L2P tree looks
    /// like post-replay. `captured.len() ≤ MAX_RANGE_DELETE_CAPTURED`;
    /// larger ranges are auto-split by [`Db::range_delete`] into
    /// multiple consecutive records.
    L2pRangeDelete {
        vol_ord: VolumeOrdinal,
        start: Lba,
        end: Lba,
        captured: Vec<(Lba, L2pValue)>,
    },
    /// Range-shaped variant of [`L2pRemap`](Self::L2pRemap): apply the
    /// same per-LBA remap semantics to `[start_lba .. start_lba + values.len())`
    /// of one volume in a single WAL record. Always unguarded; the dedup
    /// hit path keeps using per-LBA `L2pRemap` because dedup hits are
    /// scattered (different LBAs → different existing PBAs) and benefit
    /// nothing from range coalescing.
    ///
    /// Each `values[i]` is the full 36 B `L2pValue` for `start_lba + i`.
    /// Per `BlockmapValue` layout (SPEC §3.1) the head 8 B is the target
    /// PBA — same contract `L2pRemap` already relies on for the decref /
    /// incref decision table.
    ///
    /// `values.len() ≤ MAX_REMAP_RANGE_LBAS`; decoders reject larger
    /// counts. Apply re-uses the existing leaf-run batching in
    /// `apply_l2p_bucket` — N LBAs that share one `leaf_idx = lba >> 7`
    /// collapse to one tree descend + CoW cascade.
    L2pRemapRange {
        vol_ord: VolumeOrdinal,
        start_lba: Lba,
        values: Box<[L2pValue]>,
    },
    DedupPut {
        hash: Hash8,
        value: DedupValue,
    },
    DedupPutGuarded {
        hash: Hash8,
        value: DedupValue,
        pba_guard: Pba,
        min_rc: u32,
    },
    DedupDelete {
        hash: Hash8,
    },
    /// Tombstone `hash` only if the current forward index value is
    /// exactly `old_value`.
    DedupCompareDelete {
        hash: Hash8,
        old_value: DedupValue,
    },
    /// Replace `hash` with `new_value` only if the current forward
    /// index value is exactly `old_value`.
    DedupComparePut {
        hash: Hash8,
        old_value: DedupValue,
        new_value: DedupValue,
    },
    Incref {
        pba: Pba,
        delta: u32,
    },
    Decref {
        pba: Pba,
        delta: u32,
    },
    /// Drop snapshot `id` by decrementing the header refcount of every
    /// page reachable from the snapshot's shard roots (plus the
    /// snapshot's `l2p_roots_page` metadata page). `pages` lists every
    /// page to touch, enumerated via a read-only structural walk at
    /// plan time. The walk is safe to pre-compute because the snapshot's
    /// tree topology is immutable — COW copies pages, it never mutates
    /// them in place.
    ///
    /// Apply semantics (see `apply_op_bare`): for each pid, read the
    /// page, decrement rc by 1, stamp `generation = lsn`, rewrite as
    /// Free if the new rc is 0. Idempotent on replay via the generation
    /// check (`page.generation >= lsn ⇒ skip`).
    ///
    /// `pba_decrefs` is S4's leaf-rc-suppress compensation: every pba
    /// the snapshot still referenced but the current tree has diverged
    /// from (`DiffEntry::RemovedInB` / `DiffEntry::Changed` between
    /// snap root and current root) — one `decref(pba, 1)` per entry
    /// during apply (SPEC §3.3). Ordering doesn't matter (refcount is
    /// commutative) but duplicates are retained: the same pba can
    /// appear N times in the list and each produces one decref, same
    /// as onyx's packed-slot multi-LBA share pattern.
    DropSnapshot {
        id: SnapshotId,
        pages: Vec<PageId>,
        pba_decrefs: Vec<Pba>,
    },
    /// Register a fresh volume with `shard_count` empty shard roots. The
    /// apply path allocates the per-shard paged-tree roots; the manifest-
    /// level insertion into the `volumes` table is done by the caller of
    /// `apply_op_bare` (mirrors the `DropSnapshot` split).
    CreateVolume {
        ord: VolumeOrdinal,
        shard_count: u32,
    },
    /// Drop volume `ord`, decrementing the refcount of every page in
    /// `pages` (collected at plan time via a read-only walk of the
    /// volume's shard trees). Idempotent on replay via the
    /// `page.generation >= lsn` check, same protocol as `DropSnapshot`.
    DropVolume {
        ord: VolumeOrdinal,
        pages: Vec<PageId>,
    },
    /// VDO-style writable clone: create `new_ord` whose initial shard
    /// roots are the `src_shard_roots` inlined here (taken from the
    /// source snapshot's shard_roots at plan time). Apply increfs every
    /// root pid and inserts the volume into the in-memory map; the
    /// manifest-level VolumeEntry insertion is the caller's job.
    ///
    /// Recording `src_shard_roots` inline (rather than re-reading them
    /// from the manifest at replay time) keeps replay oblivious to
    /// later `DropSnapshot` records that may have raced against this
    /// clone in the source WAL.
    CloneVolume {
        src_ord: VolumeOrdinal,
        new_ord: VolumeOrdinal,
        src_snap_id: SnapshotId,
        src_shard_roots: Vec<PageId>,
    },
}

mod codec;

pub use codec::{decode_body, encode_body, encoded_body_len, try_encode_body};

#[cfg(test)]
mod tests;
