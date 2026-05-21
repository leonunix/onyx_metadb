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
//! | tag | mnemonic            | payload                                                                                          |    size    |
//! |-----|---------------------|--------------------------------------------------------------------------------------------------|------------|
//! | 01  | `L2P_PUT`           | vol_ord (2 B BE) + lba (8 B BE) + value (36 B)                                                   |    47      |
//! | 02  | `L2P_DELETE`        | vol_ord (2 B BE) + lba (8 B BE)                                                                  |    10      |
//! | 03  | `L2P_REMAP`         | vol_ord (2 B BE) + lba (8 B BE) + new_value (36 B) + guard_tag (1 B) + [guard]                   |  48 / 60   |
//! | 04  | `L2P_RANGE_DELETE`  | vol_ord (2 B BE) + start (8 B BE) + end (8 B BE) + count (4 B BE) + (lba,value)×count            |  22+44n    |
//! | 05  | `L2P_REMAP_RANGE`   | vol_ord (2 B BE) + start_lba (8 B BE) + count (4 B BE) + value×count (36 B each)                 |  15+36n    |
//! | 10  | `DEDUP_PUT`         | hash (8 B) + value (28 B) + old_pba_tag (1 B) + [old_pba (8 B BE)]                               |  38 / 46   |
//! | 11  | `DEDUP_DEL`         | hash (8 B) + old_pba_tag (1 B) + [old_pba (8 B BE)]                                              |  10 / 18   |
//! | 14  | `DEDUP_PUT_GUARDED` | hash (8 B) + value (28 B) + pba_guard (8 B BE) + min_rc (4 B BE) + old_pba_tag (1 B) + [old_pba] |  50 / 58   |
//! | 15  | `DEDUP_COMPARE_DELETE` | hash (8 B) + old_value (28 B)                                                                 |    37      |
//! | 16  | `DEDUP_COMPARE_PUT` | hash (8 B) + old_value (28 B) + new_value (28 B)                                                 |    65      |
//! | 30  | `DROP_SNAPSHOT`     | id (8 B BE) + page_count (4 B BE) + pid×page_count + decref_count (4 B BE) + pba×decref_count | 16+8(n+m) |
//! | 40  | `CREATE_VOLUME`     | ord (2 B BE) + shard_count (4 B BE)                                                 |     6    |
//! | 41  | `DROP_VOLUME`       | ord (2 B BE) + count (4 B BE) + pid×count                                           |   6+8n   |
//! | 42  | `CLONE_VOLUME`      | src_ord (2 B BE) + new_ord (2 B BE) + snap_id (8 B BE) + shard_count (4 B BE) + pid×shard_count | 16+8n |
//! | 43  | `FREE_PBAS`         | vol_ord (2 B BE) + count (4 B BE) + pba×count                                       |   6+8n   |
//! | 44  | `PROMOTION_CHUNK`   | vol_ord (2 B BE) + count (4 B BE) + pba×count + cursor_tag (1 B) + [cursor (8 B BE)] | 7/15+8n |
//! | 45  | `PROMOTION_COMPLETE`| vol_ord (2 B BE)                                                                    |     2    |
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
/// v5 (0xB5): added `FreePbas` (tag 0x43) — Phase 3 (no-refcount-hot-path)
/// plumbing for the Lineage GC consumer to batch-emit PBA free decisions
/// once Phase 5 pulls hot-path refcount writes out of `apply_l2p_remap`.
/// The current binary emits this op only as a forward-looking smoke test;
/// production GC paths in Phase 3 are gated default-off behind a config
/// flag. Apply is idempotent (decref-if-positive + free-on-zero).
/// v6 (0xB6): added `PromotionChunk` (tag 0x44) + `PromotionComplete`
/// (tag 0x45) — Phase 4 Step 5 background clone-promotion walker. The
/// walker emits per-chunk incref batches for the clone's shared lineage
/// while advancing a per-volume cursor; when the cursor reaches the end
/// it emits `PromotionComplete` to clear `parent_vol_ord` /
/// `promotion_cursor` so the parent's Lineage GC can resume.
/// v7 (0xB7): [[no-refcount-hot-path-design]] Phase 5 retired the
/// standalone `Incref` / `Decref` ops (tags 0x20 / 0x21) along with the
/// `Db::incref_pba` / `Db::decref_pba` API. Global PBA refcount is now
/// only mutated by `FreePbas` (Lineage GC), `PromotionChunk` (clone
/// promotion walker), and the volume-lifecycle ops — never per-write.
/// Tags 0x20 / 0x21 are reserved and must not be reused.
/// v8 (0xB8): [[no-refcount-hot-path-design]] Phase 5 follow-up.
/// `DedupPut` / `DedupPutGuarded` / `DedupDelete` now carry an embedded
/// `old_pba: Option<Pba>` captured at `Transaction::commit` time. Apply
/// uses this value verbatim to compute the rc decref/incref pair instead
/// of re-reading the on-disk dedup_index. The on-disk dedup_index data
/// pages are written eagerly per op (only the meta page is checkpoint-
/// gated; see `lifecycle.rs::open_with_config_and_faults`), so WAL
/// replay's `apply_op_bare` previously observed a state that already
/// reflected post-checkpoint ops and computed the wrong rc deltas. The
/// embedded value breaks that asymmetric-persistence trap. Encoding adds
/// a 1-byte discriminator (`OLD_PBA_NONE` / `OLD_PBA_SOME`) followed by
/// 8 bytes when present.
pub const WAL_BODY_SCHEMA_VERSION: u8 = 0xB8;

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

/// `DedupPut` / `DedupPutGuarded` / `DedupDelete` `old_pba`
/// discriminator: no prior entry. Apply stages incref(new_pba) only
/// (DedupPut/Guarded) or skips rc work entirely (DedupDelete).
pub const OLD_PBA_NONE: u8 = 0x00;
/// `DedupPut` / `DedupPutGuarded` / `DedupDelete` `old_pba`
/// discriminator: prior entry existed; 8 bytes BE follow with the
/// previous `value.head_pba()` captured at `Transaction::commit` time.
/// Apply uses this PBA for the decref side of the rc work without
/// re-reading the on-disk dedup_index.
pub const OLD_PBA_SOME: u8 = 0x01;
pub const TAG_DEDUP_PUT: u8 = 0x10;
pub const TAG_DEDUP_DELETE: u8 = 0x11;
// Tags 0x12 / 0x13 were `DEDUP_REVERSE_PUT` / `DEDUP_REVERSE_DELETE` and
// retired in schema 0xB3. Do not reuse — a 0xB2 segment that survives
// past the WAL_BODY_SCHEMA_VERSION reject would otherwise decode as a
// different op.
pub const TAG_DEDUP_PUT_GUARDED: u8 = 0x14;
pub const TAG_DEDUP_COMPARE_DELETE: u8 = 0x15;
pub const TAG_DEDUP_COMPARE_PUT: u8 = 0x16;
// Tags 0x20 / 0x21 were `INCREF` / `DECREF` and retired in schema 0xB7
// ([[no-refcount-hot-path-design]] Phase 5). Do not reuse — a 0xB6
// segment that survives past the `WAL_BODY_SCHEMA_VERSION` reject would
// otherwise decode as a different op.
pub const TAG_DROP_SNAPSHOT: u8 = 0x30;
pub const TAG_CREATE_VOLUME: u8 = 0x40;
pub const TAG_DROP_VOLUME: u8 = 0x41;
pub const TAG_CLONE_VOLUME: u8 = 0x42;
pub const TAG_FREE_PBAS: u8 = 0x43;
pub const TAG_PROMOTION_CHUNK: u8 = 0x44;
pub const TAG_PROMOTION_COMPLETE: u8 = 0x45;

/// Cursor discriminator on [`WalOp::PromotionChunk`]: payload ends after
/// the PBA list; the walker has reached the end of the clone's L2P and
/// the apply path will clear `promotion_cursor` to `None`.
pub const PROMOTION_CHUNK_CURSOR_NONE: u8 = 0x00;
/// Cursor discriminator on [`WalOp::PromotionChunk`]: an 8-byte
/// big-endian LBA follows, recording the next LBA the walker intends
/// to visit on its subsequent chunk.
pub const PROMOTION_CHUNK_CURSOR_SOME: u8 = 0x01;

/// Maximum PBAs in a single [`WalOp::FreePbas`] record. The Lineage GC
/// worker batches at most one dead-list segment's worth of records per
/// emission, and a Phase 2 segment holds ≤ ~10k records (166 in the head
/// page + 168 per continuation page × `seg_page_count`). 65536 gives
/// plenty of slack for future segment sizing without bloating the record
/// (`6 + 8 * 65536 ≈ 512 KiB`). Decoders reject larger counts to bound
/// allocation under a corrupt length prefix.
pub const MAX_FREE_PBAS_PER_OP: usize = 65536;

/// Maximum PBAs per [`WalOp::PromotionChunk`]. The clone-promotion walker
/// is paced one chunk per `commit_ops` call; large chunks let the walker
/// amortize WAL framing and apply-gate cost, but a too-large chunk both
/// inflates worst-case body bytes (`15 + 8 * 65536 ≈ 512 KiB` at the cap)
/// and lengthens the time an apply holds the global apply gate. 65536
/// matches `MAX_FREE_PBAS_PER_OP` so both lineage-traffic ops share the
/// same backpressure bound; the walker driver clamps its chunk size well
/// below this in production.
pub const MAX_PROMOTION_CHUNK_PBAS: usize = 65536;

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
    /// Insert or overwrite `hash → value` in the dedup_index.
    /// `old_pba = Some(p)` means the dedup_index entry for `hash`
    /// before this op pointed at PBA `p` (whose
    /// `value.head_pba() != new_value.head_pba()` resolution is the
    /// caller's job — when they match the rc work collapses to a
    /// no-op, otherwise apply stages `decref(old_pba) + incref(new)`).
    /// `old_pba = None` means there was no prior entry; apply stages
    /// `incref(new)` only. Captured at `Transaction::commit` so apply
    /// is deterministic from the WAL alone — required for replay
    /// correctness given dedup_index data pages are written eagerly
    /// per op.
    DedupPut {
        hash: Hash8,
        value: DedupValue,
        old_pba: Option<Pba>,
    },
    DedupPutGuarded {
        hash: Hash8,
        value: DedupValue,
        pba_guard: Pba,
        min_rc: u32,
        old_pba: Option<Pba>,
    },
    /// Tombstone `hash`. `old_pba = Some(p)` means the dedup_index
    /// entry for `hash` before this op pointed at PBA `p`; apply
    /// stages `decref(p)`. `old_pba = None` is a no-op delete (no
    /// prior entry — happens when an upstream layer issued a defensive
    /// `delete_dedup` on a hash that was never put).
    DedupDelete {
        hash: Hash8,
        old_pba: Option<Pba>,
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
    /// Phase 3 (no-refcount-hot-path) plumbing for the Lineage GC
    /// consumer. Emits the PBAs that GC has decided are dead based on
    /// dead-list semantics + snapshot-pin fast filter. Apply is
    /// **idempotent**: for each pba, read the current refcount, skip the
    /// op if the refcount is positive (some other reference still pins
    /// it — Phase 3 keeps RC apply on the hot path, so this is normal),
    /// otherwise free the PBA via the existing rc=0 free path and
    /// surface it in [`ApplyOutcome::FreePbas`]. Replay is safe because
    /// onyx-side retire is a set operation.
    ///
    /// `vol_ord` is carried for replay observability — apply itself
    /// doesn't consult per-volume state; the PBA refcount table is
    /// global.
    FreePbas {
        vol_ord: VolumeOrdinal,
        pbas: Box<[Pba]>,
    },
    /// [[no-refcount-hot-path-design]] Phase 4 Step 5 background clone-
    /// promotion walker emission. One chunk's worth of "this clone now
    /// owns a lineage on these PBAs" work:
    ///
    /// * `pba_increfs` — incref every PBA by 1. Apply is **non-idempotent**
    ///   in isolation (incref twice = double the lineage count), so
    ///   replay safety rides on `promotion_cursor`'s advance: a chunk
    ///   that already landed in a prior apply is detected by the cursor
    ///   already being at or beyond `next_cursor` and the apply skips
    ///   the incref pass (see [`super::super::db::apply::apply_promotion_chunk`]).
    /// * `next_cursor = Some(lba)` — bump `promotion_cursor` to `lba` so
    ///   the walker resumes from there after a crash. `None` means the
    ///   walker has reached the end of the clone's L2P with this chunk;
    ///   apply clears `promotion_cursor` and a follow-up
    ///   [`PromotionComplete`](Self::PromotionComplete) is expected to
    ///   release the cross-volume snap_pin on the parent.
    ///
    /// Decision A from the plan: the walker handles **only** rc
    /// bookkeeping. Upgrading exclusive → shared dedup_index entries is
    /// deferred to dedup-on-write so the walker does not need to read
    /// LV3 / re-hash content.
    PromotionChunk {
        vol_ord: VolumeOrdinal,
        pba_increfs: Box<[Pba]>,
        next_cursor: Option<Lba>,
    },
    /// [[no-refcount-hot-path-design]] Phase 4 Step 5. Emitted by the
    /// promotion walker after its final
    /// [`PromotionChunk`](Self::PromotionChunk) lands, this clears the
    /// clone's `parent_vol_ord` and `promotion_cursor` so the parent
    /// volume's Lineage GC stops treating the clone as a pin point. Apply
    /// is idempotent: clearing two already-`None` fields is a no-op,
    /// matching the `WalOp::DropSnapshot` / `WalOp::DropVolume`
    /// replay-after-crash pattern.
    PromotionComplete { vol_ord: VolumeOrdinal },
}

mod codec;

pub use codec::{decode_body, encode_body, encoded_body_len, try_encode_body};

#[cfg(test)]
mod tests;
