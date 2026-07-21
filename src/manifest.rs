//! Double-buffered manifest living in pages 0 and 1 of the page store.
//!
//! v6 introduced per-volume L2P roots. v8 added dedup-lane sharding:
//! `dedup_index` and `dedup_reverse` are now N-way sharded, so each LSM
//! is stored as independent shard-level chains rather than one flat chain.
//!
//! v8 body layout:
//! - fixed header (52 bytes); the slot that v7 used for the single
//!   `dedup_index` level count is reinterpreted as the dedup shard
//!   count, and the v7 `dedup_reverse` level-count slot is reserved.
//!   The version field at offset 0 disambiguates v7 from v8 readers.
//! - refcount shard roots (inline)
//! - per-shard `dedup_index` level chains: one `(level_count: u32,
//!   heads: PageId[level_count])` block per shard
//! - per-shard `dedup_reverse` level chains: same shape
//! - snapshot table (one 32-byte `SnapshotEntry` per row)
//! - volume table (inline-encoded [`VolumeEntry`] rows)
//!
//! v7 manifests still decode (treated as `dedup_shards = 1`); the next
//! manifest commit upgrades them to v8 in place. Older v3/v4/v5 bodies
//! are still rejected.

use std::mem::size_of;
use std::sync::Arc;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::{
    Bfg, Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, SnapshotId, VolumeOrdinal,
};

/// Version of the current manifest body layout.
///
/// v10 (compact leaf v3): the on-disk leaf-payload format changed —
/// per-leaf `base_seq` (8 B) added to the header, per-slot `seq` shrunk
/// from u64 to u32 delta, and `unit_lba_count` dropped from the unit
/// dict (recovered from `unit_original_size / 4096`).
///
/// v12 (compact leaf v4): added per-leaf `base_birth_lsn` (8 B) to the
/// header and per-unit `birth_delta` (4 B) to the unit dict, growing
/// `LEAF_VALUE_SIZE` 36 → 44 B and tightening `MAX_UNITS_PER_LEAF`
/// from 128 → 110. This gives the dead-list enough birth metadata to decide
/// which displaced pages remain snapshot-pinned. Old v11 (and
/// earlier) manifests are hard-rejected on open — no on-disk migration.
///
/// v13 (per-volume dead-list): added `dead_list_head_pid` and
/// `dead_list_tail_pid` (16 B total) to each `VolumeEntry`'s fixed
/// header. Anchors the append-only chain of segment pages that records
/// every L2P overwrite's `(pba, birth_lsn, death_lsn)` triple so lineage GC can
/// retire overwritten PBAs only after snapshots and descendants stop pinning
/// them. Old v12 manifests
/// are hard-rejected on open — no on-disk migration.
///
/// v14 (lineage tracking): grew each `VolumeEntry`'s fixed
/// header by 20 B for the trio `parent_vol_ord` (2 B + 2 B pad),
/// `branched_at_lsn` (8 B), `promotion_cursor` (8 B). These power
/// cross-volume snap_pin in Lineage GC and the background promotion
/// walker that lazily turns Clone-Eager L2P shares into independent
/// lineages by incref'ing the global rc per shared PBA. Old v13
/// manifests are hard-rejected on open — no on-disk migration.
///
/// v15: added `checkpoint_bfg: u64` at the end
/// of the fixed header (offset 52). `OFF_VARIABLE_START` shifts from
/// 52 → 60. `checkpoint_lsn` is retained and continues to drive WAL
/// prune + recovery; `checkpoint_bfg` is the durable BFG epoch counter
/// maintained by the [`crate::bfg::BfgStateMachine`]. Old v14 manifests
/// are hard-rejected on open — no on-disk migration.
///
/// (Compact leaf format independently bumped from v4 → v5: per-leaf
/// `base_pba` (8 B) added to the leaf header and per-unit `base_pba`
/// (8 B) shrunk to a u32 `pba_delta` (4 B) against it, restoring
/// `MAX_UNITS_PER_LEAF` from v4's 110 back to the v3 value of 128 =
/// LEAF_ENTRY_COUNT. Leaf version is checked per page in
/// `paged::leaf_compact`; manifest body version stays at 14 because
/// no manifest field changes.)
///
/// v16: added two u64 fields for buffer-backed recovery to the
/// fixed header at offsets 60 and 68:
///
/// - `last_processed_buffer_seq` (offset 60): the highest LV2 buffer
///   entry seq whose flusher-derived metadata mutations are covered by
///   this checkpoint's page roots. On crash recovery, onyx replays
///   buffer entries with seq > this value through the flusher to
///   reconstruct in-memory metadb state. Zero in databases that still
///   run the WAL-authoritative path; set by onyx's checkpoint hook
///   once `metadb_journal_mode = "buffer"` is on.
///
/// - `lifecycle_replay_seq` (offset 68): the highest lifecycle-log
///   record seq whose effects are covered by this checkpoint. The
///   handful of ops that the buffer cannot carry (DropSnapshot,
///   DropVolume, CloneVolume, PromotionChunk/Complete, CreateVolume,
///   TakeSnapshot, Discard) live in `/mnt/onyx-meta/lifecycle.log`;
///   recovery replays records with seq > this value.
///
/// `OFF_VARIABLE_START` shifts from 60 → 76. Old v15 manifests are
/// hard-rejected on open — no on-disk migration.
///
/// v17: added a
/// second top-level shard group — the per-L2P-page refcount store
/// (`crate::l2p_page_rc::L2pPageRc`, the eventual replacement for the
/// page-header `refcount` byte). The fixed-header `OFF_RESERVED_28`
/// slot now carries `l2p_page_rc_shard_count: u32`, and the variable
/// region grows two arrays right after the refcount arrays:
/// `l2p_page_rc_shard_roots [count × 8]` then
/// `l2p_page_rc_durable_seq [count × 8]`. The page-rc shard count
/// equals the refcount/L2P shard count (`shards_per_partition`) and
/// the fold rides the SAME BFG boundary, so its per-shard durable_seq
/// mirrors the refcount durable_seq and joins the
/// `min(durable_seq[]) == checkpoint_lsn` invariant. `OFF_VARIABLE_START`
/// is unchanged (the count reuses the existing reserved header slot).
/// Old v16 manifests are hard-rejected on open — no on-disk migration; onyx
/// rebuilds metadb on schema change.
///
/// v18: adds a per-snapshot page-deadlist, a second independent dead-list of
/// L2P metadata `PageId`s. The existing per-volume dead-list tracks data
/// `Pba`s and is untouched. The head volume accumulates page-deaths during COW
/// into a live chain anchored by `VolumeEntry.page_dead_list_{head,tail}_pid`;
/// each snapshot seals its slice into an immutable chain anchored by
/// `SnapshotEntry.page_dead_list_tail_pid`. Both reuse the `DeadListSegment`
/// codec; PAGE vs PBA is distinguished only by which anchor owns the chain,
/// never by format. The page-deadlist lets `drop_snapshot` free the right L2P
/// pages without the explicit page-rc. Old v17 manifests are hard-rejected.
///
/// v19: adds a per-clone page-livelist, a third independent chain. It records
/// ALLOC/FREE events for a clone's clone-private L2P pages and is anchored by
/// `VolumeEntry.page_live_list_{head,tail}_pid`. Also adds the sticky
/// `VOLUME_FLAG_CLONE_LINEAGE` flag. Dropping a clone reads this log to free
/// the clone's private subtree without the explicit page-rc. Old v18 manifests
/// are hard-rejected.
///
/// v20: adds the promoted-PBA log, a fourth independent per-volume chain of raw
/// `Pba`s the promotion walker incref'd into the global refcount. It is
/// anchored by `VolumeEntry.promoted_log_{head,tail}_pid` and reuses the
/// `LiveListSegment` codec. `drop_volume` of a clone-lineage volume reads it to
/// decref those PBAs survivor-gated, closing the permanent promotion over-pin
/// leak. Old v19 manifests are hard-rejected.
///
/// v21: appends `SnapshotEntry.capture_watermark: Lsn` at offset 40 (grows
/// `SNAPSHOT_ENTRY_SIZE` 40 → 48). It is the EXACT highest lsn folded into the
/// snapshot's captured roots (`max(root.birth_lsn)`), NOT `created_lsn`
/// (=last_applied, an UPPER bound that races ahead of the fold under concurrent
/// writers + background BFG threads). The birth COW-kill oracle
/// (`tree.set_youngest_snap`) feeds on `max(capture_watermark)` so
/// `birth_lsn(P) <= youngest ⟺ P ∈ a snapshot's roots` stays exact —
/// `created_lsn` keeps its `last_applied` value for its other consumers
/// (dedup `any_snap_pins` rc-suppression, lineage, drop ordering). Durable so
/// replay reproduces the same gate (SnapInfo rebuilt from this on reopen). Net
/// cost: +8 B/snapshot; old v20 manifests are hard-rejected (fresh rebuild).
///
/// v22: deletes the per-L2P-page refcount store added in v17. The two
/// variable-region arrays
/// (`l2p_page_rc_shard_roots`, `l2p_page_rc_durable_seq`) are gone, and the
/// `OFF_L2P_PAGE_RC_SHARDS` header slot (offset 28) reverts to reserved
/// (zero-filled). All other offsets are unchanged. Old v21-and-earlier
/// manifests are hard-rejected on open — no on-disk migration (fresh rebuild;
/// onyx rebuilds metadb on schema change). page-rc free decisions were
/// replaced by page-rc-independent deadlist / livelist / reachability data, so
/// nothing reads the deleted store.
///
/// v23: moves the volume catalog and the snapshot table OUT of the single
/// manifest page into two independent COW byte-stream chains
/// ([`crate::manifest::catalog`]). The manifest page no longer inlines either
/// table; instead the fixed header carries two new `PageId` anchors at offsets
/// 76 and 84 — `volume_catalog_head_pid` and `snapshot_catalog_head_pid` —
/// and the variable region keeps only refcount roots + per-shard durable_seq +
/// dedup heads. `OFF_VARIABLE_START` shifts 76 → 92. This removes the
/// single-page `{volumes + snapshots}` capacity wall (which capped a database
/// at ~10-30 volumes, making `config.max_volumes` unreachable): both tables are
/// now O(pages). Each commit COWs both chains to fresh pids and frees the
/// displaced 2-generation-old chain after the new manifest slot is durable, so
/// the double-buffered slot fallback is preserved (see
/// [`crate::manifest::store::ManifestStore::commit`]). Old v22-and-earlier
/// manifests are hard-rejected on open — no on-disk migration (fresh rebuild;
/// onyx rebuilds metadb on schema change).
///
/// v24 (fixed-capacity device support, Phase 3): appends two u64 fields to the
/// fixed header after the catalog anchors — `page_high_water` (offset 92) and
/// `journal_ring_head` (offset 100). `OFF_VARIABLE_START` shifts 92 → 108.
///
/// - `page_high_water`: an upper bound on every page id this manifest's roots
///   reach, sampled at commit time after all page allocations. On a
///   fixed-capacity (chunklet) device — which has no file length to trust and
///   no filesystem hole-punch to reclaim leaked pages — the open path rebuilds
///   the free list by a **bounded** scan `[FIRST_DATA_PAGE, page_high_water)`
///   instead of trusting EOF, closing the "forced open_fast leaks freed pages
///   until the region fills" hole. Harmless on the file path (the file open
///   still scans to EOF).
///
/// - `journal_ring_head`: the prune boundary of the lifecycle-journal ring on
///   a device-backed journal (Phase 3c). Reserved here (bumped in the same
///   version) so the journal-ring work needs no second manifest bump; `0` on a
///   file-backed journal.
///
/// Old v23-and-earlier manifests are hard-rejected on open — no on-disk
/// migration (fresh rebuild; onyx rebuilds metadb on schema change).
///
/// v25 (online cuckoo dedup-index resize): appends one `PageId` to the fixed
/// header — `dedup_migration_old_head` (offset 108) — so the cuckoo dedup index
/// can grow its bucket modulus online with zero downtime via an incremental
/// two-table migration. `OFF_VARIABLE_START` shifts 108 → 116.
///
/// - `dedup_migration_old_head`: [`NULL_PAGE`] in the steady state (a single
///   cuckoo table, phase = Single). During a resize (phase = Growing) it is the
///   meta-page id of the OLD (smaller, frozen) cuckoo table, while
///   `dedup_index_shard_heads[0][0]` points at the NEW (larger) table that all
///   live writes now target. The migration walker copies OLD's entries into NEW
///   and the swap-to-Single (drop OLD, clear this field) happens atomically at a
///   checkpoint barrier — so a crash-recovered manifest is always exactly one of
///   Single(old) / Growing(new+old) / Single(new). This is the ONLY new durable
///   state the resize needs; each table's `bucket_count` already lives in its
///   own cuckoo meta `head_extra`, and the migration cursor is not persisted
///   (idempotent put-if-absent re-walks from 0 on reopen).
///
/// v26 keeps the v25 byte layout but changes the semantic routing of PBA
/// refcounts from `hash(pba)` to `hash(pba / 336)`. v25 remains readable and
/// writable with its legacy routing; rewriting a v25 manifest must retain v25.
/// The version is therefore a durable semantic gate even though no offsets move.
/// The divisor 336 is fixed by v26, not derived from the current array layout;
/// changing the physical entries-per-page requires a new manifest version and
/// an explicit routing/migration decision.
/// Old v24-and-earlier manifests remain hard-rejected on open.
///
/// v27 (L3 durable refcount delta-run segments): appends a third refcount-shard
/// array to the VARIABLE region, right after `refcount_durable_seq` — the
/// per-shard `refcount_delta_run_heads: PageId[refcount_shard_count]`, each the
/// mutable head of a COW segment-directory chain ([`NULL_PAGE`] when the shard
/// has no un-condensed segments). `Manifest::decode` does NOT walk these chains
/// (the heads are raw pids read later in the open path); v25/v26 decode
/// synthesizes an all-[`NULL_PAGE`] array so the field is always parallel to
/// `refcount_shard_roots`. `OFF_VARIABLE_START` is unchanged (the new array is
/// in the variable region, not the fixed header).
///
/// Version policy is **persist-gated, not binary-gated**. v27 is written ONLY
/// when `rc_delta_run_persist_enabled` is on: a v26 database upgrades to v27
/// lazily on its first persist commit (the v10→v11 precedent), a fresh database
/// is still created at v26, and a persist-off run keeps re-committing the
/// database's existing version verbatim (v25/v26/v27) — the off-arm never emits
/// the new array, so it stays byte-identical to the pre-feature binary. v25 +
/// persist is a refused config (the v27 upgrade would lose the legacy-routing
/// marker). v27 hard-rejects on ≤v26 binaries (they never learn version 27) and
/// this binary keeps hard-rejecting ≤v24.
pub const MANIFEST_BODY_VERSION: u32 = 26;
pub const LEGACY_MANIFEST_BODY_VERSION: u32 = 25;
/// Manifest version written once `rc_delta_run_persist_enabled` is on (see the
/// [`MANIFEST_BODY_VERSION`] doc). Adds the `refcount_delta_run_heads` array.
pub const DELTA_RUN_MANIFEST_BODY_VERSION: u32 = 27;

/// Every manifest body version this binary can decode + re-encode. v25 (legacy
/// refcount routing), v26 (current default), v27 (delta-run persist).
#[inline]
pub(crate) fn is_supported_body_version(version: u32) -> bool {
    matches!(
        version,
        LEGACY_MANIFEST_BODY_VERSION | MANIFEST_BODY_VERSION | DELTA_RUN_MANIFEST_BODY_VERSION
    )
}

// v8 body layout. Fixed header is the same shape as v7 except:
//   - OFF_DEDUP_LEVEL_COUNT is reinterpreted as OFF_DEDUP_SHARDS
//     (number of dedup_index / dedup_reverse shards; same N for both).
//   - OFF_DEDUP_REVERSE_LEVEL_COUNT becomes reserved/future flags.
// The variable region begins at `OFF_VARIABLE_START` and holds:
//   refcount_shard_roots [refcount_shard_count × 8]
//   refcount_durable_seq [refcount_shard_count × 8]            (v11+)
//   (v17 had l2p_page_rc_shard_roots/durable_seq here; DELETED in v22)
//   for each of dedup_shards shards:
//     level_count u32 + level_count × 8 bytes  (dedup_index)
//   snapshots [snapshot_count × SNAPSHOT_ENTRY_SIZE]
//   volumes   [per-entry inline, see encode_volume_entry_inline]
const OFF_BODY_VERSION: usize = 0;
const OFF_CHECKPOINT_LSN: usize = 4;
const OFF_FREE_LIST_HEAD: usize = 12;
const OFF_SHARD_COUNT: usize = 20;
const OFF_DEDUP_SHARDS: usize = 24;
// v7 used this slot for `dedup_reverse_level_count`; v8..v16 reserved it
// (zero-filled). v17 repurposed it for `l2p_page_rc_shard_count: u32`;
// v22 DELETED the per-L2P-page refcount store, so the slot reverts to
// reserved (zero-filled). Decoders dispatch on the version field above.
const OFF_RESERVED_28: usize = 28;
const OFF_NEXT_SNAPSHOT_ID: usize = 32;
const OFF_NEXT_VOLUME_ORD: usize = 40;
// 42..44 reserved for alignment / future flags
const OFF_SNAPSHOT_COUNT: usize = 44;
const OFF_VOLUME_COUNT: usize = 48;
// v15 inserts `checkpoint_bfg: u64` at offset 52. All earlier offsets are
// unchanged so v14 readers reject the new layout via the version field at
// offset 0 (decode hard-rejects v14).
const OFF_CHECKPOINT_BFG: usize = 52;
// v16 inserts two more u64 fields after `checkpoint_bfg`:
//   offset 60: `last_processed_buffer_seq` (onyx LV2 buffer replay watermark)
//   offset 68: `lifecycle_replay_seq` (metadb lifecycle-log replay watermark)
// See `MANIFEST_BODY_VERSION` doc comment for the contract.
const OFF_LAST_PROCESSED_BUFFER_SEQ: usize = 60;
const OFF_LIFECYCLE_REPLAY_SEQ: usize = 68;
// v23 inserts the two catalog-chain head pids into the fixed header:
//   offset 76: `volume_catalog_head_pid`   (head of the volume-catalog chain)
//   offset 84: `snapshot_catalog_head_pid` (head of the snapshot-table chain)
// Both are real allocated pids (a chain always has >= 1 page); the variable
// region — refcount roots + durable_seq + dedup heads, no inline volumes /
// snapshots — now starts at 92.
const OFF_VOLUME_CATALOG_HEAD: usize = 76;
const OFF_SNAPSHOT_CATALOG_HEAD: usize = 84;
// v24 appends two u64 fields to the fixed header (see `MANIFEST_BODY_VERSION`):
//   offset 92:  `page_high_water`    (bounded-scan upper bound on device open)
//   offset 100: `journal_ring_head`  (lifecycle-journal ring prune boundary; v3c)
const OFF_PAGE_HIGH_WATER: usize = 92;
const OFF_JOURNAL_RING_HEAD: usize = 100;
// v25 appends one PageId to the fixed header (see `MANIFEST_BODY_VERSION`):
//   offset 108: `dedup_migration_old_head`  (OLD cuckoo table head during an
//               online resize; NULL_PAGE when not migrating)
const OFF_DEDUP_MIGRATION_OLD_HEAD: usize = 108;
const OFF_VARIABLE_START: usize = 116;

/// Per-snapshot row size on disk. v6 packs: id(8) + vol_ord(2) + 6 pad +
/// l2p_roots_page(8) + created_lsn(8). v18 (BFG) appends
/// `page_dead_list_tail_pid(8)` at offset 32, growing the row 32 → 40.
/// The 6 bytes at 10..16 stay reserved for future per-snapshot flags.
const SNAPSHOT_ENTRY_SIZE: usize = 48;
/// Byte offset of `page_dead_list_tail_pid` within a v18 snapshot row.
const OFF_SNAP_PAGE_DEADLIST_TAIL: usize = 32;
/// v21: byte offset of `capture_watermark` within the snapshot row.
const OFF_SNAP_CAPTURE_WATERMARK: usize = 40;

const _: () = {
    assert!(OFF_BODY_VERSION + 4 == OFF_CHECKPOINT_LSN);
    assert!(OFF_CHECKPOINT_LSN + 8 == OFF_FREE_LIST_HEAD);
    assert!(OFF_FREE_LIST_HEAD + 8 == OFF_SHARD_COUNT);
    assert!(OFF_SHARD_COUNT + 4 == OFF_DEDUP_SHARDS);
    assert!(OFF_DEDUP_SHARDS + 4 == OFF_RESERVED_28);
    assert!(OFF_RESERVED_28 + 4 == OFF_NEXT_SNAPSHOT_ID);
    assert!(OFF_NEXT_SNAPSHOT_ID + 8 == OFF_NEXT_VOLUME_ORD);
    assert!(OFF_NEXT_VOLUME_ORD + 4 == OFF_SNAPSHOT_COUNT);
    assert!(OFF_SNAPSHOT_COUNT + 4 == OFF_VOLUME_COUNT);
    assert!(OFF_VOLUME_COUNT + 4 == OFF_CHECKPOINT_BFG);
    assert!(OFF_CHECKPOINT_BFG + 8 == OFF_LAST_PROCESSED_BUFFER_SEQ);
    assert!(OFF_LAST_PROCESSED_BUFFER_SEQ + 8 == OFF_LIFECYCLE_REPLAY_SEQ);
    assert!(OFF_LIFECYCLE_REPLAY_SEQ + 8 == OFF_VOLUME_CATALOG_HEAD);
    assert!(OFF_VOLUME_CATALOG_HEAD + 8 == OFF_SNAPSHOT_CATALOG_HEAD);
    assert!(OFF_SNAPSHOT_CATALOG_HEAD + 8 == OFF_PAGE_HIGH_WATER);
    assert!(OFF_PAGE_HIGH_WATER + 8 == OFF_JOURNAL_RING_HEAD);
    assert!(OFF_JOURNAL_RING_HEAD + 8 == OFF_DEDUP_MIGRATION_OLD_HEAD);
    assert!(OFF_DEDUP_MIGRATION_OLD_HEAD + 8 == OFF_VARIABLE_START);
    assert!(SNAPSHOT_ENTRY_SIZE == 48);
    assert!(OFF_SNAP_PAGE_DEADLIST_TAIL + 8 == OFF_SNAP_CAPTURE_WATERMARK);
    assert!(OFF_SNAP_CAPTURE_WATERMARK + 8 == SNAPSHOT_ENTRY_SIZE);
};

/// Maximum number of shard roots that fit in one snapshot-roots page.
pub const MAX_SHARD_ROOTS_PER_PAGE: usize = PAGE_PAYLOAD_SIZE / size_of::<PageId>();

const DEDUP_META_HEAD_GROUPS: usize = 1;

/// Maximum number of snapshot entries that fit in a v8 manifest with the
/// given refcount shard count, ignoring volume and dedup level usage.
/// Upper bound; real capacity is lower once the volume table / dedup
/// levels eat into the variable region.
pub fn max_snapshots_for_shards(refcount_shard_count: usize) -> usize {
    max_snapshots_for_layout(refcount_shard_count, 0, 0, 0)
}

/// Snapshot-table capacity given the full manifest layout. All inputs
/// are lengths / byte counts that end up in the variable region ahead
/// of the snapshot table.
///
/// `dedup_head_group_count` is the number of manifest head groups
/// stored for the dedup_index. v9 retired the legacy paged_reverse
/// alongside the WAL `DedupReverse*` ops, so only the cuckoo dedup_index
/// contributes header bytes here.
pub fn max_snapshots_for_layout(
    refcount_shard_count: usize,
    dedup_head_group_count: usize,
    total_dedup_level_count: usize,
    volumes_budget_bytes: usize,
) -> usize {
    let refcount_bytes = match refcount_shard_count.checked_mul(size_of::<PageId>()) {
        Some(v) => v,
        None => return 0,
    };
    // v11: per-refcount-shard `durable_seq[i]: Lsn` array mirrors
    // `refcount_shard_roots`, written immediately after the roots. Same
    // length (refcount_shard_count), same element size (8 B). v27 adds a
    // third parallel array (delta-run heads); this upper-bound helper counts
    // it unconditionally so the estimate is safe for a v27 layout too.
    let refcount_durable_seq_bytes = match refcount_bytes.checked_mul(2) {
        Some(v) => v,
        None => return 0,
    };
    // One index (dedup_index) carries one u32 level-count header per
    // manifest head group.
    let dedup_header_bytes = match dedup_head_group_count.checked_mul(size_of::<u32>()) {
        Some(v) => v,
        None => return 0,
    };
    let dedup_bytes = match total_dedup_level_count.checked_mul(size_of::<PageId>()) {
        Some(v) => v,
        None => return 0,
    };
    let used = match OFF_VARIABLE_START
        .checked_add(refcount_bytes)
        .and_then(|v| v.checked_add(refcount_durable_seq_bytes))
        .and_then(|v| v.checked_add(dedup_header_bytes))
        .and_then(|v| v.checked_add(dedup_bytes))
        .and_then(|v| v.checked_add(volumes_budget_bytes))
    {
        Some(v) => v,
        None => return 0,
    };
    if used > PAGE_PAYLOAD_SIZE {
        return 0;
    }
    (PAGE_PAYLOAD_SIZE - used) / SNAPSHOT_ENTRY_SIZE
}

/// Bytes the v23 manifest variable region occupies: refcount roots +
/// per-shard durable_seq + dedup head groups (+ their level pids). Volumes and
/// snapshots are NOT here in v23 — they chain out-of-line — so this is the full
/// in-page footprint after the fixed header. Saturating so an absurd shard
/// count surfaces as "exceeds payload" rather than wrapping.
fn variable_region_bytes(
    refcount_shard_count: usize,
    dedup_head_group_count: usize,
    total_dedup_level_count: usize,
    body_version: u32,
) -> usize {
    // v27 adds a third parallel refcount-shard array (delta-run heads) after
    // roots + durable_seq; v25/v26 carry only the first two.
    let refcount_arrays = if body_version >= DELTA_RUN_MANIFEST_BODY_VERSION {
        3
    } else {
        2
    };
    let refcount = refcount_shard_count
        .saturating_mul(size_of::<PageId>())
        .saturating_mul(refcount_arrays);
    let dedup_headers = dedup_head_group_count.saturating_mul(size_of::<u32>());
    let dedup_levels = total_dedup_level_count.saturating_mul(size_of::<PageId>());
    OFF_VARIABLE_START
        .saturating_add(refcount)
        .saturating_add(dedup_headers)
        .saturating_add(dedup_levels)
}

/// One snapshot's manifest entry. v6 tracks the owning volume's
/// ordinal and the snapshot's L2P shard-root vector (materialised via
/// a [`PageType::SnapshotRoots`] page). Refcount state is global and
/// never snapshotted — .5b retired it. Commit 6 always stamps
/// `vol_ord = 0`; commit 9 picks it up for per-volume snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub id: SnapshotId,
    pub vol_ord: VolumeOrdinal,
    pub l2p_roots_page: PageId,
    pub created_lsn: Lsn,
    pub l2p_shard_roots: Box<[PageId]>,
    /// v18 (BFG): tail of this snapshot's immutable
    /// page-deadlist chain — the L2P metadata `PageId`s that died off the
    /// head while this snapshot pinned them. `NULL_PAGE` while empty. The
    /// head is implicit at `prev_seg_pid == NULL_PAGE`; walk it backward
    /// with [`crate::deadlist::walk_chain_pages`]. `drop_snapshot`
    /// consumes it (free-or-merge by birth). Not persisted in
    /// `SnapshotRoots`; lives inline in the snapshot row.
    pub page_dead_list_tail_pid: PageId,
    /// v21 (BFG): the EXACT fold-watermark of this snapshot's
    /// captured roots = `max(root.birth_lsn)` over its shard roots. The
    /// birth-authoritative COW-kill oracle uses `max(capture_watermark)` (NOT
    /// `created_lsn`, an upper bound that races ahead of the fold under
    /// concurrency) so `birth_lsn(P) <= youngest ⟺ P captured` stays exact.
    /// `created_lsn` keeps its `last_applied` value for dedup/lineage/ordering.
    pub capture_watermark: Lsn,
}

impl SnapshotEntry {
    fn needs_l2p_roots_page(&self) -> bool {
        self.l2p_roots_page == NULL_PAGE
    }
}

// ---- Manifest v6 building blocks ---------------------------------------
//
// Not plugged into the live encode/decode path yet — the write path still
// emits v5. These types + codecs let the wire format flip in one atomic
// change while the current v5 soak keeps running without drift.

pub(crate) mod catalog;
mod snapshot_roots;
mod store;
mod volume;

use catalog::CatalogKind;
pub(crate) use snapshot_roots::{load_snapshot_roots, write_snapshot_roots_page};
pub use store::{LoadedManifest, ManifestStore};
pub(crate) use store::{catalog_chain_pids_all_slots, free_list_reserve_pids_all_slots};
pub use volume::{
    VOLUME_ENTRY_FIXED_SIZE, VOLUME_FLAG_CLONE_LINEAGE, VOLUME_FLAG_DROP_PENDING, VolumeEntry,
    decode_volume_entry_inline, encode_volume_entry_inline, volume_entry_inline_size,
};

/// Decoded manifest body (v25 or v26).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub body_version: u32,
    /// Greatest LSN whose WAL record has been applied to durable state.
    pub checkpoint_lsn: Lsn,
    /// BFG: greatest BFG that the `BfgSyncThread` has
    /// marked synced. Always satisfies `checkpoint_bfg + 1 <= open_bfg`
    /// at runtime. Persisted in the v15 fixed header (offset 52);
    /// recovery reconstructs `BfgStateMachine` from this value.
    pub checkpoint_bfg: Bfg,
    /// Highest LV2 buffer entry seq whose flusher-derived metadata mutations are covered by this
    /// checkpoint's page roots. Zero while `metadb_journal_mode = "wal"`
    /// (the WAL is still authoritative); set by onyx's checkpoint hook
    /// in `"buffer"` mode. On recovery, onyx replays buffer entries
    /// with seq > this value through the flusher pipeline.
    pub last_processed_buffer_seq: u64,
    /// Highest lifecycle-log record seq whose effects are covered by this checkpoint. Records with
    /// seq > this value are replayed before the buffer.
    pub lifecycle_replay_seq: u64,
    /// v23: head page of the volume-catalog COW chain
    /// ([`crate::manifest::catalog`]). The [`volumes`](Self::volumes) `Vec` is
    /// the runtime source of truth; this anchor is where the chain that
    /// reconstructs it on open lives. Populated by `decode`; assigned afresh
    /// every commit (the chain is COW'd each commit, never reused in place).
    /// `NULL_PAGE` only in a never-committed [`Manifest::empty`].
    pub volume_catalog_head_pid: PageId,
    /// v23: head page of the snapshot-table COW chain. Mirrors
    /// [`volume_catalog_head_pid`](Self::volume_catalog_head_pid) for
    /// [`snapshots`](Self::snapshots).
    pub snapshot_catalog_head_pid: PageId,
    /// v24: an upper bound on every page id this manifest's roots reach,
    /// sampled at commit time after all page allocations. On a fixed-capacity
    /// device the open path rebuilds the free list by a bounded scan
    /// `[FIRST_DATA_PAGE, page_high_water)` instead of trusting file EOF (which
    /// doesn't exist on a raw block window). `0` in a never-committed
    /// [`Manifest::empty`]; the file path also carries it but doesn't depend on
    /// it (the file open scans to EOF).
    pub page_high_water: u64,
    /// v24: prune boundary (ring head block) of a device-backed
    /// lifecycle-journal ring (Phase 3c). `0` on a file-backed journal.
    pub journal_ring_head: u64,
    /// v25: OLD cuckoo dedup-index table head during an online modulus resize,
    /// or [`NULL_PAGE`] in the steady state (phase = Single). When set (phase =
    /// Growing) `dedup_index_shard_heads[0][0]` points at the NEW (larger)
    /// table that live writes target, and this anchors the OLD (smaller, frozen)
    /// table the migration walker is draining. The Single→Growing→Single
    /// transitions are driven solely by manifest commit, so recovery sees a
    /// clean 3-state machine. Each table's `bucket_count` lives in its own
    /// cuckoo meta `head_extra`; the migration cursor is not persisted.
    pub dedup_migration_old_head: PageId,
    /// Head of the persisted free-list page chain, or [`NULL_PAGE`].
    pub free_list_head: PageId,
    /// Current per-shard PBA-refcount B+tree roots. Refcount is a global
    /// running tally — not per-volume — so its roots stay at the
    /// manifest top level. Per-volume L2P roots live inside
    /// [`volumes`](Self::volumes) instead.
    pub refcount_shard_roots: Box<[PageId]>,
    /// Per-refcount-shard durable LSN watermark. `durable_seq[i]` is the
    /// highest LSN whose refcount-shard `i` deltas are on disk. Same
    /// length as `refcount_shard_roots`.
    ///
    /// durable-seq rollout (per-shard durable-seq) of the manifest v11 schema: the global
    /// `checkpoint_lsn` equals `min(refcount_durable_seq[..] ∪ each
    /// volume's l2p_shard_durable_seq[..])`. WAL prune, recovery, and
    /// onyx buffer reclaim still consume `checkpoint_lsn`, so this
    /// array is observability-only for now. follow-up amortization will flip
    /// consumers to per-shard reads so partial sample can re-enable
    /// without pinning the global floor on cold shards.
    pub refcount_durable_seq: Box<[Lsn]>,
    /// v27: per-refcount-shard head of the durable delta-run segment directory
    /// (a COW page chain listing un-condensed segment descriptors). Same length
    /// as [`refcount_shard_roots`](Self::refcount_shard_roots); [`NULL_PAGE`]
    /// when a shard has no un-condensed segments. Written only at
    /// v27+ (`rc_delta_run_persist_enabled`); synthesized all-[`NULL_PAGE`] when
    /// decoding a v25/v26 body so the array is always parallel to the roots.
    /// `Manifest::decode` does NOT walk these chains — the heads are raw pids
    /// consumed by the open path's condense-on-open replay.
    pub refcount_delta_run_heads: Box<[PageId]>,
    /// Number of dedup apply lanes. Power of two, recorded at create time;
    /// changing it requires recreating the database.
    pub dedup_shards: u32,
    /// `dedup_index` meta head groups. In the current cuckoo layout this
    /// has one group containing the cuckoo meta page id; the legacy field
    /// name is kept to avoid a wider manifest rename.
    pub dedup_index_shard_heads: Box<[Box<[PageId]>]>,
    /// Monotonic counter: next snapshot id to hand out.
    pub next_snapshot_id: u64,
    /// Monotonic counter: next volume ordinal to hand out. Initialised
    /// to `1` because `0` is reserved for the bootstrap volume that
    /// every database creates on `open` / `create`.
    pub next_volume_ord: VolumeOrdinal,
    /// Registered snapshots, in order of creation.
    pub snapshots: Vec<SnapshotEntry>,
    /// Registered volumes, in ordinal order.
    pub volumes: Vec<VolumeEntry>,
}

impl Manifest {
    /// Freshly-created empty manifest for a brand-new database. Caller
    /// fills in refcount roots and the bootstrap volume entry before
    /// committing. Defaults to `dedup_shards = 1`; override before
    /// committing if `Db::create` was given a higher shard count.
    pub fn empty() -> Self {
        Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn: 0,
            checkpoint_bfg: 0,
            last_processed_buffer_seq: 0,
            lifecycle_replay_seq: 0,
            volume_catalog_head_pid: NULL_PAGE,
            snapshot_catalog_head_pid: NULL_PAGE,
            page_high_water: 0,
            journal_ring_head: 0,
            dedup_migration_old_head: NULL_PAGE,
            free_list_head: NULL_PAGE,
            refcount_shard_roots: Vec::new().into_boxed_slice(),
            refcount_durable_seq: Vec::new().into_boxed_slice(),
            refcount_delta_run_heads: Vec::new().into_boxed_slice(),
            dedup_shards: 1,
            dedup_index_shard_heads: vec![Vec::new().into_boxed_slice()].into_boxed_slice(),
            next_snapshot_id: 1,
            next_volume_ord: 1,
            snapshots: Vec::new(),
            volumes: Vec::new(),
        }
    }

    /// Number of refcount shards tracked at the top level. Per-volume
    /// L2P shard counts live on each [`VolumeEntry`].
    pub fn shard_count(&self) -> usize {
        self.refcount_shard_roots.len()
    }

    /// Sum of level counts across every `dedup_index` shard, used by
    /// the manifest size budget.
    pub fn total_dedup_index_levels(&self) -> usize {
        self.dedup_index_shard_heads.iter().map(|s| s.len()).sum()
    }

    /// Find a snapshot by id.
    pub fn find_snapshot(&self, id: SnapshotId) -> Option<&SnapshotEntry> {
        self.snapshots.iter().find(|e| e.id == id)
    }

    /// Dry-run [`encode`](Self::encode) against a scratch page. Returns
    /// `Err` with the same diagnostic `encode` would have produced if
    /// this manifest does not fit in a single page. Callers use this
    /// before applying irreversible side effects (refcount bumps, page
    /// writes) so a doomed commit surfaces as `Err` *before* leaking
    /// refcount state on disk.
    pub(crate) fn check_encodable(&self) -> Result<()> {
        let mut probe = Page::new(PageHeader::new(PageType::Manifest, 0));
        // v23: the catalog is chained, so a dry-run only validates the small
        // refcount/dedup region + the durable_seq invariant — it can no longer
        // reject for `{volumes + snapshots}` overflow (that wall is gone). The
        // real chain heads are unknown at probe time; `NULL_PAGE` is fine
        // because the head bytes do not affect the fit check.
        self.encode(&mut probe, NULL_PAGE, NULL_PAGE)
    }

    /// durable-seq rollout (per-shard durable-seq) tripwire: `checkpoint_lsn` must equal the
    /// min of every per-shard `durable_seq` (refcount + each volume's
    /// L2P shards) that this manifest carries. Returns `Err` so the
    /// debug build can fail the commit before writing a drifted page;
    /// release builds skip the check (caller is responsible for
    /// gating).
    fn assert_durable_seq_invariant(&self) -> Result<()> {
        // Empty manifest (`Manifest::empty()` before any shards are
        // wired up): nothing to check. Real manifests always have at
        // least one refcount shard by the time they're committed.
        let mut min_seen: Option<Lsn> = None;
        for &v in self.refcount_durable_seq.iter() {
            min_seen = Some(min_seen.map_or(v, |m| m.min(v)));
        }
        for vol in &self.volumes {
            for &v in vol.l2p_shard_durable_seq.iter() {
                min_seen = Some(min_seen.map_or(v, |m| m.min(v)));
            }
        }
        if let Some(m) = min_seen
            && m != self.checkpoint_lsn
        {
            return Err(MetaDbError::Corruption(format!(
                "manifest durable_seq invariant broken: min(per-shard)={m}, \
                 checkpoint_lsn={}",
                self.checkpoint_lsn
            )));
        }
        if self.refcount_durable_seq.len() != self.refcount_shard_roots.len() {
            return Err(MetaDbError::Corruption(format!(
                "manifest refcount_durable_seq length {} != refcount_shard_roots length {}",
                self.refcount_durable_seq.len(),
                self.refcount_shard_roots.len(),
            )));
        }
        // v27's delta-run heads array is kept parallel to the roots (all-NULL
        // when decoded from a v25/v26 body). An EMPTY array is also accepted as
        // "not yet materialized" — a manifest built by mutating roots directly
        // (create / test) leaves it empty and `encode` synthesizes all-NULL;
        // `prepare_commit` normalizes it so the committed copy matches decode.
        if !self.refcount_delta_run_heads.is_empty()
            && self.refcount_delta_run_heads.len() != self.refcount_shard_roots.len()
        {
            return Err(MetaDbError::Corruption(format!(
                "manifest refcount_delta_run_heads length {} != refcount_shard_roots length {}",
                self.refcount_delta_run_heads.len(),
                self.refcount_shard_roots.len(),
            )));
        }
        for vol in &self.volumes {
            if vol.l2p_shard_durable_seq.len() != vol.l2p_shard_roots.len() {
                return Err(MetaDbError::Corruption(format!(
                    "manifest volume {} l2p_shard_durable_seq length {} != \
                     l2p_shard_roots length {}",
                    vol.ord,
                    vol.l2p_shard_durable_seq.len(),
                    vol.l2p_shard_roots.len(),
                )));
            }
        }
        Ok(())
    }

    /// The durable **frontier**: the MAX of every per-shard `durable_seq`
    /// (refcount + every volume's L2P shards) — the symmetric
    /// counterpart of [`Self::assert_durable_seq_invariant`]'s `min` walk
    /// (which is `checkpoint_lsn`, the recovery floor). Returns 0 for an
    /// empty manifest.
    ///
    /// Recovery uses this to resume the new-op LSN allocator above EVERY
    /// durable page generation. `checkpoint_lsn = min(durable_seq)` can sit
    /// far below an active shard's `durable_seq` when buffer shards are
    /// uneven/uncompacted (buffer mode), yet that active shard holds durable
    /// array/tree pages whose `generation == its durable_seq`. Resuming the
    /// allocator at `checkpoint_lsn + 1` would hand out LSNs *below* those
    /// generations, so a fresh op's stage-time replay-skip (`page_lsn >=
    /// lsn`) would wrongly drop it. A page is only durable in the same fold
    /// cycle that advances its shard's `durable_seq` to `wal_checkpoint >=
    /// page.generation`, so `max(durable_seq) >=` every durable page
    /// generation: resuming above it is collision-free.
    pub(crate) fn max_durable_seq(&self) -> Lsn {
        let mut max_seen: Lsn = 0;
        for &v in self.refcount_durable_seq.iter() {
            max_seen = max_seen.max(v);
        }
        for vol in &self.volumes {
            for &v in vol.l2p_shard_durable_seq.iter() {
                max_seen = max_seen.max(v);
            }
        }
        max_seen
    }

    /// Encode the manifest's fixed header + variable region (refcount roots /
    /// durable_seq / dedup heads) into `page`. v23: the volume catalog and the
    /// snapshot table are NOT inlined — they live in the COW chains anchored by
    /// `vol_head` / `snap_head`, which the caller
    /// ([`crate::manifest::store::ManifestStore::commit`]) has already built +
    /// fsynced. [`check_encodable`](Self::check_encodable) passes `NULL_PAGE`
    /// for a dry-run fit probe.
    fn encode(&self, page: &mut Page, vol_head: PageId, snap_head: PageId) -> Result<()> {
        self.assert_durable_seq_invariant()?;
        if !is_supported_body_version(self.body_version) {
            return Err(MetaDbError::InvalidArgument(format!(
                "unsupported manifest body version {} for encode; expected v{}, v{}, or v{}",
                self.body_version,
                LEGACY_MANIFEST_BODY_VERSION,
                MANIFEST_BODY_VERSION,
                DELTA_RUN_MANIFEST_BODY_VERSION
            )));
        }
        let refcount_shard_count = self.refcount_shard_roots.len();
        if refcount_shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest refcount shard count {} exceeds page capacity {}",
                refcount_shard_count, MAX_SHARD_ROOTS_PER_PAGE,
            )));
        }
        let dedup_shards = self.dedup_shards as usize;
        if dedup_shards == 0 || !dedup_shards.is_power_of_two() {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest dedup_shards {dedup_shards} must be a positive power of two",
            )));
        }
        if self.dedup_index_shard_heads.len() != DEDUP_META_HEAD_GROUPS {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest dedup meta-head outer length {} must be {DEDUP_META_HEAD_GROUPS}",
                self.dedup_index_shard_heads.len(),
            )));
        }

        // v23: only refcount roots + per-shard durable_seq + dedup heads share
        // the manifest page now (volumes / snapshots chain out-of-line). The
        // former `max_snapshots_for_layout` single-page gate is gone — the
        // catalog is O(pages). What remains is a sanity check that this
        // fixed-but-bounded region still fits; it only bites at absurd shard
        // counts (~245+).
        let region_used = variable_region_bytes(
            refcount_shard_count,
            DEDUP_META_HEAD_GROUPS,
            self.total_dedup_index_levels(),
            self.body_version,
        );
        if region_used > PAGE_PAYLOAD_SIZE {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest refcount/dedup region {region_used} B exceeds page payload {PAGE_PAYLOAD_SIZE} B",
            )));
        }

        let p = page.payload_mut();
        p.fill(0);
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&self.body_version.to_le_bytes());
        p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
            .copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
            .copy_from_slice(&self.free_list_head.to_le_bytes());
        p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4]
            .copy_from_slice(&(refcount_shard_count as u32).to_le_bytes());
        p[OFF_DEDUP_SHARDS..OFF_DEDUP_SHARDS + 4]
            .copy_from_slice(&(dedup_shards as u32).to_le_bytes());
        // v22: OFF_RESERVED_28 (the former v17 l2p_page_rc shard-count slot) is
        // reserved again — left zero-filled by `p.fill(0)` above.
        let _ = OFF_RESERVED_28;
        p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
            .copy_from_slice(&self.next_snapshot_id.to_le_bytes());
        p[OFF_NEXT_VOLUME_ORD..OFF_NEXT_VOLUME_ORD + 2]
            .copy_from_slice(&self.next_volume_ord.to_le_bytes());
        // OFF_NEXT_VOLUME_ORD + 2 .. OFF_SNAPSHOT_COUNT = 2 bytes reserved (already zero-filled).
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
            .copy_from_slice(&(self.snapshots.len() as u32).to_le_bytes());
        p[OFF_VOLUME_COUNT..OFF_VOLUME_COUNT + 4]
            .copy_from_slice(&(self.volumes.len() as u32).to_le_bytes());
        p[OFF_CHECKPOINT_BFG..OFF_CHECKPOINT_BFG + 8]
            .copy_from_slice(&self.checkpoint_bfg.to_le_bytes());
        p[OFF_LAST_PROCESSED_BUFFER_SEQ..OFF_LAST_PROCESSED_BUFFER_SEQ + 8]
            .copy_from_slice(&self.last_processed_buffer_seq.to_le_bytes());
        p[OFF_LIFECYCLE_REPLAY_SEQ..OFF_LIFECYCLE_REPLAY_SEQ + 8]
            .copy_from_slice(&self.lifecycle_replay_seq.to_le_bytes());
        // v23: catalog chain head anchors. Real allocated pids on a committed
        // manifest; `NULL_PAGE` only in a probe / never-committed empty.
        p[OFF_VOLUME_CATALOG_HEAD..OFF_VOLUME_CATALOG_HEAD + 8]
            .copy_from_slice(&vol_head.to_le_bytes());
        p[OFF_SNAPSHOT_CATALOG_HEAD..OFF_SNAPSHOT_CATALOG_HEAD + 8]
            .copy_from_slice(&snap_head.to_le_bytes());
        // v24: bounded-scan upper bound + device journal-ring prune boundary.
        p[OFF_PAGE_HIGH_WATER..OFF_PAGE_HIGH_WATER + 8]
            .copy_from_slice(&self.page_high_water.to_le_bytes());
        p[OFF_JOURNAL_RING_HEAD..OFF_JOURNAL_RING_HEAD + 8]
            .copy_from_slice(&self.journal_ring_head.to_le_bytes());
        // v25: OLD cuckoo table head during an online resize (NULL_PAGE = none).
        p[OFF_DEDUP_MIGRATION_OLD_HEAD..OFF_DEDUP_MIGRATION_OLD_HEAD + 8]
            .copy_from_slice(&self.dedup_migration_old_head.to_le_bytes());

        let mut off = OFF_VARIABLE_START;
        for root in self.refcount_shard_roots.iter().copied() {
            p[off..off + 8].copy_from_slice(&root.to_le_bytes());
            off += 8;
        }
        // v11: per-refcount-shard durable_seq follows the roots array.
        // Length is implied by refcount_shard_count; no header.
        for seq in self.refcount_durable_seq.iter().copied() {
            p[off..off + 8].copy_from_slice(&seq.to_le_bytes());
            off += 8;
        }
        // v27: per-refcount-shard delta-run segment-directory head follows
        // durable_seq. Only written at v27+; v25/v26 stop after durable_seq so
        // an older binary's decode never sees these bytes. Exactly
        // `refcount_shard_count` entries are written — an empty in-memory array
        // (not yet materialized) synthesizes all-[`NULL_PAGE`], matching decode.
        if self.body_version >= DELTA_RUN_MANIFEST_BODY_VERSION {
            for i in 0..refcount_shard_count {
                let head = self
                    .refcount_delta_run_heads
                    .get(i)
                    .copied()
                    .unwrap_or(NULL_PAGE);
                p[off..off + 8].copy_from_slice(&head.to_le_bytes());
                off += 8;
            }
        }
        // v22: the v17 L2P-page-rc roots/durable_seq arrays are deleted; the
        // dedup head groups follow the refcount arrays directly now.
        for shard_heads in self.dedup_index_shard_heads.iter() {
            p[off..off + 4].copy_from_slice(&(shard_heads.len() as u32).to_le_bytes());
            off += 4;
            for head in shard_heads.iter().copied() {
                p[off..off + 8].copy_from_slice(&head.to_le_bytes());
                off += 8;
            }
        }
        // v23: volumes + snapshots are NOT inlined — they live in the catalog
        // chains anchored above (`vol_head` / `snap_head`). The page ends after
        // the dedup heads.
        debug_assert!(off <= PAGE_PAYLOAD_SIZE);
        Ok(())
    }

    /// Serialise the volume catalog into the byte stream that
    /// [`crate::manifest::catalog`] splits across the volume chain. Reuses the
    /// inline per-entry codec; the entries are simply concatenated.
    pub(crate) fn encode_volume_catalog_bytes(&self) -> Result<Vec<u8>> {
        let total: usize = self
            .volumes
            .iter()
            .map(|v| volume_entry_inline_size(v.shard_count as usize))
            .sum();
        let mut buf = vec![0u8; total];
        let mut off = 0usize;
        for entry in &self.volumes {
            encode_volume_entry_inline(entry, &mut buf, &mut off)?;
        }
        debug_assert_eq!(off, total);
        Ok(buf)
    }

    /// Serialise the snapshot table into the byte stream that
    /// [`crate::manifest::catalog`] splits across the snapshot chain. Each
    /// 48-byte row mirrors the former inline layout; every entry must carry a
    /// durable `l2p_roots_page` (the check that used to live in `encode`).
    pub(crate) fn encode_snapshot_catalog_bytes(&self) -> Result<Vec<u8>> {
        for entry in &self.snapshots {
            if entry.needs_l2p_roots_page() {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} is missing l2p_roots_page",
                    entry.id,
                )));
            }
        }
        let mut buf = vec![0u8; self.snapshots.len() * SNAPSHOT_ENTRY_SIZE];
        for (i, entry) in self.snapshots.iter().enumerate() {
            let r = &mut buf[i * SNAPSHOT_ENTRY_SIZE..(i + 1) * SNAPSHOT_ENTRY_SIZE];
            r[0..8].copy_from_slice(&entry.id.to_le_bytes());
            r[8..10].copy_from_slice(&entry.vol_ord.to_le_bytes());
            // r[10..16] reserved / zero
            r[16..24].copy_from_slice(&entry.l2p_roots_page.to_le_bytes());
            r[24..32].copy_from_slice(&entry.created_lsn.to_le_bytes());
            r[OFF_SNAP_PAGE_DEADLIST_TAIL..OFF_SNAP_PAGE_DEADLIST_TAIL + 8]
                .copy_from_slice(&entry.page_dead_list_tail_pid.to_le_bytes());
            r[OFF_SNAP_CAPTURE_WATERMARK..OFF_SNAP_CAPTURE_WATERMARK + 8]
                .copy_from_slice(&entry.capture_watermark.to_le_bytes());
        }
        Ok(buf)
    }

    fn decode(page: &Page, page_store: &PageStore) -> Result<Self> {
        let p = page.payload();
        let body_version = u32::from_le_bytes(
            p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
                .try_into()
                .unwrap(),
        );
        match body_version {
            25 => Self::decode_v25(page, page_store),
            26 => Self::decode_v26(page, page_store),
            27 => Self::decode_v27(page, page_store),
            other => Err(MetaDbError::Corruption(format!(
                "unsupported manifest body version {other}; only v25, v26 and v27 are readable"
            ))),
        }
    }

    fn decode_v25(page: &Page, page_store: &PageStore) -> Result<Self> {
        Self::decode_body(page, page_store, 25)
    }

    fn decode_v26(page: &Page, page_store: &PageStore) -> Result<Self> {
        Self::decode_body(page, page_store, 26)
    }

    fn decode_v27(page: &Page, page_store: &PageStore) -> Result<Self> {
        Self::decode_body(page, page_store, 27)
    }

    fn decode_body(page: &Page, page_store: &PageStore, version: u32) -> Result<Self> {
        let p = page.payload();
        let checkpoint_lsn = u64::from_le_bytes(
            p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
                .try_into()
                .unwrap(),
        );
        let checkpoint_bfg = u64::from_le_bytes(
            p[OFF_CHECKPOINT_BFG..OFF_CHECKPOINT_BFG + 8]
                .try_into()
                .unwrap(),
        );
        let last_processed_buffer_seq = u64::from_le_bytes(
            p[OFF_LAST_PROCESSED_BUFFER_SEQ..OFF_LAST_PROCESSED_BUFFER_SEQ + 8]
                .try_into()
                .unwrap(),
        );
        let lifecycle_replay_seq = u64::from_le_bytes(
            p[OFF_LIFECYCLE_REPLAY_SEQ..OFF_LIFECYCLE_REPLAY_SEQ + 8]
                .try_into()
                .unwrap(),
        );
        let free_list_head = u64::from_le_bytes(
            p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
                .try_into()
                .unwrap(),
        );
        let refcount_shard_count =
            u32::from_le_bytes(p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4].try_into().unwrap())
                as usize;
        if refcount_shard_count > MAX_SHARD_ROOTS_PER_PAGE {
            return Err(MetaDbError::Corruption(format!(
                "manifest refcount shard_count {refcount_shard_count} exceeds page capacity {MAX_SHARD_ROOTS_PER_PAGE}",
            )));
        }
        // v22: OFF_RESERVED_28 (the former v17 l2p_page_rc shard-count slot)
        // is reserved/zero again and ignored.
        let _ = OFF_RESERVED_28;
        let dedup_shards = u32::from_le_bytes(
            p[OFF_DEDUP_SHARDS..OFF_DEDUP_SHARDS + 4]
                .try_into()
                .unwrap(),
        );
        let dedup_shards_usize = dedup_shards as usize;
        if dedup_shards_usize == 0 || !dedup_shards_usize.is_power_of_two() {
            return Err(MetaDbError::Corruption(format!(
                "manifest dedup_shards {dedup_shards} must be a positive power of two",
            )));
        }
        let (next_snapshot_id, next_volume_ord, snapshot_count, volume_count) =
            decode_tail_counters(p);

        let mut off = OFF_VARIABLE_START;
        let refcount_shard_roots = read_u64_vec(p, &mut off, refcount_shard_count);
        // v11 stores per-refcount-shard durable_seq right after the
        // roots array; v10 has no on-disk array and we synthesise it
        // from `checkpoint_lsn` (durable-seq rollout upgrade path: next manifest
        // commit re-encodes with the actual atomics).
        let refcount_durable_seq: Box<[Lsn]> = if version >= 11 {
            read_u64_vec(p, &mut off, refcount_shard_count)
        } else {
            vec![checkpoint_lsn; refcount_shard_count].into_boxed_slice()
        };
        // v27: per-shard delta-run segment-directory heads follow durable_seq.
        // v25/v26 bodies never wrote them, so synthesize an all-NULL array kept
        // parallel to the roots (the field is version-independent in memory).
        let refcount_delta_run_heads: Box<[PageId]> =
            if version >= DELTA_RUN_MANIFEST_BODY_VERSION {
                read_u64_vec(p, &mut off, refcount_shard_count)
            } else {
                vec![NULL_PAGE; refcount_shard_count].into_boxed_slice()
            };
        // v22: the v17 L2P-page-rc roots/durable_seq arrays are deleted; the
        // dedup head groups follow the refcount arrays directly.

        let mut dedup_index_shard_heads = Vec::with_capacity(DEDUP_META_HEAD_GROUPS);
        let mut total_index_levels = 0usize;
        for _ in 0..DEDUP_META_HEAD_GROUPS {
            let level_count = u32::from_le_bytes(p[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            total_index_levels = total_index_levels.saturating_add(level_count);
            dedup_index_shard_heads.push(read_u64_vec(p, &mut off, level_count));
        }

        // The v22 single-page snapshot-capacity gate is gone — the snapshot
        // table is chained, so `snapshot_count` is bounded only by the chain.
        let _ = total_index_levels;

        // v23: the volume catalog + snapshot table live in two COW chains
        // anchored in the fixed header. Walk each chain back into its byte
        // stream and decode the rows. A torn / missing chain page surfaces as
        // `Err` here, so `ManifestStore::load_latest` cleanly falls back to the
        // other (intact) slot.
        let volume_catalog_head_pid = u64::from_le_bytes(
            p[OFF_VOLUME_CATALOG_HEAD..OFF_VOLUME_CATALOG_HEAD + 8]
                .try_into()
                .unwrap(),
        );
        let snapshot_catalog_head_pid = u64::from_le_bytes(
            p[OFF_SNAPSHOT_CATALOG_HEAD..OFF_SNAPSHOT_CATALOG_HEAD + 8]
                .try_into()
                .unwrap(),
        );
        // v24 fixed-header additions.
        let page_high_water = u64::from_le_bytes(
            p[OFF_PAGE_HIGH_WATER..OFF_PAGE_HIGH_WATER + 8]
                .try_into()
                .unwrap(),
        );
        let journal_ring_head = u64::from_le_bytes(
            p[OFF_JOURNAL_RING_HEAD..OFF_JOURNAL_RING_HEAD + 8]
                .try_into()
                .unwrap(),
        );
        // v25 fixed-header addition.
        let dedup_migration_old_head = u64::from_le_bytes(
            p[OFF_DEDUP_MIGRATION_OLD_HEAD..OFF_DEDUP_MIGRATION_OLD_HEAD + 8]
                .try_into()
                .unwrap(),
        );
        let volume_bytes =
            catalog::read_catalog_chain(page_store, volume_catalog_head_pid, CatalogKind::Volumes)?;
        let volumes = decode_volume_catalog_bytes(&volume_bytes, volume_count, version)?;
        let snapshot_bytes = catalog::read_catalog_chain(
            page_store,
            snapshot_catalog_head_pid,
            CatalogKind::Snapshots,
        )?;
        let snapshots = decode_snapshot_catalog_bytes(&snapshot_bytes, snapshot_count, page_store)?;

        Ok(Self {
            body_version: version,
            checkpoint_lsn,
            checkpoint_bfg,
            last_processed_buffer_seq,
            lifecycle_replay_seq,
            volume_catalog_head_pid,
            snapshot_catalog_head_pid,
            page_high_water,
            journal_ring_head,
            dedup_migration_old_head,
            free_list_head,
            refcount_shard_roots,
            refcount_durable_seq,
            refcount_delta_run_heads,
            dedup_shards,
            dedup_index_shard_heads: dedup_index_shard_heads.into_boxed_slice(),
            next_snapshot_id,
            next_volume_ord,
            snapshots,
            volumes,
        })
    }
}

fn decode_tail_counters(p: &[u8]) -> (u64, VolumeOrdinal, usize, usize) {
    let next_snapshot_id = u64::from_le_bytes(
        p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
            .try_into()
            .unwrap(),
    );
    let next_volume_ord = u16::from_le_bytes(
        p[OFF_NEXT_VOLUME_ORD..OFF_NEXT_VOLUME_ORD + 2]
            .try_into()
            .unwrap(),
    );
    let snapshot_count = u32::from_le_bytes(
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let volume_count = u32::from_le_bytes(
        p[OFF_VOLUME_COUNT..OFF_VOLUME_COUNT + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    (
        next_snapshot_id,
        next_volume_ord,
        snapshot_count,
        volume_count,
    )
}

/// Decode the volume catalog from the byte stream walked out of the volume
/// chain. `volume_count` (from the fixed header) drives the loop; the decoded
/// entries must consume the stream exactly, or the chain is corrupt.
fn decode_volume_catalog_bytes(
    bytes: &[u8],
    volume_count: usize,
    body_version: u32,
) -> Result<Vec<VolumeEntry>> {
    let mut volumes = Vec::with_capacity(volume_count);
    let mut off = 0usize;
    for _ in 0..volume_count {
        volumes.push(decode_volume_entry_inline(bytes, &mut off, body_version)?);
    }
    if off != bytes.len() {
        return Err(MetaDbError::Corruption(format!(
            "manifest volume catalog: {volume_count} entries consumed {off} of {} chain bytes",
            bytes.len(),
        )));
    }
    Ok(volumes)
}

/// Decode the snapshot table from the byte stream walked out of the snapshot
/// chain. Each row is a fixed [`SNAPSHOT_ENTRY_SIZE`] block (v23 layout); the
/// per-snapshot L2P shard roots are loaded from each entry's `SnapshotRoots`
/// page exactly as the legacy inline path did.
fn decode_snapshot_catalog_bytes(
    bytes: &[u8],
    snapshot_count: usize,
    page_store: &PageStore,
) -> Result<Vec<SnapshotEntry>> {
    let expected = snapshot_count.saturating_mul(SNAPSHOT_ENTRY_SIZE);
    if bytes.len() != expected {
        return Err(MetaDbError::Corruption(format!(
            "manifest snapshot catalog: {snapshot_count} rows need {expected} bytes, chain has {}",
            bytes.len(),
        )));
    }
    let mut snapshots = Vec::with_capacity(snapshot_count);
    for i in 0..snapshot_count {
        let r = &bytes[i * SNAPSHOT_ENTRY_SIZE..(i + 1) * SNAPSHOT_ENTRY_SIZE];
        let id = u64::from_le_bytes(r[0..8].try_into().unwrap());
        let vol_ord = u16::from_le_bytes(r[8..10].try_into().unwrap());
        let l2p_roots_page = u64::from_le_bytes(r[16..24].try_into().unwrap());
        let created_lsn = u64::from_le_bytes(r[24..32].try_into().unwrap());
        let page_dead_list_tail_pid = u64::from_le_bytes(
            r[OFF_SNAP_PAGE_DEADLIST_TAIL..OFF_SNAP_PAGE_DEADLIST_TAIL + 8]
                .try_into()
                .unwrap(),
        );
        let capture_watermark = u64::from_le_bytes(
            r[OFF_SNAP_CAPTURE_WATERMARK..OFF_SNAP_CAPTURE_WATERMARK + 8]
                .try_into()
                .unwrap(),
        );
        let l2p_shard_roots = load_snapshot_roots(page_store, l2p_roots_page)?;
        snapshots.push(SnapshotEntry {
            id,
            vol_ord,
            l2p_roots_page,
            created_lsn,
            l2p_shard_roots,
            page_dead_list_tail_pid,
            capture_watermark,
        });
    }
    Ok(snapshots)
}

fn read_u64_vec(p: &[u8], off: &mut usize, count: usize) -> Box<[PageId]> {
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(u64::from_le_bytes(p[*off..*off + 8].try_into().unwrap()));
        *off += 8;
    }
    out.into_boxed_slice()
}

#[doc(hidden)]
pub fn decode_page_for_fuzz(page: &Page, page_store: &PageStore) -> Result<Manifest> {
    Manifest::decode(page, page_store)
}

#[cfg(test)]
mod tests;
