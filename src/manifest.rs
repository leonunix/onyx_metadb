//! Double-buffered manifest living in pages 0 and 1 of the page store.
//!
//! Phase 7 commit 6 stepped the wire format from v5 to v6 (per-volume
//! L2P roots). Phase 2 of dedup-lane sharding bumps it to v8 by
//! N-way-sharding `dedup_index` and `dedup_reverse`: each LSM is now a
//! list of independent shard-level chains rather than one flat chain.
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
    Lsn, MANIFEST_PAGE_A, MANIFEST_PAGE_B, NULL_PAGE, PageId, SnapshotId, Txg, VolumeOrdinal,
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
/// from 128 → 110. Powers Phase 1 of the
/// [[no-refcount-hot-path-design]] dead-list mechanism. Old v11 (and
/// earlier) manifests are hard-rejected on open — no on-disk migration.
///
/// v13 (per-volume dead-list): added `dead_list_head_pid` and
/// `dead_list_tail_pid` (16 B total) to each `VolumeEntry`'s fixed
/// header. Anchors the append-only chain of segment pages that records
/// every L2P overwrite's `(pba, birth_lsn, death_lsn)` triple for
/// Phase 2 of the [[no-refcount-hot-path-design]]. Old v12 manifests
/// are hard-rejected on open — no on-disk migration.
///
/// v14 (Phase 4 lineage tracking): grew each `VolumeEntry`'s fixed
/// header by 20 B for the trio `parent_vol_ord` (2 B + 2 B pad),
/// `branched_at_lsn` (8 B), `promotion_cursor` (8 B). These power
/// cross-volume snap_pin in Lineage GC and the background promotion
/// walker that lazily turns Clone-Eager L2P shares into independent
/// lineages by incref'ing the global rc per shared PBA. Old v13
/// manifests are hard-rejected on open — no on-disk migration.
///
/// v15 (ZFS-TXG-clone Phase 4): added `checkpoint_txg: u64` at the end
/// of the fixed header (offset 52). `OFF_VARIABLE_START` shifts from
/// 52 → 60. `checkpoint_lsn` is retained and continues to drive WAL
/// prune + recovery; `checkpoint_txg` is the durable TXG counter
/// maintained by the [`crate::txg::TxgStateMachine`]. Old v14 manifests
/// are hard-rejected on open — no on-disk migration (zfs-txg-clone.md
/// explicitly waives backcompat).
///
/// (Compact leaf format independently bumped from v4 → v5: per-leaf
/// `base_pba` (8 B) added to the leaf header and per-unit `base_pba`
/// (8 B) shrunk to a u32 `pba_delta` (4 B) against it, restoring
/// `MAX_UNITS_PER_LEAF` from v4's 110 back to the v3 value of 128 =
/// LEAF_ENTRY_COUNT. Leaf version is checked per page in
/// `paged::leaf_compact`; manifest body version stays at 14 because
/// no manifest field changes.)
///
/// v16 (buffer-as-sole-journal Phase A): added two u64 fields to the
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
/// hard-rejected on open — no on-disk migration (see plan
/// `ethereal-exploring-pretzel.md` Phase A; backcompat is explicitly
/// waived for the WAL-removal track).
pub const MANIFEST_BODY_VERSION: u32 = 16;

// v8 body layout. Fixed header is the same shape as v7 except:
//   - OFF_DEDUP_LEVEL_COUNT is reinterpreted as OFF_DEDUP_SHARDS
//     (number of dedup_index / dedup_reverse shards; same N for both).
//   - OFF_DEDUP_REVERSE_LEVEL_COUNT becomes reserved/future flags.
// The variable region begins at `OFF_VARIABLE_START` and holds:
//   refcount_shard_roots [refcount_shard_count × 8]
//   for each of dedup_shards shards:
//     level_count u32 + level_count × 8 bytes  (dedup_index)
//   for each of dedup_shards shards:
//     level_count u32 + level_count × 8 bytes  (dedup_reverse)
//   snapshots [snapshot_count × SNAPSHOT_ENTRY_SIZE]
//   volumes   [per-entry inline, see encode_volume_entry_inline]
const OFF_BODY_VERSION: usize = 0;
const OFF_CHECKPOINT_LSN: usize = 4;
const OFF_FREE_LIST_HEAD: usize = 12;
const OFF_SHARD_COUNT: usize = 20;
const OFF_DEDUP_SHARDS: usize = 24;
// v7 used this slot for `dedup_reverse_level_count`; v8 reserves it
// (zero-filled). Decoders dispatch on the version field above.
const OFF_RESERVED_28: usize = 28;
const OFF_NEXT_SNAPSHOT_ID: usize = 32;
const OFF_NEXT_VOLUME_ORD: usize = 40;
// 42..44 reserved for alignment / future flags
const OFF_SNAPSHOT_COUNT: usize = 44;
const OFF_VOLUME_COUNT: usize = 48;
// v15 inserts `checkpoint_txg: u64` at offset 52. All earlier offsets are
// unchanged so v14 readers reject the new layout via the version field at
// offset 0 (decode hard-rejects v14).
const OFF_CHECKPOINT_TXG: usize = 52;
// v16 inserts two more u64 fields after `checkpoint_txg`:
//   offset 60: `last_processed_buffer_seq` (onyx LV2 buffer replay watermark)
//   offset 68: `lifecycle_replay_seq` (metadb lifecycle-log replay watermark)
// See `MANIFEST_BODY_VERSION` doc comment for the contract.
const OFF_LAST_PROCESSED_BUFFER_SEQ: usize = 60;
const OFF_LIFECYCLE_REPLAY_SEQ: usize = 68;
const OFF_VARIABLE_START: usize = 76;

/// Per-snapshot row size on disk. v6 packs: id(8) + vol_ord(2) + 6 pad +
/// l2p_roots_page(8) + created_lsn(8). We keep the row at 32 bytes so
/// `max_snapshots_for_layout` math stays identical to v5 for callers
/// that just want "how many fit" — v6 drops the dead `refcount_roots_page`
/// slot, and the freed 6 bytes are reserved for future per-snapshot flags
/// without forcing another format bump.
const SNAPSHOT_ENTRY_SIZE: usize = 32;

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
    assert!(OFF_VOLUME_COUNT + 4 == OFF_CHECKPOINT_TXG);
    assert!(OFF_CHECKPOINT_TXG + 8 == OFF_LAST_PROCESSED_BUFFER_SEQ);
    assert!(OFF_LAST_PROCESSED_BUFFER_SEQ + 8 == OFF_LIFECYCLE_REPLAY_SEQ);
    assert!(OFF_LIFECYCLE_REPLAY_SEQ + 8 == OFF_VARIABLE_START);
    assert!(SNAPSHOT_ENTRY_SIZE == 32);
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
    // length (refcount_shard_count), same element size (8 B).
    let refcount_durable_seq_bytes = refcount_bytes;
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

/// One snapshot's manifest entry. v6 tracks the owning volume's
/// ordinal and the snapshot's L2P shard-root vector (materialised via
/// a [`PageType::SnapshotRoots`] page). Refcount state is global and
/// never snapshotted — Phase 6.5b retired it. Commit 6 always stamps
/// `vol_ord = 0`; commit 9 picks it up for per-volume snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub id: SnapshotId,
    pub vol_ord: VolumeOrdinal,
    pub l2p_roots_page: PageId,
    pub created_lsn: Lsn,
    pub l2p_shard_roots: Box<[PageId]>,
}

impl SnapshotEntry {
    fn needs_l2p_roots_page(&self) -> bool {
        self.l2p_roots_page == NULL_PAGE
    }
}

// ---- Phase 7 / manifest v6 building blocks -------------------------------
//
// Not plugged into the live encode/decode path yet — the write path still
// emits v5. These types + codecs land now so Phase B can flip the wire
// format in one atomic change. Keeping them as standalone additive code
// during Phase A means the 8a soak can keep running against v5 without
// drift.

mod snapshot_roots;
mod store;
mod volume;

pub(crate) use snapshot_roots::{load_snapshot_roots, write_snapshot_roots_page};
pub use store::{LoadedManifest, ManifestStore};
pub use volume::{
    VOLUME_ENTRY_FIXED_SIZE, VOLUME_FLAG_DROP_PENDING, VolumeEntry, decode_volume_entry_inline,
    encode_volume_entry_inline, volume_entry_inline_size,
};

/// Decoded manifest body (v16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub body_version: u32,
    /// Greatest LSN whose WAL record has been applied to durable state.
    pub checkpoint_lsn: Lsn,
    /// ZFS-TXG-clone Phase 4: greatest TXG that the `TxgSyncThread` has
    /// marked synced. Always satisfies `checkpoint_txg + 1 <= open_txg`
    /// at runtime. Persisted in the v15 fixed header (offset 52);
    /// recovery reconstructs `TxgStateMachine` from this value.
    pub checkpoint_txg: Txg,
    /// Buffer-as-sole-journal Phase A: the highest LV2 buffer entry seq
    /// whose flusher-derived metadata mutations are covered by this
    /// checkpoint's page roots. Zero while `metadb_journal_mode = "wal"`
    /// (the WAL is still authoritative); set by onyx's checkpoint hook
    /// in `"buffer"` mode. On recovery, onyx replays buffer entries
    /// with seq > this value through the flusher pipeline.
    pub last_processed_buffer_seq: u64,
    /// Buffer-as-sole-journal Phase A: the highest lifecycle-log record
    /// seq whose effects are covered by this checkpoint. Records with
    /// seq > this value are replayed before the buffer.
    pub lifecycle_replay_seq: u64,
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
    /// Stage 1 (Tier 2.B) of the manifest v11 schema: the global
    /// `checkpoint_lsn` equals `min(refcount_durable_seq[..] ∪ each
    /// volume's l2p_shard_durable_seq[..])`. WAL prune, recovery, and
    /// onyx buffer reclaim still consume `checkpoint_lsn`, so this
    /// array is observability-only for now. Stage 2 will flip
    /// consumers to per-shard reads so partial sample can re-enable
    /// without pinning the global floor on cold shards.
    pub refcount_durable_seq: Box<[Lsn]>,
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
            checkpoint_txg: 0,
            last_processed_buffer_seq: 0,
            lifecycle_replay_seq: 0,
            free_list_head: NULL_PAGE,
            refcount_shard_roots: Vec::new().into_boxed_slice(),
            refcount_durable_seq: Vec::new().into_boxed_slice(),
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
        self.encode(&mut probe)
    }

    /// Stage 1 (Tier 2.B) tripwire: `checkpoint_lsn` must equal the
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

    fn encode(&self, page: &mut Page) -> Result<()> {
        self.assert_durable_seq_invariant()?;
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
        let total_dedup_levels = self.total_dedup_index_levels();

        let volumes_budget_bytes: usize = self
            .volumes
            .iter()
            .map(|v| volume_entry_inline_size(v.shard_count as usize))
            .sum();

        let max_snapshots = max_snapshots_for_layout(
            refcount_shard_count,
            DEDUP_META_HEAD_GROUPS,
            total_dedup_levels,
            volumes_budget_bytes,
        );
        if self.snapshots.len() > max_snapshots {
            return Err(MetaDbError::InvalidArgument(format!(
                "manifest snapshot count {} exceeds capacity {max_snapshots}",
                self.snapshots.len(),
            )));
        }

        for entry in &self.snapshots {
            if entry.needs_l2p_roots_page() {
                return Err(MetaDbError::InvalidArgument(format!(
                    "snapshot {} is missing l2p_roots_page",
                    entry.id,
                )));
            }
        }

        let p = page.payload_mut();
        p.fill(0);
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
            .copy_from_slice(&MANIFEST_BODY_VERSION.to_le_bytes());
        p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
            .copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8]
            .copy_from_slice(&self.free_list_head.to_le_bytes());
        p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4]
            .copy_from_slice(&(refcount_shard_count as u32).to_le_bytes());
        p[OFF_DEDUP_SHARDS..OFF_DEDUP_SHARDS + 4]
            .copy_from_slice(&(dedup_shards as u32).to_le_bytes());
        // OFF_RESERVED_28 stays zero-filled; reserved for future flags.
        p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
            .copy_from_slice(&self.next_snapshot_id.to_le_bytes());
        p[OFF_NEXT_VOLUME_ORD..OFF_NEXT_VOLUME_ORD + 2]
            .copy_from_slice(&self.next_volume_ord.to_le_bytes());
        // OFF_NEXT_VOLUME_ORD + 2 .. OFF_SNAPSHOT_COUNT = 2 bytes reserved (already zero-filled).
        p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
            .copy_from_slice(&(self.snapshots.len() as u32).to_le_bytes());
        p[OFF_VOLUME_COUNT..OFF_VOLUME_COUNT + 4]
            .copy_from_slice(&(self.volumes.len() as u32).to_le_bytes());
        p[OFF_CHECKPOINT_TXG..OFF_CHECKPOINT_TXG + 8]
            .copy_from_slice(&self.checkpoint_txg.to_le_bytes());
        p[OFF_LAST_PROCESSED_BUFFER_SEQ..OFF_LAST_PROCESSED_BUFFER_SEQ + 8]
            .copy_from_slice(&self.last_processed_buffer_seq.to_le_bytes());
        p[OFF_LIFECYCLE_REPLAY_SEQ..OFF_LIFECYCLE_REPLAY_SEQ + 8]
            .copy_from_slice(&self.lifecycle_replay_seq.to_le_bytes());

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
        for shard_heads in self.dedup_index_shard_heads.iter() {
            p[off..off + 4].copy_from_slice(&(shard_heads.len() as u32).to_le_bytes());
            off += 4;
            for head in shard_heads.iter().copied() {
                p[off..off + 8].copy_from_slice(&head.to_le_bytes());
                off += 8;
            }
        }
        for entry in &self.snapshots {
            p[off..off + 8].copy_from_slice(&entry.id.to_le_bytes());
            p[off + 8..off + 10].copy_from_slice(&entry.vol_ord.to_le_bytes());
            // p[off + 10..off + 16] = reserved / zero
            p[off + 16..off + 24].copy_from_slice(&entry.l2p_roots_page.to_le_bytes());
            p[off + 24..off + 32].copy_from_slice(&entry.created_lsn.to_le_bytes());
            off += SNAPSHOT_ENTRY_SIZE;
        }
        for entry in &self.volumes {
            encode_volume_entry_inline(entry, p, &mut off)?;
        }
        Ok(())
    }

    fn decode(page: &Page, page_store: &PageStore) -> Result<Self> {
        let p = page.payload();
        let body_version = u32::from_le_bytes(
            p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4]
                .try_into()
                .unwrap(),
        );
        match body_version {
            16 => Self::decode_v16(page, page_store),
            other => Err(MetaDbError::Corruption(format!(
                "unsupported manifest body version {other}; only v16 \
                 (buffer-as-sole-journal Phase A: last_processed_buffer_seq + \
                 lifecycle_replay_seq) is readable — older databases (v7/v8 \
                 carried the retired dedup_reverse section; v9 carried compact \
                 leaf v2 with the 100-unit cap; v10/v11 used compact leaf v3 \
                 which predates birth_lsn; v12 had birth_lsn but no per-volume \
                 dead-list anchor; v13 had dead-list but no lineage tracking; \
                 v14 had lineage tracking but no checkpoint_txg; v15 had \
                 checkpoint_txg but no buffer-replay watermarks) must be \
                 rebuilt"
            ))),
        }
    }

    fn decode_v16(page: &Page, page_store: &PageStore) -> Result<Self> {
        Self::decode_body(page, page_store, 16)
    }

    fn decode_body(page: &Page, page_store: &PageStore, version: u32) -> Result<Self> {
        let p = page.payload();
        let checkpoint_lsn = u64::from_le_bytes(
            p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
                .try_into()
                .unwrap(),
        );
        let checkpoint_txg = u64::from_le_bytes(
            p[OFF_CHECKPOINT_TXG..OFF_CHECKPOINT_TXG + 8]
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
        // from `checkpoint_lsn` (Stage 1 upgrade path: next manifest
        // commit re-encodes with the actual atomics).
        let refcount_durable_seq: Box<[Lsn]> = if version >= 11 {
            read_u64_vec(p, &mut off, refcount_shard_count)
        } else {
            vec![checkpoint_lsn; refcount_shard_count].into_boxed_slice()
        };

        let mut dedup_index_shard_heads = Vec::with_capacity(DEDUP_META_HEAD_GROUPS);
        let mut total_index_levels = 0usize;
        for _ in 0..DEDUP_META_HEAD_GROUPS {
            let level_count = u32::from_le_bytes(p[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            total_index_levels = total_index_levels.saturating_add(level_count);
            dedup_index_shard_heads.push(read_u64_vec(p, &mut off, level_count));
        }

        let max_snapshots = max_snapshots_for_layout(
            refcount_shard_count,
            DEDUP_META_HEAD_GROUPS,
            total_index_levels,
            0,
        );
        if snapshot_count > max_snapshots {
            return Err(MetaDbError::Corruption(format!(
                "manifest snapshot_count {snapshot_count} exceeds capacity {max_snapshots}",
            )));
        }

        let snapshots = decode_snapshots(p, &mut off, snapshot_count, page_store)?;
        let mut volumes = decode_volumes(p, &mut off, volume_count, version)?;
        if version < 11 {
            // v10 upgrade: backfill every volume's per-L2P-shard
            // durable_seq from the single `checkpoint_lsn`. The next
            // commit re-encodes as v11 with real per-shard atomics.
            for vol in &mut volumes {
                vol.l2p_shard_durable_seq =
                    vec![checkpoint_lsn; vol.shard_count as usize].into_boxed_slice();
            }
        }

        Ok(Self {
            body_version: MANIFEST_BODY_VERSION,
            checkpoint_lsn,
            checkpoint_txg,
            last_processed_buffer_seq,
            lifecycle_replay_seq,
            free_list_head,
            refcount_shard_roots,
            refcount_durable_seq,
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

fn decode_snapshots(
    p: &[u8],
    off: &mut usize,
    snapshot_count: usize,
    page_store: &PageStore,
) -> Result<Vec<SnapshotEntry>> {
    let mut snapshots = Vec::with_capacity(snapshot_count);
    for _ in 0..snapshot_count {
        let id = u64::from_le_bytes(p[*off..*off + 8].try_into().unwrap());
        let vol_ord = u16::from_le_bytes(p[*off + 8..*off + 10].try_into().unwrap());
        let l2p_roots_page = u64::from_le_bytes(p[*off + 16..*off + 24].try_into().unwrap());
        let created_lsn = u64::from_le_bytes(p[*off + 24..*off + 32].try_into().unwrap());
        let l2p_shard_roots = load_snapshot_roots(page_store, l2p_roots_page)?;
        snapshots.push(SnapshotEntry {
            id,
            vol_ord,
            l2p_roots_page,
            created_lsn,
            l2p_shard_roots,
        });
        *off += SNAPSHOT_ENTRY_SIZE;
    }
    Ok(snapshots)
}

fn decode_volumes(
    p: &[u8],
    off: &mut usize,
    volume_count: usize,
    body_version: u32,
) -> Result<Vec<VolumeEntry>> {
    let mut volumes = Vec::with_capacity(volume_count);
    for _ in 0..volume_count {
        let entry = decode_volume_entry_inline(p, off, body_version)?;
        volumes.push(entry);
    }
    Ok(volumes)
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
