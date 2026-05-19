use super::*;

/// Flag bits on a [`VolumeEntry`].
pub const VOLUME_FLAG_DROP_PENDING: u8 = 0x01;

/// One entry in the v6 manifest `volumes` table.
///
/// `l2p_shard_roots` is stored inline when it fits in the residual page
/// budget; v6 will spill to an external `SnapshotRoots` page (reusing the
/// existing page type + codec) past the threshold. This struct is the
/// in-memory representation; see [`encode_volume_entry_inline`] /
/// [`decode_volume_entry_inline`] for the on-disk form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeEntry {
    pub ord: VolumeOrdinal,
    pub shard_count: u32,
    pub l2p_shard_roots: Box<[PageId]>,
    /// Per-L2P-shard durable LSN watermark, same length as
    /// `l2p_shard_roots`. v11 schema (Tier 2.B Stage 1): persisted
    /// alongside the roots so each shard's `last_flushed_lsn` survives
    /// crash recovery independently. v10 manifests synthesise this
    /// from `checkpoint_lsn` on first open; the next manifest commit
    /// re-encodes as v11 with real per-shard values. Observability-only
    /// for now — consumers (WAL prune, onyx reclaim) still go through
    /// `Manifest::checkpoint_lsn`.
    pub l2p_shard_durable_seq: Box<[Lsn]>,
    pub created_lsn: Lsn,
    pub flags: u8,
    /// Oldest dead-list segment page (Phase 2 / [[no-refcount-hot-path-design]]).
    /// `NULL_PAGE` while the chain is empty. Phase 3 GC consumes from here
    /// forward via the segment's `prev_seg_pid` back-link.
    pub dead_list_head_pid: PageId,
    /// Newest dead-list segment page (apply-time append anchor for the next
    /// checkpoint flush). `NULL_PAGE` while the chain is empty.
    pub dead_list_tail_pid: PageId,
}

/// Size of a [`VolumeEntry`]'s fixed header when encoded inline. The
/// variable `l2p_shard_roots` tail follows immediately.
///
/// v13 grew the fixed header by 16 B (dead-list head/tail page ids).
pub const VOLUME_ENTRY_FIXED_SIZE: usize = 2 /* ord */
    + 4 /* shard_count */
    + 8 /* created_lsn */
    + 1 /* flags */
    + 1 /* reserved / alignment */
    + 8 /* dead_list_head_pid */
    + 8 /* dead_list_tail_pid */;

/// Inline-encoded byte length of a volume entry with the given shard count.
///
/// v11 adds a per-shard `durable_seq: Lsn` array of the same length as
/// the roots array, immediately after it. v10 layout was just the roots
/// tail; current encoders always emit v11.
pub fn volume_entry_inline_size(shard_count: usize) -> usize {
    VOLUME_ENTRY_FIXED_SIZE + shard_count * size_of::<PageId>() + shard_count * size_of::<Lsn>()
}

/// Encode `entry` inline into `buf[off..off+len]` and advance `off`.
///
/// Fails with [`MetaDbError::InvalidArgument`] if the buffer does not have
/// enough residual bytes or if `entry.shard_count` doesn't match the
/// length of `entry.l2p_shard_roots`.
pub fn encode_volume_entry_inline(
    entry: &VolumeEntry,
    buf: &mut [u8],
    off: &mut usize,
) -> Result<()> {
    if entry.l2p_shard_roots.len() != entry.shard_count as usize {
        return Err(MetaDbError::InvalidArgument(format!(
            "volume {} has {} roots, declared shard_count {}",
            entry.ord,
            entry.l2p_shard_roots.len(),
            entry.shard_count,
        )));
    }
    if entry.l2p_shard_durable_seq.len() != entry.shard_count as usize {
        return Err(MetaDbError::InvalidArgument(format!(
            "volume {} has {} durable_seq entries, declared shard_count {}",
            entry.ord,
            entry.l2p_shard_durable_seq.len(),
            entry.shard_count,
        )));
    }
    let needed = volume_entry_inline_size(entry.shard_count as usize);
    if buf.len() < *off + needed {
        return Err(MetaDbError::InvalidArgument(format!(
            "volume entry requires {needed} bytes, only {} available",
            buf.len().saturating_sub(*off),
        )));
    }
    buf[*off..*off + 2].copy_from_slice(&entry.ord.to_le_bytes());
    buf[*off + 2..*off + 6].copy_from_slice(&entry.shard_count.to_le_bytes());
    buf[*off + 6..*off + 14].copy_from_slice(&entry.created_lsn.to_le_bytes());
    buf[*off + 14] = entry.flags;
    buf[*off + 15] = 0; // reserved
    buf[*off + 16..*off + 24].copy_from_slice(&entry.dead_list_head_pid.to_le_bytes());
    buf[*off + 24..*off + 32].copy_from_slice(&entry.dead_list_tail_pid.to_le_bytes());
    *off += VOLUME_ENTRY_FIXED_SIZE;
    for root in entry.l2p_shard_roots.iter().copied() {
        buf[*off..*off + 8].copy_from_slice(&root.to_le_bytes());
        *off += 8;
    }
    // v11: per-L2P-shard durable_seq follows the roots array.
    for seq in entry.l2p_shard_durable_seq.iter().copied() {
        buf[*off..*off + 8].copy_from_slice(&seq.to_le_bytes());
        *off += 8;
    }
    Ok(())
}

/// Decode one volume entry inline from `buf[off..]` and advance `off`.
///
/// `body_version` controls whether the per-shard `l2p_shard_durable_seq`
/// array is read from disk (v11+) or left empty for the manifest-level
/// upgrade path to backfill from `checkpoint_lsn` (v10).
pub fn decode_volume_entry_inline(
    buf: &[u8],
    off: &mut usize,
    body_version: u32,
) -> Result<VolumeEntry> {
    if buf.len() < *off + VOLUME_ENTRY_FIXED_SIZE {
        return Err(MetaDbError::Corruption(format!(
            "volume entry truncated: expected {VOLUME_ENTRY_FIXED_SIZE} header bytes, {} remain",
            buf.len().saturating_sub(*off),
        )));
    }
    let ord = u16::from_le_bytes(buf[*off..*off + 2].try_into().unwrap());
    let shard_count = u32::from_le_bytes(buf[*off + 2..*off + 6].try_into().unwrap());
    let created_lsn = u64::from_le_bytes(buf[*off + 6..*off + 14].try_into().unwrap());
    let flags = buf[*off + 14];
    // buf[*off + 15] reserved
    let dead_list_head_pid = u64::from_le_bytes(buf[*off + 16..*off + 24].try_into().unwrap());
    let dead_list_tail_pid = u64::from_le_bytes(buf[*off + 24..*off + 32].try_into().unwrap());
    *off += VOLUME_ENTRY_FIXED_SIZE;
    let needed_roots = shard_count as usize * size_of::<PageId>();
    if buf.len() < *off + needed_roots {
        return Err(MetaDbError::Corruption(format!(
            "volume {ord} roots truncated: need {needed_roots}, {} remain",
            buf.len().saturating_sub(*off),
        )));
    }
    let mut roots = Vec::with_capacity(shard_count as usize);
    for _ in 0..shard_count {
        roots.push(u64::from_le_bytes(buf[*off..*off + 8].try_into().unwrap()));
        *off += 8;
    }
    // v11 stores per-L2P-shard durable_seq right after roots; v10 has
    // none on disk. Caller (manifest decode_body) will fill v10 entries
    // with checkpoint_lsn after this returns. Empty slice is a stable
    // marker for "needs upgrade" without exposing an Option.
    let l2p_shard_durable_seq: Box<[Lsn]> = if body_version >= 11 {
        let needed_seqs = shard_count as usize * size_of::<Lsn>();
        if buf.len() < *off + needed_seqs {
            return Err(MetaDbError::Corruption(format!(
                "volume {ord} durable_seq truncated: need {needed_seqs}, {} remain",
                buf.len().saturating_sub(*off),
            )));
        }
        let mut seqs = Vec::with_capacity(shard_count as usize);
        for _ in 0..shard_count {
            seqs.push(u64::from_le_bytes(buf[*off..*off + 8].try_into().unwrap()));
            *off += 8;
        }
        seqs.into_boxed_slice()
    } else {
        Box::new([])
    };
    Ok(VolumeEntry {
        ord,
        shard_count,
        l2p_shard_roots: roots.into_boxed_slice(),
        l2p_shard_durable_seq,
        created_lsn,
        flags,
        dead_list_head_pid,
        dead_list_tail_pid,
    })
}
