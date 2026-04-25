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
    pub created_lsn: Lsn,
    pub flags: u8,
}

/// Size of a [`VolumeEntry`]'s fixed header when encoded inline. The
/// variable `l2p_shard_roots` tail follows immediately.
pub const VOLUME_ENTRY_FIXED_SIZE: usize = 2 /* ord */
    + 4 /* shard_count */
    + 8 /* created_lsn */
    + 1 /* flags */
    + 1 /* reserved / alignment */;

/// Inline-encoded byte length of a volume entry with the given shard count.
pub fn volume_entry_inline_size(shard_count: usize) -> usize {
    VOLUME_ENTRY_FIXED_SIZE + shard_count * size_of::<PageId>()
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
    *off += VOLUME_ENTRY_FIXED_SIZE;
    for root in entry.l2p_shard_roots.iter().copied() {
        buf[*off..*off + 8].copy_from_slice(&root.to_le_bytes());
        *off += 8;
    }
    Ok(())
}

/// Decode one volume entry inline from `buf[off..]` and advance `off`.
pub fn decode_volume_entry_inline(buf: &[u8], off: &mut usize) -> Result<VolumeEntry> {
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
    Ok(VolumeEntry {
        ord,
        shard_count,
        l2p_shard_roots: roots.into_boxed_slice(),
        created_lsn,
        flags,
    })
}
