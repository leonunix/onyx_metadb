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
//! [tag: 1B][payload: per tag] × N
//! ```
//!
//! Tag table:
//!
//! | tag | mnemonic            | payload                                                                             |   size   |
//! |-----|---------------------|-------------------------------------------------------------------------------------|----------|
//! | 01  | `L2P_PUT`           | vol_ord (2 B BE) + lba (8 B BE) + value (28 B)                                      |    38    |
//! | 02  | `L2P_DELETE`        | vol_ord (2 B BE) + lba (8 B BE)                                                     |    10    |
//! | 10  | `DEDUP_PUT`         | hash (32 B) + value (28 B)                                                          |    60    |
//! | 11  | `DEDUP_DEL`         | hash (32 B)                                                                         |    32    |
//! | 12  | `DEDUP_REVERSE_PUT` | pba (8 B BE) + hash (32 B)                                                          |    40    |
//! | 13  | `DEDUP_REVERSE_DEL` | pba (8 B BE) + hash (32 B)                                                          |    40    |
//! | 20  | `INCREF`            | pba (8 B BE) + delta (4 B BE)                                                       |    12    |
//! | 21  | `DECREF`            | pba (8 B BE) + delta (4 B BE)                                                       |    12    |
//! | 30  | `DROP_SNAPSHOT`     | id (8 B BE) + count (4 B BE) + pid×count                                            |  12+8n   |
//! | 40  | `CREATE_VOLUME`     | ord (2 B BE) + shard_count (4 B BE)                                                 |     6    |
//! | 41  | `DROP_VOLUME`       | ord (2 B BE) + count (4 B BE) + pid×count                                           |   6+8n   |
//! | 42  | `CLONE_VOLUME`      | src_ord (2 B BE) + new_ord (2 B BE) + snap_id (8 B BE) + shard_count (4 B BE) + pid×shard_count | 16+8n |
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

use crate::error::{MetaDbError, Result};
use crate::lsm::{DedupValue, Hash32};
use crate::paged::L2pValue;
use crate::types::{Lba, PageId, Pba, SnapshotId, VolumeOrdinal};

pub const TAG_L2P_PUT: u8 = 0x01;
pub const TAG_L2P_DELETE: u8 = 0x02;
pub const TAG_DEDUP_PUT: u8 = 0x10;
pub const TAG_DEDUP_DELETE: u8 = 0x11;
pub const TAG_DEDUP_REVERSE_PUT: u8 = 0x12;
pub const TAG_DEDUP_REVERSE_DELETE: u8 = 0x13;
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
    DedupPut {
        hash: Hash32,
        value: DedupValue,
    },
    DedupDelete {
        hash: Hash32,
    },
    /// Record that `pba` owns `hash`. Stored in `dedup_reverse` so a
    /// later `decref_pba → 0` can prefix-scan by PBA and tombstone the
    /// corresponding `dedup_index` entries.
    DedupReversePut {
        pba: Pba,
        hash: Hash32,
    },
    /// Tombstone a `(pba, hash)` entry in `dedup_reverse`.
    DedupReverseDelete {
        pba: Pba,
        hash: Hash32,
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
    DropSnapshot {
        id: SnapshotId,
        pages: Vec<PageId>,
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

impl WalOp {
    /// Append the encoded bytes of this op to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            WalOp::L2pPut {
                vol_ord,
                lba,
                value,
            } => {
                out.push(TAG_L2P_PUT);
                out.extend_from_slice(&vol_ord.to_be_bytes());
                out.extend_from_slice(&lba.to_be_bytes());
                out.extend_from_slice(&value.0);
            }
            WalOp::L2pDelete { vol_ord, lba } => {
                out.push(TAG_L2P_DELETE);
                out.extend_from_slice(&vol_ord.to_be_bytes());
                out.extend_from_slice(&lba.to_be_bytes());
            }
            WalOp::DedupPut { hash, value } => {
                out.push(TAG_DEDUP_PUT);
                out.extend_from_slice(hash);
                out.extend_from_slice(&value.0);
            }
            WalOp::DedupDelete { hash } => {
                out.push(TAG_DEDUP_DELETE);
                out.extend_from_slice(hash);
            }
            WalOp::DedupReversePut { pba, hash } => {
                out.push(TAG_DEDUP_REVERSE_PUT);
                out.extend_from_slice(&pba.to_be_bytes());
                out.extend_from_slice(hash);
            }
            WalOp::DedupReverseDelete { pba, hash } => {
                out.push(TAG_DEDUP_REVERSE_DELETE);
                out.extend_from_slice(&pba.to_be_bytes());
                out.extend_from_slice(hash);
            }
            WalOp::Incref { pba, delta } => {
                out.push(TAG_INCREF);
                out.extend_from_slice(&pba.to_be_bytes());
                out.extend_from_slice(&delta.to_be_bytes());
            }
            WalOp::Decref { pba, delta } => {
                out.push(TAG_DECREF);
                out.extend_from_slice(&pba.to_be_bytes());
                out.extend_from_slice(&delta.to_be_bytes());
            }
            WalOp::DropSnapshot { id, pages } => {
                out.push(TAG_DROP_SNAPSHOT);
                out.extend_from_slice(&id.to_be_bytes());
                let count: u32 = pages
                    .len()
                    .try_into()
                    .expect("DropSnapshot page count fits in u32");
                out.extend_from_slice(&count.to_be_bytes());
                for pid in pages {
                    out.extend_from_slice(&pid.to_be_bytes());
                }
            }
            WalOp::CreateVolume { ord, shard_count } => {
                out.push(TAG_CREATE_VOLUME);
                out.extend_from_slice(&ord.to_be_bytes());
                out.extend_from_slice(&shard_count.to_be_bytes());
            }
            WalOp::DropVolume { ord, pages } => {
                out.push(TAG_DROP_VOLUME);
                out.extend_from_slice(&ord.to_be_bytes());
                let count: u32 = pages
                    .len()
                    .try_into()
                    .expect("DropVolume page count fits in u32");
                out.extend_from_slice(&count.to_be_bytes());
                for pid in pages {
                    out.extend_from_slice(&pid.to_be_bytes());
                }
            }
            WalOp::CloneVolume {
                src_ord,
                new_ord,
                src_snap_id,
                src_shard_roots,
            } => {
                out.push(TAG_CLONE_VOLUME);
                out.extend_from_slice(&src_ord.to_be_bytes());
                out.extend_from_slice(&new_ord.to_be_bytes());
                out.extend_from_slice(&src_snap_id.to_be_bytes());
                let count: u32 = src_shard_roots
                    .len()
                    .try_into()
                    .expect("CloneVolume shard count fits in u32");
                out.extend_from_slice(&count.to_be_bytes());
                for pid in src_shard_roots {
                    out.extend_from_slice(&pid.to_be_bytes());
                }
            }
        }
    }

    /// Serialized length of this op in bytes.
    pub fn encoded_len(&self) -> usize {
        match self {
            WalOp::L2pPut { .. } => 1 + 2 + 8 + 28,
            WalOp::L2pDelete { .. } => 1 + 2 + 8,
            WalOp::DedupPut { .. } => 1 + 32 + 28,
            WalOp::DedupDelete { .. } => 1 + 32,
            WalOp::DedupReversePut { .. } | WalOp::DedupReverseDelete { .. } => 1 + 8 + 32,
            WalOp::Incref { .. } | WalOp::Decref { .. } => 1 + 8 + 4,
            WalOp::DropSnapshot { pages, .. } => 1 + 8 + 4 + pages.len() * 8,
            WalOp::CreateVolume { .. } => 1 + 2 + 4,
            WalOp::DropVolume { pages, .. } => 1 + 2 + 4 + pages.len() * 8,
            WalOp::CloneVolume {
                src_shard_roots, ..
            } => 1 + 2 + 2 + 8 + 4 + src_shard_roots.len() * 8,
        }
    }
}

/// Append many ops into a fresh body buffer.
pub fn encode_body(ops: &[WalOp]) -> Vec<u8> {
    let total = ops.iter().map(|op| op.encoded_len()).sum();
    let mut out = Vec::with_capacity(total);
    for op in ops {
        op.encode(&mut out);
    }
    out
}

/// Decode a WAL record body back into a vector of ops. Fails with
/// [`MetaDbError::Corruption`] on any short read or unknown tag.
pub fn decode_body(mut body: &[u8]) -> Result<Vec<WalOp>> {
    let mut out = Vec::new();
    while !body.is_empty() {
        let (op, rest) = decode_one(body)?;
        out.push(op);
        body = rest;
    }
    Ok(out)
}

fn decode_one(body: &[u8]) -> Result<(WalOp, &[u8])> {
    let tag = *body.first().ok_or_else(short_read)?;
    let payload = &body[1..];
    match tag {
        TAG_L2P_PUT => {
            require_len(payload, 38, "L2P_PUT")?;
            let vol_ord = u16::from_be_bytes(payload[..2].try_into().unwrap());
            let lba = u64::from_be_bytes(payload[2..10].try_into().unwrap());
            let mut v = [0u8; 28];
            v.copy_from_slice(&payload[10..38]);
            Ok((
                WalOp::L2pPut {
                    vol_ord,
                    lba,
                    value: L2pValue(v),
                },
                &payload[38..],
            ))
        }
        TAG_L2P_DELETE => {
            require_len(payload, 10, "L2P_DELETE")?;
            let vol_ord = u16::from_be_bytes(payload[..2].try_into().unwrap());
            let lba = u64::from_be_bytes(payload[2..10].try_into().unwrap());
            Ok((WalOp::L2pDelete { vol_ord, lba }, &payload[10..]))
        }
        TAG_DEDUP_PUT => {
            require_len(payload, 60, "DEDUP_PUT")?;
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&payload[..32]);
            let mut v = [0u8; 28];
            v.copy_from_slice(&payload[32..60]);
            Ok((
                WalOp::DedupPut {
                    hash,
                    value: DedupValue(v),
                },
                &payload[60..],
            ))
        }
        TAG_DEDUP_DELETE => {
            require_len(payload, 32, "DEDUP_DELETE")?;
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&payload[..32]);
            Ok((WalOp::DedupDelete { hash }, &payload[32..]))
        }
        TAG_DEDUP_REVERSE_PUT | TAG_DEDUP_REVERSE_DELETE => {
            require_len(payload, 40, "DEDUP_REVERSE")?;
            let pba = u64::from_be_bytes(payload[..8].try_into().unwrap());
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&payload[8..40]);
            let op = if tag == TAG_DEDUP_REVERSE_PUT {
                WalOp::DedupReversePut { pba, hash }
            } else {
                WalOp::DedupReverseDelete { pba, hash }
            };
            Ok((op, &payload[40..]))
        }
        TAG_INCREF | TAG_DECREF => {
            require_len(payload, 12, "INCREF/DECREF")?;
            let pba = u64::from_be_bytes(payload[..8].try_into().unwrap());
            let delta = u32::from_be_bytes(payload[8..12].try_into().unwrap());
            let op = if tag == TAG_INCREF {
                WalOp::Incref { pba, delta }
            } else {
                WalOp::Decref { pba, delta }
            };
            Ok((op, &payload[12..]))
        }
        TAG_DROP_SNAPSHOT => {
            require_len(payload, 12, "DROP_SNAPSHOT header")?;
            let id = u64::from_be_bytes(payload[..8].try_into().unwrap());
            let count = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
            let pages_bytes = count
                .checked_mul(8)
                .ok_or_else(|| MetaDbError::Corruption("DROP_SNAPSHOT count overflow".into()))?;
            require_len(&payload[12..], pages_bytes, "DROP_SNAPSHOT page list")?;
            let mut pages = Vec::with_capacity(count);
            let mut cursor = 12usize;
            for _ in 0..count {
                let pid = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
                pages.push(pid);
                cursor += 8;
            }
            Ok((WalOp::DropSnapshot { id, pages }, &payload[cursor..]))
        }
        TAG_CREATE_VOLUME => {
            require_len(payload, 6, "CREATE_VOLUME")?;
            let ord = u16::from_be_bytes(payload[..2].try_into().unwrap());
            let shard_count = u32::from_be_bytes(payload[2..6].try_into().unwrap());
            Ok((WalOp::CreateVolume { ord, shard_count }, &payload[6..]))
        }
        TAG_DROP_VOLUME => {
            require_len(payload, 6, "DROP_VOLUME header")?;
            let ord = u16::from_be_bytes(payload[..2].try_into().unwrap());
            let count = u32::from_be_bytes(payload[2..6].try_into().unwrap()) as usize;
            let pages_bytes = count
                .checked_mul(8)
                .ok_or_else(|| MetaDbError::Corruption("DROP_VOLUME count overflow".into()))?;
            require_len(&payload[6..], pages_bytes, "DROP_VOLUME page list")?;
            let mut pages = Vec::with_capacity(count);
            let mut cursor = 6usize;
            for _ in 0..count {
                let pid = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
                pages.push(pid);
                cursor += 8;
            }
            Ok((WalOp::DropVolume { ord, pages }, &payload[cursor..]))
        }
        TAG_CLONE_VOLUME => {
            require_len(payload, 16, "CLONE_VOLUME header")?;
            let src_ord = u16::from_be_bytes(payload[..2].try_into().unwrap());
            let new_ord = u16::from_be_bytes(payload[2..4].try_into().unwrap());
            let src_snap_id = u64::from_be_bytes(payload[4..12].try_into().unwrap());
            let shard_count = u32::from_be_bytes(payload[12..16].try_into().unwrap()) as usize;
            let roots_bytes = shard_count
                .checked_mul(8)
                .ok_or_else(|| MetaDbError::Corruption("CLONE_VOLUME count overflow".into()))?;
            require_len(&payload[16..], roots_bytes, "CLONE_VOLUME roots")?;
            let mut roots = Vec::with_capacity(shard_count);
            let mut cursor = 16usize;
            for _ in 0..shard_count {
                let pid = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
                roots.push(pid);
                cursor += 8;
            }
            Ok((
                WalOp::CloneVolume {
                    src_ord,
                    new_ord,
                    src_snap_id,
                    src_shard_roots: roots,
                },
                &payload[cursor..],
            ))
        }
        other => Err(MetaDbError::Corruption(format!(
            "unknown WAL op tag 0x{other:02x}"
        ))),
    }
}

fn require_len(buf: &[u8], need: usize, what: &str) -> Result<()> {
    if buf.len() < need {
        Err(MetaDbError::Corruption(format!(
            "WAL {what}: expected {need} bytes of payload, got {}",
            buf.len()
        )))
    } else {
        Ok(())
    }
}

fn short_read() -> MetaDbError {
    MetaDbError::Corruption("WAL op body truncated (expected tag)".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(n: u8) -> L2pValue {
        let mut x = [0u8; 28];
        x[0] = n;
        L2pValue(x)
    }

    fn dv(n: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = n;
        DedupValue(x)
    }

    fn h(n: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = n;
        x
    }

    #[test]
    fn single_op_round_trip() {
        let ops = vec![WalOp::L2pPut {
            vol_ord: 0,
            lba: 42,
            value: v(7),
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 2 + 8 + 28);
        let decoded = decode_body(&body).unwrap();
        assert_eq!(decoded, ops);
    }

    #[test]
    fn multi_op_round_trip_preserves_order() {
        let ops = vec![
            WalOp::L2pPut {
                vol_ord: 0,
                lba: 1,
                value: v(1),
            },
            WalOp::DedupPut {
                hash: h(2),
                value: dv(3),
            },
            WalOp::Incref { pba: 4, delta: 5 },
            WalOp::DedupDelete { hash: h(6) },
            WalOp::Decref { pba: 7, delta: 1 },
            WalOp::L2pDelete { vol_ord: 7, lba: 8 },
        ];
        let body = encode_body(&ops);
        let decoded = decode_body(&body).unwrap();
        assert_eq!(decoded, ops);
    }

    #[test]
    fn empty_body_decodes_as_empty_vec() {
        assert!(decode_body(&[]).unwrap().is_empty());
    }

    #[test]
    fn unknown_tag_is_corruption() {
        let body = vec![0xFF, 0, 0, 0];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("unknown WAL op tag")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn truncated_payload_is_corruption() {
        // L2P_PUT expects 38 bytes of payload (vol_ord + lba + value);
        // a 10-byte tail is short.
        let body = vec![TAG_L2P_PUT, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("L2P_PUT")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn l2p_put_vol_ord_round_trip_max_u16() {
        // Explicitly exercise the 2-byte vol_ord field at the high end of
        // u16 so a decoder that treats it as signed or narrows it to u8
        // would fail here.
        let ops = vec![
            WalOp::L2pPut {
                vol_ord: 0xABCD,
                lba: 0xDEAD_BEEF_CAFE_F00D,
                value: v(0xAB),
            },
            WalOp::L2pDelete {
                vol_ord: u16::MAX - 1,
                lba: 0x1234_5678_9ABC_DEF0,
            },
        ];
        let body = encode_body(&ops);
        assert_eq!(body.len(), (1 + 2 + 8 + 28) + (1 + 2 + 8));
        assert_eq!(decode_body(&body).unwrap(), ops);
    }

    #[test]
    fn l2p_put_truncated_vol_header_is_corruption() {
        // Only 1 byte of payload — even the vol_ord isn't complete.
        let body = vec![TAG_L2P_PUT, 0x00];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("L2P_PUT")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn l2p_delete_truncated_vol_header_is_corruption() {
        let body = vec![TAG_L2P_DELETE, 0x00];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("L2P_DELETE")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn drop_snapshot_round_trip_empty_pages() {
        let ops = vec![WalOp::DropSnapshot {
            id: 42,
            pages: Vec::new(),
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 8 + 4);
        let decoded = decode_body(&body).unwrap();
        assert_eq!(decoded, ops);
    }

    #[test]
    fn drop_snapshot_round_trip_many_pages() {
        let pages: Vec<u64> = (100..200).collect();
        let ops = vec![WalOp::DropSnapshot {
            id: u64::MAX - 1,
            pages: pages.clone(),
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 8 + 4 + pages.len() * 8);
        let decoded = decode_body(&body).unwrap();
        assert_eq!(decoded, ops);
    }

    #[test]
    fn drop_snapshot_survives_interleaving() {
        let ops = vec![
            WalOp::L2pPut {
                vol_ord: 0,
                lba: 1,
                value: v(1),
            },
            WalOp::DropSnapshot {
                id: 7,
                pages: vec![10, 11, 12],
            },
            WalOp::Incref { pba: 20, delta: 1 },
        ];
        let body = encode_body(&ops);
        let decoded = decode_body(&body).unwrap();
        assert_eq!(decoded, ops);
    }

    #[test]
    fn drop_snapshot_truncated_header_is_corruption() {
        let body = vec![TAG_DROP_SNAPSHOT, 0, 0, 0, 0];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("DROP_SNAPSHOT header")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn drop_snapshot_truncated_page_list_is_corruption() {
        // count=3 but only 2 pids worth of payload
        let mut body = vec![TAG_DROP_SNAPSHOT];
        body.extend_from_slice(&7u64.to_be_bytes());
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(&1u64.to_be_bytes());
        body.extend_from_slice(&2u64.to_be_bytes());
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("DROP_SNAPSHOT page list")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn encoded_len_matches_encode_output() {
        let ops = vec![
            WalOp::L2pPut {
                vol_ord: 0,
                lba: 1,
                value: v(1),
            },
            WalOp::DedupPut {
                hash: h(2),
                value: dv(3),
            },
            WalOp::Incref { pba: 4, delta: 5 },
        ];
        let expected: usize = ops.iter().map(|op| op.encoded_len()).sum();
        assert_eq!(encode_body(&ops).len(), expected);
    }

    #[test]
    fn create_volume_round_trip() {
        let ops = vec![WalOp::CreateVolume {
            ord: 0xABCD,
            shard_count: 16,
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 2 + 4);
        assert_eq!(decode_body(&body).unwrap(), ops);
    }

    #[test]
    fn drop_volume_round_trip() {
        let ops = vec![WalOp::DropVolume {
            ord: 42,
            pages: (100..120).collect(),
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 2 + 4 + 20 * 8);
        assert_eq!(decode_body(&body).unwrap(), ops);
    }

    #[test]
    fn drop_volume_empty_pages_round_trip() {
        let ops = vec![WalOp::DropVolume {
            ord: 0,
            pages: Vec::new(),
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 2 + 4);
        assert_eq!(decode_body(&body).unwrap(), ops);
    }

    #[test]
    fn clone_volume_round_trip() {
        let ops = vec![WalOp::CloneVolume {
            src_ord: 7,
            new_ord: 42,
            src_snap_id: 0xDEAD_BEEF,
            src_shard_roots: vec![100, 101, 102, 103, 104, 105, 106, 107],
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 2 + 2 + 8 + 4 + 8 * 8);
        assert_eq!(decode_body(&body).unwrap(), ops);
    }

    #[test]
    fn clone_volume_zero_shards_round_trip() {
        // Boundary: a 0-shard clone decodes cleanly (vol with no shards
        // isn't useful but the codec must not misread).
        let ops = vec![WalOp::CloneVolume {
            src_ord: 1,
            new_ord: 2,
            src_snap_id: 3,
            src_shard_roots: Vec::new(),
        }];
        let body = encode_body(&ops);
        assert_eq!(decode_body(&body).unwrap(), ops);
    }

    #[test]
    fn volume_ops_interleave_with_legacy_ops() {
        let ops = vec![
            WalOp::CreateVolume {
                ord: 1,
                shard_count: 4,
            },
            WalOp::L2pPut {
                vol_ord: 1,
                lba: 100,
                value: v(7),
            },
            WalOp::Incref { pba: 50, delta: 2 },
            WalOp::DropVolume {
                ord: 99,
                pages: vec![200, 201],
            },
            WalOp::CloneVolume {
                src_ord: 3,
                new_ord: 4,
                src_snap_id: 77,
                src_shard_roots: vec![10, 11],
            },
            WalOp::DedupDelete { hash: h(9) },
        ];
        let body = encode_body(&ops);
        assert_eq!(decode_body(&body).unwrap(), ops);
    }

    #[test]
    fn create_volume_truncated_is_corruption() {
        let body = vec![TAG_CREATE_VOLUME, 0x00, 0x01];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("CREATE_VOLUME")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn drop_volume_truncated_header_is_corruption() {
        let body = vec![TAG_DROP_VOLUME, 0x00];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("DROP_VOLUME header")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn drop_volume_truncated_page_list_is_corruption() {
        let mut body = vec![TAG_DROP_VOLUME];
        body.extend_from_slice(&7u16.to_be_bytes());
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(&1u64.to_be_bytes());
        body.extend_from_slice(&2u64.to_be_bytes());
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("DROP_VOLUME page list")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn clone_volume_truncated_header_is_corruption() {
        let body = vec![TAG_CLONE_VOLUME, 0x00, 0x01, 0x00, 0x02];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("CLONE_VOLUME header")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn clone_volume_truncated_roots_is_corruption() {
        let mut body = vec![TAG_CLONE_VOLUME];
        body.extend_from_slice(&1u16.to_be_bytes()); // src_ord
        body.extend_from_slice(&2u16.to_be_bytes()); // new_ord
        body.extend_from_slice(&3u64.to_be_bytes()); // src_snap_id
        body.extend_from_slice(&2u32.to_be_bytes()); // shard_count=2
        body.extend_from_slice(&10u64.to_be_bytes()); // only one root
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("CLONE_VOLUME roots")),
            e => panic!("{e}"),
        }
    }
}
