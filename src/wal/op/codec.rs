use super::*;

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
            WalOp::L2pRemap {
                vol_ord,
                lba,
                new_value,
                guard,
            } => {
                out.push(TAG_L2P_REMAP);
                out.extend_from_slice(&vol_ord.to_be_bytes());
                out.extend_from_slice(&lba.to_be_bytes());
                out.extend_from_slice(&new_value.0);
                match guard {
                    None => out.push(L2P_REMAP_GUARD_NONE),
                    Some((pba, min_rc)) => {
                        out.push(L2P_REMAP_GUARD_SOME);
                        out.extend_from_slice(&pba.to_be_bytes());
                        out.extend_from_slice(&min_rc.to_be_bytes());
                    }
                }
            }
            WalOp::L2pRangeDelete {
                vol_ord,
                start,
                end,
                captured,
            } => {
                out.push(TAG_L2P_RANGE_DELETE);
                out.extend_from_slice(&vol_ord.to_be_bytes());
                out.extend_from_slice(&start.to_be_bytes());
                out.extend_from_slice(&end.to_be_bytes());
                let count: u32 = captured
                    .len()
                    .try_into()
                    .expect("L2pRangeDelete captured count fits in u32");
                out.extend_from_slice(&count.to_be_bytes());
                for (lba, value) in captured {
                    out.extend_from_slice(&lba.to_be_bytes());
                    out.extend_from_slice(&value.0);
                }
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
            WalOp::DropSnapshot {
                id,
                pages,
                pba_decrefs,
            } => {
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
                let decref_count: u32 = pba_decrefs
                    .len()
                    .try_into()
                    .expect("DropSnapshot pba_decrefs count fits in u32");
                out.extend_from_slice(&decref_count.to_be_bytes());
                for pba in pba_decrefs {
                    out.extend_from_slice(&pba.to_be_bytes());
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
            WalOp::L2pRemap { guard, .. } => {
                let base = 1 + 2 + 8 + 28 + 1;
                base + if guard.is_some() { 8 + 4 } else { 0 }
            }
            WalOp::L2pRangeDelete { captured, .. } => 1 + 2 + 8 + 8 + 4 + captured.len() * (8 + 28),
            WalOp::DedupPut { .. } => 1 + 32 + 28,
            WalOp::DedupDelete { .. } => 1 + 32,
            WalOp::DedupReversePut { .. } | WalOp::DedupReverseDelete { .. } => 1 + 8 + 32,
            WalOp::Incref { .. } | WalOp::Decref { .. } => 1 + 8 + 4,
            WalOp::DropSnapshot {
                pages, pba_decrefs, ..
            } => 1 + 8 + 4 + pages.len() * 8 + 4 + pba_decrefs.len() * 8,
            WalOp::CreateVolume { .. } => 1 + 2 + 4,
            WalOp::DropVolume { pages, .. } => 1 + 2 + 4 + pages.len() * 8,
            WalOp::CloneVolume {
                src_shard_roots, ..
            } => 1 + 2 + 2 + 8 + 4 + src_shard_roots.len() * 8,
        }
    }
}

/// Append many ops into a fresh body buffer. The body is prefixed with
/// [`WAL_BODY_SCHEMA_VERSION`]; empty `ops` still produces a 1-byte body
/// (version byte only) so recovery distinguishes "a writer committed
/// zero ops" from "body was never written".
pub fn encode_body(ops: &[WalOp]) -> Vec<u8> {
    let total = 1 + ops.iter().map(|op| op.encoded_len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.push(WAL_BODY_SCHEMA_VERSION);
    for op in ops {
        op.encode(&mut out);
    }
    out
}

/// Decode a WAL record body back into a vector of ops. Fails with
/// [`MetaDbError::Corruption`] on missing/mismatched schema version,
/// any short read, or any unknown tag.
///
/// A pre-v1 body starts with an op tag (`≤ 0x42`) rather than
/// `WAL_BODY_SCHEMA_VERSION` (`0xB1`), so recovery from an old segment
/// hits the "expected 0xB1" branch before it interprets any payload.
pub fn decode_body(body: &[u8]) -> Result<Vec<WalOp>> {
    let first = *body.first().ok_or_else(|| {
        MetaDbError::Corruption(format!(
            "WAL body truncated: expected {} bytes (schema version prefix), got 0",
            1usize,
        ))
    })?;
    if first != WAL_BODY_SCHEMA_VERSION {
        return Err(MetaDbError::Corruption(format!(
            "metadb WAL body version 0x{first:02x} found, expected 0x{:02x} \
             — cross-check Phase A migration",
            WAL_BODY_SCHEMA_VERSION,
        )));
    }
    let mut body = &body[1..];
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
        TAG_L2P_REMAP => {
            // Fixed header before the guard discriminator.
            require_len(payload, 39, "L2P_REMAP header")?;
            let vol_ord = u16::from_be_bytes(payload[..2].try_into().unwrap());
            let lba = u64::from_be_bytes(payload[2..10].try_into().unwrap());
            let mut new_value = [0u8; 28];
            new_value.copy_from_slice(&payload[10..38]);
            let guard_tag = payload[38];
            match guard_tag {
                L2P_REMAP_GUARD_NONE => Ok((
                    WalOp::L2pRemap {
                        vol_ord,
                        lba,
                        new_value: L2pValue(new_value),
                        guard: None,
                    },
                    &payload[39..],
                )),
                L2P_REMAP_GUARD_SOME => {
                    require_len(&payload[39..], 12, "L2P_REMAP guard payload")?;
                    let pba = u64::from_be_bytes(payload[39..47].try_into().unwrap());
                    let min_rc = u32::from_be_bytes(payload[47..51].try_into().unwrap());
                    Ok((
                        WalOp::L2pRemap {
                            vol_ord,
                            lba,
                            new_value: L2pValue(new_value),
                            guard: Some((pba, min_rc)),
                        },
                        &payload[51..],
                    ))
                }
                other => Err(MetaDbError::Corruption(format!(
                    "L2P_REMAP: unknown guard tag 0x{other:02x} \
                     (expected 0x{L2P_REMAP_GUARD_NONE:02x} or 0x{L2P_REMAP_GUARD_SOME:02x})"
                ))),
            }
        }
        TAG_L2P_RANGE_DELETE => {
            require_len(payload, 22, "L2P_RANGE_DELETE header")?;
            let vol_ord = u16::from_be_bytes(payload[..2].try_into().unwrap());
            let start = u64::from_be_bytes(payload[2..10].try_into().unwrap());
            let end = u64::from_be_bytes(payload[10..18].try_into().unwrap());
            let count = u32::from_be_bytes(payload[18..22].try_into().unwrap()) as usize;
            let entry_size = 8 + 28;
            let body_bytes = count
                .checked_mul(entry_size)
                .ok_or_else(|| MetaDbError::Corruption("L2P_RANGE_DELETE count overflow".into()))?;
            require_len(&payload[22..], body_bytes, "L2P_RANGE_DELETE captured list")?;
            let mut captured = Vec::with_capacity(count);
            let mut cursor = 22usize;
            for _ in 0..count {
                let lba = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
                let mut value_bytes = [0u8; 28];
                value_bytes.copy_from_slice(&payload[cursor + 8..cursor + 8 + 28]);
                captured.push((lba, L2pValue(value_bytes)));
                cursor += entry_size;
            }
            Ok((
                WalOp::L2pRangeDelete {
                    vol_ord,
                    start,
                    end,
                    captured,
                },
                &payload[cursor..],
            ))
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
            // S4 appended a second count+pba list for the leaf-rc-suppress
            // compensation decrefs (see SPEC §3.3). Phase A bumps the
            // body schema version, so any pre-S4 body would have been
            // rejected at the version byte; at this point we always
            // expect the trailer.
            require_len(&payload[cursor..], 4, "DROP_SNAPSHOT decref count")?;
            let decref_count =
                u32::from_be_bytes(payload[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let decref_bytes = decref_count.checked_mul(8).ok_or_else(|| {
                MetaDbError::Corruption("DROP_SNAPSHOT pba_decrefs count overflow".into())
            })?;
            require_len(
                &payload[cursor..],
                decref_bytes,
                "DROP_SNAPSHOT pba_decrefs list",
            )?;
            let mut pba_decrefs = Vec::with_capacity(decref_count);
            for _ in 0..decref_count {
                let pba = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
                pba_decrefs.push(pba);
                cursor += 8;
            }
            Ok((
                WalOp::DropSnapshot {
                    id,
                    pages,
                    pba_decrefs,
                },
                &payload[cursor..],
            ))
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
