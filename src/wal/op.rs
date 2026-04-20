//! WAL operation record codec.
//!
//! One WAL record body is a concatenation of tagged fixed-size ops. The
//! ops describe every mutation the `Db` can replay after a crash: L2P
//! puts and deletes, dedup puts and deletes, and PBA refcount
//! increments and decrements. Each op carries exactly the bytes needed
//! to reproduce the mutation; there is no length prefix because the
//! tag determines the payload size.
//!
//! Snapshot take / drop deliberately do NOT live in the WAL: they are
//! always committed by a manifest write that advances
//! `checkpoint_lsn`, so recovery never needs to replay them.
//!
//! # Body layout
//!
//! ```text
//! [tag: 1B][payload: fixed per tag] × N
//! ```
//!
//! Tag table:
//!
//! | tag | mnemonic            | payload                                     | size |
//! |-----|---------------------|---------------------------------------------|------|
//! | 01  | `L2P_PUT`           | lba (8 B BE) + value (28 B)                 |  36  |
//! | 02  | `L2P_DELETE`        | lba (8 B BE)                                |   8  |
//! | 10  | `DEDUP_PUT`         | hash (32 B) + value (28 B)                  |  60  |
//! | 11  | `DEDUP_DEL`         | hash (32 B)                                 |  32  |
//! | 12  | `DEDUP_REVERSE_PUT` | pba (8 B BE) + hash (32 B)                  |  40  |
//! | 13  | `DEDUP_REVERSE_DEL` | pba (8 B BE) + hash (32 B)                  |  40  |
//! | 20  | `INCREF`            | pba (8 B BE) + delta (4 B BE)               |  12  |
//! | 21  | `DECREF`            | pba (8 B BE) + delta (4 B BE)               |  12  |
//!
//! Keys use big-endian so byte order matches numeric order; that's
//! consistent with the rest of metadb.

use crate::btree::L2pValue;
use crate::error::{MetaDbError, Result};
use crate::lsm::{DedupValue, Hash32};
use crate::types::{Lba, Pba};

pub const TAG_L2P_PUT: u8 = 0x01;
pub const TAG_L2P_DELETE: u8 = 0x02;
pub const TAG_DEDUP_PUT: u8 = 0x10;
pub const TAG_DEDUP_DELETE: u8 = 0x11;
pub const TAG_DEDUP_REVERSE_PUT: u8 = 0x12;
pub const TAG_DEDUP_REVERSE_DELETE: u8 = 0x13;
pub const TAG_INCREF: u8 = 0x20;
pub const TAG_DECREF: u8 = 0x21;

/// One mutation op as stored in a WAL record body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalOp {
    L2pPut {
        lba: Lba,
        value: L2pValue,
    },
    L2pDelete {
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
}

impl WalOp {
    /// Append the encoded bytes of this op to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            WalOp::L2pPut { lba, value } => {
                out.push(TAG_L2P_PUT);
                out.extend_from_slice(&lba.to_be_bytes());
                out.extend_from_slice(&value.0);
            }
            WalOp::L2pDelete { lba } => {
                out.push(TAG_L2P_DELETE);
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
        }
    }

    /// Serialized length of this op in bytes.
    pub fn encoded_len(&self) -> usize {
        match self {
            WalOp::L2pPut { .. } => 1 + 8 + 28,
            WalOp::L2pDelete { .. } => 1 + 8,
            WalOp::DedupPut { .. } => 1 + 32 + 28,
            WalOp::DedupDelete { .. } => 1 + 32,
            WalOp::DedupReversePut { .. } | WalOp::DedupReverseDelete { .. } => 1 + 8 + 32,
            WalOp::Incref { .. } | WalOp::Decref { .. } => 1 + 8 + 4,
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
            require_len(payload, 36, "L2P_PUT")?;
            let lba = u64::from_be_bytes(payload[..8].try_into().unwrap());
            let mut v = [0u8; 28];
            v.copy_from_slice(&payload[8..36]);
            Ok((
                WalOp::L2pPut {
                    lba,
                    value: L2pValue(v),
                },
                &payload[36..],
            ))
        }
        TAG_L2P_DELETE => {
            require_len(payload, 8, "L2P_DELETE")?;
            let lba = u64::from_be_bytes(payload[..8].try_into().unwrap());
            Ok((WalOp::L2pDelete { lba }, &payload[8..]))
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
            lba: 42,
            value: v(7),
        }];
        let body = encode_body(&ops);
        assert_eq!(body.len(), 1 + 8 + 28);
        let decoded = decode_body(&body).unwrap();
        assert_eq!(decoded, ops);
    }

    #[test]
    fn multi_op_round_trip_preserves_order() {
        let ops = vec![
            WalOp::L2pPut {
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
            WalOp::L2pDelete { lba: 8 },
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
        // A L2P_PUT expects 36 bytes but only 10 are present.
        let body = vec![TAG_L2P_PUT, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        match decode_body(&body).unwrap_err() {
            MetaDbError::Corruption(msg) => assert!(msg.contains("L2P_PUT")),
            e => panic!("{e}"),
        }
    }

    #[test]
    fn encoded_len_matches_encode_output() {
        let ops = vec![
            WalOp::L2pPut {
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
}
