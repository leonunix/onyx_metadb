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
    // +1 for the schema version prefix
    assert_eq!(body.len(), 1 + 1 + 2 + 8 + 28);
    assert_eq!(body[0], WAL_BODY_SCHEMA_VERSION);
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
    // A v1 body with zero ops is still a 1-byte buffer (just the
    // version prefix). A truly empty buffer hits the "truncated
    // before the version byte" branch.
    assert!(decode_body(&[WAL_BODY_SCHEMA_VERSION]).unwrap().is_empty());
}

#[test]
fn completely_empty_body_is_corruption() {
    match decode_body(&[]).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(
            msg.contains("schema version prefix"),
            "unexpected msg: {msg}",
        ),
        e => panic!("{e}"),
    }
}

#[test]
fn legacy_body_without_schema_version_is_rejected() {
    // Hand-craft a "pre-Phase-A" body: starts directly with the
    // L2P_PUT tag (0x01). A modern decoder must refuse it loudly,
    // not silently interpret the tag as a version byte.
    let mut body = Vec::new();
    body.push(TAG_L2P_PUT); // 0x01: not the schema version byte
    body.extend_from_slice(&0u16.to_be_bytes()); // vol_ord
    body.extend_from_slice(&42u64.to_be_bytes()); // lba
    body.extend_from_slice(&[0u8; 28]); // value
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(msg.contains("body version"), "unexpected msg: {msg}");
            assert!(
                msg.contains(&format!("0x{:02x}", WAL_BODY_SCHEMA_VERSION)),
                "expected version byte mentioned: {msg}",
            );
            assert!(
                msg.contains("Phase A migration"),
                "expected migration hint: {msg}",
            );
        }
        e => panic!("{e}"),
    }
}

#[test]
fn future_body_version_is_rejected_loudly() {
    // Simulate a WAL body written by a newer metadb that bumps the
    // schema byte. Current binary must refuse rather than parse.
    let mut body = vec![WAL_BODY_SCHEMA_VERSION.wrapping_add(1)];
    body.extend_from_slice(&[0u8; 4]);
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(msg.contains("body version"), "unexpected msg: {msg}",)
        }
        e => panic!("{e}"),
    }
}

#[test]
fn unknown_tag_is_corruption() {
    let body = vec![WAL_BODY_SCHEMA_VERSION, 0xFF, 0, 0, 0];
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(msg.contains("unknown WAL op tag")),
        e => panic!("{e}"),
    }
}

#[test]
fn truncated_payload_is_corruption() {
    // L2P_PUT expects 38 bytes of payload (vol_ord + lba + value);
    // a 10-byte tail is short.
    let body = vec![
        WAL_BODY_SCHEMA_VERSION,
        TAG_L2P_PUT,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ];
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
    assert_eq!(body.len(), 1 + (1 + 2 + 8 + 28) + (1 + 2 + 8));
    assert_eq!(decode_body(&body).unwrap(), ops);
}

/// Helper: prepend the schema-version byte so the hand-crafted body
/// hits the targeted truncation branch in `decode_one` instead of
/// the version-rejection branch.
fn ver_prefix(tail: &[u8]) -> Vec<u8> {
    let mut v = vec![WAL_BODY_SCHEMA_VERSION];
    v.extend_from_slice(tail);
    v
}

#[test]
fn l2p_put_truncated_vol_header_is_corruption() {
    // Only 1 byte of payload — even the vol_ord isn't complete.
    let body = ver_prefix(&[TAG_L2P_PUT, 0x00]);
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(msg.contains("L2P_PUT")),
        e => panic!("{e}"),
    }
}

#[test]
fn l2p_delete_truncated_vol_header_is_corruption() {
    let body = ver_prefix(&[TAG_L2P_DELETE, 0x00]);
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
        pba_decrefs: Vec::new(),
    }];
    let body = encode_body(&ops);
    // schema(1) + tag(1) + id(8) + page_count(4) + decref_count(4)
    assert_eq!(body.len(), 1 + 1 + 8 + 4 + 4);
    let decoded = decode_body(&body).unwrap();
    assert_eq!(decoded, ops);
}

#[test]
fn drop_snapshot_round_trip_many_pages() {
    let pages: Vec<u64> = (100..200).collect();
    let ops = vec![WalOp::DropSnapshot {
        id: u64::MAX - 1,
        pages: pages.clone(),
        pba_decrefs: Vec::new(),
    }];
    let body = encode_body(&ops);
    assert_eq!(body.len(), 1 + 1 + 8 + 4 + pages.len() * 8 + 4);
    let decoded = decode_body(&body).unwrap();
    assert_eq!(decoded, ops);
}

#[test]
fn drop_snapshot_round_trip_with_pba_decrefs() {
    // S4: the trailer list carries leaf-rc-suppress compensation
    // decrefs. Encoding must preserve order and allow duplicates
    // (onyx packed-slot scenario: same pba N times, each is one
    // decref during apply).
    let pba_decrefs = vec![100u64, 200, 100, 300];
    let ops = vec![WalOp::DropSnapshot {
        id: 9,
        pages: vec![10, 11],
        pba_decrefs: pba_decrefs.clone(),
    }];
    let body = encode_body(&ops);
    assert_eq!(
        body.len(),
        1 + 1 + 8 + 4 + 2 * 8 + 4 + pba_decrefs.len() * 8,
    );
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
            pba_decrefs: vec![50, 51],
        },
        WalOp::Incref { pba: 20, delta: 1 },
    ];
    let body = encode_body(&ops);
    let decoded = decode_body(&body).unwrap();
    assert_eq!(decoded, ops);
}

#[test]
fn drop_snapshot_truncated_header_is_corruption() {
    let body = ver_prefix(&[TAG_DROP_SNAPSHOT, 0, 0, 0, 0]);
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(msg.contains("DROP_SNAPSHOT header")),
        e => panic!("{e}"),
    }
}

#[test]
fn drop_snapshot_truncated_page_list_is_corruption() {
    // count=3 but only 2 pids worth of payload
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_DROP_SNAPSHOT];
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
fn drop_snapshot_truncated_pba_decrefs_count_is_corruption() {
    // Pages present but the trailer's 4-byte decref_count is absent.
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_DROP_SNAPSHOT];
    body.extend_from_slice(&1u64.to_be_bytes()); // id
    body.extend_from_slice(&1u32.to_be_bytes()); // page_count=1
    body.extend_from_slice(&42u64.to_be_bytes()); // single page
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(
            msg.contains("DROP_SNAPSHOT decref count"),
            "unexpected msg: {msg}",
        ),
        e => panic!("{e}"),
    }
}

#[test]
fn drop_snapshot_truncated_pba_decrefs_list_is_corruption() {
    // decref_count=3 but only 2 pbas follow.
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_DROP_SNAPSHOT];
    body.extend_from_slice(&1u64.to_be_bytes()); // id
    body.extend_from_slice(&0u32.to_be_bytes()); // no pages
    body.extend_from_slice(&3u32.to_be_bytes()); // decref_count=3
    body.extend_from_slice(&100u64.to_be_bytes());
    body.extend_from_slice(&200u64.to_be_bytes());
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(
            msg.contains("DROP_SNAPSHOT pba_decrefs list"),
            "unexpected msg: {msg}",
        ),
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
    let expected: usize = 1 + ops.iter().map(|op| op.encoded_len()).sum::<usize>();
    assert_eq!(encode_body(&ops).len(), expected);
}

#[test]
fn create_volume_round_trip() {
    let ops = vec![WalOp::CreateVolume {
        ord: 0xABCD,
        shard_count: 16,
    }];
    let body = encode_body(&ops);
    assert_eq!(body.len(), 1 + 1 + 2 + 4);
    assert_eq!(decode_body(&body).unwrap(), ops);
}

#[test]
fn drop_volume_round_trip() {
    let ops = vec![WalOp::DropVolume {
        ord: 42,
        pages: (100..120).collect(),
    }];
    let body = encode_body(&ops);
    assert_eq!(body.len(), 1 + 1 + 2 + 4 + 20 * 8);
    assert_eq!(decode_body(&body).unwrap(), ops);
}

#[test]
fn drop_volume_empty_pages_round_trip() {
    let ops = vec![WalOp::DropVolume {
        ord: 0,
        pages: Vec::new(),
    }];
    let body = encode_body(&ops);
    assert_eq!(body.len(), 1 + 1 + 2 + 4);
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
    assert_eq!(body.len(), 1 + 1 + 2 + 2 + 8 + 4 + 8 * 8);
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
    let body = vec![WAL_BODY_SCHEMA_VERSION, TAG_CREATE_VOLUME, 0x00, 0x01];
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(msg.contains("CREATE_VOLUME")),
        e => panic!("{e}"),
    }
}

#[test]
fn drop_volume_truncated_header_is_corruption() {
    let body = vec![WAL_BODY_SCHEMA_VERSION, TAG_DROP_VOLUME, 0x00];
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(msg.contains("DROP_VOLUME header")),
        e => panic!("{e}"),
    }
}

#[test]
fn drop_volume_truncated_page_list_is_corruption() {
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_DROP_VOLUME];
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
    let body = vec![
        WAL_BODY_SCHEMA_VERSION,
        TAG_CLONE_VOLUME,
        0x00,
        0x01,
        0x00,
        0x02,
    ];
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(msg.contains("CLONE_VOLUME header")),
        e => panic!("{e}"),
    }
}

#[test]
fn l2p_remap_no_guard_round_trip() {
    let ops = vec![WalOp::L2pRemap {
        vol_ord: 0xABCD,
        lba: 0xDEAD_BEEF_CAFE_F00D,
        new_value: v(0x42),
        guard: None,
    }];
    let body = encode_body(&ops);
    // schema(1) + tag(1) + vol_ord(2) + lba(8) + value(28) + guard_tag(1)
    assert_eq!(body.len(), 1 + 40);
    assert_eq!(decode_body(&body).unwrap(), ops);
}

#[test]
fn l2p_remap_with_guard_round_trip() {
    let ops = vec![WalOp::L2pRemap {
        vol_ord: 7,
        lba: 12345,
        new_value: v(0xAB),
        guard: Some((0x0123_4567_89AB_CDEF, 5)),
    }];
    let body = encode_body(&ops);
    // schema(1) + base(40) + pba(8) + min_rc(4)
    assert_eq!(body.len(), 1 + 52);
    assert_eq!(decode_body(&body).unwrap(), ops);
}

#[test]
fn l2p_remap_interleaves_with_other_ops() {
    let ops = vec![
        WalOp::L2pRemap {
            vol_ord: 1,
            lba: 1,
            new_value: v(1),
            guard: None,
        },
        WalOp::Incref { pba: 99, delta: 3 },
        WalOp::L2pRemap {
            vol_ord: 2,
            lba: 2,
            new_value: v(2),
            guard: Some((42, u32::MAX)),
        },
        WalOp::DedupDelete { hash: h(3) },
        WalOp::L2pDelete { vol_ord: 1, lba: 1 },
    ];
    let body = encode_body(&ops);
    assert_eq!(decode_body(&body).unwrap(), ops);
}

#[test]
fn l2p_remap_truncated_header_is_corruption() {
    // Full 39-byte header, no guard tag yet.
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_L2P_REMAP];
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&0u64.to_be_bytes());
    body.extend_from_slice(&[0u8; 20]); // only 20 of 28 new_value bytes
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(msg.contains("L2P_REMAP header"), "unexpected msg: {msg}")
        }
        e => panic!("{e}"),
    }
}

#[test]
fn l2p_remap_truncated_guard_payload_is_corruption() {
    // Header + guard=Some tag but only 8/12 guard bytes.
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_L2P_REMAP];
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&2u64.to_be_bytes());
    body.extend_from_slice(&[0u8; 28]);
    body.push(L2P_REMAP_GUARD_SOME);
    body.extend_from_slice(&10u64.to_be_bytes()); // pba — missing min_rc
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(msg.contains("L2P_REMAP guard"), "unexpected msg: {msg}")
        }
        e => panic!("{e}"),
    }
}

#[test]
fn l2p_remap_unknown_guard_tag_is_corruption() {
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_L2P_REMAP];
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&0u64.to_be_bytes());
    body.extend_from_slice(&[0u8; 28]);
    body.push(0x7F); // unrecognised guard discriminator
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(msg.contains("unknown guard tag"), "unexpected msg: {msg}")
        }
        e => panic!("{e}"),
    }
}

#[test]
fn l2p_remap_encoded_len_matches_encode_output() {
    let cases = vec![
        WalOp::L2pRemap {
            vol_ord: 0,
            lba: 0,
            new_value: v(0),
            guard: None,
        },
        WalOp::L2pRemap {
            vol_ord: u16::MAX,
            lba: u64::MAX,
            new_value: v(0xFF),
            guard: Some((u64::MAX, u32::MAX)),
        },
    ];
    for op in cases {
        let expected = op.encoded_len();
        let mut buf = Vec::new();
        op.encode(&mut buf);
        assert_eq!(buf.len(), expected, "mismatch for {op:?}");
    }
}

#[test]
fn l2p_range_delete_empty_captured_round_trip() {
    let ops = vec![WalOp::L2pRangeDelete {
        vol_ord: 0xABCD,
        start: 100,
        end: 200,
        captured: Vec::new(),
    }];
    let body = encode_body(&ops);
    // schema(1) + tag(1) + vol_ord(2) + start(8) + end(8) + count(4)
    assert_eq!(body.len(), 1 + 1 + 22);
    assert_eq!(decode_body(&body).unwrap(), ops);
}

#[test]
fn l2p_range_delete_round_trip_preserves_order_and_duplicates() {
    // Dedup case: multiple lbas pointing at the same pba — order
    // and duplicates must survive the codec because apply needs
    // to emit one decref per entry (SPEC §4.7).
    fn val(pba: u64) -> L2pValue {
        let mut v = [0u8; 28];
        v[..8].copy_from_slice(&pba.to_be_bytes());
        L2pValue(v)
    }
    let captured = vec![
        (10u64, val(100)),
        (11, val(100)),
        (12, val(100)),
        (13, val(200)),
    ];
    let ops = vec![WalOp::L2pRangeDelete {
        vol_ord: 7,
        start: 10,
        end: 14,
        captured: captured.clone(),
    }];
    let body = encode_body(&ops);
    assert_eq!(body.len(), 1 + 1 + 22 + captured.len() * (8 + 28));
    match &decode_body(&body).unwrap()[0] {
        WalOp::L2pRangeDelete { captured: c, .. } => assert_eq!(c, &captured),
        other => panic!("expected L2pRangeDelete, got {other:?}"),
    }
}

#[test]
fn l2p_range_delete_interleaves_with_other_ops() {
    fn val(pba: u64) -> L2pValue {
        let mut v = [0u8; 28];
        v[..8].copy_from_slice(&pba.to_be_bytes());
        L2pValue(v)
    }
    let ops = vec![
        WalOp::L2pRangeDelete {
            vol_ord: 1,
            start: 0,
            end: 10,
            captured: vec![(0, val(100)), (5, val(200))],
        },
        WalOp::Incref { pba: 300, delta: 4 },
        WalOp::L2pRangeDelete {
            vol_ord: 2,
            start: u64::MAX - 10,
            end: u64::MAX,
            captured: Vec::new(),
        },
        WalOp::L2pDelete {
            vol_ord: 3,
            lba: 42,
        },
    ];
    let body = encode_body(&ops);
    assert_eq!(decode_body(&body).unwrap(), ops);
}

#[test]
fn l2p_range_delete_truncated_header_is_corruption() {
    // Only 10 of 22 bytes of header present.
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_L2P_RANGE_DELETE];
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&[0u8; 8]); // start only
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(
            msg.contains("L2P_RANGE_DELETE header"),
            "unexpected msg: {msg}"
        ),
        e => panic!("{e}"),
    }
}

#[test]
fn l2p_range_delete_truncated_captured_list_is_corruption() {
    // count=3 but only 2 (lba,pba) pairs follow.
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_L2P_RANGE_DELETE];
    body.extend_from_slice(&1u16.to_be_bytes()); // vol_ord
    body.extend_from_slice(&0u64.to_be_bytes()); // start
    body.extend_from_slice(&10u64.to_be_bytes()); // end
    body.extend_from_slice(&3u32.to_be_bytes()); // count=3
    // Two pairs only:
    body.extend_from_slice(&0u64.to_be_bytes());
    body.extend_from_slice(&100u64.to_be_bytes());
    body.extend_from_slice(&1u64.to_be_bytes());
    body.extend_from_slice(&200u64.to_be_bytes());
    match decode_body(&body).unwrap_err() {
        MetaDbError::Corruption(msg) => assert!(
            msg.contains("L2P_RANGE_DELETE captured list"),
            "unexpected msg: {msg}"
        ),
        e => panic!("{e}"),
    }
}

#[test]
fn l2p_range_delete_encoded_len_matches_encode_output() {
    fn val(pba: u64) -> L2pValue {
        let mut v = [0u8; 28];
        v[..8].copy_from_slice(&pba.to_be_bytes());
        L2pValue(v)
    }
    let captured: Vec<(u64, L2pValue)> = (0..5000).map(|i| (i, val(i * 2))).collect();
    let op = WalOp::L2pRangeDelete {
        vol_ord: 0,
        start: 0,
        end: 5000,
        captured,
    };
    let expected = op.encoded_len();
    let mut buf = Vec::new();
    op.encode(&mut buf);
    assert_eq!(buf.len(), expected);
}

#[test]
fn clone_volume_truncated_roots_is_corruption() {
    let mut body = vec![WAL_BODY_SCHEMA_VERSION, TAG_CLONE_VOLUME];
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
