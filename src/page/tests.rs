use super::*;

#[test]
fn header_round_trip() {
    let h = PageHeader {
        page_type: PageType::L2pLeaf,
        version: PAGE_VERSION,
        key_count: 42,
        flags: 0xDEAD_BEEF,
        generation: 0x1234_5678_9ABC_DEF0,
        refcount: 7,
    };
    let mut p = Page::zeroed();
    p.write_header(&h);
    p.seal();
    let read = p.header().unwrap();
    assert_eq!(read.page_type, h.page_type);
    assert_eq!(read.version, h.version);
    assert_eq!(read.key_count, h.key_count);
    assert_eq!(read.flags, h.flags);
    assert_eq!(read.generation, h.generation);
    assert_eq!(read.refcount, h.refcount);
}

#[test]
fn verify_succeeds_on_sealed_page() {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
    p.payload_mut()[0..4].copy_from_slice(b"test");
    p.seal();
    p.verify(7).unwrap();
}

#[test]
fn verify_catches_payload_bit_flip() {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
    p.payload_mut()[0] = 0xAB;
    p.seal();
    assert!(p.verify(0).is_ok());

    p.bytes_mut()[100] ^= 0x01;
    match p.verify(42).unwrap_err() {
        MetaDbError::PageChecksumMismatch { page_id, .. } => {
            assert_eq!(page_id, 42);
        }
        e => panic!("wrong error: {e}"),
    }
}

#[test]
fn verify_catches_header_bit_flip() {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
    p.seal();
    assert!(p.verify(0).is_ok());
    p.set_key_count(999); // not resealed
    assert!(matches!(
        p.verify(0).unwrap_err(),
        MetaDbError::PageChecksumMismatch { .. }
    ));
}

#[test]
fn crc_is_independent_of_crc_field_contents() {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
    p.seal();
    let crc1 = p.compute_crc();
    put_u32_le(&mut *p.bytes, OFF_CRC, 0xFFFF_FFFF);
    let crc2 = p.compute_crc();
    assert_eq!(crc1, crc2, "CRC must be identical regardless of CRC field");
}

#[test]
fn magic_mismatch_reports_page_id() {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
    p.seal();
    put_u32_le(&mut *p.bytes, OFF_MAGIC, 0xDEAD_BEEF);
    match p.verify(42).unwrap_err() {
        MetaDbError::PageMagicMismatch { page_id, found } => {
            assert_eq!(page_id, 42);
            assert_eq!(found, 0xDEAD_BEEF);
        }
        e => panic!("wrong error: {e}"),
    }
}

#[test]
fn version_mismatch_reports_page_id() {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
    p.bytes_mut()[OFF_VERSION] = 99;
    p.seal();
    match p.verify(42).unwrap_err() {
        MetaDbError::PageVersionUnsupported { page_id, version } => {
            assert_eq!(page_id, 42);
            assert_eq!(version, 99);
        }
        e => panic!("wrong error: {e}"),
    }
}

#[test]
fn page_type_round_trip() {
    for t in [
        PageType::Free,
        PageType::L2pLeaf,
        PageType::L2pInternal,
        PageType::LsmData,
        PageType::FreeListNode,
        PageType::Manifest,
    ] {
        assert_eq!(PageType::from_u8(t as u8).unwrap(), t);
    }
    assert!(PageType::from_u8(99).is_err());
}

#[test]
fn crc_is_deterministic_and_sensitive() {
    let make = |seed: u8| {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 100));
        p.payload_mut()[..8].copy_from_slice(&[seed; 8]);
        p.seal();
        p
    };
    let a1 = make(1);
    let a2 = make(1);
    let b = make(2);
    assert_eq!(a1.compute_crc(), a2.compute_crc());
    assert_ne!(a1.compute_crc(), b.compute_crc());
}

#[test]
fn empty_page_verify_fails() {
    let p = Page::zeroed();
    assert!(matches!(
        p.verify(0).unwrap_err(),
        MetaDbError::PageMagicMismatch { .. }
    ));
}

#[test]
fn type_header_access_is_disjoint_from_payload() {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
    p.type_header_mut()
        .copy_from_slice(&[0xAAu8; TYPE_HEADER_SIZE]);
    p.payload_mut().fill(0x55);
    p.seal();
    assert!(p.verify(0).is_ok());
    assert_eq!(p.type_header(), &[0xAAu8; TYPE_HEADER_SIZE]);
    assert_eq!(p.payload()[0], 0x55);
}
