//! Small helper entrypoints for `cargo-fuzz` targets.

use std::path::PathBuf;
use std::process;
use std::sync::{Arc, OnceLock};

use crate::config::PAGE_SIZE;
use crate::lsm::BloomFilter;
use crate::manifest::decode_page_for_fuzz;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::wal::WalRecordIter;
use crate::wal::op::decode_body;

#[doc(hidden)]
pub fn wal_record_decode(input: &[u8]) {
    let mut iter = WalRecordIter::new(input);
    for record in iter.by_ref() {
        let _ = decode_body(record.body);
    }
    let _ = iter.stopped();
}

#[doc(hidden)]
pub fn page_header_decode(input: &[u8]) {
    if input.len() != PAGE_SIZE {
        return;
    }
    let mut bytes = [0u8; PAGE_SIZE];
    bytes.copy_from_slice(input);
    let page = Page::from_raw_bytes(bytes);
    let _ = page.header();
    let _ = page.verify(0);
}

#[doc(hidden)]
pub fn manifest_body_decode(input: &[u8]) {
    if input.len() != PAGE_SIZE {
        return;
    }
    let mut bytes = [0u8; PAGE_SIZE];
    bytes.copy_from_slice(input);
    let page = Page::from_raw_bytes(bytes);
    let _ = decode_page_for_fuzz(&page, fuzz_page_store());
}

#[doc(hidden)]
pub fn bloom_bits_decode(input: &[u8]) {
    if input.is_empty() {
        return;
    }
    let bits = if input.len() > 1 {
        input[1..].to_vec()
    } else {
        vec![0u8; 1]
    };
    let bit_count = (bits.len() as u32).saturating_mul(8).max(8);
    let hash_count = (input[0] as u32 % 16).max(1);
    let bloom = BloomFilter::from_parts(bits, bit_count, hash_count);
    let mut hash = [0u8; 32];
    for chunk in input.chunks(8).take(4).enumerate() {
        let (idx, bytes) = chunk;
        let start = idx * 8;
        hash[start..start + bytes.len()].copy_from_slice(bytes);
    }
    let _ = bloom.maybe_contains(&hash);
}

fn fuzz_page_store() -> &'static PageStore {
    static STORE: OnceLock<Arc<PageStore>> = OnceLock::new();
    STORE.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("onyx-metadb-fuzz-{}", process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path: PathBuf = dir.join("pages.onyx_meta");
        match PageStore::open(&path) {
            Ok(store) => Arc::new(store),
            Err(_) => {
                let _ = std::fs::remove_file(&path);
                Arc::new(PageStore::create(&path).expect("create fuzz page store"))
            }
        }
    })
}
