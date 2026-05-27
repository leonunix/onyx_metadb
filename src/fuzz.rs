//! Small helper entrypoints for `cargo-fuzz` targets.

use std::path::PathBuf;
use std::process;
use std::sync::{Arc, OnceLock};

use crate::config::PAGE_SIZE;
use crate::manifest::decode_page_for_fuzz;
use crate::page::Page;
use crate::page_store::PageStore;

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
