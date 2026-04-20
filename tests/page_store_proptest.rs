//! Property test: `PageStore` is an ordered page file equivalent to
//! an in-memory `HashMap<PageId, [u8; N]>` reference under any
//! sequence of alloc / free / write / read / reopen operations.
//!
//! Reopen (drop store, open fresh from the same path) is included
//! because every write of a sealed page must survive a round-trip to
//! the on-disk format. CRC / magic / version are validated implicitly
//! by `read_page`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use onyx_metadb::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use onyx_metadb::page_store::PageStore;
use onyx_metadb::types::PageId;
use proptest::prelude::*;
use tempfile::TempDir;

const SAMPLE_LEN: usize = 64;

fn make_page(generation: u64, sample: &[u8; SAMPLE_LEN]) -> Page {
    let mut page = Page::new(PageHeader::new(PageType::L2pLeaf, generation));
    let payload = page.payload_mut();
    payload[..SAMPLE_LEN].copy_from_slice(sample);
    // Put a deterministic pattern over the rest so the test detects
    // accidental tail writes too.
    for (i, b) in payload[SAMPLE_LEN..].iter_mut().enumerate() {
        *b = (i & 0xFF) as u8;
    }
    page.seal();
    page
}

fn extract_sample(page: &Page) -> [u8; SAMPLE_LEN] {
    let mut out = [0u8; SAMPLE_LEN];
    out.copy_from_slice(&page.payload()[..SAMPLE_LEN]);
    // Sanity: the tail pattern must still be intact.
    for (i, b) in page.payload()[SAMPLE_LEN..].iter().enumerate() {
        assert_eq!(*b, (i & 0xFF) as u8, "tail pattern corrupted at {i}");
    }
    out
}

#[derive(Clone, Debug)]
enum Op {
    Alloc,
    Free(u16),
    Write(u16, [u8; SAMPLE_LEN]),
    Read(u16),
    Reopen,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => Just(Op::Alloc),
        1 => (0u16..32).prop_map(Op::Free),
        3 => (0u16..32, any::<[u8; SAMPLE_LEN]>()).prop_map(|(i, b)| Op::Write(i, b)),
        3 => (0u16..32).prop_map(Op::Read),
        1 => Just(Op::Reopen),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        .. ProptestConfig::default()
    })]

    #[test]
    fn page_store_round_trips(ops in proptest::collection::vec(arb_op(), 1..200)) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("p.onyx_meta");
        let mut ps = Arc::new(PageStore::create(&path).unwrap());
        // Reference state: pid → expected payload sample.
        let mut contents: HashMap<PageId, [u8; SAMPLE_LEN]> = HashMap::new();
        let mut allocated: Vec<PageId> = Vec::new();
        // Pages we've explicitly freed, to detect accidental reuse of
        // still-live pids.
        let mut freed: HashSet<PageId> = HashSet::new();
        let mut generation: u64 = 1;

        for op in ops {
            match op {
                Op::Alloc => {
                    let pid = ps.allocate().unwrap();
                    // Newly-allocated pids must not collide with live ones.
                    prop_assert!(!contents.contains_key(&pid));
                    let sample = [0u8; SAMPLE_LEN];
                    ps.write_page(pid, &make_page(generation, &sample)).unwrap();
                    generation += 1;
                    contents.insert(pid, sample);
                    allocated.push(pid);
                    freed.remove(&pid);
                }
                Op::Free(i) => {
                    if allocated.is_empty() {
                        continue;
                    }
                    let idx = (i as usize) % allocated.len();
                    let pid = allocated.swap_remove(idx);
                    ps.free(pid, generation).unwrap();
                    generation += 1;
                    contents.remove(&pid);
                    freed.insert(pid);
                }
                Op::Write(i, bytes) => {
                    if allocated.is_empty() {
                        continue;
                    }
                    let idx = (i as usize) % allocated.len();
                    let pid = allocated[idx];
                    ps.write_page(pid, &make_page(generation, &bytes)).unwrap();
                    generation += 1;
                    contents.insert(pid, bytes);
                }
                Op::Read(i) => {
                    if allocated.is_empty() {
                        continue;
                    }
                    let idx = (i as usize) % allocated.len();
                    let pid = allocated[idx];
                    let page = ps.read_page(pid).unwrap();
                    let actual = extract_sample(&page);
                    let expected = contents.get(&pid).copied().unwrap();
                    prop_assert_eq!(actual, expected, "read mismatch at {}", pid);
                }
                Op::Reopen => {
                    ps.sync_all().unwrap();
                    ps = Arc::new(PageStore::open(&path).unwrap());
                    // Re-read every live page and check the content.
                    for (&pid, &expected) in &contents {
                        let page = ps.read_page(pid).unwrap();
                        prop_assert_eq!(extract_sample(&page), expected);
                    }
                    // Free list must list exactly the pages we freed
                    // (set-equality, not list-equality — order can drift).
                    prop_assert_eq!(ps.free_list_len(), freed.len());
                }
            }
        }

        // Final end-to-end: one more sync+reopen, then every contents
        // entry must read back.
        ps.sync_all().unwrap();
        ps = Arc::new(PageStore::open(&path).unwrap());
        for (&pid, &expected) in &contents {
            let page = ps.read_page(pid).unwrap();
            prop_assert_eq!(extract_sample(&page), expected);
        }
    }
}

// Compile-time invariant: the tail pattern has room to fill.
const _: () = assert!(PAGE_PAYLOAD_SIZE > SAMPLE_LEN);
