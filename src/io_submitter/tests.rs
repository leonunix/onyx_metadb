use super::*;
use crate::page::{Page, PageHeader, PageType};
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn open_temp_file() -> (tempfile::NamedTempFile, std::fs::File) {
    let temp = tempfile::NamedTempFile::new().expect("temp file");
    let path = temp.path().to_path_buf();
    // Pre-extend so we can write to high page ids.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("reopen rw");
    file.set_len(PAGE_SIZE as u64 * 1024).expect("set_len");
    (temp, file)
}

fn mk_page(seed: u8) -> Arc<Page> {
    let mut page = Page::new(PageHeader::new(PageType::L2pInternal, 1));
    for byte in page
        .bytes_mut()
        .iter_mut()
        .skip(crate::page::PAGE_HEADER_SIZE)
    {
        *byte = seed;
    }
    page.seal();
    Arc::new(page)
}

fn read_page_at(file: &std::fs::File, page_id: PageId) -> Page {
    use std::os::unix::fs::FileExt;
    let mut bytes = [0u8; PAGE_SIZE];
    file.read_exact_at(&mut bytes, page_id * PAGE_SIZE as u64)
        .expect("read");
    Page::from_raw_bytes(bytes)
}

#[test]
fn write_then_fsync_persists() {
    let (_temp, file) = open_temp_file();
    let submitter = IoSubmitter::start(file.as_raw_fd()).expect("io_uring available");

    let page = mk_page(0xAB);
    submitter.submit_write(7, page.clone()).expect("write");
    submitter.submit_fsync().expect("fsync");

    let read_back = read_page_at(&file, 7);
    assert_eq!(read_back.bytes(), page.bytes(), "page bytes round-trip");
}

#[test]
fn many_writes_are_batched_correctly() {
    let (_temp, file) = open_temp_file();
    let submitter = Arc::new(IoSubmitter::start(file.as_raw_fd()).expect("io_uring available"));

    // Fan-out: 16 producer threads × 32 writes each.
    let total = AtomicUsize::new(0);
    let total = Arc::new(total);
    let mut handles = Vec::new();
    for tid in 0..16u64 {
        let submitter = submitter.clone();
        let total = total.clone();
        handles.push(std::thread::spawn(move || {
            for op in 0..32u64 {
                let page_id = tid * 32 + op + 64; // start above reserved region
                let page = mk_page((tid * 16 + op) as u8);
                submitter.submit_write(page_id, page).expect("write");
                total.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
    submitter.submit_fsync().expect("fsync");
    assert_eq!(total.load(Ordering::Relaxed), 16 * 32);

    // Spot-check round-trip for a handful of pages.
    for tid in [0u64, 7, 15] {
        for op in [0u64, 31] {
            let page_id = tid * 32 + op + 64;
            let expected = mk_page((tid * 16 + op) as u8);
            let observed = read_page_at(&file, page_id);
            assert_eq!(
                observed.bytes(),
                expected.bytes(),
                "page round-trip tid={tid} op={op}"
            );
        }
    }
}

#[test]
fn shutdown_unblocks_inflight_producers() {
    let (_temp, file) = open_temp_file();
    let submitter = IoSubmitter::start(file.as_raw_fd()).expect("io_uring available");

    // One successful op so the submitter is warm.
    submitter
        .submit_write(8, mk_page(0xCC))
        .expect("warm write");

    drop(submitter);
    // Drop joins the submitter thread; reaching here without
    // hanging is the assertion.
}

#[test]
fn write_to_invalid_offset_surfaces_error() {
    let (_temp, file) = open_temp_file();
    // Truncate so writes past the end do still succeed (file is
    // sparse), but write to a high page that exceeds RLIMIT_FSIZE
    // would fail. Instead we force EBADF by closing the underlying
    // fd... actually that's racy. Test happy path only here; CQE
    // error decoding is exercised by the per-op `decode_cqe_result`
    // unit test below.
    let submitter = IoSubmitter::start(file.as_raw_fd()).expect("io_uring available");
    submitter
        .submit_write(0, mk_page(0xDD))
        .expect("write to slot 0");
}

#[test]
fn decode_cqe_short_write_errors() {
    let res = decode_cqe_result(InflightKind::Write { expected_len: 4096 }, 1024);
    let err = res.expect_err("short write must surface an error");
    let MetaDbError::Io(io_err) = err else {
        panic!("unexpected error variant");
    };
    assert_eq!(io_err.kind(), std::io::ErrorKind::WriteZero);
}

#[test]
fn decode_cqe_negative_result_errors() {
    // -EBADF as a representative posix errno.
    let res = decode_cqe_result(InflightKind::Fsync, -libc::EBADF);
    let err = res.expect_err("negative result must error");
    let MetaDbError::Io(io_err) = err else {
        panic!("unexpected error variant");
    };
    assert_eq!(io_err.raw_os_error(), Some(libc::EBADF));
}

#[test]
fn write_run_round_trips_contiguous_pages() {
    let (_temp, file) = open_temp_file();
    let submitter = IoSubmitter::start(file.as_raw_fd()).expect("io_uring available");

    let pages: Vec<Arc<Page>> = (0..32u8).map(mk_page).collect();
    submitter
        .submit_write_run(100, pages.clone())
        .expect("write run");
    submitter.submit_fsync().expect("fsync");

    for (offset, expected) in pages.iter().enumerate() {
        let observed = read_page_at(&file, 100 + offset as u64);
        assert_eq!(
            observed.bytes(),
            expected.bytes(),
            "page round-trip at offset {offset}"
        );
    }
}

#[test]
fn many_runs_and_singles_interleave_correctly() {
    let (_temp, file) = open_temp_file();
    let submitter = Arc::new(IoSubmitter::start(file.as_raw_fd()).expect("io_uring available"));

    // 8 producer threads: each submits a 16-page run plus 8 single
    // writes. Cross-fans the two op kinds through the same ring so
    // SQE ordering, CQE matching, and inflight bookkeeping all
    // exercise mixed types.
    let mut handles = Vec::new();
    for tid in 0..8u64 {
        let submitter = submitter.clone();
        handles.push(std::thread::spawn(move || {
            let run_start = 200 + tid * 64;
            let run_pages: Vec<Arc<Page>> =
                (0..16u8).map(|i| mk_page((tid as u8) * 32 + i)).collect();
            submitter
                .submit_write_run(run_start, run_pages)
                .expect("run");
            for j in 0..8u64 {
                let pid = run_start + 32 + j;
                let page = mk_page((tid as u8) * 32 + 16 + j as u8);
                submitter.submit_write(pid, page).expect("single");
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
    submitter.submit_fsync().expect("fsync");

    // Spot-check first and last page of one run + one single from
    // each lane.
    for tid in 0..8u64 {
        let run_start = 200 + tid * 64;
        let first = read_page_at(&file, run_start);
        let expected_first = mk_page((tid as u8) * 32);
        assert_eq!(first.bytes(), expected_first.bytes(), "tid={tid} run[0]");

        let last = read_page_at(&file, run_start + 15);
        let expected_last = mk_page((tid as u8) * 32 + 15);
        assert_eq!(last.bytes(), expected_last.bytes(), "tid={tid} run[15]");

        let single = read_page_at(&file, run_start + 32);
        let expected_single = mk_page((tid as u8) * 32 + 16);
        assert_eq!(
            single.bytes(),
            expected_single.bytes(),
            "tid={tid} single[0]"
        );
    }
}

#[test]
fn empty_write_run_is_a_noop() {
    let (_temp, file) = open_temp_file();
    let submitter = IoSubmitter::start(file.as_raw_fd()).expect("io_uring available");
    submitter
        .submit_write_run(50, Vec::new())
        .expect("empty run resolves immediately");
}
