use crossbeam_channel::{Receiver, Sender, bounded};
#[cfg(target_os = "linux")]
use parking_lot::Mutex;
use std::fs::File;
use std::io;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::error::{MetaDbError, Result};
use crate::page::Page;
use crate::types::PageId;

use super::raw_io::{new_read_uring, read_page_raw, read_pages_raw};

const PAGE_READ_POOL_WORKERS_MAX: usize = 8;
const PAGE_READ_BATCH_MAX: usize = 64;
const PAGE_READ_BATCH_WINDOW: Duration = Duration::from_micros(8);
const PAGE_READ_CHANNEL_CAP: usize = 512;

struct PageReadRequest {
    page_id: PageId,
    reply: Sender<Result<Page>>,
}

pub(super) struct PageReadPool {
    sender: Option<Sender<PageReadRequest>>,
    workers: Vec<JoinHandle<()>>,
}

impl PageReadPool {
    pub(super) fn start(file: &File) -> Result<Self> {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get().div_ceil(16))
            .unwrap_or(1)
            .clamp(1, PAGE_READ_POOL_WORKERS_MAX);
        let (tx, rx) = bounded::<PageReadRequest>(PAGE_READ_CHANNEL_CAP);
        let mut handles = Vec::with_capacity(workers);
        for worker_idx in 0..workers {
            let worker_file = file.try_clone()?;
            let rx = rx.clone();
            let join = thread::Builder::new()
                .name(format!("metadb-page-read-{worker_idx}"))
                .spawn(move || page_read_worker_loop(worker_file, rx))
                .map_err(MetaDbError::Io)?;
            handles.push(join);
        }
        Ok(Self {
            sender: Some(tx),
            workers: handles,
        })
    }

    pub(super) fn read_page(&self, page_id: PageId) -> Result<Page> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| MetaDbError::Io(io::Error::other("page read pool already shut down")))?;
        let (reply_tx, reply_rx) = bounded(1);
        sender
            .send(PageReadRequest {
                page_id,
                reply: reply_tx,
            })
            .map_err(|_| MetaDbError::Io(io::Error::other("page read pool closed")))?;
        reply_rx
            .recv()
            .map_err(|_| MetaDbError::Io(io::Error::other("page read worker dropped reply")))?
    }
}

impl Drop for PageReadPool {
    fn drop(&mut self) {
        self.sender.take();
        for join in self.workers.drain(..) {
            let _ = join.join();
        }
    }
}

fn page_read_worker_loop(file: File, rx: Receiver<PageReadRequest>) {
    #[cfg(target_os = "linux")]
    let read_uring = Mutex::new(new_read_uring());
    let mut batch = Vec::with_capacity(PAGE_READ_BATCH_MAX);
    loop {
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => return,
        };
        batch.clear();
        batch.push(first);
        let deadline = Instant::now() + PAGE_READ_BATCH_WINDOW;
        loop {
            while batch.len() < PAGE_READ_BATCH_MAX {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }
            if batch.len() >= PAGE_READ_BATCH_MAX {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline.saturating_duration_since(now)) {
                Ok(req) => batch.push(req),
                Err(_) => break,
            }
        }

        let page_ids: Vec<PageId> = batch.iter().map(|req| req.page_id).collect();
        #[cfg(target_os = "linux")]
        let pages_result = read_pages_raw(&file, &page_ids, Some(&read_uring));
        #[cfg(not(target_os = "linux"))]
        let pages_result = read_pages_raw(&file, &page_ids, None);

        match pages_result {
            Ok(pages) => {
                for (req, page) in batch.drain(..).zip(pages.into_iter()) {
                    let _ = req.reply.send(page.verify(req.page_id).map(|()| page));
                }
            }
            Err(_) => {
                for req in batch.drain(..) {
                    let result = read_page_raw(&file, req.page_id).and_then(|page| {
                        page.verify(req.page_id)?;
                        Ok(page)
                    });
                    let _ = req.reply.send(result);
                }
            }
        }
    }
}
