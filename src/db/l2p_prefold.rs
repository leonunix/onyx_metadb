//! One-generation look-ahead for the BFG checkpoint pipeline.
//!
//! The normal sync worker freezes the current generation's dirty checkpoint,
//! then delegates an already-Quiescing successor's serial L2P fold here while
//! it seals, writes, and publishes the current generation. The sync worker
//! waits for the ticket before returning, so the successor cannot be promoted
//! to Syncing until its prefold has finished.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use parking_lot::{Condvar, Mutex};

use crate::error::Result;
use crate::types::Bfg;

pub(super) type PrefoldWorkFn = Arc<dyn Fn(Bfg) -> Result<bool> + Send + Sync + 'static>;

pub(super) struct PrefoldTicket {
    token: u64,
    pub(super) bfg: Bfg,
}

struct Request {
    token: u64,
    bfg: Bfg,
}

#[derive(Default)]
struct State {
    next_token: u64,
    request: Option<Request>,
    running: bool,
    completion: Option<(u64, Result<bool>)>,
}

struct Inner {
    state: Mutex<State>,
    cv: Condvar,
    shutdown: AtomicBool,
    work: PrefoldWorkFn,
}

pub(super) struct L2pPrefoldWorker {
    inner: Arc<Inner>,
    handle: Option<JoinHandle<()>>,
}

impl L2pPrefoldWorker {
    pub(super) fn start(work: PrefoldWorkFn) -> Self {
        let inner = Arc::new(Inner {
            state: Mutex::new(State::default()),
            cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            work,
        });
        let worker = Arc::clone(&inner);
        let handle = thread::Builder::new()
            .name("metadb-l2p-prefold".into())
            .spawn(move || run_worker(worker))
            .expect("metadb: failed to spawn L2P prefold worker");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    /// Submit at most one look-ahead fold. The sync worker consumes the
    /// completion before the next request, so a busy result means this cycle
    /// simply falls back to the serial path.
    pub(super) fn request(&self, bfg: Bfg) -> Option<PrefoldTicket> {
        let mut state = self.inner.state.lock();
        if self.inner.shutdown.load(Ordering::Acquire)
            || state.request.is_some()
            || state.running
            || state.completion.is_some()
        {
            return None;
        }
        state.next_token = state.next_token.wrapping_add(1).max(1);
        let token = state.next_token;
        state.request = Some(Request { token, bfg });
        self.inner.cv.notify_one();
        Some(PrefoldTicket { token, bfg })
    }

    pub(super) fn wait(&self, ticket: PrefoldTicket) -> Result<bool> {
        let mut state = self.inner.state.lock();
        loop {
            if state
                .completion
                .as_ref()
                .is_some_and(|(token, _)| *token == ticket.token)
            {
                let (_, result) = state.completion.take().expect("completion disappeared");
                return result;
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Ok(false);
            }
            self.inner.cv.wait(&mut state);
        }
    }

    pub(super) fn stop(&mut self) {
        if self.inner.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.cv.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for L2pPrefoldWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_worker(inner: Arc<Inner>) {
    crate::affinity::bind_current(crate::affinity::ThreadRole::BfgSync, 2);
    loop {
        let request = {
            let mut state = inner.state.lock();
            while state.request.is_none() && !inner.shutdown.load(Ordering::Acquire) {
                inner.cv.wait(&mut state);
            }
            if inner.shutdown.load(Ordering::Acquire) {
                break;
            }
            state.running = true;
            state.request.take().expect("prefold wake without request")
        };

        let result = (inner.work)(request.bfg);
        let mut state = inner.state.lock();
        state.running = false;
        state.completion = Some((request.token, result));
        inner.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn request_runs_once_and_wait_returns_result() {
        let seen = Arc::new(AtomicU64::new(0));
        let seen_work = Arc::clone(&seen);
        let work: PrefoldWorkFn = Arc::new(move |bfg| {
            seen_work.store(bfg, Ordering::Release);
            Ok(true)
        });
        let mut worker = L2pPrefoldWorker::start(work);
        let ticket = worker.request(9).expect("request should be accepted");
        assert_eq!(ticket.bfg, 9);
        assert!(worker.wait(ticket).unwrap());
        assert_eq!(seen.load(Ordering::Acquire), 9);
        worker.stop();
    }
}
