//! Multi-lane WAL facade.
//!
//! `WalSet` keeps the existing global-LSN recovery model while splitting
//! the fsync path across several independent writer threads. The outer
//! mutex assigns a total-order LSN and sends the record into one lane;
//! each lane appends/fsyncs its own segment stream.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::config::Config;
use crate::error::{MetaDbError, Result};
use crate::metrics::MetaMetrics;
use crate::testing::faults::FaultController;
use crate::types::Lsn;

use super::record::WAL_MAX_BODY;
use super::segment::{list_segments, prune_segments};
use super::writer::Wal;

const WAL_LANE_DIR_PREFIX: &str = "lane-";

/// A set of WAL writer lanes sharing one global LSN allocator.
pub struct WalSet {
    lanes: Vec<Wal>,
    state: Mutex<WalSetState>,
}

struct WalSetState {
    next_lsn: Lsn,
    failed: Option<String>,
}

impl WalSet {
    pub(crate) fn create_with_metrics(
        dir: &Path,
        config: &Config,
        start_lsn: Lsn,
        faults: Arc<FaultController>,
        metrics: Arc<MetaMetrics>,
    ) -> Result<Self> {
        let lane_count = wal_lane_count(config);
        std::fs::create_dir_all(dir)?;
        let mut lanes = Vec::with_capacity(lane_count);
        if lane_count == 1 {
            lanes.push(Wal::create_with_metrics(
                dir, config, start_lsn, faults, metrics,
            )?);
        } else {
            for lane in 0..lane_count {
                lanes.push(Wal::create_with_metrics(
                    &lane_dir(dir, lane),
                    config,
                    start_lsn,
                    faults.clone(),
                    metrics.clone(),
                )?);
            }
        }
        Ok(Self {
            lanes,
            state: Mutex::new(WalSetState {
                next_lsn: start_lsn,
                failed: None,
            }),
        })
    }

    pub(crate) fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// Submit to lane 0. Kept for lifecycle callers whose operation is
    /// intentionally global/serial.
    #[allow(dead_code)]
    pub(crate) fn submit(&self, body: Vec<u8>) -> Result<Lsn> {
        self.submit_to(0, body)
    }

    /// Submit to a chosen lane while preserving one global LSN space.
    pub(crate) fn submit_to(&self, lane: usize, body: Vec<u8>) -> Result<Lsn> {
        self.submit_to_reserved(lane, body, |_| {})
    }

    /// Submit to a chosen lane and run `reserve(lsn)` while the global
    /// LSN allocator mutex is still held.
    ///
    /// Db's dispatch scheduler uses this to publish each commit's shard
    /// footprint before any higher LSN can be allocated. Without that
    /// reservation window, a fast higher-LSN WAL lane could dispatch into
    /// a shard before a slower lower-LSN commit had even registered that
    /// it needs the same shard.
    pub(crate) fn submit_to_reserved<F>(
        &self,
        lane: usize,
        body: Vec<u8>,
        reserve: F,
    ) -> Result<Lsn>
    where
        F: FnOnce(Lsn),
    {
        if body.len() > WAL_MAX_BODY {
            return Err(MetaDbError::InvalidArgument(format!(
                "WAL body too large: {} bytes exceeds WAL_MAX_BODY {WAL_MAX_BODY}",
                body.len()
            )));
        }
        if self.lanes.is_empty() {
            return Err(MetaDbError::Corruption("WalSet has no lanes".into()));
        }
        let lane = lane % self.lanes.len();
        let (lsn, ack) = {
            let mut state = self.state.lock();
            if let Some(msg) = &state.failed {
                return Err(MetaDbError::Corruption(format!("wal set failed: {msg}")));
            }
            let lsn = state.next_lsn;
            state.next_lsn = state
                .next_lsn
                .checked_add(1)
                .ok_or(MetaDbError::OutOfSpace)?;
            let ack = match self.lanes[lane].submit_assigned_async(lsn, body) {
                Ok(ack) => ack,
                Err(err) => {
                    let msg = err.to_string();
                    state.failed = Some(msg);
                    return Err(err);
                }
            };
            reserve(lsn);
            (lsn, ack)
        };

        match ack.wait() {
            Ok(acked) if acked == lsn => Ok(acked),
            Ok(acked) => Err(MetaDbError::Corruption(format!(
                "wal lane acked lsn {acked}, expected {lsn}",
            ))),
            Err(err) => {
                let msg = err.to_string();
                self.state.lock().failed = Some(msg);
                Err(err)
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn shutdown(&self) -> Result<()> {
        let mut first_error = None;
        for lane in &self.lanes {
            if let Err(err) = lane.shutdown() {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

pub(crate) fn wal_lane_count(config: &Config) -> usize {
    config.wal_lanes.max(1) as usize
}

pub(crate) fn lane_dir(root: &Path, lane: usize) -> PathBuf {
    root.join(format!("{WAL_LANE_DIR_PREFIX}{lane:04}"))
}

/// WAL directories to inspect during recovery/pruning.
///
/// Includes the root itself when legacy single-lane segments are present,
/// then all `lane-####` subdirectories in lane order.
pub(crate) fn replay_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    if !list_segments(root)?.is_empty() {
        dirs.push(root.to_path_buf());
    }

    let rd = match std::fs::read_dir(root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(dirs),
        Err(e) => return Err(e.into()),
    };
    let mut lane_dirs = Vec::new();
    for entry in rd {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(suffix) = name.strip_prefix(WAL_LANE_DIR_PREFIX) else {
            continue;
        };
        if let Ok(lane) = suffix.parse::<usize>() {
            lane_dirs.push((lane, entry.path()));
        }
    }
    lane_dirs.sort_by_key(|(lane, _)| *lane);
    dirs.extend(lane_dirs.into_iter().map(|(_, path)| path));
    Ok(dirs)
}

pub(crate) fn prune_all_segments(root: &Path, checkpoint_lsn: Lsn) -> Result<usize> {
    let mut removed = 0;
    for dir in replay_dirs(root)? {
        removed += prune_segments(&dir, checkpoint_lsn)?;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::ApplyOutcome;
    use crate::wal::{WalOp, encode_body};
    use tempfile::TempDir;

    fn cfg() -> Config {
        let mut cfg = Config::new("unused");
        cfg.group_commit_timeout_us = 50;
        cfg.wal_lanes = 4;
        cfg
    }

    fn hash(byte: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = byte;
        h
    }

    fn value(byte: u8) -> crate::dedup_types::DedupValue {
        let mut v = [0u8; 28];
        v[0] = byte;
        crate::dedup_types::DedupValue(v)
    }

    #[test]
    fn wal_set_assigns_global_lsns_and_replays_in_order() {
        let dir = TempDir::new().unwrap();
        let cfg = cfg();
        let wal = WalSet::create_with_metrics(
            dir.path(),
            &cfg,
            1,
            FaultController::new(),
            Arc::new(MetaMetrics::new()),
        )
        .unwrap();

        let ops = [
            WalOp::DedupPut {
                hash: hash(1),
                value: value(1),
            },
            WalOp::DedupPut {
                hash: hash(2),
                value: value(2),
            },
            WalOp::DedupPut {
                hash: hash(3),
                value: value(3),
            },
        ];

        assert_eq!(wal.submit_to(2, encode_body(&ops[0..1])).unwrap(), 1);
        assert_eq!(wal.submit_to(0, encode_body(&ops[1..2])).unwrap(), 2);
        assert_eq!(wal.submit_to(2, encode_body(&ops[2..3])).unwrap(), 3);
        wal.shutdown().unwrap();

        let mut seen = Vec::new();
        let outcome = crate::recovery::replay_wal_set_into(dir.path(), 1, |lsn, op| {
            seen.push((lsn, op.clone()));
            Ok(ApplyOutcome::Dedup)
        })
        .unwrap();

        assert_eq!(outcome.merged.first_lsn, Some(1));
        assert_eq!(outcome.merged.last_lsn, Some(3));
        assert_eq!(outcome.merged.record_count, 3);
        assert_eq!(
            seen.into_iter().map(|(lsn, _)| lsn).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
}
