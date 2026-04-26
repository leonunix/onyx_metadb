use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::env;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{self, Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::testing::onyx_model::{OnyxRefModel, onyx_dedup_value, onyx_hash, onyx_l2p_value};
use onyx_metadb::{
    ApplyOutcome, Db, DedupValue, Hash32, L2pValue, Pba, SnapshotId, VerifyOptions, VolumeOrdinal,
    verify_path,
};
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const KEY_SLOTS_PER_THREAD: u64 = 256;
const MAX_LIVE_VOLUMES: usize = 4;
const BOOTSTRAP_VOL: VolumeOrdinal = 0;
const ONYX_MAX_LBA: u64 = 512;
const ONYX_MAX_PBA: Pba = 256;
const DEFAULT_CLEANUP_BATCH_SIZE: usize = 1024;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            if !err.is_empty() {
                eprintln!("metadb-soak: {err}");
            }
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let mode = Mode::parse(env::args().skip(1))?;
    match mode {
        Mode::Parent(cfg) => run_parent(cfg),
        Mode::Child(cfg) => run_child(cfg),
    }
}

include!("metadb-soak/config.rs");
include!("metadb-soak/model.rs");
include!("metadb-soak/parent.rs");
include!("metadb-soak/child.rs");
include!("metadb-soak/ops.rs");
include!("metadb-soak/process.rs");
include!("metadb-soak/util.rs");
