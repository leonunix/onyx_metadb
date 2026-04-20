# onyx-metadb Testing Strategy

**Principle**: correctness is not a feeling. Every claim in `DESIGN.md` has
a machine-checkable counterpart here.

Bugs in a metadata store show up as lost data or silent corruption, often
weeks after the buggy commit. The only defense is automated testing that
catches regressions before code merges.

## 1. Layers

The test pyramid, from cheapest to most expensive:

```
         ┌──────────────────┐
         │  soak  (days)    │  phase 7-8
         ├──────────────────┤
         │  integration     │  phase 6+
         ├──────────────────┤
         │  crash injection │  all phases
         ├──────────────────┤
         │  property tests  │  all phases
         ├──────────────────┤
         │  unit tests      │  all phases
         ├──────────────────┤
         │  fuzz            │  phase 1+
         └──────────────────┘
```

Every phase in `ROADMAP.md` must land with tests covering the relevant
layers. No phase completes without its test deliverable.

## 2. Invariants (the checkable claims)

A central module `src/testing/invariants.rs` provides functions that assert
structural truths. Every property test and crash test calls these after
each operation (under `--features paranoid`).

### Page store invariants
- Every allocated page's CRC matches its header.
- Free list ∪ allocated set = all pages in file. No overlap.
- Page refcount ≥ number of parent pointers pointing to it.

### B+tree invariants (per partition, per shard)
- **Ordering**: for every leaf, keys strictly ascending.
- **Separator invariant**: for every internal page with children `c0..cn`
  and separator keys `k1..kn`:
  - `max_key(c_i) < k_{i+1} ≤ min_key(c_{i+1})`.
- **Fill thresholds**: every non-root page has fill ∈ [underflow_pct,
  100%]. (Root is allowed below, since it can have 1 entry.)
- **Depth**: all leaves at the same depth.
- **Reachability**: every page reachable from the root matches the set of
  pages claimed as "this tree's pages".

### Refcount / snapshot invariants
- For every page, `header.refcount` equals the number of current-root-or-
  snapshot-root parent pointers that reach it across all snapshots.
- After `drop_snapshot(s)`, no page uniquely owned by `s` remains
  allocated. "Uniquely owned" means: in `s`'s tree, and not in any other
  snapshot's tree.

### LSM invariants
- Every SST's records are sorted, no duplicates within an SST.
- For overlapping SSTs at different levels, the higher-level (younger)
  record shadows the lower-level one. A lookup that scans in level order
  returns the youngest.
- Bloom never lies about absence (false positives OK, false negatives
  never).

### Cross-index invariants
- Every `DedupEntry.pba` appears in the PBA refcount index with `refcount
  ≥ 1`.
- Every B+tree `BlockmapValue.pba` appears with `refcount ≥ 1`.
- For every hash in `dedup_index`, there exists at least one B+tree
  `BlockmapValue` with the same PBA, OR the hash is eligible for removal
  in the current txn.

### WAL / recovery invariants
- After recovery, the state equals the state that would result from
  applying every committed WAL record in LSN order from scratch, starting
  at the manifest's `L_ck` snapshot.
- No WAL record's ops are partially applied post-recovery (all-or-nothing
  per record).

### Invariant checker: `metadb-verify`

A binary that runs all invariants on a closed database directory.
Returns nonzero on any violation with a human-readable report. Used by:
- Property tests after every operation.
- Soak test after every N minutes.
- Manually during debugging.

## 3. Unit tests

Per-module, test individual functions with hand-crafted inputs. Examples:
- CRC round trip on edge-case lengths.
- Bloom false-positive rate within spec for N bits/entry.
- Page encode/decode byte-exact round trip.
- Split logic given a full page with a specific key distribution.

These catch regressions fast but don't find design bugs. Coverage target:
90% line coverage (enforced in CI via `cargo llvm-cov`).

## 4. Property tests (proptest)

The main correctness harness. Each index type has a reference model (pure
`BTreeMap` or `HashMap`) and a "shadow test":

```rust
proptest! {
    #[test]
    fn btree_matches_reference(ops in op_sequence(1000)) {
        let dir = tempdir();
        let db = Db::open(test_cfg(&dir))?;
        let p = db.create_partition("t")?;
        let mut reference: BTreeMap<Lba, BlockmapValue> = BTreeMap::new();

        for op in ops {
            match op {
                Op::Put(k, v) => {
                    let mut tx = db.begin();
                    tx.put(p, k, v);
                    tx.commit()?;
                    reference.insert(k, v);
                }
                Op::Del(k) => { /* ... */ }
                Op::Get(k) => {
                    prop_assert_eq!(
                        db.partition(p).unwrap().get(k)?,
                        reference.get(&k).copied()
                    );
                }
                Op::Range(lo, hi) => { /* check iterator vs reference */ }
            }
            #[cfg(feature = "paranoid")]
            invariants::check_all(&db)?;
        }
    }
}
```

### Generators

- `op_sequence(n)`: sequence of mixed ops with tunable weight.
- `snapshot_timeline()`: interleave snapshot/drop/put/get/range/diff ops.
- `multi_writer_schedule()`: N threads, each a sequence, interleaved by a
  seeded scheduler (deterministic replay).

### Run budget

- CI quick: 100 cases per test × 100 ops each. ~2 minutes.
- CI nightly: 10,000 cases × 10,000 ops each. ~4 hours.
- Pre-release: 100,000 cases × 10,000 ops with `paranoid`. As long as it
  takes.

### Shrinking

Proptest's built-in shrinking is mandatory — every failure must shrink to
a minimal repro. The repro is logged with the seed so it can be replayed
verbatim.

## 5. Crash injection

A fault-injection framework, enabled via `--features fault-injection`,
that lets tests kill the process (or simulate kill via panic) at named
injection points:

```rust
enum FaultPoint {
    WalFsyncBefore,
    WalFsyncAfter,
    PageWriteBefore,
    PageWriteAfter,
    ManifestFsyncBefore,
    ManifestFsyncAfter,
    CowCascadeMidParentLink,
    // ...
}

pub fn inject(pt: FaultPoint) { /* panic if configured to */ }
```

### The crash test loop

```
for seed in seeds:
    rng = seeded(seed)
    workload = random_op_sequence(rng)
    injection_point = pick_random_fault_point(rng)
    injection_schedule = pick_nth_hit(rng)

    db1 = Db::open(dir)
    reference = BTreeMap::new()

    for op in workload.take_until(injection_schedule):
        apply_to_both(db1, reference, op)
        if should_inject():
            db1.force_crash(injection_point)  // panic, skip shutdown

    # At this point the process would have died.  In the test harness we
    # drop db1 forcibly, reopen.

    db2 = Db::open(dir)
    # Every op whose txn.commit() returned Ok before the crash must be
    # present.  Every op whose commit did not return Ok may or may not be
    # present, but the state must be consistent.
    verify_recovery(db2, reference, workload, crash_after_op)
    invariants::check_all(db2)
```

This is implemented as a proptest strategy so shrinking works.

### Coverage target

Every injection point gets exercised under every index-type workload.
Tracked in CI by an injection-point-hit matrix.

## 6. Fuzzing

`cargo-fuzz` targets:
- `fuzz_wal_record_decode`: random bytes → decode → never panics.
- `fuzz_page_decode`: random 4 KiB → decode → never panics; if valid,
  round-trips.
- `fuzz_manifest_decode`: random bytes → decode → never panics.
- `fuzz_lsm_sst_decode`: random bytes → never panics; random-prefix
  lookups return consistent results.
- `fuzz_key_generator`: any sequence of u64 LBAs → never triggers panic
  in B+tree.

Goal: zero panics on malformed input. Malformed input is expected (power
failure during write); crashing the process on it is unacceptable.

Run continuously on CI's fuzz runner, report findings as regressions.

## 7. Stress / concurrency tests

Multi-threaded random workload with a deterministic scheduler. Two modes:

### Determin mode
- `loom` for fine-grained concurrency (latch acquisition, page cache
  admission). Slow but exhaustive over thread interleavings.

### Randomized mode
- Real threads, real WAL, real disk. Seeded RNG, seeded scheduler
  (chooses next thread to run). Reproducible with the seed.
- Used in phase 4 to validate sharded writer concurrency.

## 8. Soak

A separate binary `onyx-metadb-soak` with:
- Configurable workload mix (puts/gets/snapshots/drops/diffs).
- Runs for hours to days.
- Periodically calls `metadb-verify` on a checkpointed snapshot.
- Periodically triggers fault injection (if enabled).
- Emits metrics (throughput, latency histograms, page cache hit rate).
- Emits `summary.json` on exit with failure status.

Exit on first invariant violation. Log preserved for post-mortem.

## 9. Performance regression guard

Not correctness, but adjacent. `criterion` benchmarks for:
- Single-writer put latency.
- Multi-writer put throughput (1/2/4/8/16 writers).
- Point lookup latency (cold / warm cache).
- Range scan throughput.
- Snapshot take / drop latency.
- Diff throughput on identical / 1% / 10% / 100% divergent trees.

CI fails on > 5% regression vs. the last tagged release.

## 10. Tooling binaries

Ship in-tree. Each is a Rust binary with `--help`.

### `metadb-dump`

```
metadb-dump page        <path> <page_id>
metadb-dump partition   <path> <partition_id>
metadb-dump snapshot    <path> <snapshot_id>
metadb-dump manifest    <path>
metadb-dump wal         <path> [--from-lsn N] [--limit K]
metadb-dump free-list   <path>
```

Human-readable output. Used for manual debugging.

### `metadb-verify`

```
metadb-verify <path> [--strict] [--json]
```

Runs every invariant from §2. Returns 0 on success, 1 on violation,
2 on IO error. `--json` emits machine-readable report.

### `metadb-bench`

```
metadb-bench put        --writers N --partitions M --ops K [--shards S]
metadb-bench get        --threads N --keys K
metadb-bench snapshot   --partition P --keys K
metadb-bench diff       --snap-a A --snap-b B
```

Emits latency histograms (HDR) and throughput.

### `metadb-replay`

```
metadb-replay record <live-db-path> <recording.jsonl>
metadb-replay run    <recording.jsonl> [--crash-at N]
```

Record a workload against a live DB, replay deterministically against a
fresh DB. Optionally inject a crash at op N. Shrinks bug repros into
committed test fixtures.

### `metadb-faultctl`

```
metadb-faultctl list-points
metadb-faultctl set-injection <point> <nth-hit>
```

For manual fault testing of a running test harness. Not used in automated
CI.

### `metadb-import-rocks`

Phase 7 only. Read onyx-storage's RocksDB CFs, write to metadb. Includes
a `--verify` mode that diffs metadb against the source RocksDB.

## 11. CI matrix

```
quick (on every PR):
  - cargo fmt --check
  - cargo clippy -- -D warnings
  - cargo test (release)
  - proptest quick budget
  - criterion bench guard (regression check)

nightly:
  - proptest nightly budget (~4 hours)
  - crash injection full sweep (~6 hours)
  - fuzz targets (2 hours each)
  - 8-hour soak under fault injection

pre-release tag:
  - all nightly
  - 72-hour soak
  - manual chaos test on representative hardware
```

## 12. Reproducibility

Every test failure must be reproducible with one command:

```
cargo test --release failing_test_name -- --seed <SEED>
```

Seeds are logged on failure. No test uses system time, thread id, or
other non-reproducible inputs unless explicitly covered (e.g., loom
tests). Wall clock is replaced by a virtual clock in all tests that care
about time.
