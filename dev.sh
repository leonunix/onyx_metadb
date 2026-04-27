#!/usr/bin/env bash
set -euo pipefail

PROJ_ROOT="$(cd "$(dirname "$0")" && pwd)"
STATE_DIR="$PROJ_ROOT/.dev"
LOG_DIR="$STATE_DIR/logs"
RUN_ROOT="$STATE_DIR/soak"
PID_FILE="$STATE_DIR/soak.pid"
RUN_DIR_FILE="$STATE_DIR/soak-run-dir"
LOG_FILE="$LOG_DIR/soak.log"
BENCH_ROOT="$STATE_DIR/bench"
BENCH_RUN_DIR_FILE="$STATE_DIR/bench-run-dir"

DEFAULT_DURATION="${METADB_SOAK_DURATION:-24h}"
DEFAULT_OPS_PER_CYCLE="${METADB_SOAK_OPS_PER_CYCLE:-10000}"
DEFAULT_PIPELINE_DEPTH="${METADB_SOAK_PIPELINE_DEPTH:-1}"
DEFAULT_THREADS="${METADB_SOAK_THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
DEFAULT_METRICS_INTERVAL_SECS="${METADB_SOAK_METRICS_INTERVAL_SECS:-5}"
DEFAULT_CLEANUP_BATCH_SIZE="${METADB_SOAK_CLEANUP_BATCH_SIZE:-1024}"
DEFAULT_ONYX_MAX_PBA="${METADB_SOAK_ONYX_MAX_PBA:-100000000}"
DEFAULT_FAULT_DENSITY_PCT="${METADB_SOAK_FAULT_DENSITY_PCT:-25}"
DEFAULT_SEED="${METADB_SOAK_SEED:-1592625758}"
DEFAULT_WORKLOAD="${METADB_SOAK_WORKLOAD:-onyx}"
DEFAULT_EVENT_VERBOSITY="${METADB_SOAK_EVENT_VERBOSITY:-summary}"
DEFAULT_SNAPSHOTS_ENABLED="${METADB_SOAK_SNAPSHOTS:-1}"
DEFAULT_RESTART_INTERVAL="${METADB_SOAK_RESTART_INTERVAL:-}"
DEFAULT_EXTRA_ARGS="${METADB_SOAK_EXTRA_ARGS:-}"

BENCH_DEFAULT_THREADS="${METADB_BENCH_THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
BENCH_DEFAULT_GET_OPS="${METADB_BENCH_GET_OPS:-200000}"
BENCH_DEFAULT_MULTI_OPS="${METADB_BENCH_MULTI_OPS:-20000}"
BENCH_DEFAULT_MULTI_BATCH_SIZE="${METADB_BENCH_MULTI_BATCH_SIZE:-64}"
BENCH_DEFAULT_PREFILL_BATCH_SIZE="${METADB_BENCH_PREFILL_BATCH_SIZE:-1024}"
BENCH_DEFAULT_PREFILL_FLUSH_KEYS="${METADB_BENCH_PREFILL_FLUSH_KEYS:-8000000}"
BENCH_DEFAULT_META_OPS="${METADB_BENCH_META_OPS:-50000}"
BENCH_DEFAULT_PUT_OPS="${METADB_BENCH_PUT_OPS:-20000}"
BENCH_DEFAULT_GET_WARMUP="${METADB_BENCH_GET_WARMUP_OPS:-20000}"
BENCH_DEFAULT_MULTI_WARMUP="${METADB_BENCH_MULTI_WARMUP_OPS:-5000}"
BENCH_DEFAULT_SHARDS="${METADB_BENCH_SHARDS:-16}"
BENCH_DEFAULT_CACHE_MB="${METADB_BENCH_CACHE_MB:-512}"
BENCH_DEFAULT_SEED="${METADB_BENCH_SEED:-1592625758}"
BENCH_DEFAULT_OVERWRITE_PCT="${METADB_BENCH_OVERWRITE_PCT:-40}"
BENCH_DEFAULT_DEDUP_HIT_PCT="${METADB_BENCH_DEDUP_HIT_PCT:-20}"
BENCH_DEFAULT_SIZE="${METADB_BENCH_SIZE:-1g}"

mkdir -p "$STATE_DIR" "$LOG_DIR" "$RUN_ROOT" "$BENCH_ROOT"

quote_shell_arg() {
    printf "%q" "$1"
}

save_run_dir() {
    printf '%s\n' "$1" > "$RUN_DIR_FILE"
}

saved_run_dir() {
    if [[ -f "$RUN_DIR_FILE" ]]; then
        head -n 1 "$RUN_DIR_FILE"
    fi
}

clear_run_dir() {
    rm -f "$RUN_DIR_FILE"
}

save_bench_run_dir() {
    printf '%s\n' "$1" > "$BENCH_RUN_DIR_FILE"
}

saved_bench_run_dir() {
    if [[ -f "$BENCH_RUN_DIR_FILE" ]]; then
        head -n 1 "$BENCH_RUN_DIR_FILE"
    fi
}

is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

parse_duration_secs() {
    local raw="${1:-}"
    if [[ -z "$raw" ]]; then
        echo "duration is required" >&2
        exit 1
    fi
    if [[ "$raw" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "$raw"
        return
    fi
    if [[ "$raw" =~ ^([0-9]+)([smhd])$ ]]; then
        local value="${BASH_REMATCH[1]}"
        local unit="${BASH_REMATCH[2]}"
        case "$unit" in
            s) printf '%s\n' "$value" ;;
            m) printf '%s\n' "$((value * 60))" ;;
            h) printf '%s\n' "$((value * 3600))" ;;
            d) printf '%s\n' "$((value * 86400))" ;;
        esac
        return
    fi
    echo "invalid duration: $raw (expected e.g. 3600, 30m, 12h, 7d)" >&2
    exit 1
}

parse_size_bytes() {
    local raw="${1:-}"
    local value unit
    if [[ -z "$raw" ]]; then
        echo "size is required" >&2
        exit 1
    fi
    if [[ "$raw" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "$raw"
        return
    fi
    if [[ "$raw" =~ ^([0-9]+)([kKmMgGtT])$ ]]; then
        value="${BASH_REMATCH[1]}"
        unit="${BASH_REMATCH[2]}"
        case "$unit" in
            k|K) printf '%s\n' "$((value * 1024))" ;;
            m|M) printf '%s\n' "$((value * 1024 * 1024))" ;;
            g|G) printf '%s\n' "$((value * 1024 * 1024 * 1024))" ;;
            t|T) printf '%s\n' "$((value * 1024 * 1024 * 1024 * 1024))" ;;
        esac
        return
    fi
    echo "invalid size: $raw (expected e.g. 1073741824, 1g, 10g)" >&2
    exit 1
}

ensure_release_binaries() {
    echo "Building release binaries..."
    (
        cd "$PROJ_ROOT"
        cargo build --release --bin metadb-soak --bin metadb-verify
    )
}

ensure_bench_binaries() {
    echo "Building release benchmark binaries..."
    (
        cd "$PROJ_ROOT"
        cargo build --release --bin metadb-bench --bin rocksdb-bench
    )
}

run_dir_for_start() {
    if [[ -n "${METADB_SOAK_RUN_DIR:-}" ]]; then
        printf '%s\n' "$METADB_SOAK_RUN_DIR"
    else
        printf '%s/%s\n' "$RUN_ROOT" "$(date -u +%Y%m%dT%H%M%SZ)"
    fi
}

workload_arg() {
    case "$1" in
        legacy) printf '%s\n' "--legacy-mix" ;;
        onyx) printf '%s\n' "--onyx-mix" ;;
        concurrent|onyx-concurrent) printf '%s\n' "--onyx-concurrent-mix" ;;
        *)
            echo "unknown soak workload: $1 (expected legacy, onyx, concurrent)" >&2
            exit 1
            ;;
    esac
}

event_verbosity_arg() {
    case "$1" in
        summary|quiet) printf '%s\n' "--events-summary" ;;
        ops|verbose) printf '%s\n' "--events-ops" ;;
        *)
            echo "unknown event verbosity: $1 (expected summary or ops)" >&2
            exit 1
            ;;
    esac
}

snapshots_arg() {
    case "$1" in
        1|true|yes|on) printf '%s\n' "" ;;
        0|false|no|off) printf '%s\n' "--no-snapshots" ;;
        *)
            echo "unknown snapshots setting: $1 (expected 1/0 or true/false)" >&2
            exit 1
            ;;
    esac
}

stop_process_group() {
    local pid="$1"
    local pgid
    pgid="$(ps -o pgid= -p "$pid" | tr -d ' ' || true)"
    if [[ -n "$pgid" ]]; then
        kill -- "-$pgid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    else
        kill "$pid" 2>/dev/null || true
    fi
}

parse_soak_run_args() {
    PARSED_DURATION="$DEFAULT_DURATION"
    PARSED_WORKLOAD="$DEFAULT_WORKLOAD"
    PARSED_RESTART_INTERVAL="$DEFAULT_RESTART_INTERVAL"
    PARSED_PIPELINE_DEPTH="$DEFAULT_PIPELINE_DEPTH"
    PARSED_METRICS_INTERVAL_SECS="$DEFAULT_METRICS_INTERVAL_SECS"
    PARSED_CLEANUP_BATCH_SIZE="$DEFAULT_CLEANUP_BATCH_SIZE"
    PARSED_ONYX_MAX_PBA="$DEFAULT_ONYX_MAX_PBA"

    local positional=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --restart-interval)
                shift
                if [[ $# -eq 0 ]]; then
                    echo "--restart-interval needs a value" >&2
                    exit 1
                fi
                PARSED_RESTART_INTERVAL="$1"
                ;;
            --restart-interval=*)
                PARSED_RESTART_INTERVAL="${1#*=}"
                ;;
            --pipeline-depth)
                shift
                if [[ $# -eq 0 ]]; then
                    echo "--pipeline-depth needs a value" >&2
                    exit 1
                fi
                PARSED_PIPELINE_DEPTH="$1"
                ;;
            --pipeline-depth=*)
                PARSED_PIPELINE_DEPTH="${1#*=}"
                ;;
            --metrics-interval-secs)
                shift
                if [[ $# -eq 0 ]]; then
                    echo "--metrics-interval-secs needs a value" >&2
                    exit 1
                fi
                PARSED_METRICS_INTERVAL_SECS="$1"
                ;;
            --metrics-interval-secs=*)
                PARSED_METRICS_INTERVAL_SECS="${1#*=}"
                ;;
            --cleanup-batch-size)
                shift
                if [[ $# -eq 0 ]]; then
                    echo "--cleanup-batch-size needs a value" >&2
                    exit 1
                fi
                PARSED_CLEANUP_BATCH_SIZE="$1"
                ;;
            --cleanup-batch-size=*)
                PARSED_CLEANUP_BATCH_SIZE="${1#*=}"
                ;;
            --onyx-max-pba)
                shift
                if [[ $# -eq 0 ]]; then
                    echo "--onyx-max-pba needs a value" >&2
                    exit 1
                fi
                PARSED_ONYX_MAX_PBA="$1"
                ;;
            --onyx-max-pba=*)
                PARSED_ONYX_MAX_PBA="${1#*=}"
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            --*)
                echo "unknown soak option: $1" >&2
                exit 1
                ;;
            *)
                positional+=("$1")
                ;;
        esac
        shift
    done

    if [[ ${#positional[@]} -gt 3 ]]; then
        echo "too many soak arguments" >&2
        exit 1
    fi
    if [[ ${#positional[@]} -ge 1 ]]; then
        PARSED_DURATION="${positional[0]}"
    fi
    if [[ ${#positional[@]} -ge 2 ]]; then
        PARSED_WORKLOAD="${positional[1]}"
    fi
    if [[ ${#positional[@]} -eq 3 ]]; then
        # Compatibility for the briefly-supported positional form:
        # ./dev.sh start 24h concurrent 2h
        PARSED_RESTART_INTERVAL="${positional[2]}"
    fi
}

cmd_start() {
    local duration="${1:-$DEFAULT_DURATION}"
    local workload="${2:-$DEFAULT_WORKLOAD}"
    local restart_interval="${3:-$DEFAULT_RESTART_INTERVAL}"
    local pipeline_depth="${4:-$DEFAULT_PIPELINE_DEPTH}"
    local metrics_interval_secs="${5:-$DEFAULT_METRICS_INTERVAL_SECS}"
    local cleanup_batch_size="${6:-$DEFAULT_CLEANUP_BATCH_SIZE}"
    local onyx_max_pba="${7:-$DEFAULT_ONYX_MAX_PBA}"
    local duration_secs run_dir workload_flag events_flag snapshots_flag qroot qrun_dir qduration qops qpipeline qthreads qmetrics_interval qcleanup_batch qonyx_max_pba qfault qseed qrestart qextra cmd

    if is_running; then
        echo "soak already running (pid $(cat "$PID_FILE"))"
        echo "log: $LOG_FILE"
        exit 0
    fi

    duration_secs="$(parse_duration_secs "$duration")"
    workload_flag="$(workload_arg "$workload")"
    events_flag="$(event_verbosity_arg "$DEFAULT_EVENT_VERBOSITY")"
    snapshots_flag="$(snapshots_arg "$DEFAULT_SNAPSHOTS_ENABLED")"
    run_dir="$(run_dir_for_start)"
    mkdir -p "$run_dir"
    save_run_dir "$run_dir"
    ensure_release_binaries

    qroot="$(quote_shell_arg "$PROJ_ROOT")"
    qrun_dir="$(quote_shell_arg "$run_dir")"
    qduration="$(quote_shell_arg "$duration_secs")"
    qops="$(quote_shell_arg "$DEFAULT_OPS_PER_CYCLE")"
    qpipeline="$(quote_shell_arg "$pipeline_depth")"
    qthreads="$(quote_shell_arg "$DEFAULT_THREADS")"
    qmetrics_interval="$(quote_shell_arg "$metrics_interval_secs")"
    qcleanup_batch="$(quote_shell_arg "$cleanup_batch_size")"
    qonyx_max_pba="$(quote_shell_arg "$onyx_max_pba")"
    qfault="$(quote_shell_arg "$DEFAULT_FAULT_DENSITY_PCT")"
    qseed="$(quote_shell_arg "$DEFAULT_SEED")"
    qrestart="$(quote_shell_arg "$restart_interval")"
    qextra="$DEFAULT_EXTRA_ARGS"

    cmd="cd $qroot && exec target/release/metadb-soak $qrun_dir --duration-secs $qduration --ops-per-cycle $qops --pipeline-depth $qpipeline --threads $qthreads --metrics-interval-secs $qmetrics_interval --cleanup-batch-size $qcleanup_batch --onyx-max-pba $qonyx_max_pba --fault-density-pct $qfault --seed $qseed $workload_flag $events_flag"
    if [[ -n "$snapshots_flag" ]]; then
        cmd="$cmd $snapshots_flag"
    fi
    if [[ -n "$restart_interval" ]]; then
        cmd="$cmd --restart-interval $qrestart"
    fi
    if [[ -n "$qextra" ]]; then
        cmd="$cmd $qextra"
    fi

    if command -v setsid >/dev/null 2>&1; then
        setsid bash -lc "$cmd" > "$LOG_FILE" 2>&1 &
    else
        bash -lc "$cmd" > "$LOG_FILE" 2>&1 &
    fi
    local pid=$!
    echo "$pid" > "$PID_FILE"

    echo "soak started"
    echo "  pid:      $pid"
    echo "  run dir:  $run_dir"
    echo "  log:      $LOG_FILE"
    echo "  duration: $duration ($duration_secs s)"
    echo "  workload: $workload"
    echo "  ops/cycle: $DEFAULT_OPS_PER_CYCLE"
    echo "  pipeline depth: $pipeline_depth"
    echo "  cleanup batch: $cleanup_batch_size"
    echo "  onyx max pba: $onyx_max_pba"
    echo "  metrics:  $run_dir/metrics.jsonl (${metrics_interval_secs}s)"
    echo "  events:   $DEFAULT_EVENT_VERBOSITY"
    echo "  snapshots: $DEFAULT_SNAPSHOTS_ENABLED"
    if [[ -n "$restart_interval" ]]; then
        echo "  restart interval: $restart_interval"
    fi
}

cmd_stop() {
    if ! is_running; then
        echo "soak not running"
        rm -f "$PID_FILE"
        return 0
    fi
    local pid
    pid="$(cat "$PID_FILE")"
    stop_process_group "$pid"
    rm -f "$PID_FILE"
    echo "soak stopped (was pid $pid)"
}

cmd_restart() {
    local duration="${1:-$DEFAULT_DURATION}"
    local workload="${2:-$DEFAULT_WORKLOAD}"
    local restart_interval="${3:-$DEFAULT_RESTART_INTERVAL}"
    local pipeline_depth="${4:-$DEFAULT_PIPELINE_DEPTH}"
    local metrics_interval_secs="${5:-$DEFAULT_METRICS_INTERVAL_SECS}"
    local cleanup_batch_size="${6:-$DEFAULT_CLEANUP_BATCH_SIZE}"
    local onyx_max_pba="${7:-$DEFAULT_ONYX_MAX_PBA}"
    cmd_stop || true
    sleep 1
    cmd_start "$duration" "$workload" "$restart_interval" "$pipeline_depth" "$metrics_interval_secs" "$cleanup_batch_size" "$onyx_max_pba"
}

cmd_status() {
    local run_dir summary_path events_path
    run_dir="$(saved_run_dir || true)"
    if is_running; then
        echo "soak: running (pid $(cat "$PID_FILE"))"
    else
        echo "soak: stopped"
    fi
    if [[ -n "$run_dir" ]]; then
        echo "run dir: $run_dir"
        summary_path="$run_dir/summary.json"
        events_path="$run_dir/events.jsonl"
        [[ -f "$summary_path" ]] && echo "summary: $summary_path"
        [[ -f "$events_path" ]] && echo "events:  $events_path"
    else
        echo "run dir: (none yet)"
    fi
    echo "log: $LOG_FILE"
}

cmd_logs() {
    touch "$LOG_FILE"
    tail -f "$LOG_FILE"
}

cmd_events() {
    local run_dir
    run_dir="$(saved_run_dir || true)"
    if [[ -z "$run_dir" ]]; then
        echo "no saved run dir" >&2
        exit 1
    fi
    touch "$run_dir/events.jsonl"
    tail -f "$run_dir/events.jsonl"
}

cmd_metrics() {
    local run_dir metrics_path
    run_dir="$(saved_run_dir || true)"
    if [[ -z "$run_dir" ]]; then
        echo "no saved soak run dir"
        exit 1
    fi
    metrics_path="$run_dir/metrics.jsonl"
    if [[ ! -f "$metrics_path" ]]; then
        echo "metrics file not found: $metrics_path"
        exit 1
    fi
    tail -f "$metrics_path"
}

cmd_metrics_summary() {
    local metrics_path samples
    samples="${2:-12}"
    if [[ -n "${1:-}" ]]; then
        if [[ -d "$1" ]]; then
            metrics_path="$1/metrics.jsonl"
        else
            metrics_path="$1"
        fi
    else
        local run_dir
        run_dir="$(saved_run_dir || true)"
        if [[ -z "$run_dir" ]]; then
            echo "no saved soak run dir"
            exit 1
        fi
        metrics_path="$run_dir/metrics.jsonl"
    fi
    if [[ ! -f "$metrics_path" ]]; then
        echo "metrics file not found: $metrics_path"
        exit 1
    fi
    python3 "$PROJ_ROOT/scripts/metadb_metrics_summary.py" "$metrics_path" --samples "$samples"
}

cmd_summary() {
    local run_dir
    run_dir="$(saved_run_dir || true)"
    if [[ -z "$run_dir" ]]; then
        echo "no saved run dir" >&2
        exit 1
    fi
    cat "$run_dir/summary.json"
}

cmd_verify() {
    local run_dir
    run_dir="$(saved_run_dir || true)"
    if [[ -z "$run_dir" ]]; then
        echo "no saved run dir" >&2
        exit 1
    fi
    ensure_release_binaries
    (
        cd "$PROJ_ROOT"
        exec target/release/metadb-verify "$run_dir" --strict
    )
}

bench_run_dir() {
    local target="$1"
    local scenario="$2"
    local size_label="$3"
    local stamp
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    printf '%s/%s-%s-%s-%s\n' "$BENCH_ROOT" "$stamp" "$target" "$scenario" "$size_label"
}

bench_scenario_arg() {
    case "$1" in
        prefill) printf '%s\n' "l2p-prefill" ;;
        put) printf '%s\n' "l2p-put" ;;
        get) printf '%s\n' "l2p-get" ;;
        multi-get) printf '%s\n' "l2p-multi-get" ;;
        meta-tx) printf '%s\n' "meta-tx" ;;
        *)
            echo "unknown bench scenario: $1" >&2
            exit 1
            ;;
    esac
}

bench_scenarios() {
    local scenario="$1"
    case "$scenario" in
        all)
            printf '%s\n' prefill get multi-get meta-tx
            ;;
        prefill|put|get|multi-get|meta-tx)
            printf '%s\n' "$scenario"
            ;;
        *)
            echo "unknown bench scenario: $scenario" >&2
            exit 1
            ;;
    esac
}

bench_prefill_needed() {
    case "$1" in
        get|multi-get) return 0 ;;
        *) return 1 ;;
    esac
}

bench_key_count_for_bytes() {
    local bytes="$1"
    # L2P payload value is 28B; use ceil division for equivalent key count.
    printf '%s\n' "$(((bytes + 27) / 28))"
}

run_bench_backend() {
    local backend="$1"
    local scenario="$2"
    local run_dir="$3"
    local size_bytes="${4:-0}"
    local path_tag="${5:-$scenario}"
    local reuse_existing="${6:-0}"
    local skip_prefill="${7:-0}"
    local scenario_arg json_path bench_path key_space
    local -a cmd

    scenario_arg="$(bench_scenario_arg "$scenario")"
    bench_path="$run_dir/$backend-$path_tag"
    json_path="$run_dir/$backend-$scenario.json"
    cmd=(
        "$PROJ_ROOT/target/release/$backend-bench"
        "$scenario_arg"
        "--path" "$bench_path"
        "--threads" "$BENCH_DEFAULT_THREADS"
        "--prefill-batch-size" "$BENCH_DEFAULT_PREFILL_BATCH_SIZE"
        "--seed" "$BENCH_DEFAULT_SEED"
        "--json"
    )
    [[ "$backend" == "metadb" ]] && cmd+=("--prefill-flush-keys" "$BENCH_DEFAULT_PREFILL_FLUSH_KEYS")
    if [[ "$reuse_existing" == "1" ]]; then
        cmd+=("--reuse-existing")
    else
        cmd+=("--reset")
    fi
    if [[ "$skip_prefill" == "1" ]]; then
        cmd+=("--skip-prefill")
    fi

    case "$scenario" in
        prefill)
            key_space="$(bench_key_count_for_bytes "$size_bytes")"
            cmd+=(
                "--ops" 1
                "--key-space" "$key_space"
                "--prefill-bytes" "$size_bytes"
            )
            [[ "$backend" == "metadb" ]] && cmd+=("--shards" "$BENCH_DEFAULT_SHARDS")
            [[ "$backend" == "metadb" || "$backend" == "rocksdb" ]] && cmd+=("--cache-mb" "$BENCH_DEFAULT_CACHE_MB")
            ;;
        put)
            cmd+=(
                "--ops" "$BENCH_DEFAULT_PUT_OPS"
                "--cache-mb" "$BENCH_DEFAULT_CACHE_MB"
            )
            [[ "$backend" == "metadb" ]] && cmd+=("--shards" "$BENCH_DEFAULT_SHARDS")
            ;;
        get)
            key_space="$(bench_key_count_for_bytes "$size_bytes")"
            cmd+=(
                "--ops" "$BENCH_DEFAULT_GET_OPS"
                "--key-space" "$key_space"
                "--prefill-bytes" "$size_bytes"
                "--warmup-ops" "$BENCH_DEFAULT_GET_WARMUP"
            )
            [[ "$backend" == "metadb" ]] && cmd+=("--shards" "$BENCH_DEFAULT_SHARDS")
            [[ "$backend" == "metadb" || "$backend" == "rocksdb" ]] && cmd+=("--cache-mb" "$BENCH_DEFAULT_CACHE_MB")
            ;;
        multi-get)
            key_space="$(bench_key_count_for_bytes "$size_bytes")"
            cmd+=(
                "--ops" "$BENCH_DEFAULT_MULTI_OPS"
                "--key-space" "$key_space"
                "--prefill-bytes" "$size_bytes"
                "--batch-size" "$BENCH_DEFAULT_MULTI_BATCH_SIZE"
                "--warmup-ops" "$BENCH_DEFAULT_MULTI_WARMUP"
            )
            [[ "$backend" == "metadb" ]] && cmd+=("--shards" "$BENCH_DEFAULT_SHARDS")
            [[ "$backend" == "metadb" || "$backend" == "rocksdb" ]] && cmd+=("--cache-mb" "$BENCH_DEFAULT_CACHE_MB")
            ;;
        meta-tx)
            cmd+=(
                "--ops" "$BENCH_DEFAULT_META_OPS"
                "--threads" "$BENCH_DEFAULT_THREADS"
                "--key-space" "${METADB_BENCH_META_KEY_SPACE:-200000}"
                "--overwrite-pct" "$BENCH_DEFAULT_OVERWRITE_PCT"
                "--dedup-hit-pct" "$BENCH_DEFAULT_DEDUP_HIT_PCT"
            )
            [[ "$backend" == "metadb" ]] && cmd+=("--shards" "$BENCH_DEFAULT_SHARDS")
            ;;
    esac

    echo "Running $backend $scenario ..." >&2
    "${cmd[@]}" > "$json_path"
    printf '%s\n' "$json_path"
}

print_bench_summary() {
    python3 - <<'PY' "$1"
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
data = json.loads(p.read_text())
print(f"backend: {p.stem}")
print(f"  scenario: {data['scenario']}")
if data.get('prefill_keys', 0):
    print(
        f"  prefill: keys={data['prefill_keys']} bytes={data['prefill_bytes']} "
        f"secs={data['prefill_elapsed_secs']:.3f} "
        f"keys/s={data['prefill_ops_per_sec']:.2f} bytes/s={data['prefill_bytes_per_sec']:.2f}"
    )
print(
    f"  measure: ops={data['ops']} items={data['items']} secs={data['elapsed_secs']:.3f} "
    f"ops/s={data['ops_per_sec']:.2f} items/s={data['items_per_sec']:.2f}"
)
lat = data['latency_us']
print(
    f"  latency_us: avg={lat['avg']:.2f} p50={lat['p50']} "
    f"p95={lat['p95']} p99={lat['p99']} max={lat['max']}"
)
cache = data.get('cache_delta')
if cache:
    print(
        f"  cache: hits={cache['hits']} misses={cache['misses']} "
        f"evictions={cache['evictions']} pages={cache['current_pages']} bytes={cache['current_bytes']}"
    )
PY
}

print_bench_compare() {
    python3 - <<'PY' "$1" "$2"
import json, pathlib, sys
a = json.loads(pathlib.Path(sys.argv[1]).read_text())
b = json.loads(pathlib.Path(sys.argv[2]).read_text())
def ratio(fast, slow):
    return fast / slow if slow else 0.0
print(f"scenario: {a['scenario']}")
if a.get('prefill_keys', 0) or b.get('prefill_keys', 0):
    print("prefill:")
    print(f"  metadb  keys/s={a.get('prefill_ops_per_sec', 0):.2f} bytes/s={a.get('prefill_bytes_per_sec', 0):.2f}")
    print(f"  rocksdb keys/s={b.get('prefill_ops_per_sec', 0):.2f} bytes/s={b.get('prefill_bytes_per_sec', 0):.2f}")
print("measure:")
print(f"  metadb  ops/s={a['ops_per_sec']:.2f} items/s={a['items_per_sec']:.2f}")
print(f"  rocksdb ops/s={b['ops_per_sec']:.2f} items/s={b['items_per_sec']:.2f}")
print("latency_us:")
for key in ('avg', 'p50', 'p95', 'p99', 'max'):
    print(f"  {key}: metadb={a['latency_us'][key]} rocksdb={b['latency_us'][key]}")
faster = 'metadb' if a['ops_per_sec'] >= b['ops_per_sec'] else 'rocksdb'
r = ratio(max(a['ops_per_sec'], b['ops_per_sec']), min(a['ops_per_sec'], b['ops_per_sec']))
print(f"winner_by_ops: {faster} ({r:.2f}x)")
PY
}

cmd_bench() {
    local target="${1:-compare}"
    local scenario="${2:-multi-get}"
    local size_label="${3:-$BENCH_DEFAULT_SIZE}"
    local size_bytes=0
    local run_dir metadb_json rocksdb_json current_scenario
    local metadb_read_tag rocksdb_read_tag
    local -a scenarios

    case "$target" in
        metadb|rocksdb|compare) ;;
        *)
            echo "unknown bench target: $target" >&2
            exit 1
            ;;
    esac
    mapfile -t scenarios < <(bench_scenarios "$scenario")

    if [[ "$scenario" == "all" || "$scenario" == "prefill" || "$scenario" == "get" || "$scenario" == "multi-get" ]]; then
        size_bytes="$(parse_size_bytes "$size_label")"
    elif ! bench_prefill_needed "$scenario"; then
        size_label="na"
    fi

    run_dir="$(bench_run_dir "$target" "$scenario" "$size_label")"
    mkdir -p "$run_dir"
    save_bench_run_dir "$run_dir"
    ensure_bench_binaries
    metadb_read_tag="readset"
    rocksdb_read_tag="readset"

    for current_scenario in "${scenarios[@]}"; do
        if [[ ${#scenarios[@]} -gt 1 ]]; then
            echo ""
            echo "=== $current_scenario ==="
        fi
        case "$target" in
            metadb)
                case "$current_scenario" in
                    prefill)
                        metadb_json="$(run_bench_backend metadb prefill "$run_dir" "$size_bytes" "$metadb_read_tag" 0 0)"
                        ;;
                    get|multi-get)
                        metadb_json="$(run_bench_backend metadb "$current_scenario" "$run_dir" "$size_bytes" "$metadb_read_tag" 1 1)"
                        ;;
                    *)
                        metadb_json="$(run_bench_backend metadb "$current_scenario" "$run_dir" "$size_bytes")"
                        ;;
                esac
                print_bench_summary "$metadb_json"
                ;;
            rocksdb)
                case "$current_scenario" in
                    prefill)
                        rocksdb_json="$(run_bench_backend rocksdb prefill "$run_dir" "$size_bytes" "$rocksdb_read_tag" 0 0)"
                        ;;
                    get|multi-get)
                        rocksdb_json="$(run_bench_backend rocksdb "$current_scenario" "$run_dir" "$size_bytes" "$rocksdb_read_tag" 1 1)"
                        ;;
                    *)
                        rocksdb_json="$(run_bench_backend rocksdb "$current_scenario" "$run_dir" "$size_bytes")"
                        ;;
                esac
                print_bench_summary "$rocksdb_json"
                ;;
            compare)
                case "$current_scenario" in
                    prefill)
                        metadb_json="$(run_bench_backend metadb prefill "$run_dir" "$size_bytes" "$metadb_read_tag" 0 0)"
                        rocksdb_json="$(run_bench_backend rocksdb prefill "$run_dir" "$size_bytes" "$rocksdb_read_tag" 0 0)"
                        ;;
                    get|multi-get)
                        metadb_json="$(run_bench_backend metadb "$current_scenario" "$run_dir" "$size_bytes" "$metadb_read_tag" 1 1)"
                        rocksdb_json="$(run_bench_backend rocksdb "$current_scenario" "$run_dir" "$size_bytes" "$rocksdb_read_tag" 1 1)"
                        ;;
                    *)
                        metadb_json="$(run_bench_backend metadb "$current_scenario" "$run_dir" "$size_bytes")"
                        rocksdb_json="$(run_bench_backend rocksdb "$current_scenario" "$run_dir" "$size_bytes")"
                        ;;
                esac
                print_bench_compare "$metadb_json" "$rocksdb_json"
                ;;
        esac
    done

    echo "bench dir: $run_dir"
}

usage() {
    echo "Usage: $0 {start|stop|restart|status|logs|events|summary|verify|bench} [...]"
    echo ""
    echo "  start   [duration] [workload] [--restart-interval 2h] [--pipeline-depth N] [--cleanup-batch-size N] [--onyx-max-pba N]  Start soak in background"
    echo "  stop                Stop the running soak"
    echo "  restart [duration] [workload] [--restart-interval 2h] [--pipeline-depth N] [--cleanup-batch-size N] [--onyx-max-pba N]  Restart the soak"
    echo "  status              Show pid, run dir, summary path"
    echo "  logs                Tail the soak stdout/stderr log"
    echo "  events              Tail events.jsonl for the current run"
    echo "  metrics             Tail metrics.jsonl for the current run"
    echo "  metrics-summary [path|run-dir] [samples]  Summarize metrics rates and bottlenecks"
    echo "  summary             Print summary.json for the current run"
    echo "  verify              Run metadb-verify --strict on the current run dir"
    echo "  bench               Run metadb / rocksdb / compare benchmark"
    echo ""
    echo "Examples:"
    echo "  ./dev.sh start"
    echo "  ./dev.sh start 1h"
    echo "  ./dev.sh start 2h concurrent"
    echo "  ./dev.sh start 24h concurrent --restart-interval 2h --pipeline-depth 128 --cleanup-batch-size 1024 --onyx-max-pba 100000000"
    echo "  METADB_SOAK_RESTART_INTERVAL=2h METADB_SOAK_OPS_PER_CYCLE=1000000 METADB_SOAK_PIPELINE_DEPTH=128 METADB_SOAK_CLEANUP_BATCH_SIZE=1024 METADB_SOAK_ONYX_MAX_PBA=100000000 ./dev.sh start 24h concurrent"
    echo "  ./dev.sh restart 7d"
    echo "  ./dev.sh status"
    echo "  ./dev.sh logs"
    echo "  ./dev.sh metrics"
    echo "  ./dev.sh metrics-summary"
    echo "  ./dev.sh metrics-summary .dev/soak/20260425T235719Z 24"
    echo "  ./dev.sh bench compare get 1g"
    echo "  ./dev.sh bench compare all 10g"
    echo "  ./dev.sh bench compare put"
    echo "  ./dev.sh bench metadb multi-get 10g"
    echo "  ./dev.sh bench rocksdb meta-tx"
    echo ""
    echo "Environment overrides:"
    echo "  METADB_SOAK_DURATION=$DEFAULT_DURATION"
    echo "  METADB_SOAK_OPS_PER_CYCLE=$DEFAULT_OPS_PER_CYCLE"
    echo "  METADB_SOAK_PIPELINE_DEPTH=$DEFAULT_PIPELINE_DEPTH"
    echo "  METADB_SOAK_THREADS=$DEFAULT_THREADS"
    echo "  METADB_SOAK_METRICS_INTERVAL_SECS=$DEFAULT_METRICS_INTERVAL_SECS"
    echo "  METADB_SOAK_CLEANUP_BATCH_SIZE=$DEFAULT_CLEANUP_BATCH_SIZE"
    echo "  METADB_SOAK_ONYX_MAX_PBA=$DEFAULT_ONYX_MAX_PBA"
    echo "  METADB_SOAK_FAULT_DENSITY_PCT=$DEFAULT_FAULT_DENSITY_PCT"
    echo "  METADB_SOAK_SEED=$DEFAULT_SEED"
    echo "  METADB_SOAK_WORKLOAD=$DEFAULT_WORKLOAD        # legacy|onyx|concurrent"
    echo "  METADB_SOAK_EVENT_VERBOSITY=$DEFAULT_EVENT_VERBOSITY  # summary|ops"
    echo "  METADB_SOAK_SNAPSHOTS=$DEFAULT_SNAPSHOTS_ENABLED      # 1|0"
    echo "  METADB_SOAK_RESTART_INTERVAL=$DEFAULT_RESTART_INTERVAL  # e.g. 2h; empty means every cycle"
    echo "  METADB_SOAK_RUN_DIR=<explicit run dir>"
    echo "  METADB_SOAK_EXTRA_ARGS='<extra metadb-soak args>'"
    echo "  METADB_BENCH_THREADS=$BENCH_DEFAULT_THREADS"
    echo "  METADB_BENCH_SIZE=$BENCH_DEFAULT_SIZE"
    echo "  METADB_BENCH_GET_OPS=$BENCH_DEFAULT_GET_OPS"
    echo "  METADB_BENCH_MULTI_OPS=$BENCH_DEFAULT_MULTI_OPS"
    echo "  METADB_BENCH_MULTI_BATCH_SIZE=$BENCH_DEFAULT_MULTI_BATCH_SIZE"
    echo "  METADB_BENCH_PREFILL_BATCH_SIZE=$BENCH_DEFAULT_PREFILL_BATCH_SIZE"
    echo "  METADB_BENCH_PREFILL_FLUSH_KEYS=$BENCH_DEFAULT_PREFILL_FLUSH_KEYS"
    echo "  METADB_BENCH_META_OPS=$BENCH_DEFAULT_META_OPS"
    echo "  METADB_BENCH_PUT_OPS=$BENCH_DEFAULT_PUT_OPS"
    echo "  METADB_BENCH_CACHE_MB=$BENCH_DEFAULT_CACHE_MB"
}

case "${1:-}" in
    start)
        shift
        parse_soak_run_args "$@"
        cmd_start "$PARSED_DURATION" "$PARSED_WORKLOAD" "$PARSED_RESTART_INTERVAL" "$PARSED_PIPELINE_DEPTH" "$PARSED_METRICS_INTERVAL_SECS" "$PARSED_CLEANUP_BATCH_SIZE" "$PARSED_ONYX_MAX_PBA"
        ;;
    stop)    cmd_stop ;;
    restart)
        shift
        parse_soak_run_args "$@"
        cmd_restart "$PARSED_DURATION" "$PARSED_WORKLOAD" "$PARSED_RESTART_INTERVAL" "$PARSED_PIPELINE_DEPTH" "$PARSED_METRICS_INTERVAL_SECS" "$PARSED_CLEANUP_BATCH_SIZE" "$PARSED_ONYX_MAX_PBA"
        ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    events)  cmd_events ;;
    metrics) cmd_metrics ;;
    metrics-summary) cmd_metrics_summary "${2:-}" "${3:-12}" ;;
    summary) cmd_summary ;;
    verify)  cmd_verify ;;
    bench)   cmd_bench "${2:-compare}" "${3:-multi-get}" "${4:-$BENCH_DEFAULT_SIZE}" ;;
    *)       usage ;;
esac
