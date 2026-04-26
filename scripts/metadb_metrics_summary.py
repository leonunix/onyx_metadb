#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize metadb soak metrics.jsonl counters as rates and latencies."
    )
    parser.add_argument("path", type=Path, help="Path to metrics.jsonl")
    parser.add_argument(
        "--samples",
        type=int,
        default=12,
        help="Use the last N samples for the main window (default: 12)",
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=2,
        help="Also print a short recent window using the last N samples (default: 2)",
    )
    args = parser.parse_args()

    rows = load_rows(args.path)
    if len(rows) < 2:
        print(f"need at least 2 metric samples, got {len(rows)}", file=sys.stderr)
        return 1

    print(f"file: {args.path}")
    print(f"samples: {len(rows)}")
    print_window("main", rows[-max(args.samples, 2) :])
    if args.recent > 1 and len(rows) > args.recent:
        print()
        print_window("recent", rows[-args.recent :])
    return 0


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{path}:{line_no}: invalid json: {e}") from e
            if "ts_ms" not in row or "metrics" not in row:
                raise SystemExit(f"{path}:{line_no}: expected ts_ms and metrics fields")
            rows.append(row)
    return rows


def print_window(name: str, rows: list[dict]) -> None:
    first = rows[0]
    last = rows[-1]
    dt = (last["ts_ms"] - first["ts_ms"]) / 1000.0
    if dt <= 0:
        print(f"{name}: non-positive window ({dt:.3f}s)")
        return

    fm = first["metrics"]["meta"]
    lm = last["metrics"]["meta"]
    fc = first["metrics"]["cache"]
    lc = last["metrics"]["cache"]

    def delta(path: str) -> int:
        table = lm if path.startswith("meta.") else lc
        base = fm if path.startswith("meta.") else fc
        key = path.split(".", 1)[1]
        return int(table.get(key, 0)) - int(base.get(key, 0))

    def rate(path: str) -> float:
        return delta(path) / dt

    def avg(total_path: str, count_path: str) -> float:
        count = delta(count_path)
        if count <= 0:
            return 0.0
        return delta(total_path) / count

    lsn_delta = int(last["metrics"]["last_applied_lsn"]) - int(first["metrics"]["last_applied_lsn"])
    high_water = int(last["metrics"]["high_water"])
    commit_success = delta("meta.commit_success")
    commit_ops = delta("meta.commit_ops")
    wal_batches = delta("meta.wal_batches")
    wal_records = delta("meta.wal_records")
    wal_fsyncs = delta("meta.wal_fsyncs")
    range_delete_calls = delta("meta.range_delete_calls")
    cleanup_calls = delta("meta.cleanup_calls")

    print(f"{name}: {dt:.1f}s ({fmt_ts(first['ts_ms'])} -> {fmt_ts(last['ts_ms'])})")
    print(f"  throughput")
    print(f"    lsn/s:               {lsn_delta / dt:,.0f}")
    print(f"    tx/s:                {commit_success / dt:,.0f}")
    print(f"    logical ops/s:       {commit_ops / dt:,.0f}")
    print(f"    wal records/s:       {wal_records / dt:,.0f}")
    print(f"    wal batches/s:       {wal_batches / dt:,.0f}")
    print(f"    avg wal batch:       {wal_records / wal_batches if wal_batches else 0:.2f}")
    print(f"    max wal batch:       {int(lm.get('wal_batch_records_max', 0)):,}")
    print(f"  latency averages")
    print(f"    commit total:        {avg('meta.commit_total_us', 'meta.commit_success'):,.1f} us")
    print(f"    wal submit wait:     {avg('meta.commit_wal_submit_us', 'meta.commit_success'):,.1f} us")
    print(f"    drop_gate wait:      {avg('meta.commit_drop_gate_wait_us', 'meta.commit_success'):,.1f} us")
    print(f"    lsn/apply wait:      {avg('meta.commit_apply_wait_us', 'meta.commit_success'):,.1f} us")
    print(f"    apply_gate wait:     {avg('meta.commit_apply_gate_wait_us', 'meta.commit_success'):,.3f} us")
    print(f"    apply actual:        {avg('meta.commit_apply_us', 'meta.commit_success'):,.1f} us")
    print(f"    wal fsync:           {avg('meta.wal_fsync_us', 'meta.wal_fsyncs'):,.1f} us")
    print(f"    wal write:           {avg('meta.wal_write_us', 'meta.wal_batches'):,.1f} us")
    print(f"  max latencies")
    print(f"    commit total max:    {int(lm.get('commit_total_max_us', 0)):,} us")
    print(f"    wal submit max:      {int(lm.get('commit_wal_submit_max_us', 0)):,} us")
    print(f"    drop_gate max:       {int(lm.get('commit_drop_gate_wait_max_us', 0)):,} us")
    print(f"    apply wait max:      {int(lm.get('commit_apply_wait_max_us', 0)):,} us")
    print(f"    fsync max:           {int(lm.get('wal_fsync_max_us', 0)):,} us")
    if range_delete_calls or cleanup_calls:
        print(f"  op-kind hotspots")
        if range_delete_calls:
            print(f"    range_delete/s:      {range_delete_calls / dt:,.2f}")
            print(f"    range noop/s:        {rate('meta.range_delete_noop'):,.2f}")
            print(
                f"    range captured/call: {delta('meta.range_delete_captured_entries') / range_delete_calls:,.1f}"
            )
            print(
                f"    range chunks/call:   {delta('meta.range_delete_chunks') / range_delete_calls:,.2f}"
            )
            print(
                f"    range total avg:     {avg('meta.range_delete_total_us', 'meta.range_delete_success'):,.1f} us"
            )
            print(
                f"    range scan avg:      {avg('meta.range_delete_scan_us', 'meta.range_delete_calls'):,.1f} us"
            )
            print(
                f"    range wal avg:       {avg('meta.range_delete_wal_us', 'meta.range_delete_chunks'):,.1f} us"
            )
            print(
                f"    range apply wait:    {avg('meta.range_delete_apply_wait_us', 'meta.range_delete_chunks'):,.1f} us"
            )
            print(
                f"    range apply avg:     {avg('meta.range_delete_apply_us', 'meta.range_delete_chunks'):,.1f} us"
            )
            print(f"    range total max:     {int(lm.get('range_delete_total_max_us', 0)):,} us")
        if cleanup_calls:
            print(f"    cleanup/s:           {cleanup_calls / dt:,.2f}")
            print(f"    cleanup noop/s:      {rate('meta.cleanup_noop'):,.2f}")
            print(f"    cleanup pbas/call:   {delta('meta.cleanup_pbas') / cleanup_calls:,.1f}")
            print(
                f"    cleanup hashes/call: {delta('meta.cleanup_hashes_found') / cleanup_calls:,.1f}"
            )
            print(f"    cleanup tx ops/call: {delta('meta.cleanup_tx_ops') / cleanup_calls:,.1f}")
            print(
                f"    cleanup total avg:   {avg('meta.cleanup_total_us', 'meta.cleanup_success'):,.1f} us"
            )
            print(
                f"    cleanup scan avg:    {avg('meta.cleanup_scan_us', 'meta.cleanup_calls'):,.1f} us"
            )
            print(
                f"    cleanup check avg:   {avg('meta.cleanup_forward_check_us', 'meta.cleanup_forward_checks'):,.1f} us"
            )
            print(
                f"    cleanup commit avg:  {avg('meta.cleanup_commit_us', 'meta.cleanup_success'):,.1f} us"
            )
            print(f"    cleanup total max:   {int(lm.get('cleanup_total_max_us', 0)):,} us")
    print(f"  cache/state")
    print(f"    high_water pages:    {high_water:,}")
    print(f"    cache hit/s:         {rate('cache.hits'):,.0f}")
    print(f"    cache miss/s:        {rate('cache.misses'):,.1f}")
    print(f"    current cache MB:    {int(lc.get('current_bytes', 0)) / (1024 * 1024):,.1f}")
    print(f"    pinned cache MB:     {int(lc.get('pinned_bytes', 0)) / (1024 * 1024):,.1f}")
    print(f"  hints")
    for hint in hints(lm, commit_success, wal_batches, wal_records, avg, delta):
        print(f"    - {hint}")


def hints(lm: dict, commit_success: int, wal_batches: int, wal_records: int, avg_fn, delta_fn) -> list[str]:
    out = []
    if commit_success <= 0:
        return ["no committed transactions in this window"]

    wal_submit = avg_fn("meta.commit_wal_submit_us", "meta.commit_success")
    drop_gate = avg_fn("meta.commit_drop_gate_wait_us", "meta.commit_success")
    apply_wait = avg_fn("meta.commit_apply_wait_us", "meta.commit_success")
    apply_gate = avg_fn("meta.commit_apply_gate_wait_us", "meta.commit_success")
    apply_actual = avg_fn("meta.commit_apply_us", "meta.commit_success")
    fsync = avg_fn("meta.wal_fsync_us", "meta.wal_fsyncs")
    batch = wal_records / wal_batches if wal_batches else 0.0
    total = max(avg_fn("meta.commit_total_us", "meta.commit_success"), 1.0)
    range_calls = delta_fn("meta.range_delete_calls")
    cleanup_calls = delta_fn("meta.cleanup_calls")

    if wal_submit / total > 0.45:
        out.append(f"WAL submit dominates ({wal_submit / total:.0%} of avg commit); check fsync and batch size.")
    if fsync > 300:
        out.append(f"fsync is expensive (~{fsync:.0f} us); group commit size matters.")
    if batch < 8:
        out.append(f"WAL batches are small ({batch:.1f} records avg, max {int(lm.get('wal_batch_records_max', 0))}); more submitter threads may help.")
    if drop_gate / total > 0.20:
        out.append(f"drop_gate wait is high ({drop_gate / total:.0%}); range-delete/snapshot/lifecycle writers are serializing commits.")
    if apply_wait / total > 0.15:
        out.append(f"LSN apply ordering wait is visible ({apply_wait / total:.0%}); apply is queuing behind earlier commits.")
    if apply_gate > 10:
        out.append(f"apply_gate wait is non-trivial (~{apply_gate:.1f} us); flush/snapshot may be blocking apply.")
    if apply_actual / total < 0.05:
        out.append("actual index apply is tiny; bottleneck is outside BTree/LSM mutation work.")
    if range_calls:
        range_total = avg_fn("meta.range_delete_total_us", "meta.range_delete_success")
        range_scan = avg_fn("meta.range_delete_scan_us", "meta.range_delete_calls")
        if range_total > 1000:
            out.append(f"range_delete is visible (~{range_total:.0f} us/call); scan portion is ~{range_scan:.0f} us.")
    if cleanup_calls:
        cleanup_total = avg_fn("meta.cleanup_total_us", "meta.cleanup_success")
        cleanup_scan = avg_fn("meta.cleanup_scan_us", "meta.cleanup_calls")
        cleanup_check = avg_fn("meta.cleanup_forward_check_us", "meta.cleanup_forward_checks")
        if cleanup_total > 1000:
            out.append(
                f"dedup cleanup is visible (~{cleanup_total:.0f} us/call); scan ~{cleanup_scan:.0f} us, forward check ~{cleanup_check:.0f} us."
            )
    if not out:
        out.append("no obvious single bottleneck in this window")
    return out


def fmt_ts(ts_ms: int) -> str:
    return f"{ts_ms / 1000:.3f}"


if __name__ == "__main__":
    raise SystemExit(main())
