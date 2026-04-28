# metadb-onyx-soak 使用手册

独立的 metadb 压力测试工具，**完全模拟 onyx 的 flush_writer + AsyncCheckpoint
负载形态**，让你在不重启 onyx 引擎的前提下迭代 metadb 的写路径性能问题
（特别是 `apply_gate` / `install` / `tree.write` 争锁这一类）。

## 它和 metadb-soak / metadb-bench 的区别

| 工具 | 目的 | 缺什么 |
|---|---|---|
| `metadb-soak` | crash-safety、WAL replay、ref-model 校验 | 每个 cycle 是 ops → FLUSH → snapshot 串行的，**写者和 flusher 不并发**，打不出 apply_gate / install 争锁 |
| `metadb-bench meta-tx` | 测固定 op 数的吞吐 | **不调 `db.flush()`**，dirty pages 一直堆，看不到 install 长尾 |
| `metadb-onyx-soak` | **全流程性能复现** | 不做 crash / verify，纯压性能 |

## 复现什么场景

实测对齐 2026-04-28 onyx 生产 trace：

```
4 writer threads        持续 tx.commit()，每 commit 500 个 l2p_remap
1 flusher thread        每 50ms db.flush()（onyx AsyncCheckpoint 同款）
4 reader threads        持续 db.multi_get(8 LBAs)，覆盖 epoch-pin 路径
LBA 空间 16M            对应 64GB volume / 4KB block
page cache 4 GiB        匹配 onyx 默认
WAL lanes 1             默认优先 group commit；可用 `--wal-lanes N` 显式压并行 fsync
dedup 默认关闭          `--dedup-hit-pct 30` 开启 dedup 并模拟 onyx 的 dedup 命中率
cleanup batch 256       PBA refcount 归零积满 256 个就调
                        cleanup_dedup_for_dead_pbas（onyx 写回路同款）
```

每个 commit 的 500 个 l2p_remap 在 metadb apply 路径上自动产生
1000 个隐式 refcount op（decref old + incref new）。默认不生成 dedup WAL ops，
用于模拟纯压缩。显式传 `--dedup-hit-pct N` 后，**dedup hit 路径**会触发
dedup_index 的 `guard` rc 验证 + l2p_remap with guard；**dedup miss 路径**
额外加 `put_dedup` + `register_dedup_reverse`。整体打到 metadb 的就是 onyx
flush_writer 一比一的 op shape。

## 快速开始

```bash
cd /root/onyx_storage/metadb
cargo build --release --bin metadb-onyx-soak
```

### 5 分钟全量 run（默认）

```bash
./target/release/metadb-onyx-soak --reset
```

`--reset` 删掉旧 db 重建。不加就在已有 db 上接着跑（增量压力）。

### 60 秒快速迭代（开发改一行就跑一次）

```bash
./target/release/metadb-onyx-soak --reset --duration-secs 60 --warmup-secs 10
```

60 秒已经能稳定打出 install 长尾，足够判断一个改动是改善还是变差。

### 对应 onyx 后台吞吐目标

| onyx 场景 | metadb 等效目标 | 命令 |
|---|---|---|
| 纯压缩 2 GB/s 用户 IO | `avg_ops/s ≥ 370_000` | `--target-ops-per-sec 370000` |
| 带 dedup 500 MB/s 用户 IO | `avg_ops/s ≥ 120_000` | `--dedup-hit-pct 30 --target-ops-per-sec 120000` |

换算依据：从 2026-04-28 trace 的 `76 MiB/s 用户写 → 14k ops/s metadb` 比例线性外推。dedup 命中率 30% 是生产观察到的常态。

### dedup 压力

```bash
./target/release/metadb-onyx-soak --reset --duration-secs 90 \
  --dedup-hit-pct 30 --cleanup-batch 256 \
  --target-ops-per-sec 120000
```

dedup 路径产生的额外 WAL ops（`put_dedup` + `register_dedup_reverse` + 周期性
`cleanup_dedup_for_dead_pbas`）会一起打到 metadb，是 onyx 完整写入流程的等价
负载。不传 `--dedup-hit-pct` 才是纯压缩模式；传 `--dedup-hit-pct 0` 表示开启
dedup 且 0% 命中（也就是 100% miss）。

## 输出格式

每 5 秒一行：

```
[t=  5.1s] commits=   652 (  129/s) ops= 326000 ( 64731/s) commit_p50= 30.7ms p99= 64.5ms max= 81.4ms | commit wal=3.5ms wait=0.4ms gate=0.2ms apply=26.4ms | wal batches=640 fsyncs=640 batch_max=4 write_avg=0.1ms fsync_avg=2.4ms submit_avg=3.8ms | op_us l2p=13.8 rc=2.9 dedup=0.0 | flush=  2 gate_max=18336us install_max=1014148us total_max=2746495us | l2p_priv=     0 retired=    0 apply_q=  4 cache= 10% | reads=1455877 (289081/s) read_p99=  0.0ms
```

字段含义：

| 字段 | 含义 | 异常信号 |
|---|---|---|
| `commits/s`、`ops/s` | 写吞吐 | 跑着跑着持续下降 = 后台堆积，metadb 没跟上 |
| `commit_p50/p99/max` | 写延迟分位（窗口内） | p99 > 几百 ms 说明被 flush / install 阻塞 |
| `commit wal/wait/gate/apply` | commit 内部拆分 | wal 大 = group commit / fsync 问题；apply 大 = BTree/refcount 热路径问题 |
| `wal batches/fsyncs/batch_max` | WAL group commit 形态 | batch_max 长期 1-2 说明 lane 分散或同步提交太少 |
| `write_avg/fsync_avg/submit_avg` | WAL 写入 / fsync / submit 等待 | fsync_avg 大时优先看 WAL lane 数和设备同步能力 |
| `op_us l2p/rc/dedup` | apply 变体平均耗时 | l2p 或 rc 增长说明 apply 热路径退化 |
| `flush=N` | 这 5 秒完成的 flush 次数 | 50ms 间隔理论 100 次/5s；远低于说明 flush 自己很慢 |
| `gate_max` | flush 等 `apply_gate.write()` 的累计 max（μs） | > 几百 ms 说明 apply_gate 修复退化 |
| `install_max` | 单次 flush 的 install 阶段 max（μs） | **核心瓶颈指标**，目标 < 2 秒 |
| `total_max` | 单次 flush 的总 max（μs） | install + gate_wait + io 之和 |
| `l2p_priv` / `retired` | metadb 内 dirty/retired 页数 | 0 ≠ 真的没有；可能是 try_lock 失败（说明锁正被 install/lane 持有，是个 _间接_ 信号） |
| `apply_q` | 当前 lane 排队的 op 数 | 持续 > 0 说明 lane 处理跟不上 dispatch |
| `cache` | page cache 用量百分比 | 长期 100% 说明 dirty 化清不掉 |
| `reads/s` | reader 吞吐 | 突然降至 0 = epoch-pin 或 read_view 卡住 |
| `read_p99` | read 延迟分位 | 通常 <1ms；变大说明 ReadView 路径被影响 |

> **关于 `l2p_priv=0` 的特殊解读**：`pending_state()` 用 `try_read`/`try_lock`
> 避免被 install 卡死 status socket，**拿不到锁就跳过那个 shard**。所以
> `l2p_priv=0` 但 `cache=60%` 这种组合，意味着「metadb 内部正在 install
> 持锁」——本身就是 install 长尾的间接证据。

## PASS/FAIL 判定

跑完打印 summary 和三条门控：

```
samples=11 duration=55.3s commits=1969 ops=984500 avg_ops/s=17802
commit p99 max across windows = 213.5 ms
throughput last/mid = 13696/16561 ops/s (83%)
flush install_max(running) = 14875 ms   gate_wait_max(running) = 97 ms

  [FAIL] install_max < 2000 ms  (got 14875 ms)
  [PASS] commit P99   < 500 ms  (got 213.5 ms)
  [PASS] throughput stability >= 80%  (got 83%)
FAIL
```

| 门控 | 默认阈值 | 翻参数 |
|---|---|---|
| `install_max` | < 2000 ms | `--target-install-max-ms` |
| commit P99（任意 5s 窗口的 max） | < 500 ms | `--target-commit-p99-ms` |
| 吞吐稳定性（最后 1/4 窗口的 avg_ops/s ÷ 中段 1/2 窗口的 avg_ops/s） | ≥ 80% | 暂未参数化，改源码里的 `>= 0.80` |
| 平均吞吐 `avg_ops/s` | 0（不检查） | `--target-ops-per-sec`（推荐：纯压缩 370000，dedup 120000） |

进程 exit code：`0 = PASS`，`1 = FAIL`，`2 = parse error`。可以直接 CI 化。

## 工作流：改一版 → 验一版

```bash
# 1. baseline
./target/release/metadb-onyx-soak --reset --duration-secs 60 \
  2>&1 | tee runs/baseline.log
# → install_max=14875 ms, FAIL

# 2. 改代码（比如 cap apply_l2p_bucket 的 chunk 大小）
$EDITOR src/db/commit.rs
cargo build --release --bin metadb-onyx-soak

# 3. 验
./target/release/metadb-onyx-soak --reset --duration-secs 60 \
  2>&1 | tee runs/cap-bucket-128.log
# → install_max=? 看是否下降

# 4. 对比关键行
grep "install_max(running)" runs/*.log
grep -E "^\[(PASS|FAIL)\]" runs/*.log
```

每次跑前 `--reset`：保证起始状态干净，install 第一次峰值通常出现在 dirty 涨
到一定量后。如果不 reset，老 dirty 已经在 cache 里，启动就 install，复现路径
不一致。

## 调参指南

### 想压更狠

```bash
./target/release/metadb-onyx-soak --reset \
  --writers 8 --ops-per-commit 1000 --flush-interval-ms 20
```

注意 writers > 物理核数收益开始递减；ops-per-commit 太大会让 wal record 接近 batch 上限。

### 想模拟轻负载（看是不是 metadb 在轻负载也有抖动）

```bash
./target/release/metadb-onyx-soak --reset \
  --writers 1 --ops-per-commit 50 --flush-interval-ms 100
```

### 关掉 reader（只看写路径，排除 epoch-pin / ReadView 干扰）

```bash
./target/release/metadb-onyx-soak --reset --readers 0
```

### 关掉 flusher（看 metadb 不 flush 时的 commit 上限）

`--flush-interval-ms 0` 当前没特判，要的话改源码。临时方法：把 interval 设很大，比如 `--flush-interval-ms 999000000`。

## 源码位置

- bin: [`src/bin/metadb-onyx-soak.rs`](../src/bin/metadb-onyx-soak.rs)（~430 行单文件）
- 复用 onyx-shape l2p value 编码：[`src/testing/onyx_model.rs`](../src/testing/onyx_model.rs#L400)
- 内部用的 metadb API：`Db::begin()`, `Db::multi_get()`, `Db::flush()`,
  `Db::metrics_snapshot()`, `Db::pending_state()`

不依赖任何 onyx-storage 代码，**纯打 metadb**。

## 已知限制

1. **不做正确性校验**——读结果不和 ref-model 对比。修出现 hang 或 panic 当然能看出来，
   但「commit 成功了，但 value 错了」这类 bug 这个工具发现不了，要走 `metadb-soak` 的 verify 路径。
2. **不做 crash / fault injection**——`metadb-soak` 已经覆盖；这里的目标是性能复现。
3. **flush metric 是 cumulative running max**——某次 flush 的 max 一直保留到 run 结束，
   不是 5s 窗口内的 max。如果想看「这 5s 内 install 是不是又出了一个尖峰」，
   要看 `install_max(running)` 是不是涨了。Δ 计算可以加，目前没加是想保持这个
   工具够轻。
4. **dedup workload 暂未模拟**——现在所有 commit 都走 dedup miss 路径
   （fresh PBA + 不调 `put_dedup`）。要测 dedup 路径需要扩展 writer_loop
   带 hit-rate 配置 + dedup_hit guard 路径。
