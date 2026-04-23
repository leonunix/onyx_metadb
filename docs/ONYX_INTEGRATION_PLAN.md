# Onyx Integration — Phase A 实施计划

**版本**：v1  2026-04-23
**上游规格**：[`ONYX_INTEGRATION_SPEC.md`](ONYX_INTEGRATION_SPEC.md)（对接书 v1）
**目标**：把 SPEC 里的改造拆成若干可独立完成、可顺序衔接的 session。

本文件不重复 SPEC 的语义定义；只回答两件事：
1. **怎么拆 session**：每个 session 的输入、交付物、退出判据。
2. **哪些地方需要现场判断**：开放问题清单（谁/何时/建议）。


项目经理额外要求的几件事情
1. 但rs文件不要过长，如果过长请拆分模块（期待单文件代码不超过1k）
2. dedup之类的测试请参考一下onyx的rocksdb的测试，几十个坑都在那里面复现过
3. 已经完成项目记得更新这张表
4. commit里面不要带上你自己的名字
5. 完成一个步骤。记得commit

## 进度表

| Session | 内容 | 状态 |
|---------|------|------|
| S1 | commit_with_outcomes pub + WAL body schema bump + ApplyOutcome 槽位 | **done** (2026-04-23) |
| S2 | `WalOp::L2pRemap` | pending |
| S3 | `WalOp::L2pRangeDelete` | pending |
| S4 | `DropSnapshot` 扩展 + `cleanup_dedup_for_dead_pbas` | pending |
| S5 | 综合 proptest + metadb-soak workload 扩展 | pending |
| S6 | 性能基准 + 24h soak + 门控签收 | pending |

---

## 参考实现：onyx RocksDB 路径（已稳定运行，踩过数百个 refcount 坑）

**实施时强烈建议先读 onyx 侧的现有实现再动笔**。SPEC §3.1 的 decref/incref 决策
表、`self_decrement` 同 pba net 0 补偿、newly_zeroed 严格语义、dedup cleanup 批量
化、hole fill 顺序等等——onyx-storage 已经把这些坑全踩完了，现在是生产级稳定状
态。metadb 侧相当于把**相同语义换一个存储后端**，不是从头设计。

主要参考文件（都在 `/root/onyx_storage/src/meta/store/`）：

| onyx 函数 / 文件 | 对应 metadb 工作项 | 关注点 |
|---|---|---|
| [`blockmap.rs::atomic_batch_write_packed`](../../src/meta/store/blockmap.rs#L262) | S2 `L2pRemap` apply | `self_decrement` 就是 SPEC §3.1 "same pba net 0" 的原版；net_increment 计算逻辑直接迁移 |
| [`blockmap.rs::atomic_batch_write`](../../src/meta/store/blockmap.rs#L191) | S2 `L2pRemap` apply（非 packed 分支） | newly_zeroed 收集方式、old_pba decrement 合并 |
| [`blockmap.rs`](../../src/meta/store/blockmap.rs) 的 `lock_blockmap_keys` / `lock_refcount_pbas` | S2/S3 跨 shard 锁序 | onyx 的 striped lock 先 blockmap keys 后 refcount pbas，metadb 类比 shard index 升序取锁 |
| [`dedup.rs::cleanup_dedup_for_pbas_batch`](../../src/meta/store/dedup.rs#L410) | S4 `cleanup_dedup_for_dead_pbas` | **几乎是 1:1 对应**，直接读这个函数实现 metadb 版 |
| [`dedup.rs`](../../src/meta/store/dedup.rs) 的 dedup hit / `newly_zeroed` 返回路径 | S4 单元测试的竞态 case | onyx 已经写过 "hash 被另一 pba 重新注册不误删" 的测试，照抄断言 |
| [`blockmap.rs`](../../src/meta/store/blockmap.rs) 的 write_hole_fill + 注释 | S2 guard 语义对应 | metadata 验证先于物理覆盖的不变量，metadb 侧靠 `L2pRemap { guard }` 守 |
| `src/buffer/writer.rs` 里 flusher 的 batch writer 调用序列 | S2/S5 集成场景 | 展示 onyx adapter 端将来如何连续发 L2pRemap 并消费 freed_pba |

**重要原则**：
- **语义一致优先于接口优雅**。onyx 代码里的 `self_decrement`、`newly_zeroed` 返回
  `(Pba, decrement, blocks)` 三元组这些现象，背后都是事故复盘——不要凭直觉简化
  语义，有疑问先去 `git log` 对应文件找修 bug 的 commit。
- **测试断言抄作业**。onyx `src/meta/store/tests.rs`、`src/meta/store/blockmap.rs`
  里的 tests 模块、`src/meta/store/dedup.rs` 里的 tests 模块，是 SPEC §4 所有不变
  量的已知正确行为样本。metadb 侧 reference model 行为对标这些。
- **不要在 metadb 里发明 onyx 没踩过的语义**。SPEC 明确 "metadb 是 onyx 定制 db"
  （见 memory: `feedback_metadb_is_onyx_specific`），遇到和 onyx 现有行为不一致的
  设计选择，优先对齐 onyx。

---

## 0. 拆分原则

- 每个 session 有明确 **入口状态** 和 **退出状态**，只靠 SPEC + 本文件 + 现有代码
  就能独立开干，不依赖前任 session 的私人上下文。
- 每个 session 结束时 `cargo test` + `cargo test -- --ignored` 全绿；禁止半落地。
- 单元测试和基础 fault-injection 在各 session 内闭环；**跨 op 的综合 proptest**
  独立成 session（S5），避免每个 session 都重复实现 reference model。
- 24 小时 soak 归到收尾 session（S6）的签收动作，不算"编码"工作量。

### 依赖关系

```
                   ┌─────────┐
                   │   S1    │ 基础：commit_with_outcomes pub + WAL schema bump
                   └────┬────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
     ┌─────┐         ┌─────┐         ┌─────┐
     │ S2  │         │ S3  │         │ S4  │
     │Remap│         │Range│         │Drop │   三者并行可行，但共享 ApplyOutcome
     └──┬──┘         │ Del │         │Snap │   / 编码常量，建议串行提交
        │            └──┬──┘         │+dedp│
        │               │            │cleanp│
        │               │            └──┬──┘
        └───────┬───────┴───────────────┘
                ▼
             ┌─────┐
             │ S5  │ 综合 proptest + soak workload 扩展
             └──┬──┘
                ▼
             ┌─────┐
             │ S6  │ 性能基准 + 文档 + Phase A → B 门控签收
             └─────┘
```

S2/S3/S4 之间没有 **语义** 依赖，但都改 `WalOp` enum / `ApplyOutcome` enum / WAL
编解码，合并冲突难免。推荐串行交付，减少 rebase 成本。

---

## S1 — 基础设施（commit_with_outcomes pub + schema bump + 预留槽位）

### 入口

master 分支（SPEC v1 已落、本 plan 已落），`cargo test` 全绿。

### 交付物

1. **`Transaction::commit_with_outcomes` 升级为 `pub`**
   - 文件：[`src/tx.rs:152`](../src/tx.rs#L152)
   - 改 `pub(crate)` → `pub`，文档注释里写明 "onyx adapter 主入口，commit() 内部调
     此函数后丢弃 outcomes"。
   - `commit()` 保持不变，仍 `map(|(lsn, _)| lsn)`。
   - 不变量（放在 doc comment）：`outcomes.len() == ops.len()` 严格成立。

2. **WAL body schema 版本 bump**
   - 调研点：当前 WAL record header 是否已经携带版本号。若无，需要和 `manifest.rs`
     里的 `body_version: u32` 区分（manifest 用的是 manifest 版本，不是 WAL body
     版本）。
   - 决策（落在本 session 的 commit message / PR 描述里）：版本号放 WAL record
     header 还是 record body 的第一个字节。优先 header，便于 recovery 路径在解
     body 之前就拒绝。
   - 旧版本 WAL：返回 `OnyxError::Corruption`（或 metadb 的等价错误），附带
     `"metadb WAL body version N found, expected M — cross-check Phase A migration"`
     错误文案。
   - 和 SPEC §7 "不做 WAL 兼容" 对齐；不需要 fall-back 解码器。

3. **`ApplyOutcome` enum 预留槽位**
   - 文件：[`src/tx.rs:37`](../src/tx.rs#L37)
   - 新增 3 个变体的占位定义：
     - `L2pRemap { applied: bool, prev: Option<L2pValue>, freed_pba: Option<Pba> }`
     - `RangeDelete { freed_pbas: Vec<Pba> }`
     - `DropSnapshot` 扩展（加 `freed_pbas: Vec<Pba>` 字段，已有字段保留）
   - S1 不实现 apply 逻辑；`apply_op` 里对新 `WalOp` 变体 `unreachable!()` 即可，
     S2~S4 再填。这样每个后续 session 的 diff 集中在自己的 variant。

4. **文档**
   - `metadb/CLAUDE.md` 的「和 onyx-storage 的关系」段加一句 "SPEC 实施进度见
     `docs/ONYX_INTEGRATION_PLAN.md`"。

### 测试

- 现有 290 unit + 25 integration 必须全绿（本 session 只改接口可见性和预留槽位）。
- 新增一条 test：打开一个 schema N-1 版本的 WAL（hand-crafted bytes），恢复必须
  返回明确错误，不 panic、不 silent skip。

### 退出

- `cargo test` + `cargo test -- --ignored` 全绿。
- Commit 明确标 schema bump 的版本号值。

### 规模预估

~150 行改动，半天。

---

## S2 — `WalOp::L2pRemap`（热路径核心）

### 入口

S1 已落。`WalOp` enum 现有变体保持不动。

### 交付物

1. **`WalOp::L2pRemap` variant**
   - 文件：[`src/wal/op.rs:70`](../src/wal/op.rs#L70)
   - 字段：`vol_ord: VolumeOrdinal (u16)`, `lba: Lba (u64)`, `new_value: L2pValue
     (28B)`, `guard: Option<(Pba, u32)>`。
   - 编解码字节布局严格对齐 SPEC §3.1（tag 1B + vol_ord 2B + lba 8B + new_value
     28B + guard_tag 1B + 可选 [pba 8B + min_rc 4B]，总 48B/60B）。
   - `encoded_len()` / `encode()` / `decode()` 按既有风格补齐；`op.rs` 末尾的
     round-trip 测试加一条。

2. **`Transaction::l2p_remap(vol_ord, lba, new_value, guard) -> &mut Self`**
   - 文件：[`src/tx.rs`](../src/tx.rs)
   - 放在 `insert` / `delete` 附近。doc 里注明"这是 onyx 热路径，替代 insert +
     incref + decref 组合；insert/delete/incref/decref 仍保留给非 refcount 场景"。

3. **leaf_was_shared 捕获**（**本 session 最需要判断的点**）
   - 现状：[`paged::tree::insert_at_lsn`](../src/paged/tree.rs#L343) 只返回
     `Option<L2pValue>`。leaf CoW 是否发生（即 leaf 在 CoW 之前 rc 是否 > 1）在
     内部已知，但未外抛。
   - 推荐做法：新增 `insert_at_lsn_with_share_info(lba, value, lsn) ->
     Result<InsertOutcome>`，返回结构体 `{ prev: Option<L2pValue>, leaf_was_shared:
     bool }`。保留旧 `insert_at_lsn` 作薄 wrapper。
   - **判据**：`leaf_was_shared` = 走到 leaf 那一层的 `cow_for_write` 之前，该
     leaf page 的 `rc > 1`。在 [`paged/cache.rs::cow_for_write`](../src/paged/cache.rs#L343)
     里读到的 pre-COW rc 就是答案。需要把这个 bit 从 cow_for_write 沿调用栈往上
     穿。
   - 另一个等价思路：leaf CoW 产生了新 PageId（返回值 != 入参）本身就指示 shared。
     但 "rc=1 的 page 也会因为 lsn 推进而走 `reuse_page` 路径"，不等同 shared。
     **以 pre-COW rc 判断为准，不要用 PageId 差异判断**。
   - 同理补 `delete_at_lsn_with_share_info`，S3 会用。

4. **Apply 逻辑**
   - 文件：[`src/db.rs`](../src/db.rs) 的 `apply_op` 匹配分支附近（参考
     [L962-L1012](../src/db.rs#L962) 的 L2pPut / Incref 处理）。
   - **参考原型**：[`onyx/src/meta/store/blockmap.rs::atomic_batch_write_packed`](../../src/meta/store/blockmap.rs#L262)。
     `self_decrement` 就是 SPEC §3.1 决策表里 "same pba + !leaf_shared → net 0"
     的原版实现。`net_increment = new_refcount - self_decrement` 的算式对应
     metadb 单 op 版本的 `do_incref` 判断。实施时先把 onyx 这 50 行读懂再动笔。
   - 骨架：
     ```rust
     WalOp::L2pRemap { vol_ord, lba, new_value, guard } => {
         // 1. guard check
         if let Some((gp, min_rc)) = guard {
             let cur = refcount_shard(gp).get(gp)?;
             if cur < *min_rc {
                 return Ok(ApplyOutcome::L2pRemap {
                     applied: false, prev: None, freed_pba: None,
                 });
             }
         }
         // 2. L2P write，拿 prev + leaf_was_shared
         let outcome = tree.insert_at_lsn_with_share_info(lba, new_value, lsn)?;
         // 3. 按 SPEC §3.1 表决定 decref / incref
         let old_pba = outcome.prev.map(|v| head_pba(&v));
         let new_pba = head_pba(&new_value);
         let do_decref = old_pba.is_some()
             && !outcome.leaf_was_shared
             && old_pba != Some(new_pba);
         let do_incref = !(old_pba.is_some()
             && !outcome.leaf_was_shared
             && old_pba == Some(new_pba));
         let freed_pba = if do_decref {
             match refcount_shard(old_pba.unwrap()).decref_to_maybe_zero(...) {
                 DecrefResult::HitZero => Some(old_pba.unwrap()),
                 _ => None,
             }
         } else { None };
         if do_incref { refcount_shard(new_pba).incref(new_pba, 1)?; }
         Ok(ApplyOutcome::L2pRemap { applied: true, prev: outcome.prev, freed_pba })
     }
     ```
   - **锁序**：guard 读 refcount 和后续 refcount 写必须在同一把 shard 锁内（SPEC
     §4.3）。如果 `gp`、`old_pba`、`new_pba` 跨 shard，取锁顺序按 shard index 升序
     （和 `metadb/CLAUDE.md` 的「锁序」段对齐），避免和写路径死锁。

5. **L2pValue 头 8B 读取 helper**
   - 新增 `L2pValue::head_pba(&self) -> Pba`：`Pba(u64::from_be_bytes(self.0[..8].try_into().unwrap()))`。
   - 放在 `paged/value.rs` 或同模块。文档里写明"这是 Onyx 编码契约的暴露；改
     BlockmapValue 头 8B 布局会破坏 metadb apply 语义"。

6. **单元测试**（覆盖 SPEC §4.1 / §4.2 / §4.3 / §4.6）
   - 四象限 guard 测试：(guard 通过 / 不通过) × (L2P 有前值 / 无前值)
   - "same pba 原地 overwrite 不改 refcount"
   - "same pba + leaf shared（snapshot 在） → incref 而不是 no-op"
   - "不同 pba + leaf exclusive → decref old + incref new"
   - "不同 pba + leaf shared → 只 incref new，old 抑制 decref"
   - "decref 到 0 → freed_pba = Some(old)"
   - "decref 未到 0 → freed_pba = None"
   - "guard 拒绝 → L2P 和 refcount 都不动（读一次再比对）"
   - packed slot 场景：3 条 L2pRemap 同 prev.pba（dedup 多 LBA 共享 pba），refcount
     最终 delta 正确累减

7. **Fault injection**
   - `FaultPoint::CommitPostWalBeforeApply` × L2pRemap：crash 后 replay 必须正确
     重建 refcount + L2P 状态，不产生"同一次 decref 减两次"。
   - `FaultPoint::CommitPostApplyBeforeLsnBump` × L2pRemap：同上，重点测
     `page.generation >= lsn` 的幂等挡板对新 op 有效。

8. **`Db` 层入口（可选便捷包装）**
   - 暂不加。adapter 直接用 `db.begin_tx().l2p_remap(...).commit_with_outcomes()`。

### 测试清单

- 新增 unit tests ~15 条
- 新增 proptest（对应 §4.1 / §4.3）：随机 guard+leaf_shared 组合，对 reference
  implementation 比对。单个 op 级别的 proptest，非混合 workload。
- 新增 fault-injection integration test 2 条。

### 退出

- 覆盖率 ≥ 90%（用 `cargo llvm-cov` 或等价工具；本仓库要求 90%）。
- `cargo test` + `cargo test -- --ignored` 全绿。
- `ApplyOutcome::L2pRemap` 的三个字段在所有代码路径下都正确 populate（未 populate
  的路径 `debug_assert!(false, ...)`）。

### 规模预估

~800 行改动（含测试），2-3 天。

---

## S3 — `WalOp::L2pRangeDelete`

### 入口

S1 + S2 已落。

### 交付物

1. **`WalOp::L2pRangeDelete` variant**
   - 字段：`vol_ord: VolumeOrdinal`, `start: Lba`, `end: Lba`, `captured: Vec<(Lba, Pba)>`。
   - 编码：tag 1B + vol_ord 2B + start 8B + end 8B + count 4B + captured 条目
     (Lba 8B + Pba 8B) × N。
   - `encoded_len()` 正确考虑 captured 长度。

2. **`Db::range_delete(vol_ord, start, end) -> Result<Lsn>`**
   - Plan 阶段：
     - 取 `drop_gate.read()` + 对应 L2P shard mutex（按 shard index 升序）
     - 扫描 `[start, end)`，收集 `(lba, head_pba(value))`
     - 释放锁
   - 若 captured.len() > 65536，拆多次 commit（每次一条 WAL，transparent）
   - Commit 一条 `WalOp::L2pRangeDelete`

3. **Apply 逻辑**
   - 对每条 `(lba, old_pba)`：
     - `tree.delete_at_lsn_with_share_info(lba, lsn)` → 拿 `leaf_was_shared`
     - `leaf_was_shared == false` → `decref(old_pba, 1)`，到零进 `freed_pbas`
     - `leaf_was_shared == true` → 抑制 decref
   - 输出 `ApplyOutcome::RangeDelete { freed_pbas }`

4. **单元测试**（覆盖 §4.7）
   - 单 shard 内常规 range delete
   - 跨多个 L2P shard 的 range（range 横跨几个 shard 的 LBA 分片）
   - captured 里多条指向同一 pba（dedup 场景）→ 逐条 decref，refcount 最终归零
     正确报告 freed
   - captured.len() 触发拆分阈值（人工制造 65537 条），验证拆分后结果等价
   - range 在 snapshot 下执行：所有 leaf shared → freed_pbas 为空

5. **Fault injection**
   - 同 S2 的两个 fault point × L2pRangeDelete
   - 特别注意 captured 很长时的 WAL 记录尺寸，可能触达 group commit batch 上限，
     Phase 8b group commit 路径需要接受大 body（若当前实现有硬上限需调大或确保
     单条 op 独占一次提交）。

### 退出

- `cargo test` 全绿
- 覆盖率 ≥ 90%

### 规模预估

~500 行改动，1-2 天（比 S2 简单，无 guard 分支，leaf-rc-suppress 复用 S2 的 helper）。

---

## S4 — `DropSnapshot` 扩展 + `cleanup_dedup_for_dead_pbas`

### 入口

S1 已落。S2/S3 建议已落（共用 `head_pba` helper 和 `decref_to_maybe_zero` 分支）。

### 交付物

1. **`WalOp::DropSnapshot` 扩展**
   - 现有：`{ id, pages }`（[`src/wal/op.rs:209`](../src/wal/op.rs#L209)）
   - 扩展为：`{ id, pages, pba_decrefs: Vec<Pba> }`
   - 编码追加 `count 4B + pba × 8B × N`，放在 pages 之后。
   - 按 SPEC §7 不做兼容，schema bump 已在 S1 完成，直接覆盖。

2. **Plan 阶段扩展**
   - 现有 drop_snapshot plan 持 `drop_gate.write() + apply_gate.write()`；额外调
     [`diff_with_current(snap)`](../src/db.rs#L1527)
   - 筛 "snap 有值但 current 已不同" 的项，把 snap 侧 pba 装进 `pba_decrefs`
   - 不去重：同 pba 多次出现就多次 decref（refcount 可加，结果一致；去重反而破坏
     SPEC §4.7 的"按 pba 聚合不能合并"语义）。

3. **Apply 扩展**
   - 现有 pages 释放逻辑不动
   - 新增：对 `pba_decrefs` 里每个 pba 发 `decref(pba, 1)`，到零加入 `freed_pbas`
   - **不需要 leaf-rc-suppress**：drop_snapshot 正是在释放这些 leaf，不会有 leaf
     shared 的情况（drop_gate.write 已排他）
   - `ApplyOutcome::DropSnapshot` 扩加 `freed_pbas: Vec<Pba>`

4. **`Db::cleanup_dedup_for_dead_pbas(pbas: &[Pba]) -> Result<Lsn>`**
   - 文件：[`src/db.rs`](../src/db.rs)
   - **参考原型**：[`onyx/src/meta/store/dedup.rs::cleanup_dedup_for_pbas_batch`](../../src/meta/store/dedup.rs#L410)
     （几乎 1:1 对应）。onyx 版本用 `WriteBatch` 做 RocksDB 原子提交，metadb 版
     换成单个 `Transaction` 打包所有 ops；竞态保护（hash 被重新注册不误删）逻
     辑照抄。
   - 内部步骤：
     1. 调 `multi_scan_dedup_reverse_for_pba(pbas)` 拿 `(pba, hash)` 列表（SPEC §2.2）
     2. 对每个 `hash` 做 `get_dedup(hash)`；若 `entry.pba == 目标 pba` 发 `DedupDelete { hash }`
     3. 无条件对每个 `(pba, hash)` 发 `DedupReverseDelete { pba, hash }`
     4. 全部塞进一个 `Transaction`，单次 `commit()`
   - **不新增 WalOp**：复用已有 `WalOp::DedupDelete` / `WalOp::DedupReverseDelete`
     （它们本身就是 tombstone，replay 幂等）
   - 文档：注明这是 "onyx writer / dedup hit / drop_snapshot cleanup 的统一入口，
     替代 onyx 侧拼 WriteBatch"

5. **单元测试**（覆盖 §4.4 / §4.5）
   - leaf-rc-suppress 对称性：`take_snapshot → 写 N 次 → drop_snapshot` 前后 pba
     refcount 等同于 "不做 snapshot 直接写 N 次"
   - drop_snapshot 产生 freed_pbas，调 cleanup_dedup_for_dead_pbas 后 dedup_index
     里对应 hash 被删
   - 竞态保护：cleanup 前 hash 已被另一 pba 重新注册（模拟 write path 在 drop
     和 cleanup 之间 dedup hit 到同一 hash），cleanup 不误删新 entry
   - 多次 cleanup 同一批 pba（replay 场景）→ idempotent，结果一致
   - drop_snapshot 同时释放 pages 和发 pba_decrefs，两者原子（同一次 WAL 提交）

6. **Fault injection**
   - `CommitPostWalBeforeApply` × DropSnapshot（带 pba_decrefs）：replay 后 pages 释放
     + refcount 减扣都完整
   - `CommitPostApplyBeforeLsnBump` × 同
   - SPEC §5.3 的 "DropSnapshot 的 pages 释放中途 crash" —— 这条走现有
     `page.generation >= lsn` 已有保护，只需补一条显式测试确认

### 退出

- `cargo test` 全绿
- 覆盖率 ≥ 90%

### 规模预估

~600 行改动，2 天。

---

## S5 — 综合 proptest + soak workload 扩展

### 入口

S1~S4 全部落地，所有新 WalOp / API 可用。

### 交付物

1. **Reference model**（内存版）
   - 新文件：`tests/reference_model.rs` 或放 `src/testing/` 下（仅 test cfg）
   - 数据结构：
     - `l2p: HashMap<(VolumeOrdinal, Lba), L2pValue>`
     - `refcount: HashMap<Pba, u32>`
     - `dedup: HashMap<Hash32, (Pba, u32 /* live */)>`
     - `snapshots: HashMap<SnapshotId, FrozenSnapshot>`（frozen L2P 快照）
   - 操作：按 SPEC 语义模拟 L2pRemap / L2pRangeDelete / take_snapshot /
     drop_snapshot / clone_volume / cleanup_dedup_for_dead_pbas
   - 对 metadb 的 leaf-rc-suppress 规则在 reference 里用 `snapshots` 的覆盖关系
     模拟。

2. **混合 proptest**（SPEC §5.2）
   - 文件：`tests/onyx_integration_proptest.rs`
   - 生成器：随机 op 序列，长度 ≥ 10k，默认 256 seed（可环境变量调）
   - op 类型权重近似 SPEC §5.4 的 soak op mix
   - 每轮收尾对账：
     - `db.iter_refcounts()` ≡ `reference.refcount`
     - 每个 live volume 的 `range(..)` ≡ `reference.l2p` 对应分片
     - 每个 snapshot 的 `snapshot_view(id).range(..)` ≡ frozen 状态
   - 命中任何偏差：proptest shrink 后打印最小失败序列

3. **`metadb-soak` workload 扩展**
   - 文件：[`src/bin/metadb-soak.rs`](../src/bin/metadb-soak.rs)
   - 在现有 workload 基础上按 SPEC §5.4 的 op mix 加入新 op：
     - remap 60%（含 guard=Some 20% / guard=None 40%）
     - range_delete 5%
     - snapshot（take + drop）1%
     - dedup hit via L2pRemap guard 30%
     - cleanup_dedup_for_dead_pbas 4%
   - 保留既有的 `metadb-verify` 周期性调用（无 warn/err）
   - 新增一个 "refcount 总和守恒" 检查点：每隔 N op 采样 `iter_refcounts` 总和，
     对比自维护的 `incref_total - decref_total`（writer 线程记录）
   - 进程重启支持：每运行 M op 退出并 re-open，断言恢复后 verify + reference
     model 仍匹配。要求 ≥ 10 次重启。

### 测试

- 新增 proptest 一条（本身就是测试）
- 现有 metadb-soak 回归：现有 op 占比保持不变的子 workload 仍跑几分钟过

### 退出

- `cargo test onyx_integration_proptest -- --ignored --nocapture` 跑 256 seeds
  全绿（本地至少 10 分钟的烧机）
- `cargo run --release --bin metadb-soak -- --minutes 30`（30 分钟冒烟）无
  verify 报错
- 完整 24h 跑交给 S6 签收

### 规模预估

~1200 行改动（reference model + proptest + soak workload），3-4 天。

---

## S6 — 性能基准 + Phase A → B 门控签收

### 入口

S1~S5 全部落地。

### 交付物

1. **性能基准**（对齐 SPEC §8）
   - 文件：[`src/bin/metadb-bench.rs`](../src/bin/metadb-bench.rs)
   - 新增 bench case：
     - `bench_l2p_remap_single`：单 op 提交吞吐，guard=None
     - `bench_l2p_remap_single_guarded`：同上，guard=Some
     - `bench_l2p_remap_batch_1k`：每 tx 1000 条 L2pRemap
     - baseline：现有 `insert + incref + decref` 三 op 串行、`insert` batch
   - 输出格式统一（表格），方便和 `memory: metadb_bench_wins` 对比
   - **验收**：
     - 单 remap ≥ baseline(insert+incref+decref 串行) × 90%
     - batch remap ≥ baseline(纯 insert batch)
     - guard 开销 < 5%
   - 不达标不得进 Phase B。设计问题就调 apply 路径（锁粒度、shard 路由、
     allocation），不达标**不是** tuning 问题。

2. **24h soak 签收**
   - 跑 `metadb-soak` 实际 24h，命令：
     `cargo run --release --bin metadb-soak -- --hours 24 --restart-interval 2h`
   - 归档 `.dev/metadb-soak/<ts>/` 目录（summary.json、restart 日志、verify 输
     出）
   - 期间 ≥ 10 次进程重启全部 verify 无报错
   - refcount 守恒检查全程无偏差

3. **文档更新**
   - [`metadb/CLAUDE.md`](../CLAUDE.md) 的「和 onyx-storage 的关系」段更新：
     - 列出 L2pRemap / L2pRangeDelete / DropSnapshot 扩展 / cleanup_dedup_for_dead_pbas
       公共 API
     - 标注 `commit_with_outcomes` 已是主入口、insert/delete/incref/decref 仅作诊断
       保留
   - `README.md` 的 "Public API at a glance" 加条
   - [`ONYX_INTEGRATION_SPEC.md`](ONYX_INTEGRATION_SPEC.md) §10 追加 "v1 Phase A
     实施完成" 条目 + 签收日期

4. **Phase A 退出清单**（逐条勾 SPEC §9）
   - [ ] §2 API landed
   - [ ] §3 WalOp 全部 landed
   - [ ] §4 不变量有测试对应
   - [ ] §5.1/5.2/5.3 全绿
   - [ ] §5.4 24h soak 过
   - [ ] §8 性能目标达成
   - [ ] CLAUDE.md 更新
   - [ ] SPEC v1 标 "Phase A done"

### 退出

Phase B（onyx 侧切换）开工允许。

### 规模预估

~400 行改动 + 24h 墙钟时间 + 半天文档整理。总计 2-3 个工作日（含 soak 墙钟）。

---

## 开放问题（需要在 session 中 inline 决策并记录）

以下问题 SPEC 没有硬性答案，需要在对应 session 的 commit message / PR
描述里写明最终决策：

1. **`leaf_was_shared` 的外抛方式**（S2）
   - 建议：新增 `insert_at_lsn_with_share_info` 方法返回结构体；保留旧 API 作
     wrapper。不要改现有 `insert_at_lsn` 签名（会波及所有 call site）。

2. **跨 shard 锁序**（S2/S3/S4）
   - guard `gp`、`old_pba`、`new_pba` 三者可能跨不同 refcount shard。必须按 shard
     index 升序取锁。已有锁序约束在 `metadb/CLAUDE.md` 明确。

3. **captured.len() 拆分阈值**（S3）
   - SPEC 给了 65536 上限。具体实现是 `Db::range_delete` 内部 while-loop 拆分，
     adapter 不感知，或暴露给 adapter 让它自己拆？建议内部拆分，对 onyx 侧单
     次语义。

4. **`DropSnapshot` 扩展字段的迁移语义**（S4）
   - 旧版本 WAL 里的 DropSnapshot 记录在 schema bump 后拒绝读（和 S1 对齐），
     无迁移路径。确认此策略和 onyx 侧 "RocksDB 一次性进历史" 同步。

5. **proptest seed 数量的环境变量**（S5）
   - 本地默认 64 seeds 快速迭代，CI 跑 256，release 前本地再跑 ≥ 1024 过夜。不要
     硬编码 256 导致 dev loop 难忍。

6. **性能基准 baseline 的采集时机**（S6）
   - baseline 数据来自 `metadb_bench_wins` 的历史值，还是 S6 现场再跑一次旧代码？
     建议现场再跑一次（用 pre-S2 的 commit），避免环境漂移。

---

## 变更记录

- **v1** 2026-04-23：初稿。对应 SPEC v1。6 session 拆分：S1 基础 / S2 L2pRemap /
  S3 L2pRangeDelete / S4 DropSnapshot+cleanup / S5 proptest+soak / S6 bench+签收。
- **v1.1** 2026-04-23：加「参考实现」章节，把 onyx-storage 已稳定运行的 RocksDB
  refcount 路径作为参考原型列出（`atomic_batch_write_packed` / `self_decrement` /
  `cleanup_dedup_for_pbas_batch` 等）；S2 / S4 inline 回指。实施时优先对齐 onyx
  既有语义，不再重走一遍事故复盘。
