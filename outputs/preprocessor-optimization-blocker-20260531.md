# PREPROCESSOR / Extraction 阶段性阻塞总结（2026-05-31）

## 当前阶段结论

这条线已经拿到以下有效进展：

- `q23` 已修正
- `q27` 的 extraction 侧不再是主嫌疑
- benchmark settle 机制已经补到：
  - single probe
  - question-aware probe
  - 429-aware reporting
  - passive wait on rate limit
  - local probe snapshot diagnostics

当前剩余未闭合点，已经高度集中为：

- `q27` 的 **首跑验证**
- 且该验证不是被 extraction 逻辑挡住
- 而是被远端 `/api/v1/search/find` 的 **rate limit / quota** 挡住

## 已验证的硬证据

### 1. `q27` 不是 conference memory 没生成

在同一 account 上：

- 首跑 `q27`:
  - 回答为 `There is no relevant information...`
- 稍晚单独重跑：
  - `q27 = CORRECT`
  - 恢复为 `Caroline attended the LGBTQ conference on 2023-07-10`

已复现于至少两组 account：

- `acct-20260531-s67split48`
- `acct-20260530-accept47`

### 2. `q27` 的首跑假阴性是分钟级窗口，不像秒级 settle 足以覆盖

从远端已有 artifact mtime 反推：

- `s67split48`
  - 首跑到晚跑：约 `3 分 12 秒`
- `accept47`
  - 首跑到晚跑：约 `17 分 43 秒`

因此当前不能再假设：

- `15s`
- `30s`

这类秒级等待足以稳定修复 `q27` 首跑。

### 3. 当前远端 search API 本身仍受限

在不消耗 provider completion 的 search-only settle 复验中：

- even `max_probes = 1`
- `/api/v1/search/find`

仍会直接返回：

- `429 Too Many Requests`

所以当前缺的不是 benchmark 脚本能力，而是：

- **search 配额恢复后的有效远端复验窗口**

## 当前代码状态

已落地的 benchmark 收敛：

- `benchmark/locomo/openclaw/phase_a_off.py`
  - question-aware `collect_memory_visibility_probes_for_questions()`
  - `wait_for_search_visibility()` 在 `429` 下改为被动等待
  - `post_ingest_settle.local_probe_snapshot`

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`
  - 当前本地 `10 passed`

本地验证：

```bash
python3 -m pytest tests/benchmark/locomo/openclaw/test_phase_a_off.py -q -s --capture=no
python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py tests/benchmark/locomo/openclaw/test_phase_a_off.py
```

## 为什么现在应判定为阶段性阻塞

按最近连续多轮的实测，当前同一个外部阻塞反复出现：

- provider weekly quota / failover quota
- search API `429 Too Many Requests`

而这已经直接阻止了最关键的验证动作：

- 用最新 settle 机制跑一轮 **有效的 `q27` 首跑 QA**

在 search 配额未恢复前，继续推进只会重复：

- 补更多脚本细节
- 但拿不到新的有效远端首跑结论

这已经属于“同一外部状态阻塞重复出现且无法继续获得高价值验证”的情形。

## 下一步恢复条件

满足任一条件即可继续：

1. 远端 `/api/v1/search/find` 配额恢复，不再单 probe 即 429
2. provider completion quota 恢复，可安全跑最小 `q27` 首跑复验

恢复后建议的第一步：

1. 用当前最新 `phase_a_off.py`
2. 仅跑最小 `q27` 首跑验证
3. 读取：
   - `post_ingest_settle`
   - `local_probe_snapshot`
   - `ready_queries`
   - `search_attempts`
4. 判断：
   - 是 search 可见性恢复后首跑回正
   - 还是仍需把 settle 推荐量级上调到分钟级
