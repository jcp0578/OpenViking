# Benchmark settle 改动隔离说明（2026-06-01）

## 目的

当前更大 clean 子集复验显示：

- `q27` settle 修正本身有效
- 但 current version 整体版本在 `sessions 3-9 / q16-39` 上从历史 `17/21` 掉到 `14/21`

因此下一步不应继续带着整版 runtime / extraction 改动扩面，而应：

- 以历史 `17/21` clean baseline 为主
- 只保留 benchmark 脚本层的 `q27 settle` 改动
- 再复跑大子集，看是否能“保住 17/21 + 提升 q27 首跑稳定性”

## 应隔离保留的文件

只保留这两个文件的改动：

1. [benchmark/locomo/openclaw/phase_a_off.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py)
2. [tests/benchmark/locomo/openclaw/test_phase_a_off.py](/home/jcp/Agent/code/OpenViking/tests/benchmark/locomo/openclaw/test_phase_a_off.py)

## 不应混入的文件

这些属于 runtime / extraction / prompt / ingest 侧，不应在“benchmark-only 隔离复验”里一起带入：

- [benchmark/locomo/openclaw/import_to_ov.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py)
- [openviking/prompts/templates/compression/memory_extraction.yaml](/home/jcp/Agent/code/OpenViking/openviking/prompts/templates/compression/memory_extraction.yaml)
- [openviking/session/compressor.py](/home/jcp/Agent/code/OpenViking/openviking/session/compressor.py)
- [openviking/session/compressor_v2.py](/home/jcp/Agent/code/OpenViking/openviking/session/compressor_v2.py)
- [openviking/session/memory/session_extract_context_provider.py](/home/jcp/Agent/code/OpenViking/openviking/session/memory/session_extract_context_provider.py)
- [openviking/session/session.py](/home/jcp/Agent/code/OpenViking/openviking/session/session.py)

## benchmark-only 改动范围

`phase_a_off.py` 中应保留的逻辑只有这些：

1. `--post-ingest-settle-seconds`
   - ingest 完成后、QA 前进入 settle 阶段

2. search visibility settle
   - `wait_for_search_visibility()`
   - 用 `/api/v1/search/find` 确认目标 memory 已可见

3. question-aware probe 选择
   - `collect_memory_visibility_probes_for_questions()`
   - probe 不再盲选 recent event，而是优先对齐 pending QA question

4. agent-scoped namespace 支持
   - `resolve_memories_root()`
   - `build_probe_target_uri()`
   - 支持：
     - `user/<user>/agent/<agent>/memories`
     - `viking://user/<user>/agent/<agent>/memories`

5. rate limit 处理
   - `429` 下不再持续主动轮询
   - 返回：
     - `reason = rate_limited_timeout`
     - `search_attempts`
     - `passive_wait_after_rate_limit`

6. 本地 probe 快照
   - `collect_local_probe_snapshot()`
   - 输出：
     - `exists`
     - `size_bytes`
     - `mtime`

## 已验证结论

基于当前版本的 clean fresh-account 实测：

### 最小 `q27` 用例

- account:
  - `acct-20260601-q27settle55`
- 结果：
  - `q27 = CORRECT`
- `post_ingest_settle`：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["lgbtq conference"]`

### 小样本 `q23-27`

- account:
  - `acct-20260601-s67settle56`
- 结果：
  - `q23 = CORRECT`
  - `q24 = CORRECT`
  - `q25 = WRONG`
  - `q27 = CORRECT`
- `post_ingest_settle`：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["lgbtq conference attendance"]`

### 更大 clean 子集

- account:
  - `acct-20260601-q1639settle57`
- 口径：
  - `sessions 3-9 / q16-39`
- 结果：
  - `14/21`
- `post_ingest_settle`：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["pride parade"]`

这说明：

- benchmark settle 修正本身已经工作
- 但 current runtime/extraction 组合整体不适合作为新主候选

## 后续操作建议

下一步应做：

1. 切回历史 `17/21` clean baseline 对应版本
2. 只带入：
   - `phase_a_off.py`
   - `test_phase_a_off.py`
   这组 benchmark-only settle 改动
3. 重新跑：
   - `sample0 / sessions 3-9 / q16-39`
4. 目标验证：
   - 是否能保住 `17/21`
   - 同时让 `q27` 的首跑验证不再依赖 `fallback_sleep + probe_count=0`

## 当前判断

如果后续要继续追主目标，这个“benchmark-only 隔离复验”应是下一步最高优先级。

## 2026-06-01 隔离复验结果更新

已在 baseline 仓库 `/home/jcp/agent/code/OpenViking-benchonly` 上做最小 clean smoke：

- `sample0 / sessions 6-7 / q27`

### `q27benchonly61`

- `phase_a_off.py` 的 settle 已真实命中 conference memory：
  - `ok = true`
  - `ready_queries = ["lgbtq conference attendance"]`
- 但旧 baseline 的 `judge.py` 默认模型仍是：
  - `doubao-seed-2-0-pro-260215`
- 远端账号未开通该模型，因此 judge 误失败
- raw response 实际是正确的 `July 10, 2023`

### `q27benchonly62`

同步当前 benchmark `judge.py` 后，再跑同样 smoke：

- judge 恢复可用
- 但 raw response 变成：
  - `There is no relevant information ...`
- `post_ingest_settle` 虽仍 `ok = true`
- 但实际命中的 probe 变成：
  - `["beach camping"]`

进一步检查 account memory 发现：

- agent-scoped `events` 下只有：
  - `beach_camping.md`
  - `museum_visit.md`
  - `reunion_chat.md`
  - `support_picnic.md`
- **没有** conference event memory

### 更新后的判断

因此，当前“baseline + benchmark-only settle”的结论已经变成：

- benchmark-only settle 改动本身是工作的
- 但它**不能单独**把 baseline 仓库拉回 `q27` 正确
- 因为该 baseline runtime/extraction 组合本来就没有生成 `conference` 事件记忆

这意味着：

- 若目标是“保住 `17/21` 同时让 `q27` 首跑更可靠”
- 不能只带入 `phase_a_off.py`
- 至少还需要把 `q27` 对应的 conference event generation 修正也一起带回
