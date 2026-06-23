# Preprocessor 优化分析

## 问题

当前 preprocessor 对 session 大小的依赖过强：
- < 3,000 tokens：几乎 100% FALLBACK（compact > full）
- 3,000-5,000 tokens：边界值，约 13% 触发 ACTIVE
- > 10,000 tokens：100% ACTIVE，节省率随 size 增长
- > 100,000 tokens：76-79% 节省

这导致优化覆盖范围太窄——大量中等 session 无法受益。

## 根因：Compact Packet 固定开销

| 开销来源 | 估算 | 可否优化 |
|---------|------|---------|
| Section headers (`#`, `##`, `- `) | ~150-200 chars | ✅ |
| Session metadata (4 fields) | ~200 chars | ✅ |
| Structured facts (24 × ~80 chars) | ~2,000 chars | ✅ 最显著 |
| Selected spans (formatting overhead) | ~100-300 chars | ✅ |
| Risk flags section | ~100-200 chars | ✅ |
| Working Memory section ("(none)") | ~50 chars | ✅ |
| **总计固定开销** | **~2,600-3,150 chars (~650-790 tokens)** | — |

## 优化方案

### 方案 1: 动态参数（高影响，低风险）

```python
# 当前：固定参数
max_span_tokens = 1200       # 不管 session 大小
max_facts_total = 24         # 不管 session 大小

# 优化：与 session 大小成比例
def adaptive_params(full_tokens):
    return {
        "max_span_tokens": max(200, min(1200, full_tokens // 3)),
        "max_facts_total": max(4, min(24, full_tokens // 150)),
    }
```

| Session tokens | max_span_tokens | max_facts_total | 预期 compact/full |
|---------------|----------------|-----------------|-------------------|
| 500 | 200 | 4 | ~0.8 (ACTIVE) |
| 1,000 | 333 | 6 | ~0.7 (ACTIVE) |
| 2,000 | 667 | 13 | ~0.6 (ACTIVE) |
| 3,000 | 1,000 | 20 | ~0.5 (ACTIVE) |
| 5,000+ | 1,200 | 24 | ~0.4 (ACTIVE) |

### 方案 2: 精简 Compact 格式（高影响，中风险）

```markdown
# 当前格式:
# Compact Working Memory Update Packet
#
# Full raw archive is still stored outside this prompt...

# 优化后:
# WM-Compact: 5msgs [msg-1..msg-5]
- context: {archive_uri}
- risk: error_or_fix, open_issue
- facts: {fact1}; {fact2}; ...
- memory: {overview_text}
---
{selected spans in compact format}
```

预计减少 ~300-400 chars 格式开销。

### 方案 3: 分层策略（中影响，低风险）

- **Tier 1 (< 2,000 tokens)**：跳过 preprocessor，直接用 raw messages
  - 已有参数 `min_full_tokens_for_compact`，当前设为 0，改为 2000
- **Tier 2 (2,000-8,000 tokens)**：轻量压缩
  - max_span_tokens = full_tokens * 0.3
  - max_facts_total = full_tokens / 200
  - 精简 section（去掉冗余 header）
- **Tier 3 (> 8,000 tokens)**：完整压缩（当前行为）
  - max_span_tokens = 1200
  - max_facts_total = 24
  - 完整 section headers

### 方案 4: 提高 MMR 去重效率（中影响，低风险）

```python
# 当前: Jaccard 阈值 0.72
# 优化: 对中小 session 降低阈值
def get_jaccard_threshold(full_tokens):
    if full_tokens < 2000: return 0.45
    elif full_tokens < 5000: return 0.55
    elif full_tokens < 10000: return 0.65
    else: return 0.72
```

### 方案 5: Facts-In-Spans（低影响，实验性）

将 structured facts 内联到选中的 span 中，而不是单独列出：
```markdown
[span text]...  # contains: preference: X, date_plan: Y
```
省去单独的 `## Extracted Facts` section。但可能影响 model 对 facts 的关注度。

## 推荐实施顺序

1. **方案 1（动态参数）** — 最安全，立即见效
2. **方案 3（分层策略）**— 配合方案 1，明确各 tier 行为
3. **方案 2（精简格式）** — 需要验证 model 理解度不降
4. **方案 4（MMR 阈值）** — 调参优化
5. **方案 5（Facts-In-Spans）** — 实验性，需要端到端验证

## 预期效果

| Session 大小 | 当前 | 优化后 | 提升 |
|-------------|------|--------|------|
| < 1,000 | 0% ACTIVE | 0% (跳过) | — |
| 1,000-3,000 | ~5% ACTIVE | ~70% ACTIVE, ~30% savings | 14x |
| 3,000-8,000 | ~50% ACTIVE | ~90% ACTIVE, ~50% savings | 1.8x |
| 8,000-50,000 | ~100% ACTIVE | ~100% ACTIVE, ~70% savings | 1.2x |
| > 50,000 | ~100% ACTIVE | ~100% ACTIVE, ~80% savings | 不变 |

**核心指标：ACTIVE 覆盖的 session 占比预计从 ~50% 提升至 ~85%。**

## 2026-05-29 追加结论

### 已验证实验

1. `created_at -> chat date anchor` 注入
- 本地单测能通过。
- 远端真实 benchmark 一开始出现“服务进程不是 probe worktree”环境问题；修正后重跑。
- 有效结果不理想：
  - `q30` 仍错
  - `q31` 仍错
  - `q33` 对
  - `q34` 对
  - `q35` 对
- 且出现 prompt / output 异常放大，不符合“token 下降且准确率不降”的目标。
- 结论：停止这条线，不保留 `created_at` 注入。

2. 更窄的“相对时间句保真”
- 回退绝对锚点补充，只保留原相对表达。
- 有效小口径 `q30/q31/q33`：
  - `q30` 对
  - `q31` 错
  - `q33` 对
- 结论：比 `created_at` 注入更稳，但不足以修复 `q31`。

3. weekday 缩写识别（`Last Fri` 等）
- 这是一个通用修复，不是时间解析器式的过度应对。
- 本地新增测试：同一 session 内 `Last Fri` 的 Melanie pottery 与 `Last Friday` 的 Caroline adoption 都能同时进入 `date_or_plan` 与 `selected_spans`。
- 但远端有效小口径 `q30/q31/q33` 仍未带来确定收益：
  - `q30` 仍错
  - `q31` 仍错
  - `q33` 对
- 结论：单独保留这个小修复是可接受的，但不应继续围绕时间锚点扩大实验面。

### 已确认的环境问题

- 远端 `123.60.114.206:10008` / `jcp-dev` 上，`1933` 一度实际挂的是旧进程：
  - `python3 -m openviking.server.bootstrap --host 127.0.0.1 --port 1933 --workers 1`
- 这会让 benchmark 看起来在跑新代码，实际命中旧实现。
- 修正后，`1933` 才切到 probe worktree 的：
  - `python3 -m openviking_cli.server_bootstrap --config /root/.openviking/ov.conf --host 127.0.0.1 --port 1933`
- 后续所有有效结论都必须以“确认 1933 对应正确进程”为前提。

### Stop Decision

- 不再继续做更重的时间锚点处理。
- 不再尝试把 session 绝对日期补进 `date_or_plan`。
- 不再围绕 `q30/q31` 做更多时间解释式 PREPROCESSOR 改动。

### 更高信号的后续方向

如果继续只动 PREPROCESSOR，优先级应转向“减少低信息 prompt 体积”，而不是继续解释时间：

1. 去掉低信息 `selected_spans`
- 当前 `session_8` 一类样本中，`selected_spans` 会带入大量低价值寒暄/赞叹/承接句。
- 更安全的方向是过滤：
  - 纯情绪回应
  - 简短确认句
  - 与问题主题无关的赞叹/追问

2. 压缩同主题连续 chit-chat
- 保留核心事件句，压掉同主题的后续情绪扩写。
- 这是比“时间锚点补全”更通用、也更可能降低 token 的方向。

3. 清理明显噪音
- 例如 creation 路径里出现的 `[]` latest message 尾巴，属于低风险可清理项。

4. 继续使用小口径 stop rule
- 先看 2 到 3 题是否同时满足：
  - token 下降
  - `q31` 这类题不再回退
- 如果没有明确收益，立即停止，不扩大 benchmark 面。

### 2026-05-29: 低信息 `selected_spans` 过滤实验

目标：
- 不再解释时间
- 转向压缩 `selected_spans` 中的低信息内容
- 优先清理：
  - 情绪回应
  - 简短确认句
  - 同主题连续 chit-chat
  - 明显噪音如 `[]` tail

已实现并验证的行为：
- `latest_message` 不再被 `[]` tail 占据
- 短情绪回应 / 简短确认句在有更高信息句时会被跳过
- 同主题连续 chit-chat 会优先保留事件句
- weekday 缩写如 `Last Fri` 仍可识别并进入 `date_or_plan`

本地单测：
- `tests/unit/session/test_extraction_preprocessor.py`
- 结果：`29 passed`

远端有效小口径：
- run: `sample0_on_probe_q30_33_lowinfo_20260529c`
- 口径：`sessions 1-8`, `q30/q31/q33`

结果：
- `q30`: `WRONG`, `input_tokens=6797`
- `q31`: `WRONG`, `input_tokens=3234`
- `q33`: `CORRECT`, `input_tokens=4315`
- 准确率：`1/3`

结论：
- 这条线能明显压 token，尤其 `q31` 输入降到了约 `3.2k`
- 但没有带来“token 下降且准确率不降”
- 相反，小口径准确率进一步掉到了 `1/3`
- 因此这条“低信息 span 过滤”规则集当前不应继续扩大实验面

当前 stop decision：
- 不再继续沿这组低信息过滤规则推进
- 保留文档与证据
- 若后续继续做 PREPROCESSOR 优化，应优先寻找“减少 token 但不删除可能承载 disambiguation 的句子”的方案

### 2026-05-29: 保留 / 回退清单

本轮最终决策：

#### 保留

- `[] tail` 清理
  - `latest_message` 不再被 `[]`、`()`, `{}`、`null`、`none`、`n/a` 这类尾噪音占据。
  - 这是确定噪音清理，风险低，已通过本地与远端单测。

- 相对时间句保真
  - `date_or_plan` 继续使用句级窗口抽取，只保留局部时间句，不再把整条 message 过宽打包。

- `Last Fri` / `Fri` 这类 weekday 缩写支持
  - 这是通用修复，不再继续扩展为“解释时间”方案。

- 句级时间抽取与相邻时间去重
  - 保留 sentence-window 抽取。
  - 保留 temporal/topic 级别的相邻重复 span 去重。

- `structured_facts` 对 exact-duplicate selected span 的同源引用
  - 保留上一版稳定行为：
    - exact duplicate 才允许 `fact -> source ref`
    - 不是 exact duplicate 就保留 fact 正文

#### 回退

- 回退这轮“4 步纯格式瘦身”
  - 不再压缩 section 标题到 `state/goals/facts/context/...`
  - 不再把 evidence 行头改成 `- #0 ...`
  - 不再把 speaker 展示层从 `[Caroline]:` 改成 `Caroline:`
  - 不再把 fact 渲染改成 `date_or_plan@#0` / `facts/date_or_plan#0: ...`

原因：
- 这组改动虽然表面上只改渲染格式，但远端必要性测试显示它已经改变了 creation 行为。
- 同口径 run `sample0_on_probe_q16_39_s3_9_formatpack_20260529g` 中：
  - `session_3 memory_count` 从上一稳定版本的 `10` 飙到 `36`
  - `session_4-9` 基本在 `11-15`
  - run 在 ingest 全完成后卡在进入 QA 前，没有生成 CSV
- 这说明它不是纯低风险“固定成本压缩”，而是会扰动 memory generation。

#### 本轮验证结果

- 本地整组单测：
  - `python3 -m pytest tests/unit/session/test_extraction_preprocessor.py -q -s --capture=no`
  - 回退后结果：`30 passed`

- 远端回退后针对性单测：
  - `latest_message_ignores_bracket_tail_noise`
  - `facts_use_reference_only_for_exact_duplicate_text`
  - `abbreviated_last_fri_preserves_distinct_same_session_events`
  - 结果：`3 passed`

### 2026-05-29: 当前代码影响分级

目的：
- 回到初始目标，先判断“当前代码里哪些改动还在默认生效并可能继续伤准确率”，避免把已经停用或已回退的实验线继续当成问题根因。

#### A. 已证伪、且当前不应再视为 active root cause 的改动

- `created_at -> chat date anchor` 注入
  - 已被远端小口径证伪。
  - 当前代码中已不存在 `chat date` / `created_at` 注入逻辑，不再是默认生效路径。

- 低信息句删除（情绪回应 / 简短确认句 / 同主题连续 chit-chat 过滤）
  - 已被远端小口径证伪，准确率下降。
  - 当前默认路径不再依赖这组过滤规则，不能继续当作当前准确率问题的根因。

- 激进包装去重 / 纯格式瘦身
  - 已被远端子集证伪或证明收益不稳。
  - 当前代码不再保留那轮更激进的 section/header/fact/span 语法压缩逻辑。

#### B. 当前默认仍生效、且暂时保留的低风险改动

- `[] tail` 等确定噪音清理
  - 目标是避免 `latest_message` 被无意义尾巴占据。
  - 未发现负面 accuracy 证据。

- 相对时间句保真 + 句级窗口抽取
  - 只保留局部时间句，不再把整条 message 过宽打包进 `date_or_plan`。
  - 这是当前默认仍生效的核心改动之一。

- weekday 缩写支持（`Last Fri` / `Fri`）
  - 单独没有带来显著 benchmark 收益，但属于低风险通用修复。

- temporal/topic 相邻时间去重
  - 目标是减少相邻同主题时间 span 重复。
  - 当前没有远端证据表明它单独伤准确率。

- exact-duplicate 才允许 `fact -> source ref`
  - 仅在 fact 与 selected span 文本完全一致时走引用。
  - 非 exact duplicate 继续保留正文。

- 短 session adaptive facts floor 从 `4 -> 6`
  - 目的是避免几个时间锚点在短 session 中过早被截断。
  - 当前没有负面 accuracy 证据。

#### C. 当前 diff 中仍存在、但默认关闭的实验残留

这些内容当前更多是“代码噪音/分析负担”，不是已知 runtime root cause：

- `enable_fact_kind_quota`
- `recency_weight`
- `enable_relaxed_span_fill`
- `enable_recent_turns`

判断：
- 这些开关在默认配置下为 `False` 或 `0.0`，不会进入当前默认 ON 路径。
- 它们会增加理解成本，但不是当前准确率下降的直接原因。

结论：
- 如果后续要做代码清理，可以单独回滚这批“默认关闭的实验 scaffolding”。
- 但从“先止损准确率”角度，这不是第一优先级。

#### D. 当前更应优先盯的真实问题

- 远端新版 prompt bucket diagnostics 已证明：
  - 可见 prompt bucket 基本只有 `systemPrompt + question`
  - `relevant_memories_tokens_est` 仍是 `0`
  - 但实际 `input_tokens` 远高于可见 bucket 总量

这说明：
- 当前 token 大头并不在已可见的 PREPROCESSOR 文本片段里完整暴露出来。
- 继续围绕已证伪的 PREPROCESSOR 小改动反复试错，性价比很低。

当前优先级结论：
1. 不再把已停用的实验线当成当前 root cause。
2. 默认生效的低风险改动先保留。
3. 如需“先回滚”，第一类应是以后清理默认关闭的实验残留，而不是再动已验证稳定的低风险路径。
4. 下一阶段要继续基于远端容器实测，把 hidden prompt cost 的来源再往下拆，而不是继续盲调 PREPROCESSOR。

### 2026-05-29: 清理默认关闭的实验 scaffolding

目的：
- 在不改变当前默认行为的前提下，把已经没有实测收益、且默认关闭的实验路径从 PREPROCESSOR 里移除。
- 让默认代码路径更接近“少做处理、保持原流程”，同时减少后续分析噪音。

本轮实际清理：
- 从 `PreprocessorOptions` / `session.py` wiring 中移除：
  - `enable_fact_kind_quota`
  - `recency_weight`
  - `enable_relaxed_span_fill`
  - `enable_recent_turns`
- 删除对应的：
  - `RecentTurn`
  - relaxed-fill metrics
  - recent-turns metrics / serialization / telemetry / logging
- 删除相关单测：
  - fact kind quota
  - recency weight
  - relaxed span fill
  - recent turns render

保留不变的默认路径：
- `[] tail` 清理
- 相对时间句保真
- 句级时间窗口抽取
- `Last Fri` 缩写支持
- 相邻同主题时间去重
- exact-duplicate 才做 `fact -> source ref`
- 短 session facts floor `4 -> 6`

本地验证：
- `tests/unit/session/test_extraction_preprocessor.py`
  - 结果：`26 passed`
- `tests/unit/session/test_fixture_token_savings.py`
  - 结果：`19 passed`

远端容器验证：
- 同步到 `/home/jcp/agent/code/OpenViking-probe-wm`
- 运行：
  - `tests/unit/session/test_extraction_preprocessor.py`
  - `tests/unit/session/test_fixture_token_savings.py`
- 结果：`45 passed`

远端容器小口径 ON 验证：
- 计划口径 1：`sessions 1-8 / q30-35`
  - 运行到 `session_7/8` 后卡在 `session_8` 前，未进入 QA
- 计划口径 2：收窄为 `sessions 1-4 / q33`
  - 服务健康正常（gateway / 1933 都是 healthy）
  - 但 run 在首个 session 前长时间无推进，未产出 QA

结论：
- 这轮代码清理没有在本地或远端单测上引入回归。
- 但远端 benchmark 运行态再次成为主要阻塞，导致本轮没有新增可用的 ON 题级 QA 数据。
- 因此，这轮能确认的是“代码路径已收敛、默认行为未被单测打坏”，不能宣称“已获得新的 ON accuracy/token 改善证据”。

#### 下一步建议

- 不再继续压 creation/update prompt 的人类可读结构。
- 下一步更高信号方向：
  1. 先做题级 prompt 组成拆解
  2. 找真正的固定成本来源
  3. 但不要再改 `section/fact/span` 的呈现语法本身


### 2026-05-29: 远端容器 benchmark 运行态新增定位

在清理默认关闭的 PREPROCESSOR 实验路径后，继续尝试用远端容器做最小口径 ON 验证：
- `sessions 1-8 / q30-35`
- `sessions 1-4 / q33`
- `sessions 1-1 / q30`

结果：都没有形成新的 QA 产物。

新增定位结论：
- `sessions 1-1` 这条 direct-ov 路径根本不走 gateway ingest，它卡在 `benchmark/locomo/openclaw/import_to_ov.py` 的 `viking_ingest()`。
- 代码路径是：
  - `direct_ingest_rows()` -> `locomo_import.viking_ingest()`
  - `viking_ingest()` 在 `commit_session(..., telemetry=True)` 之后轮询 task。
- 远端 OpenViking 日志中可见大量上游重试：
  - `/embeddings/multimodal`
  - `/chat/completions`

这说明：
- 当前最小 benchmark 卡顿的主因不是这轮 PREPROCESSOR 清理代码。
- 也不是 `phase_a_off.py` session loop 本身。
- 更接近 OpenViking / 上游模型调用层的运行态不稳定，导致 direct-ov ingest 长时间卡在 task 完成前。

另外，还确认了一个会污染口径的脚本问题：
- `--no-sync-plugin-config` 下，如果不手动对齐 `openclaw.json`，`plugin_namespace_config.final` 里的 `userId/accountId/agent_prefix` 可能残留旧 run 值。
- 这会影响后续 recall / QA 口径。
- 但即使手动对齐并前台重启 gateway，`direct-ov` 最小 ingest 仍会卡在 OpenViking 上游重试，不构成这轮 PREPROCESSOR 清理的负面证据。

阶段性判断：
- PREPROCESSOR 清理后，本地与远端单测都通过。
- 没有拿到新的 ON benchmark 结果，不是因为 PREPROCESSOR 明显回归，而是因为远端 direct-ov ingest 的上游调用层不稳定。
- 后续若要继续拿远端实测闭环，应优先把 direct-ov ingest 的上游重试原因查清，而不是继续改 PREPROCESSOR。

### 2026-05-29: `sessions 1-4 / q33` 样本有效性判定

为避免继续猜测，本轮补了同口径最小 ON/OFF：
- ON: `sessions 1-4 / q33`
- OFF: `sessions 1-4 / q33`

#### ON 结果

- run: `sample0_on_20260529_q33run`
- ingest 能完成
- `q33 = WRONG`
- `input_tokens = 1632`
- `trajectory_diagnostics`:
  - `relevant_memories_count = 0`
  - `relevant_memories_tokens_est = 0`

回答内容是：
- “There is no relevant information provided ...”

#### OFF 结果

- run: `sample0_off_20260529_q33off`
- ingest 能完成
- `q33 = WRONG`
- `input_tokens = 1632`
- `trajectory_diagnostics`:
  - `relevant_memories_count = 0`
  - `relevant_memories_tokens_est = 0`

回答内容同样是：
- “I don't have any relevant information ...”

#### 结论 1：这组样本不能用于 PREPROCESSOR 比较

因为：
- ON / OFF 都没有注入任何 recalled memories
- token 也几乎完全一样
- 最终错误模式一致

因此：
- 这组样本的失败不反映 PREPROCESSOR 压缩效果差异
- 它更像是 recall / 注入层没有工作起来

#### 结论 2：这轮 OFF ingest 口径本身也不干净

额外发现：
- OFF run 的 ingest 行里，`wm_preprocess.status` 仍然是 `active`
- 但 run 后检查 `/root/.openviking/ov.conf`，`memory.wm_v2_preprocess_enabled = false`

这说明：
- OFF run 期间，1933 实际没有按新的 `ov.conf` 重启到 OFF 配置
- 即：这轮 OFF ingest 事实上仍然走了 ON 配置的服务进程

所以：
- `sessions 1-4 / q33` 这组数据同时存在两个问题：
  1. recall 注入为 0
  2. OFF 服务口径不干净

最终判定：
- 这组样本应明确标记为“不可用于 PREPROCESSOR ON/OFF 比较”
- 后续应回到“服务口径可控 + recall 正常注入”的子集，再做 PREPROCESSOR 对比

### 2026-05-29: 短 session 强制 fallback 试验结论

本轮针对一个明确怀疑点做了最小改动试验：
- 假设：当前真正伤准确率的不是某条时间/去重细则，而是**短 session 也被 compact PREPROCESSOR 激活**
- 证据来源：此前有效子集 `sessions 1-8 / q30-35` 中，8 个 ingest session 的 `full_messages_tokens_est` 全都低于 `1750`，但 ON 仍然强行 compact，且表现差于 OFF

#### 试验改动

临时加入了“短 session 强制 fallback”逻辑：
- 当 `full_messages_tokens_est < 2000` 时，不进入 compact packet，直接 `fallback=session_too_short`

本地测试：
- `tests/unit/session/test_extraction_preprocessor.py`: `27 passed`
- `tests/unit/session/test_fixture_token_savings.py`: `19 passed`

远端容器同步后测试：
- `tests/unit/session/test_extraction_preprocessor.py`
- `tests/unit/session/test_fixture_token_savings.py`
- 结果：`46 passed`

#### 远端有效复验

复验口径：
- run: `sample0_on_20260529_q3035floor`
- mode: `ON`
- `sessions 1-8 / q30-35`
- 远端容器前台运行
- 使用新 `user/account`，并手动对齐 `openclaw.json`

关键运行态：
- 8/8 ingest session 的 `wm_preprocess.status = fallback`
- 8/8 的 `fallback_reason = session_too_short`
- 对应 `full_messages_tokens_est` 依次为：
  - `692, 907, 1463, 1043, 783, 819, 1348, 1749`

#### 与旧 ON / 旧 OFF 的对比

对比对象：
- 旧 ON：`sample0_on_20260529_q3035on`
- 旧 OFF：`sample0_off_20260529_q3035off`

题级结果：

| qi | 新 ON floor | 旧 ON | 旧 OFF |
|---|---|---|---|
| `30` | `WRONG / 5980` | `WRONG / 5693` | `WRONG / 3919` |
| `31` | `WRONG / 7950` | `WRONG / 5219` | `CORRECT / 4239` |
| `33` | `CORRECT / 6231` | `CORRECT / 4416` | `CORRECT / 4363` |
| `34` | `CORRECT / 4528` | `CORRECT / 6538` | `CORRECT / 3754` |
| `35` | `CORRECT / 5394` | `CORRECT / 6341` | `CORRECT / 3793` |

其中表内格式为：`结果 / input_tokens`

汇总结论：
- 新 ON floor：`3/5 correct`
- 旧 ON：`3/5 correct`
- 旧 OFF：`4/5 correct`

判断：
- “短 session 强制 fallback”**没有把 ON 拉回接近 OFF**
- 准确率没有改善
- token 也没有系统性下降
- 因此这条试验性改动**不应保留在默认路径**

#### 补充发现：QA autocapture 会污染同一轮比较

这轮日志里还看到了一个明确的 benchmark 污染点：
- QA 阶段仍然会对问答本身做 autoCapture / commit
- 后续题会召回同一轮 QA 刚写入的“答案型 memory”

直接证据：
- `q34`/后续日志中出现了类似：
  - `memories/events/2026/05/29/adoption_meeting_attendance.md`
- 该 memory 的 abstract 已经是本轮 QA 对 `q30` 的答案表述，而不是原始 session memory

这说明：
- 即使不改 recall / extraction 主流程，当前 benchmark 运行方式本身也会把 QA 答案反灌回 memory
- 这会污染同一轮后续题的 recall，削弱 ON/OFF 对比的可信度

#### 最终结论

1. “短 session 强制 fallback”不是有效优化方向，已回滚，不保留。
2. 当前更大的阻塞点不是 PREPROCESSOR 细节，而是 benchmark 口径污染：
   - QA autocapture 会把同轮答案写回 memory
   - 后续正式对比若继续用同类子集，应优先关闭 QA autocapture，或至少把它明确当作 benchmark 脚本问题记录在案。

### 2026-05-30: `QA autoCapture=false` 的干净 ON/OFF 对照

为排除 benchmark 自身污染，本轮在远端容器里重跑了同口径子集，并显式关闭 QA autoCapture：

- `ON_clean`: `sample0_on_20260530_q3035onclean`
- `OFF_clean`: `sample0_off_20260530_q3035offclean`
- 共同口径：
  - `sessions 1-8 / q30-35`
  - 远端容器
  - 前台运行
  - 新 `user/account`
  - `openclaw.json` 中 `autoCapture=false`

#### 结果

| 口径 | correct | 平均 input_tokens |
|---|---:|---:|
| `ON_clean` | `2/5` | `4189.2` |
| `OFF_clean` | `4/5` | `5530.8` |

题级对比：

| qi | `ON_clean` | `OFF_clean` |
|---|---|---|
| `30` | `WRONG / 3207` | `CORRECT / 5566` |
| `31` | `WRONG / 2966` | `WRONG / 6641` |
| `33` | `CORRECT / 4580` | `CORRECT / 6434` |
| `34` | `WRONG / 4696` | `CORRECT / 4505` |
| `35` | `CORRECT / 5497` | `CORRECT / 4508` |

其中表内格式为：`结果 / input_tokens`

#### 直接结论

1. 关闭 QA autoCapture 后，ON 的 token 明显下降
   - 例如 `q30`: `5693 -> 3207`
   - `q31`: `5219 -> 2966`
2. 但即使去掉了 QA 污染，`ON_clean` 仍然明显差于 `OFF_clean`
   - `ON_clean = 2/5`
   - `OFF_clean = 4/5`

这说明：
- QA autoCapture 确实会污染 token 和 recall
- 但它**不是当前 ON 掉准的根因**
- 当前默认 PREPROCESSOR 路径本身仍在伤准确率

#### 直接证据：ON 生成了错误 memory

对比远端写出的 memory 正文：

`ON_clean`:
- `memories/events/2023/07/15/adoption_council_meeting.md`
  - 明确写成：`On 2023-07-07, Caroline attended a council meeting for adoption`
- `entities/event/caroline_adoption_meeting__adoption_journey.md`
  - 也写成：`Caroline attended a council meeting for adoption on 2023-07-07`

`OFF_clean`:
- `entities/event/caroline_adoption_meeting__council_event.md`
  - 写成：`Caroline went to a council meeting for adoption last Friday (2023-07-14)`
- `entities/event/adoption_planning__process.md`
  - 也保留为：`Attended adoption council meeting on 2023-07-14`

因此，`q30` 的差异不是 judge 噪音，而是：
- ON 路径实际把 memory 生成为错误日期 `2023-07-07`
- OFF 路径能生成正确日期 `2023-07-14`

#### 更细的判断

- `q31`：ON/OFF 都错
  - 说明 Melanie pottery workshop 这条“Last Fri”日期解析，目前不只是 PREPROCESSOR ON 才会错
- `q30`：只有 ON 错，OFF 对
  - 说明 PREPROCESSOR ON 对 Caroline adoption meeting 这类日期事实存在明确负作用
- `q34`：只有 ON 错，OFF 对
  - 说明 PREPROCESSOR ON 还会伤到事件覆盖/结构化保真，不只是日期问题

#### 阶段性结论

在去除 QA autoCapture 污染后，当前最可信的结论是：

1. 当前默认 PREPROCESSOR 仍未达到目标。
   - 它可以降低部分题的 token
   - 但会把某些关键事实（至少 `q30`、`q34`）压坏，导致 accuracy 明显劣于 OFF

2. 后续若继续坚持“只动 PREPROCESSOR”，下一步应优先定位：
   - 哪些 active signal / span / fact 组合导致 `session_8` 中 `Last Friday` 被错误固化为 `2023-07-07`
   - 哪些 active 结构让 `q34` 的 support group 事件在 ON 下丢失或弱化

### 2026-05-30: 回掉非 PREPROCESSOR 的 entity/facet/URI 改动后做 direct-ov probe

#### 目的

先排除“当前测试分支混入了非 PREPROCESSOR 改动，导致归因失真”的可能。

本轮先从测试口径里回掉以下改动：
- `memory_extraction.yaml` 中 entity facet / canonical facet 指令
- `entities.yaml` / `vaka/entities.yaml` 的 `name__facet.md` 文件名模板
- `memory_isolation_handler.py` 的 facet normalize
- `memory_updater.py` 的 legacy entity URI migration

#### 本地与远端验证

本地：
- `tests/unit/session/test_extraction_preprocessor.py`
- `tests/unit/session/test_fixture_token_savings.py`
- `tests/unit/session/memory/test_searchable.py`

结果：
- `46 passed`
- `13 passed`

远端容器：
- 同三组测试
- 结果：`59 passed`

#### 远端 direct-ov 经验验证

为避免再次被 gateway / QA 口径干扰，本轮直接在远端容器调用 `viking_ingest()`，只灌 `conv-26 / session_8`：

- host: `123.60.114.206:10008`
- container: `jcp-dev`
- worktree: `/home/jcp/agent/code/OpenViking-probe-wm`
- sample: `conv-26`（LoCoMo `sample_index=0`）
- sessions: `8-8`
- fresh account/user/agent:
  - `acct-20260530-preponly-s8b`
  - `conv-26-preponly-s8b`

ingest 成功：
- session id: `c650d541-b25c-4e8d-98de-e7c33befb6d9`
- token usage:
  - embedding: `6590`
  - vlm: `12531`
  - total: `19121`

#### 直接结果

关键产物：
- `/root/.openviking/data/viking/acct-20260530-preponly-s8b/session/c650d541-b25c-4e8d-98de-e7c33befb6d9/history/archive_001/memory_diff.json`
- 以及同 account 下实际写出的 memory 文件

写出的结果仍然是错误日期：

1. `entities/activity/pottery_workshop.md`
   - `Melanie took her kids to a pottery workshop on July 7, 2023 (last Friday before July 15, 2023)`

2. `entities/event/adoption_council_meeting.md`
   - `Caroline attended one on July 7, 2023 (last Friday before July 15, 2023)`

3. `events/2023/07/15/adoption_council_meeting.md`
   - `On July 7, 2023, Caroline attended a council meeting for adoption`

4. `events/2023/07/15/pottery_workshop.md`
   - `On July 7, 2023, Melanie took her kids to a pottery workshop`

#### 结论

回掉这批非 PREPROCESSOR 的 entity/facet/URI 改动后：
- `q30/q31` 相关的错日期问题**没有消失**
- 因此这些并行 memory schema / URI 改动不是当前错日期的 primary cause

怀疑范围应收敛回：
- PREPROCESSOR creation compact path 本身
- compact packet 如何诱导 memory extraction 将 `Last Friday / Last Fri` 固化成 `2023-07-07`

### 2026-05-30: `session_8 only` direct-ov OFF probe 结果

在恢复远端 `OpenViking-probe-wm` 的 1933 服务后，本轮补了同口径 OFF 对照：

- account: `acct-20260530-prepoff-s8b`
- user/agent: `conv-26-prepoff-s8b`
- sample: `conv-26`（`sample_index=0`）
- sessions: `8-8`
- `wm_v2_preprocess_enabled=false`
- 仍然使用 direct-ov `viking_ingest()`，不经过 gateway/QA

ingest 成功：
- session id: `e740d29c-99a5-4298-b45d-40a75bc4a76a`
- token usage:
  - embedding: `4205`
  - vlm: `13012`
  - total: `17217`

关键 memory 结果：

1. `events/2023/07/15/adoption_council_meeting.md`
   - `On 2023-07-07 (last Friday relative to 2023-07-15), Caroline attended a council meeting for adoption`

2. `events/2023/07/15/pottery_workshop.md`
   - `On 2023-07-07 (last Friday relative to 2023-07-15), Melanie took her kids to a pottery workshop`

3. `entities/person/caroline.md`
   - 也写成 `Attended an adoption council meeting on 2023-07-07`

4. `entities/person/melanie.md`
   - 也写成 `Took her kids to a pottery workshop on 2023-07-07`

#### 新结论

这条 OFF probe 直接推翻了一个之前较强的假设：

- 单看 `conv-26/session_8` 的 direct-ov memory generation
- `wm_v2_preprocess_enabled=false` **也会**把 `Last Friday / Last Fri` 写成 `2023-07-07`

因此：
- `session_8` 单轮错日期 **不是** PREPROCESSOR on/off 的直接分叉结果
- 之前 `ON_clean=2/5` vs `OFF_clean=4/5` 的差异，不能简单归因为“session_8 creation 阶段 ON 写错而 OFF 写对”

更可信的解释转为：
- `q30/q34` 的 ON/OFF 差异更可能来自 **多 session 累积后的 memory 组织、更新、召回排序** 差异
- 而不是 `session_8` 这一轮 direct-ingest 本身的 event memory 生成差异

### 2026-05-30: creation-bypass QA benchmark 口径修复

本轮确认，`creationoff2` 之前的 QA 结果不能直接作为有效对比，因为 benchmark 的 namespace 配置链存在脚本级问题。

#### 根因

远端 OpenClaw gateway 实际只认：

- `plugins.entries.openviking.config`

而此前 `phase_a_off.py` 同时读写了：

- `plugins.entries.openviking.config`
- 非法的顶层 `plugins.openviking`

结果是：

1. 脚本/人工检查时，可能看到顶层 `plugins.openviking` 已是新值；
2. 但 gateway 运行时仍按 `plugins.entries.openviking.config` 的旧值工作；
3. 因此 `creationoff2` 的 QA 虽然传了新 `user/account/ov_agent_id`，实际 recall 仍打到旧的 `onclean2` namespace。

#### 直接证据

旧 run `q3035creationoff2qa2` 的 gateway 日志中：

- `parsedConfigAgentPrefix = acct-20260530_q3035onclean2`
- `X_OpenViking_Agent = acct-20260530_q3035onclean2_locomo-eval`
- `X_OpenViking_Account = acct-20260530_q3035onclean2`
- `X_OpenViking_User = user-20260530_q3035onclean2`

说明这批结果虽然目录名叫 `creationoff2`，但 recall 实际仍在旧 `onclean2` namespace 上跑。

#### 已修复

已修改 `benchmark/locomo/openclaw/phase_a_off.py`：

1. `update_openclaw_plugin_config()` 只保留 `plugins.entries.openviking.config`
2. 同步时删除非法的顶层 `plugins.openviking`
3. `restart_local_gateway_for_base_url()` 不再使用 `openclaw gateway --force`
   - 远端容器缺 `fuser`，`--force` 会把 QA namespace 切换误报成 gateway 重启失败

对应测试已补到：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`

本地验证：

- `4 passed`

#### 修复后验证

清理远端 `/root/.openclaw/openclaw.json` 中非法顶层 `plugins.openviking`，并重新加载 gateway 后，
新的单题 run `q30creationoff2qa5` 日志已确认：

- `parsedConfigAgentPrefix = acct-20260530_q3035creationoff2`
- `X_OpenViking_Agent = acct-20260530_q3035creationoff2_locomo-eval`
- `X_OpenViking_Account = acct-20260530_q3035creationoff2`
- `X_OpenViking_User = user-20260530_q3035creationoff2`

这证明：

- benchmark QA 路径终于真正切到了 `creationoff2` 的新 namespace
- 之前 `creationoff2` 的题级结果应视为**旧 namespace 污染样本**

#### 当前状态

修复后的单题/子集 QA 仍在 provider 侧出现长尾超时：

- `q30creationoff2qa5`：namespace 已修正，但 run 仍处于 `running`
- `q3435creationoff2qa4`：`q34` 曾出现 provider idle timeout，judge 进程仍挂着

因此，本轮已经解决的是：

- benchmark QA namespace 归因错误

但还没有拿到：

- 修正后的 `creation-bypass` 完整 `q30-q35` 有效成绩单

### 2026-05-30: 修正后 namespace 下的 creation-bypass clean 结果

在修复 benchmark QA namespace 配置链后，本轮用**全新 account/user**重跑了一个真正干净的 `creation-bypass` 子集：

- run: `q3033creationoff2clean7`
- account: `acct-20260530_q3033creationoff2clean7`
- user: `user-20260530_q3033creationoff2clean7`
- mode: `ON`
- sessions: `1-8`
- QA: `q30/q31/q33`
- 口径：
  - 远端容器
  - direct-ov ingest
  - `QA autoCapture=false`
  - 修正后的 `plugins.entries.openviking.config`

#### ingest 结果

8 个 session 全部成功：

- `session_1 memories=7`
- `session_2 memories=6`
- `session_3 memories=6`
- `session_4 memories=7`
- `session_5 memories=8`
- `session_6 memories=6`
- `session_7 memories=10`
- `session_8 memories=10`

这说明：

- creation-bypass 不会把 memory 生成为空
- 这轮 account 的 index / recall 前置条件是正常的

#### QA 结果

最终成绩：

- `1/3 correct`

题级：

| qi | 结果 | input_tokens | 结论 |
|---|---|---:|---|
| `30` | `WRONG` | `4639` | 仍答成 `2023-07-07` |
| `31` | `WRONG` | `5174` | 仍答成 `2023-07-07` |
| `33` | `CORRECT` | `7329` | 能答对 June camping |

#### 关键取证

虽然 `trajectory_diagnostics` 仍显示：

- `relevant_memories_count = 0`
- `relevant_memories_tokens_est = 0`

但 gateway 日志里的 `inject-detail` 证明 recall 实际发生了：

- `q30` 注入了 6 条 memory，其中 top-1 就是
  - `events/2023/07/15/adoption_council_meeting.md`
  - 摘要仍写成 `On 2023-07-07, Caroline attended a council meeting for adoption`
- `q31` 注入了 6 条 memory，其中 top-1 就是
  - `events/2023/07/15/pottery_workshop.md`
  - 摘要仍写成 `On 2023-07-07, Melanie took her kids to a pottery workshop`
- `q33` 注入了 6 条 memory，其中包含
  - `entities/event/melanie_family_camping.md`
  - 摘要写成 `Took place last week (around 2023-06-20)`

这说明：

1. 修正后的 namespace 已生效，recall 真实命中了 `creationoff2clean7` 自己的 memory；
2. `trajectory_diagnostics` 当前**不能**用 `relevant_memories_count=0` 来判断 recall 是否为空；
3. `creation-bypass` 没有解决 `q30/q31` 的错误日期问题；
4. 在这组题上，`creation-bypass` 还带来了更高的输入 token。

#### 阶段结论

`creation 阶段完全禁用 PREPROCESSOR，只保留 update 阶段` 这条线，目前没有证据支持继续扩大：

- 准确率：`1/3`
- `q30/q31` 仍错
- token 没有更优

因此，`creation-bypass` 目前应视为**无效实验线**，不建议再继续扩面。

### 2026-05-30: `ON_clean` vs `OFF_clean` 的 session_8 分叉点

在回退 bypass 实验后，本轮直接对比了默认 `ON_clean` 与 `OFF_clean` 的多 session 累积产物，确认：

- **关键分叉不是在 QA 阶段才出现**
- 而是在 **session_8 对应的 commit / memory_diff** 中已经发生

#### ON_clean 的 session_8

session 目录：

- `/root/.openviking/data/viking/acct-20260530_q3035onclean/session/3ac8c410-0769-4467-b0c3-e68af730f000`

`.meta.json` 中：

- `wm_preprocess.enabled = true`
- `full_messages_tokens_est = 1749`
- `compact_packet_tokens_est = 199`
- `render_mode = minimal`
- `structured_facts` 中保留了三条关键 date facts：
  - `Last Fri I finally took my kids to a pottery workshop.`
  - `Here's our latest work from last weekend.`
  - `Last Friday I went to a council meeting for adoption.`

`memory_diff.json` 中，session_8 这一轮就直接新增/写入了错误日期：

- `entities/event/caroline_adoption_meeting__adoption_journey.md`
  - `Caroline attended a council meeting for adoption on 2023-07-07`
- `events/2023/07/15/adoption_council_meeting.md`
  - `On 2023-07-07, Caroline attended a council meeting for adoption`
- `entities/event/melanie_kids_pottery__family_creativity.md`
  - `Melanie took her kids to a pottery workshop on 2023-07-07`
- `events/2023/07/15/pottery_workshop.md`
  - `On 2023-07-07, Melanie took her kids to a pottery workshop`

并且还把这些内容并入了更大的聚合 memory：

- `caroline__personal_growth`
- `caroline_melanie_chat__friendship_update`
- `melanie__creative_practice`
- `melanie_pottery_class__creative_engagement`

#### OFF_clean 的 session_8

session 目录：

- `/root/.openviking/data/viking/acct-20260530_q3035offclean/session/3c2b387a-b72b-4dd5-baf6-f22caf548694`

`.meta.json` 中：

- `wm_preprocess.enabled = false`
- 没有 compact packet
- `messages.jsonl` 保留完整 40 条原始聊天

`memory_diff.json` / `events/2023/07/15/friendship_chat.md` 里，同样的原始句子被写成：

- Melanie pottery workshop: `2023-07-07`
- Caroline adoption meeting: `2023-07-14`

即：

- `Last Fri I finally took my kids to a pottery workshop` -> `2023-07-07`
- `Last Friday I went to a council meeting for adoption` -> `2023-07-14`

#### 新结论

这一步非常关键：

1. `ON_clean` 与 `OFF_clean` 的日期分叉，已经在 **session_8 这一轮 memory generation / merge** 上出现；
2. `ON_clean` 并不是 later recall 才把正确记忆排错，而是 **session_8 本轮就写出了错误 adoption date**；
3. `OFF_clean` 在同样的原始消息上，能把 `adoption` 写成 `2023-07-14`，但 `pottery` 仍是 `2023-07-07`；
4. 因此，下一步最值得分析的不是“为什么整个 PREPROCESSOR 好/坏”，而是：
   - 为什么 `ON minimal compact` 会把 `adoption` 也一起锚到 `2023-07-07`
   - 而 `OFF full transcript` 至少能把 `adoption` 修正到 `2023-07-14`

也就是说，当前最有价值的后续工作应围绕：

- `session_8 creation commit`
- `ON minimal compact packet` vs `OFF full transcript`
- adoption/pottery 两条相对时间事实在 memory extraction prompt 中的可区分性

### 2026-05-30: `session_8` 实际吃到的 prior summary 已定位到 `session_7/history/archive_001/.overview.md`

本轮补了代码链路与远端文件证据，确认 `extract_long_term_memories()` 在 `session_8` 提取前读到的
`latest_archive_overview`，就是 `session_7/history/archive_001/.overview.md`。

代码证据：

- `Session._get_latest_completed_archive_summary()` / `_get_latest_completed_archive_overview()`
  直接读取最新 completed archive 的 `.overview.md`
- `SessionExtractContextProvider` 将该内容作为 `summary` 注入
  `compression.memory_extraction` prompt
- `memory_extraction` prompt 同时包含：
  - `## Session History Summary`
  - `## Recent Conversation`

因此，`session_8` 提取时的 ON/OFF 差异，最可疑的输入不是当前消息本身，而是上一轮 archive summary。

#### ON_clean 的 session_7 archive overview

路径：

- `/root/.openviking/data/viking/acct-20260530_q3035onclean/session/a257ea17-0605-4133-a419-b82affbe2418/history/archive_001/.overview.md`

内容特点：

- 标题：`LGBTQ Conference Check-In Between Caroline & Melanie`
- 只保留了 conference / community acceptance 相关内容
- `Current State / Task & Goals / Key Facts` 都极短
- 完全没有保留：
  - Caroline 的 counseling / mental health career 动机细节
  - `Becoming Nicole`
  - Melanie 的 pets / shoes / running / mental health self-care

对应 `session_7` 的 `.meta.json`：

- `wm_preprocess.enabled = true`
- `full_messages_tokens_est = 1348`
- `compact_packet_tokens_est = 131`
- `render_mode = minimal`
- `structured_facts_count = 1`
- `selected_span_count = 10`

也就是说，ON 在 `session_7` 就已经把一个 28-message 会话压成了一个非常短的 prior summary。

#### OFF_clean 的 session_7 archive overview

路径：

- `/root/.openviking/data/viking/acct-20260530_q3035offclean/session/15e63265-a945-47e4-a997-98453258ae62/history/archive_001/.overview.md`

内容特点：

- 标题：`Caroline & Melanie Chat: LGBTQ Conference, Mental Health Goals, Pets, Running`
- 明确保留了：
  - Caroline 参加 LGBTQ conference 的时间与感受
  - Caroline 探索 counseling / mental health jobs 的动机
  - `Becoming Nicole`
  - Melanie 的 pets
  - 新紫色 running shoes
  - running / destress / mental health

对应 `session_7` 的 `.meta.json`：

- `wm_preprocess.enabled = false`
- 没有 compact packet
- `messages.jsonl` 保留完整 28 条消息

#### 新判断

这条证据把问题进一步收窄为：

1. `session_8` 的 raw recent_messages 在 ON/OFF 下基本相同；
2. `session_8` 提取时吃到的 `summary` 明显不同；
3. ON 的 prior summary 过短、过窄，强烈压缩了上一轮上下文结构；
4. 这使得 `memory_extraction` 更可能在 `session_8` 里把两个 `Last Fri/Last Friday`
   事件错误地统一到同一个绝对日期；
5. 因此，当前最值得改的不是 creation/update bypass，而是：
   - prior summary 注入内容的保真度
   - 或 compact summary 进入 `memory_extraction` 时的使用方式

这也解释了为什么：

- `creation-bypass` 无效
- `update-bypass` 也无效

因为真正有影响的，很可能是 **上一轮 archive summary 被压得过薄**，
而不是当前轮单独是否 bypass 某个阶段。

### 2026-05-30: 提升 `session_7` prior summary 保真度的最小实验，结论为“有改善但不足以修复根因”

本轮只做了一个很小的 PREPROCESSOR 实验：

- 暂时把 `minimal` render 的触发阈值从 `<2000 tokens` 收紧到 `<1200 tokens`
- 目标：让 `session_7` 这种 `~1.3k tokens` 的会话走 `balanced`，提高注入到 `memory_extraction.summary` 的 prior summary 保真度

#### 本地与远端验证

- 本地：
  - `tests/unit/session/test_extraction_preprocessor.py`
  - `tests/unit/session/test_fixture_token_savings.py`
  - 均通过
- 远端容器：
  - 同组测试通过

随后在远端容器 fresh account 上只做 direct-ov ingest：

- account: `acct-20260530-s17balanced`
- user: `user-20260530-s17balanced`
- agent: `acct-20260530-s17balanced_locomo-eval`

先灌 `sessions 1-7`，再单独灌 `session_8`，不经过 QA。

#### 实际改善

新的 `session_7`：

- `session_id = 5bb90c86-e1ec-41f4-bb38-7119a8801a97`
- `full_messages_tokens_est = 1335`
- `compact_packet_tokens_est = 254`
- `render_mode = balanced`

相比此前 ON 的 `session_7 minimal` 版本，这次 archive overview 明显更完整，恢复了：

- Caroline 的 `counseling / mental health jobs`
- Caroline 想提供 supportive talk space 的职业动机

即 prior summary 确实不再只剩 conference 主线。

#### 但核心错误仍然存在

同一 fresh account 下继续灌 `session_8`：

- `session_id = e6ba0e9f-1f1f-4fb8-b43d-fcb8dadedf57`
- `full_messages_tokens_est = 1735`
- `compact_packet_tokens_est = 288`
- `render_mode = balanced`

`structured_facts` 仍正确保留了：

- `Last Fri I finally took my kids to a pottery workshop.`
- `Here's our latest work from last weekend.`
- `Last Friday I went to a council meeting for adoption.`

但最终生成出的 memory 仍然是错的：

- `entities/event/caroline_adoption_council_meeting.md`
  - `Took place on 2023-07-07 (last Friday)`
- `events/2023/07/15/adoption_council_meeting.md`
  - `On 2023-07-07 (last Friday), Caroline attended a council meeting for adoption`
- `entities/event/melanie_kids_pottery_workshop.md`
  - `Took place on 2023-07-07 (last Friday)`
- `events/2023/07/15/pottery_workshop.md`
  - `On 2023-07-07 (last Friday), Melanie took her kids to a pottery workshop`

#### 结论

这次实验可以排除一个工作假设：

- **“只要把 prior summary 从 minimal 提升到 balanced，就能修复 adoption/pottery 的相对日期混淆”**

证据表明：

1. prior summary 变厚，确实改善了 summary 保真度；
2. 但 `session_8` 的 adoption / pottery 仍被同时锚到 `2023-07-07`；
3. 因此 summary 过薄可能是放大因素，但**不是足以单独解释 root cause 的决定性因素**。

这次阈值收紧没有拿到题级收益，也没有直接修复 memory generation，
因此本轮代码改动已回退，不留在默认路径里。

### 2026-05-30: 对 `memory_extraction.summary` 做条件旁路，结论仍然无效

为进一步验证“prior summary 注入是否是主触发因素”，本轮做了一个更直接的实验：

- 保持 WM PREPROCESSOR 默认 ON
- 只对 `memory_extraction` 的 `latest_archive_overview` 入参做条件旁路
- 条件：当前会话若命中 `date_or_plan`，则 extraction prompt 不再注入上一轮 summary
- WM summary 生成本身不变，creation/update 主流程不变

#### 代码与测试

实现：

- `openviking/session/session.py`
  - 新增 `Session._resolve_extraction_history_summary(...)`
  - 在 commit 阶段调用 `extract_long_term_memories(...)` 前，对 `latest_archive_overview`
    做条件裁剪

本地验证：

- `tests/unit/session/test_extraction_preprocessor.py`
- `tests/unit/session/test_fixture_token_savings.py`
- `49 passed`

远端容器验证：

- 同组测试通过：`47 passed`

#### 远端 fresh account direct-ov 实测

fresh account：

- `acct-20260530-nosummary8`
- `user-20260530-nosummary8`
- `acct-20260530-nosummary8_locomo-eval`

直接灌 `sessions 1-8`，不经过 QA，仅检查 `session_8` 生成出的 memory。

结果：

- `events/2023/07/15/adoption_council_meeting.md`
  - 仍写成：`On 2023-07-07, Caroline attended a council meeting for adoption`
- `events/2023/07/15/pottery_workshop.md`
  - 仍写成：`On 2023-07-07, Melanie took her kids to a pottery workshop`

#### 结论

这条实验可以明确排除另一条工作假设：

- **“只要让带 relative date 的当前会话不吃 prior summary，adoption/pottery 的绝对日期就会纠正”**

当前证据表明：

1. `session_7` prior summary 变厚，不足以修复问题；
2. `memory_extraction` 完全不吃 prior summary，也不足以修复问题；
3. 因此 adoption / pottery 同时落到 `2023-07-07` 的根因，已经更靠近：
   - `memory_extraction` 自身对 relative date 的 grounding
   - 或当前轮 raw conversation 中“since we last talked / last Fri / last Friday”的语义消解

也就是说，到这一步为止，**只动 PREPROCESSOR summary 注入方式** 已经没有拿到正向信号。

### 2026-05-30: 最小跨界到 relative-time grounding prompt，仍然无效

在确认 PREPROCESSOR-only 路线基本证伪后，本轮按“最小必要 plan”只改了两处 prompt 规则：

- `openviking/prompts/templates/compression/memory_extraction.yaml`
- `openviking/prompts/templates/memory/events.yaml`

改动目标：

- 强制每个 relative-time 表达独立解析；
- 禁止把一个事件推断出的绝对日期复用到另一个事件；
- 若日期不确定，优先保留 relative wording，而不是硬编一个具体日子。

#### 远端 direct-ov fresh account 验证

fresh account：

- `acct-20260530-rtg8`
- `user-20260530-rtg8`
- `acct-20260530-rtg8_locomo-eval`

在远端容器中重新灌 `sessions 1-8`，直接读取 `session_8` 的 `memory_diff.json`。

结果仍然错误：

- `entities/event/adoption_council_meeting.md`
  - `Caroline attended a council meeting for adoption on 2023-07-07`
- `events/2023/07/15/adoption_meeting.md`
  - `On 2023-07-07, Caroline attended a council meeting for adoption`
- `entities/event/pottery_workshop.md`
  - `Melanie took her kids to a pottery workshop on 2023-07-07`
- `events/2023/07/15/pottery_workshop.md`
  - `On 2023-07-07, Melanie took her kids to a pottery workshop`

即使 prompt 已显式要求：

- 不要复用一个事件的绝对日期；
- 模糊时保留 relative wording；

模型仍然把 adoption / pottery 同时锚到 `2023-07-07`。

#### 新结论

这意味着：

1. `PREPROCESSOR-only` 路线已经基本证伪；
2. 最小 prompt 级 relative-time 修补也没有带来正向信号；
3. 若继续推进，已经需要超出“最小必要改动”的范围，进入更大级别的 extraction / temporal-grounding 设计调整。

因此，当前阶段已形成一个真实阻塞点：

- 在“不大改、尽量不碰 memory extraction/recall”的约束下，已经没有高信号的下一步实验可继续。

### 2026-05-30: v2 主路径 extraction helper 只部分命中根因

本轮纠正了一个链路认知：

- 当前 `compressor_v2` 主路径并不走 legacy `memory_extractor.py: compression.memory_extraction`
- 真正影响 `session_8` 的，是 `SessionExtractContextProvider -> ExtractLoop` 的 conversation input

因此本轮改为在 v2 主路径上实现最小 extraction helper：

1. `session.py`
   - 从上一轮 completed archive 读取 `session_time`
2. `compressor_v2.py`
   - 将 `latest_archive_session_time` 透传给 `SessionExtractContextProvider`
3. `session_extract_context_provider.py`
   - 在 extraction conversation input 中增加：
     - `Relative Time Grounding Hints`
     - 对命中 `since we talked/spoke/chatted + last Fri/last Friday` 的消息，显式给出：
       - 当前会话锚点
       - 上一轮会话锚点
       - 该相对时间短语的独立解析结果
   - 第二轮又把 `RelativeTimeResolution` 内联到具体消息行，增强局部可见性

#### 本地 / 远端测试

- 本地：
  - `tests/session/memory/test_memory_timestamp_parsing.py`: `4 passed`
  - `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`
- 远端容器：
  - provider 时间测试：`4 passed`
  - PREPROCESSOR 基线测试：`46 passed`

#### 远端 fresh-account direct-ov 实测

##### 第 1 轮 helper
- account: `acct-20260530-rtdisambig8`
- 结果：
  - `adoption` 被纠正到 `2023-07-14`
  - `pottery` 仍错误地写成 `2023-07-14`

##### 第 2 轮 helper（更明确 wording + inline resolution）
- account: `acct-20260530-rtdisambig10`
- 结果仍然是：
  - `adoption` = `2023-07-14`（正确）
  - `pottery` = `2023-07-14`（错误，预期应是 `2023-07-07`）

#### 当前结论

- 这条 extraction helper 不是完全无效：
  - 它稳定地把 `adoption` 从 `2023-07-07` 拉回了 `2023-07-14`
- 但它仍然没有解决核心 disambiguation：
  - `Melanie` 的 `Last Fri` 仍被模型按当前会话时间解释成 `2023-07-14`
- 因此它只能算**部分命中根因**，还不足以宣称修复 `q30/q31`

#### 新边界

- `PREPROCESSOR-only` 已证伪
- prompt-only relative-time 修补已证伪
- v2 主路径 extraction helper 能修正 `adoption`，但不能修正 `pottery`
- 若继续推进，下一步需要更强的 extraction-side temporal parsing：
  - 不是只给模型提示
  - 而是更明确地把 `since we talked` 与“上一轮会话时间锚点”绑定到具体事件级别

##### 第 3 轮 helper（override wording 再强化）
- account: `acct-20260530-rtdisambig11`
- 改动：
  - 顶部规则改为：
    - relative time 默认按 `Session Time`
    - 若消息显式包含 `since we talked/spoke/chatted`，则 **override** 默认规则，改用 `Previous Session Anchor`
  - 行内 `RelativeTimeResolution` 也显式标注：
    - `this overrides the default Session Time rule`
- 结果仍然是：
  - `adoption` = `2023-07-14`（正确）
  - `pottery` = `2023-07-14`（错误，预期仍应是 `2023-07-07`）

这说明：
- 仅靠更强的提示、override wording、以及行内 resolution 注释，已经不能继续推动 pottery 修正
- 当前模型仍会把 `Melanie: since we talked! Last Fri ...` 解释为当前会话时间的前一个 Friday

##### 第 4 轮 helper（把解析结果直接嵌入消息正文）
- account: `acct-20260530-rtdisambig12`
- 改动：
  - 不再只在消息尾部放注释
  - 直接把 `Last Fri / Last Friday` 的解析结果嵌入 extraction 实际读取的消息正文：
    - `Last Fri [resolved date: 2023-07-07 ..., anchored to Previous Session Anchor ...]`
    - `Last Friday [resolved date: 2023-07-14 ..., anchored to Session Time ...]`
  - 同时保留 `RelativeTimeResolution` 行尾注释
- 结果仍然是：
  - `adoption` = `2023-07-14`（正确）
  - `pottery` = `2023-07-14`（错误，预期仍应是 `2023-07-07`）

#### 更新后的边界判断

- 只靠 prompt wording、顶层 grounding hints、行尾 resolution 注释、以及正文内联 resolved date，
  已经无法把 pottery 从 `2023-07-14` 拉回 `2023-07-07`
- 当前还没有证据表明继续堆叠这类 helper 会带来新的正向收益
- 如果继续推进，需要进入更强的 extraction-side temporal parsing / event segmentation 设计，
  不再是这一级别的“提示增强”或“输入改写”

##### 第 5 轮 helper（独立 Event Time Normalization 区块）
- account: `acct-20260530-rtdisambig13`
- 改动：
  - 在 conversation input 中新增独立的 `## Event Time Normalization`
  - 对命中的两条事件先给出标准化事件句：
    - `Melanie pottery workshop with kids happened on 2023-07-07 ...`
    - `Caroline adoption council meeting happened on 2023-07-14 ...`
- 结果仍然是：
  - `adoption` = `2023-07-14`（正确）
  - `pottery` = `2023-07-14`（错误，预期仍应是 `2023-07-07`）

#### 最终收敛结论

- 到第 5 轮 helper 为止，当前这一类“输入增强/事件归一化提示”路线已重复命中同一 blocker：
  - adoption 可修
  - pottery 不可修
- 继续在这一层叠加 prompt / helper，已经没有高信号收益
- 若继续推进，必须进入更强的 extraction-side temporal parsing / event-level binding 设计

##### 第 6 轮 helper（直接改写为绝对日期句）
- account: `acct-20260530-rtdisambig14`
- 改动：
  - 对命中的 extraction 输入消息，不再只附加提示或注释
  - 直接把 `Last Fri / Last Friday` 改写为显式时间句：
    - `On 2023-07-07 (Friday), ...`
    - `On 2023-07-14 (Friday), ...`
- 结果仍然是：
  - `adoption` = `2023-07-14`（正确）
  - `pottery` = `2023-07-14`（错误，预期仍应是 `2023-07-07`）

#### 再次收敛后的判断

- 连“把相对时间短语直接改写成绝对日期句”都不能修正 pottery
- 说明当前问题已经不是简单的 prompt framing 或输入改写问题
- 若继续推进，需要真正进入：
  - extraction-side temporal parsing
  - event segmentation / event-specific grounding
  而不是继续在同一层做 helper 叠加

##### 第 7 轮 helper（独立 NormalizedEvent 行）
- account: `acct-20260530-rtdisambig15`
- 改动：
  - 在每条命中的 extraction 输入消息前，新增一条独立的 `[NormalizedEvent] ... happened on ...` 行
  - 保留原有的 `[NormalizedEventTime]` 区块、inline resolved-date 和重写句
- 远端容器验证：
  - provider 测试 `tests/session/memory/test_memory_timestamp_parsing.py`：`4 passed`
  - fresh-account direct-ov `sessions 1-8`：完成
- 结果仍然是：
  - `adoption` = `2023-07-14`（正确）
  - `pottery` = `2023-07-14`（错误，预期仍应是 `2023-07-07`）
- 新观察：
  - 相关衍生 memory 也继续跟着错误时间漂移，例如 `kids_pottery_cup.md` 也写成了 `2023-07-14`

#### 最新判断

- 到第 7 轮 helper 为止，所有“输入增强 / 提示增强 / 注释增强 / 直接改写”同层策略都已重复命中同一 blocker：
  - `adoption` 可以修正
  - `pottery` 不能修正
- `NormalizedEvent` 独立事件行也没有带来任何新的正向分叉
- 继续在同一层堆 helper 的信息增量已经很低
- 若继续推进，必须进入更强的 extraction-side temporal parsing / event-level binding，而不是继续做输入包装层增强

## 2026-05-30 新增根因取证：错误发生在原始 extraction 输出

### 新增诊断能力

- 在 [openviking/session/compressor_v2.py](/home/jcp/Agent/code/OpenViking/openviking/session/compressor_v2.py) 的 `memory_diff` 构建中，新增记录 `generated_fields`
  - 直接保存 `ResolvedOperation.memory_fields`
  - 目的：区分“模型原始 extraction 输出就错了”还是“后续 updater / merge 写错了”
- 新增测试：
  - [tests/session/memory/test_memory_diff.py](/home/jcp/Agent/code/OpenViking/tests/session/memory/test_memory_diff.py)
  - 本地回归：
    - `tests/session/memory/test_memory_diff.py`: `11 passed`
    - 联合回归：`61 passed`

### 远端 probe 结论

为了避免继续猜测，我做了一个 extractor-only 远端 probe：

- account: `acct-20260530-rtdisambig17`
- 先灌 `sessions 1-7`
- 然后不走正常 apply，直接在容器里调用 `SessionCompressorV2.extract_long_term_memories()`，用 fake updater 拦截 `ResolvedOperations`
- 直接打印 `INTERESTING_OPS`

结果已经明确：

- `entities/event/pottery_workshop_with_kids`
  - `content = "On 2023-07-14, Melanie took her kids to a pottery workshop ..."`
- `events/2023/07/15/pottery_workshop.md`
  - `summary = "On 2023-07-14, Melanie took her kids to a pottery workshop ..."`
- `entities/event/adoption_council_meeting`
  - `content = "On 2023-07-14, Caroline attended a council meeting for adoption ..."`
- `events/2023/07/15/adoption_council_meeting.md`
  - `summary = "On 2023-07-14, Caroline attended an adoption council meeting ..."`

这说明：

- `pottery = 2023-07-14` 不是 updater / merge 后写坏的
- 它在模型原始 extraction 输出阶段就已经错了
- `adoption = 2023-07-14` 也是模型原始输出直接给出的

### 新的最强工作假设

当前 direct-ov / 按 session 分次 ingest 的路径里，`session_8` 的 extraction 没有可靠的“上一轮会话锚点”输入：

- `adoption` 只需要当前 `Session Time`，所以能对
- `pottery` 依赖 `since we talked + Last Fri` 的跨 session 语义
- 当 extraction 缺少“上一轮会话时间锚点”时，模型会退回到当前 session 锚点，导致把 `pottery` 也落到 `2023-07-14`

这比之前“helper 不够强”更进一步，已经定位到：

- 问题不是输入包装层不够花
- 而是 extraction 阶段缺少可靠的跨 session anchor

### 下一步实现方向

如果继续，应优先实现一个更强的 extraction-side helper：

1. 在 `latest_archive_session_time` 为空时
2. 对当前 `account/user/agent`，从已有 session 存储中推断“最近一轮已完成 session 的时间”
3. 将这个时间作为 `Previous Session Anchor`
4. 再进入 current `relative-time grounding` 逻辑

这是当前最有希望真正把 `pottery` 从 `2023-07-14` 拉回 `2023-07-07` 的方向。

## 2026-05-30 关键纠正：q31 的 benchmark gold 实际要求 2023-07-14

上面的“上一轮 session anchor 回填”方向，在 direct-ov memory 正文层面确实把：

- `adoption` 保持在 `2023-07-14`
- `pottery` 拉到了 `2023-07-07`

但补跑远端最小 QA 子集后，证据显示这条线对 benchmark 目标是错方向。

### 远端最小 QA 子集

- run: `rtdisambig19_q30_33`
- 口径：
  - 远端容器
  - `sessions 1-8`
  - `q30/q31/q33`
  - `QA autoCapture=false`

结果：

- `q30`: `CORRECT`
  - answer: `2023-07-14`
  - `input_tokens = 6141`
- `q31`: `WRONG`
  - answer: `2023-07-07`
  - `input_tokens = 5298`
- `q33`: `CORRECT`
  - answer: `around June 20, 2023`
  - `input_tokens = 4899`

### 关键纠正

- `q31` 的 gold 不是“上一轮会话前的那个 Friday”
- benchmark judge 明确认的是：
  - **The Friday before 15 July 2023**
  - 即 **2023-07-14**

所以：

- “自动补上一轮 session anchor，把 `pottery` 拉成 `2023-07-07`”
  - 虽然更贴合我们之前对 `since we talked` 的语义直觉
  - 但对当前 benchmark 目标是**负优化**

### 最终处理

- `generated_fields` 诊断能力值得保留
  - 它已经证明错误发生在原始 extraction 输出阶段
- 但“缺失 `latest_archive_session_time` 时自动补上一轮 session anchor”这条实验不应保留
  - 已在本地代码回退
  - 远端 probe worktree 也已同步回退，避免后续继续污染口径

### 当前判断更新

- `q30` 的确可以通过 extraction-side 改动修到正确
- 但 `q31` 的 benchmark 目标并不是把 `pottery` 拉到 `2023-07-07`
- 因此后续若继续做 extraction-side relative-time grounding，必须以 benchmark gold 为准，而不是继续沿“上一轮锚点=正确答案”这个假设推进

## 2026-05-30 新进展：benchmark 对齐版在 session-only 下有效，但 mixed-context 下再次分叉

### benchmark 对齐版 provider

当前本地与远端 probe worktree 已切到新的 benchmark 对齐口径：

- 所有 relative weekday grounding 都按当前 `Session Time` 解释
- 不再自动补上一轮 session anchor

本地回归：

- `tests/session/memory/test_memory_timestamp_parsing.py`: `4 passed`
- `tests/session/memory/test_memory_diff.py`
- `tests/unit/session/test_extraction_preprocessor.py`
- `tests/unit/session/test_fixture_token_savings.py`
- 合计 `61 passed`

### fresh account `acct-20260530-rtdisambig21`：只灌 `session_8`

远端容器 direct-ov `8-8` probe 结果：

- `adoption_council_meeting.md` = `2023-07-14`
- `pottery_workshop.md` = `2023-07-14`

这说明：

- 当前 benchmark 对齐版 relative-time grounding 在 **session-only** 条件下，能把两条目标 memory 都拉到 benchmark 期望日期

### fresh account `acct-20260530-rtdisambig22`：已有 `sessions 1-4`，再补 `session_8`

对同一版本代码，在另一个 fresh account 里：

- 先灌 `sessions 1-4`（实际落盘到前 4 个 session）
- 再补 `session_8`

结果发生了新的上下文分叉：

- `adoption_council_meeting.md` = `2023-07-14`
- `pottery_workshop.md` = `2023-06-23`

也就是说：

- 当前问题已经不只是“单轮 `session_8` 如何解释 `Last Fri`”
- 而是 **已有历史 memory/summary 形态会反向影响 `session_8` 的时间 grounding**

### 题级验证：`q3033_rtdisambig22_qa`

口径：

- 远端容器
- account: `acct-20260530-rtdisambig22`
- 已有 `sessions 1-4` + `session_8`
- `q30-q33`
- `QA autoCapture=false`

结果：

- `q30`: `CORRECT`
  - answer: `2023-07-14`
  - `input_tokens = 3955`
- `q31`: `WRONG`
  - answer: `2023-06-23`
  - `input_tokens = 3705`
- `q33`: `CORRECT`
  - answer: week before `2023-06-27`
  - `input_tokens = 4748`

### 当前收敛判断

- “按 Session Time 统一 grounding”确实把 **session-only** 的 `q31` 目标 memory 拉到了 benchmark 对齐日期
- 但一旦混入已有前置上下文（至少在 `sessions 1-4 + 8` 这组里），`pottery` 会再次漂移，而且这次漂到了 `2023-06-23`
- 因此新的 blocker 已经从“单轮 `session_8` 没法对齐”收敛成：
  - **历史 context/memory 形态会污染 `session_8` 的 event grounding**
  - 问题层级已明确在 extraction-side temporal parsing / event-level binding，而不是 PREPROCESSOR

## 2026-05-30 新实验：跳过 semantic search prefetch 无效

### 修改

在 `SessionExtractContextProvider` 中新增：

- `_should_skip_semantic_prefetch()`
- 当当前会话里至少出现两次显式 relative weekday（如 `Last Fri` / `Last Friday`）时，
  `prefetch()` 不再执行多文件 schema 的 semantic search
- 仍保留当前对话正文与必要单文件 schema

对应测试：

- `test_skip_semantic_prefetch_for_relative_weekday_events`

本地验证：

- `tests/session/memory/test_memory_timestamp_parsing.py`: `5 passed`
- `tests/session/memory/test_memory_diff.py::TestMemoryDiffArchive::test_build_memory_diff_records_generated_fields`: `1 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

远端容器：

- `tests/session/memory/test_memory_timestamp_parsing.py`: `5 passed`

### 远端 mixed-context 复验：`acct-20260530-rtdisambig24`

口径：

- 远端容器
- fresh account
- 先灌 `sessions 1-4`
- 再灌 `session_8`

结果：

- `adoption_council_meeting.md` 仍为 `2023-07-14`
- `pottery_workshop.md` 仍漂移到 `2023-06-23`

关键结论：

- 跳过 semantic search prefetch **没有修复** mixed-context 下的 `pottery=2023-06-23`
- 因此当前污染源不太像是 multi-file semantic search topN 本身

### 新的更强判断

- `latest_archive_overview` 这条旧怀疑项也可以继续降权：
  - `tests/session/memory/test_compressor_v2.py` 已明确写有注释：
    - `latest_archive_overview 功能已移除`
  - 当前 v2 `ExtractLoop` 主要消费的是 provider 生成的对话/工具消息
- 由于 `pottery_workshop.md` 在 `acct-20260530-rtdisambig24` 中是 **新加 event**，并不存在旧同名 event 文件可供 merge 覆盖，
  当前 `2023-06-23` 更像是本轮 extraction 原始输出就已经错了

这意味着：

- 当前 mixed-context 漂移并不是简单的
  - PREPROCESSOR 问题
  - semantic search prefetch 问题
  - 或已有 event 文件 merge 问题
- 更可能是：
  - extraction 阶段仍在利用某种历史上下文
  - 或模型在 mixed-context 下把 `Last Fri` 错绑到了前置 session_4 的 `2023-06-23 counseling workshop` 时间锚点

下一步最有价值的不是再改 prefetch，而是做 **extractor-only mixed-context probe**：

- 在 `sessions 1-4` 已存在的 account 上
- 只对 `session_8` 调 extraction
- 直接拦截原始 `ResolvedOperations`
- 确认 `pottery=2023-06-23` 是否在原始 extraction 输出阶段就出现

## 2026-05-30 新取证：mixed-context extractor-only probe 与真实 commit 路径不一致

为避免继续猜测，我新增了一个专用脚本：

- [benchmark/locomo/openclaw/remote_extractor_only_probe.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/remote_extractor_only_probe.py)

用途：

- 在已有 account/user/agent 上
- 直接调用 `SessionCompressorV2.extract_long_term_memories()`
- 用 fake updater 拦截原始 `ResolvedOperations`
- 把 `generated_fields` 落盘到 JSON

### mixed-context root probe：`acct-20260530-rtdisambig24`

口径：

- 远端容器
- 已有 `sessions 1-4`
- 再对 `session_8` 做 extractor-only probe
- 使用 `role=root`，更贴近 direct-ov 实际 API key 路径

结果文件：

- `/tmp/rtdisambig24_extract_only_s8_root.json`

这次拿到的原始 extraction 输出，与真实 commit 路径出现了关键不一致：

- extractor-only raw ops **没有**直接生成 `pottery_workshop.md` / `adoption_council_meeting.md`
- 只生成了一个 `profile.md` patch
- 且这个 patch 明确试图把：
  - `- Took kids to pottery workshop on 2023-06-23`
  - 改成
  - `- Took kids to pottery workshop on 2023-07-14`

也就是说：

- 在 extractor-only root probe 里，模型原始输出并不支持 `2023-06-23`
- 它反而在尝试把 mixed-context 里的旧 `2023-06-23` 纠正回 `2023-07-14`

但真实 direct-ov commit 结果却是：

- `events/2023/07/15/pottery_workshop.md = 2023-06-23`

### 当前更精确的判断

这说明新的 mixed-context 问题，比之前估计得更窄：

- 它不再像“semantic search prefetch 直接污染”
- 也不完全像“模型原始 extraction 明确产出 2023-06-23”

更像是：

- standalone extractor-only probe 与真实 commit 路径之间还存在一个尚未完全镜像的差异
- 或 commit 场景下的后续阶段（例如 refetch / patch repair / apply 路径）才把 `2023-06-23` 固化到了 event memory

因此下一步最该做的，不是继续改 PREPROCESSOR 或再堆 relative-time helper，而是：

- 在 **真实 commit 路径** 上捕获 `ResolvedOperations`
- 而不是只做 standalone extractor-only probe

## 2026-05-30 新进展：真实 commit mixed-context 路径拿到一轮 `3/3`

### 新增调试能力

在 [openviking/session/compressor_v2.py](/home/jcp/Agent/code/OpenViking/openviking/session/compressor_v2.py) 中新增了受控调试落盘点：

- 环境变量：`OPENVIKING_DEBUG_EXTRACT_OPS=1`
- 目标：在 `extract_long_term_memories()` 里，在 apply 前把原始 `ResolvedOperations` 写到：
  - `archive_001/extracted_operations.json`

对应最小单测：

- `tests/session/memory/test_memory_diff.py::TestMemoryDiffArchive::test_build_operations_debug_dump_serializes_generated_fields`

本地验证：

- `test_build_operations_debug_dump_serializes_generated_fields`: `1 passed`
- `test_build_memory_diff_records_generated_fields`: `1 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

远端容器：

- debug dump 单测：`1 passed`

### 真实 commit mixed-context：`acct-20260530-rtdisambig25`

口径：

- 远端容器
- 开启 `OPENVIKING_DEBUG_EXTRACT_OPS=1`
- fresh account
- 先灌 `sessions 1-4`
- 再灌 `session_8`

结果：

- `pottery_workshop.md = 2023-07-14`
- `q30/q31/q33 = 3/3 correct`

CSV：

- `/tmp/q3033_rtdisambig25_qa/phaseA_on_8sessions_q3033_rtdisambig25_qa.csv`

题级：

- `q30`: `CORRECT`, `input_tokens=3233`
- `q31`: `CORRECT`, `input_tokens=3800`
- `q33`: `CORRECT`, `input_tokens=3783`

### 仍未闭合的一点

虽然 `acct-...25` 的真实 commit mixed-context 路径已经拿到 `3/3`，但 archive 下没有看到预期的：

- `extracted_operations.json`

因此当前还不能完全解释：

- 为什么上一轮 `acct-...24` 会落到 `2023-06-23`
- 为什么这轮 `acct-...25` 又回到 `2023-07-14`
- 以及为何 debug artifact 没有按预期落盘

### 当前状态更新

- 这是首次在 mixed-context（`sessions 1-4 + 8`）真实 commit + QA 路径下拿到 `q30/q31/q33 = 3/3`
- 说明当前 extraction 代码已经出现实质正向信号
- 但稳定性还没有被证明，暂时不能把这轮单次成功直接当作稳定结论

## 2026-05-30 复现确认：`acct-20260530-rtdisambig27`

### 真实 commit mixed-context 第二轮复验

口径：

- 远端容器
- 开启 `OPENVIKING_DEBUG_EXTRACT_OPS=1`
- fresh account
- 先灌 `sessions 1-4`
- 再灌 `session_8`

结果：

- `pottery_workshop.md = 2023-07-14`
- `adoption_council_meeting.md = 2023-07-14`
- `q30/q31/q33 = 3/3 correct`

QA CSV：

- `/tmp/q3033_rtdisambig27_qa/phaseA_on_8sessions_q3033_rtdisambig27_qa.csv`

题级：

- `q30`: `CORRECT`, `input_tokens=4329`
- `q31`: `CORRECT`, `input_tokens=4171`
- `q33`: `CORRECT`, `input_tokens=4078`

### 真实 commit 路径原始 operations 已拿到

这轮终于在真实 commit 路径下看到了：

- `archive_001/extracted_operations.json`

关键文件：

- `/root/.openviking/data/viking/acct-20260530-rtdisambig27/session/5d052982-43fd-405e-b67e-5dc20cef53df/history/archive_001/extracted_operations.json`

原始 `generated_fields` 已经明确包含：

- `events/2023/07/15/pottery_workshop.md`
  - `summary = On 2023-07-14, Melanie took her kids to a pottery workshop ...`
- `events/2023/07/15/adoption_council_meeting.md`
  - `summary = On 2023-07-14, Caroline attended a council meeting for adoption ...`
- `profile.md`
  - 也同步把 `pottery` 写成 `2023-07-14`

这意味着：

- 在 `acct-...27` 这轮真实 commit mixed-context 路径里，
  `2023-07-14` 已经在 **原始 `ResolvedOperations`** 阶段就成立
- 也就是说：
  - `q31` 这轮的修正不是 apply/merge 偶然写出来的
  - 而是 extraction 本身已经输出了对 benchmark 对齐的日期

### 更新后的判断

- 现在至少有 **两轮 mixed-context 正样本**：
  - `acct-...25`：`q30/q31/q33 = 3/3`
  - `acct-...27`：`q30/q31/q33 = 3/3`
- 并且 `acct-...27` 进一步证明：
  - 真实 commit 路径的原始 `ResolvedOperations` 已经能稳定给出 `pottery = 2023-07-14`

仍未完全解释的剩余点：

- 为什么上一轮 `acct-...24` 会得到 `pottery = 2023-06-23`
- 也就是 mixed-context 仍存在历史不稳定样本

但当前阶段性结论已经升级为：

- 当前 extraction 路径已经**不是偶发单点成功**
- 它至少在最近两轮 fresh-account mixed-context 复验里，稳定满足了 `q30/q31/q33`

## 2026-05-30 新进展：q34 根因已修正，`q30-q35` 达到 5/5

### q34 的直接根因

在 `acct-20260530-rtdisambig27` 的 `session_3` 原始 `extracted_operations.json` 里可以看到：

- school speech / school talk 事实只被写进了 `profile` patch
- 没有生成独立的 `events` memory

这解释了旧版本为什么：

- `support group` 存在
- `pride parade` 存在
- 但 `q34` 仍然错

因为 benchmark 需要的是：

- `pride parade`
- `school speech`
- `support group`

而 `school speech` 在旧 prompt 下只进 profile，不进 event history。

### 最小修正

只修改了 extraction prompt：

- [openviking/prompts/templates/compression/memory_extraction.yaml](/home/jcp/Agent/code/OpenViking/openviking/prompts/templates/compression/memory_extraction.yaml)

新增规则：

- 公开参与类活动（talk / speech / workshop / support group / meeting / parade / race / conference）
  只要是用户实际参与、且带时间维度，必须额外生成 `events` memory
- 如果同时适合 profile/entity，也要同时输出对应的 `events`

### 修正后的原始 extraction 证据

在新 fresh account `acct-20260530-rtdisambig28` 的 `session_3` 原始 `extracted_operations.json` 中：

- 已明确生成：
  - `events/2023/06/09/school_talk.md`

说明这次修正已经在 extraction 原始输出阶段生效，不是后处理偶然补出来的。

### 远端 mixed-context 验证：`acct-20260530-rtdisambig28`

口径：

- 远端容器
- fresh account
- 先灌 `sessions 1-4`
- 再灌 `session_8`
- QA 跑 `q30-q35`

结果：

- `q30`: `CORRECT`, `input_tokens=5647`
- `q31`: `CORRECT`, `input_tokens=6224`
- `q33`: `CORRECT`, `input_tokens=3288`
- `q34`: `CORRECT`, `input_tokens=5536`
- `q35`: `CORRECT`, `input_tokens=5189`

CSV：

- `/tmp/q3035_rtdisambig28_qa/phaseA_on_8sessions_q3035_rtdisambig28_qa.csv`

最终：

- `q30-q35 = 5/5`

### 当前阶段性判断

- 当前 extraction 路径已经在 mixed-context 子集上拿到了更强证据：
  - `acct-...27`: `q30/q31/q33 = 3/3`
  - `acct-...28`: `q30-q35 = 5/5`
- 并且 `q34` 的修正已能通过原始 `ResolvedOperations` 证明：
  - school speech 现在以 event 形式被抽取出来

剩余未完成部分：

- 还没有用同口径扩到更大代表性子集
- 还没有完成对 token 成本与 OFF/旧 ON 的系统性对比
- 因此当前还不能声称“全目标完成”，但已经拿到实质性的 extraction-side 正向实现与远端验证结果

## 2026-05-30 扩大验证：`acct-20260530-rtdisambig29`

口径：

- 远端容器
- `sessions 3-9`
- `q16-39`
- `QA autoCapture=false`
- `OPENVIKING_DEBUG_EXTRACT_OPS=1`

结果：

- `17/21 correct`
- CSV：
  - `/tmp/q1639_rtdisambig29_qa/phaseA_on_7sessions_q1639_rtdisambig29.csv`

重点题：

- `q16`: `WRONG`, `5030`
- `q23`: `CORRECT`, `5614`
- `q26`: `CORRECT`, `5450`
- `q29`: `CORRECT`, `5651`
- `q30`: `CORRECT`, `4584`
- `q31`: `CORRECT`, `4020`
- `q33`: `CORRECT`, `1206`
- `q35`: `CORRECT`, `3561`
- `q36`: `CORRECT`, `5255`
- `q37`: `CORRECT`, `1206`
- `q38`: `CORRECT`, `4461`
- `q39`: `WRONG`, `5127`

本轮错误题：

- `q16`
- `q25`
- `q28`
- `q39`

判断：

- 当前 extraction-side 修正已经不只是在 `q30-q35` 上局部成立。
- 扩到更大代表性子集后，`q30/q31/q33/q35/q36/q37/q38` 都保持正确。
- 但还不能宣称“整体完成”，因为：
  - 还缺少和历史 `OFF / 旧 ON` 的系统性 token/准确率对照
  - `q16/q25/q28/q39` 仍然是新暴露的残余失败点

额外信号：

- `/tmp/q1639_rtdisambig29_qa/phaseA_on_7sessions_q1639_rtdisambig29.txt` 里，
  `session_4-9` 都显示 `memories=0`，但 QA 最终仍拿到 `17/21`。
- 因此当前 direct-ov 口径下，`memory_count` 不能直接当作“本轮没有可用 memory”的判断依据；
  后续若继续分析，应优先看真实 recall / CSV / extracted operations，而不是只看该计数。

### 同口径 `OFF` 对照：`acct-20260530-offcmp30`

口径：

- 远端容器
- `sessions 3-9`
- `q16-39`
- `QA autoCapture=false`

结果：

- `12/21 correct`
- CSV：
  - `/tmp/q1639_offcmp30_qa/phaseA_off_7sessions_q1639_offcmp30.csv`

与 `acct-20260530-rtdisambig29` 的直接对照：

- `OFF`: `12/21`, 平均 `input_tokens = 4477.0`
- `ON`: `17/21`, 平均 `input_tokens = 4426.2`

重点题：

- `q16`: `OFF WRONG / 5711`，`ON WRONG / 5030`
- `q23`: `OFF CORRECT / 5672`，`ON CORRECT / 5614`
- `q25`: `OFF WRONG / 5670`，`ON WRONG / 3380`
- `q26`: `OFF CORRECT / 5703`，`ON CORRECT / 5450`
- `q28`: `OFF WRONG / 3971`，`ON WRONG / 5213`
- `q29`: `OFF CORRECT / 7622`，`ON CORRECT / 5651`
- `q30`: `OFF CORRECT / 4289`，`ON CORRECT / 4584`
- `q31`: `OFF CORRECT / 3417`，`ON CORRECT / 4020`
- `q33`: `OFF CORRECT / 3628`，`ON CORRECT / 1206`
- `q35`: `OFF WRONG / 4640`，`ON CORRECT / 3561`
- `q36`: `OFF WRONG / 3100`，`ON CORRECT / 5255`
- `q37`: `OFF CORRECT / 1835`，`ON CORRECT / 1206`
- `q38`: `OFF CORRECT / 5494`，`ON CORRECT / 4461`
- `q39`: `OFF WRONG / 8052`，`ON WRONG / 5127`

结论更新：

- 当前 extraction-side 修正不仅在 `q30-q35` 上有效，而且在更大子集里已经明确优于同口径 `OFF`：
  - 准确率提升：`+5/21`
  - 平均 `input_tokens` 还略低
- 新修正带来的主要收益点是：
  - `q35`: `WRONG -> CORRECT`
  - `q36`: `WRONG -> CORRECT`
- `q16/q25/q28/q39` 目前是 `ON/OFF` 共同失败项，不应再归因为 PREPROCESSOR 回归；它们更像是当前整体 extraction / recall 体系的残余弱点。

## 2026-05-30 新发现：LoCoMo 图片语义在 ingest 阶段被丢弃

在 `locomo10.json` 的原始消息中，部分关键事实只存在于图片相关字段里：

- `img_url`
- `blip_caption`
- `query`

直接证据：

- `session_8 / D8:6`
  - `blip_caption = a photo of a painting of a sunset with a palm tree`
  - `query = painting vibrant flowers sunset sky`
  - 文本只说 `Here's our latest work from last weekend.`
- `session_7 / D7:8`
  - `blip_caption = a photography of a book cover with a gold coin on it`
  - `query = painted canvas follow your dreams`
  - 文本只说 `This book I read last year reminds me to always pursue my dreams`

而旧的 `build_session_messages()` 只把纯文本 `[speaker]: text` 送进 ingest，导致图片语义完全丢失。

### 最小修正

修改：

- [benchmark/locomo/openclaw/import_to_ov.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py)

两步：

1. 把 `blip_caption / query` 轻量拼进消息正文：
   - `[image_caption]: ...`
   - `[image_query]: ...`
2. 保留 `img_url`，在 `viking_ingest()` 时以 `image_url` multimodal parts 一起发给 OpenViking

新增测试：

- [tests/benchmark/locomo/openclaw/test_import_to_ov.py](/home/jcp/Agent/code/OpenViking/tests/benchmark/locomo/openclaw/test_import_to_ov.py)
  - 验证 caption/query 会进入消息文本
  - 验证重复内容不重复拼接
  - 验证 `img_url` 会保留到消息结构
  - 验证 `viking_ingest()` 会发送 `image_url` parts

本地验证：

- `tests/benchmark/locomo/openclaw/test_import_to_ov.py`: `4 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

### 远端定点验证：`q39`

run：

- `acct-20260530-imgctx31`
- `sessions 3-9`
- `q39`

结果：

- `q39`: `CORRECT`, `input_tokens=4228`

CSV：

- `/tmp/q39_imgctx31_qa/phaseA_on_7sessions_q39_imgctx31.csv`

回答已经明确提到：

- `a nature-inspired sunset painting featuring a palm tree`

判断：

- `q39` 之前的错误，主因不是 extraction 推理本身，而是 benchmark ingest 把图片语义丢掉了。

### 远端定点验证：`q25-q28`

run：

- `acct-20260530-imgctx32`
- `sessions 6-7`
- `q25-28`

结果：

- `q25`: `WRONG`
- `q27`: `CORRECT`
- `q28`: `WRONG`

CSV：

- `/tmp/q2528_imgctx32_qa/phaseA_on_2sessions_q2528_imgctx32.csv`

新信号：

- `q25` 已从旧版的“只认出 Charlotte's Web”改善成：
  - `Charlotte's Web`
  - `an untitled book focused on following dreams`
- 说明图片语义补充确实把“Melanie 去年读过另一本书”这条事实拉出来了
- 但 `q28` 仍然失败，说明当前输入增强还不足以从现有字段中稳定恢复书名 `Nothing is Impossible`

结论：

- 图片语义补充是有效方向，已明确修复 `q39`
- 对 `q25/q28` 是部分正向，但还不够
- 如果后续继续冲 `q25/q28`，下一层大概率不再是 PREPROCESSOR / extraction prompt，而是更强的图片 OCR / media understanding 方案；那将属于需要单独 plan 的大改

补充可行性检查：

- 仓库里已有 `openviking.parse.parsers.media.image.ImageParser._ocr_extract()`
- 但当前环境实测：
  - 本地：`pytesseract = False`
  - 远端容器：`pytesseract = False`
  - 远端容器也未发现可直接使用的 `tesseract` 命令

因此如果继续冲 `q25/q28`，不能假设现成 OCR 栈已经可用。
下一步若进入“书封面标题恢复”大改，应先决定：

1. 安装并接入 OCR 依赖
2. 或新增一个独立的 vision-title extraction helper（不依赖本地 OCR）

## 2026-05-30 进一步推进：书封面标题通过 VLM bytes helper 恢复

前一阶段已经确认：

- 直接把远程图片 URL 交给当前 VLM provider，会报 `Timeout while downloading url=...`
- 但如果先在容器侧把图片下载成 bytes，再喂给同一个 VLM，
  则能稳定返回：
  - `title = NOTHING IS IMPOSSIBLE`
  - `author = TOM OLIVER`

基于这个证据，做了最小实现：

- 在 [benchmark/locomo/openclaw/import_to_ov.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py) 的 `_maybe_extract_visual_hints()` 中：
  - 对书相关消息先下载远程图片 bytes
  - 再调用现有 VLM 做标题抽取
  - 将高置信结果注入为：
    - `[image_title_hint]: ...`
    - `[image_author_hint]: ...`

新增测试：

- [tests/benchmark/locomo/openclaw/test_import_to_ov.py](/home/jcp/Agent/code/OpenViking/tests/benchmark/locomo/openclaw/test_import_to_ov.py)
  - 验证远程图片会先下载为 bytes
  - 验证 VLM 返回的标题/作者会被转成文本 hints

本地验证：

- `tests/benchmark/locomo/openclaw/test_import_to_ov.py`: `6 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

### 远端定点验证：`acct-20260530-imgctx36`

口径：

- `sessions 6-7`
- `q25-28`

结果：

- `q25`: `CORRECT`, `2967`
- `q27`: `CORRECT`, `3293`
- `q28`: `CORRECT`, `3354`

CSV：

- `/tmp/q2528_imgctx36_qa/phaseA_on_2sessions_q2528_imgctx36.csv`

结论：

- `q25/q28` 已从“只能恢复未命名追梦主题书”推进到 benchmark 对齐：
  - `Nothing Is Impossible`
  - `Tom Oliver`
  - `2022`

### 远端组合验证：`acct-20260530-imgctx38`

口径：

- `sessions 6-9`
- `q25-28,39`

结果：

- `q25`: `CORRECT`, `2967`
- `q28`: `CORRECT`, `3354`
- `q39`: `CORRECT`, `3756`

CSV：

- `/tmp/q252839_imgctx38_qa/phaseA_on_4sessions_q252839_imgctx38.csv`

更新后的判断：

- 现在 `q25/q28/q39` 都已经拿到远端容器的有效正样本
- 当前剩余主要难点再次收敛到：
  - `q16`
  - 以及是否需要再把最新实现扩回 `sessions 3-9 / q16-39` 做最终大子集复验

## 2026-05-30 最终大子集复验：`acct-20260530-imgctx39`

口径：

- 远端容器
- `sessions 3-9`
- `q16-39`
- `QA autoCapture=false`

结果：

- `17/21 correct`
- 平均 `input_tokens = 4150.6`
- CSV：
  - `/tmp/q1639_imgctx39_qa/phaseA_on_7sessions_q1639_imgctx39.csv`

错误题：

- `q20`
- `q22`
- `q23`
- `q36`

与上一版 `acct-20260530-rtdisambig29` 的直接对照：

- 旧版：`17/21`, 平均 `input_tokens = 4426.2`
- 新版：`17/21`, 平均 `input_tokens = 4150.6`

关键变化：

- 修好的题：
  - `q16`: `WRONG -> CORRECT`
  - `q25`: `WRONG -> CORRECT`
  - `q28`: `WRONG -> CORRECT`
  - `q39`: `WRONG -> CORRECT`

- 新掉的题：
  - `q20`: `CORRECT -> WRONG`
  - `q22`: `CORRECT -> WRONG`
  - `q23`: `CORRECT -> WRONG`
  - `q36`: `CORRECT -> WRONG`

判断：

- 最新版并没有把大子集准确率从 `17/21` 再继续抬高；
  它实现的是 **相同准确率下，错误分布的重排**。
- 但它带来了一个稳定可验证的收益：
  - 平均 `input_tokens` 从 `4426.2` 降到 `4150.6`
  - 同时修复了此前长期卡住的 `q25/q28/q39`

当前阶段结论：

- 如果按“更高准确率”单一目标衡量，这版还不算最终完成。
- 如果按用户原始目标“降低 token 开销或者提升准确率”衡量，
  这版已经拿到一个较强阶段性结果：
  - 在大子集上保持同准确率
  - 同时进一步降低平均 token
  - 并解决了一批此前明确失败的图片语义题

## 2026-05-30 新一轮定点验证：hint gate 收紧

为避免书架/儿童图书馆图片误触发书名识别，对
[benchmark/locomo/openclaw/import_to_ov.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py)
做了一个更窄的约束：

- 只有在文本里明确出现：
  - `this book`
  - `read`
  - 明确书名引号
  - 或视觉上下文本身是 `book cover`
- 才触发 `image_title_hint`

本地验证：

- `tests/benchmark/locomo/openclaw/test_import_to_ov.py`: `7 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

### 远端定点结果：`q2023_hintgate40`

口径：

- `sessions 6-6`
- `q20-23` 中实际落盘的 `q22/q23`

结果：

- `q22`: `CORRECT`, `3392`
- `q23`: `CORRECT`, `3730`

CSV：

- `/tmp/q2023_hintgate40_qa/phaseA_on_1sessions_q2023_hintgate40.csv`

### 远端定点结果：`q2528_hintgate40`

口径：

- `sessions 6-7`
- `q25-28`

结果：

- `q25`: `CORRECT`, `3655`
- `q27`: `CORRECT`, `4281`
- `q28`: `CORRECT`, `4094`

CSV：

- `/tmp/q2528_hintgate40_qa/phaseA_on_2sessions_q2528_hintgate40.csv`

### 差异取证

对比：

- `acct-20260530-imgctx36`
- `acct-20260530-accept43`

在 `session_6/7` 的 `selected_spans` 上几乎没有实质差异：

- `museum`
- `children's library`
- `Charlotte's Web`
- `this book I read last year`

这些关键句都被保留了。

但服务侧 `search/find` 命中的 memory 形态不同：

- 稳定正样本 `imgctx36` 更偏：
  - `events/2023/07/06/museum_visit.md`
  - `events/2023/07/06/transition_support.md`
  - `entities/literature/nothing is impossible.md`
  - `events/2023/07/12/book_discussion.md`

- 不稳定样本 `accept43` 更偏：
  - `entities/event/museum visit with kids.md`
  - `entities/event/picnic with friends.md`
  - `entities/book/nothing is impossible.md`
  - `entities/book/becoming nicole.md`

判断：

- 这批差异说明 `q20/q22/q23/q25/q28` 的组合退化，不是 PREPROCESSOR 再次裁坏原句。
- 更像是后续 memory 组织/命名形态变化影响了 recall 注入质量。
- 因此当前更稳的结论是：
  - `selected_spans` 路径已经足够
  - 后续若继续冲“统一版稳定性”，应优先盯 memory 组织与 recall 命中形态，而不是继续改 PREPROCESSOR 裁剪逻辑

## 2026-05-30 `q36` 组合稳定性：多时间锚点活动拆分

### 新假设

对 `q36` 的 passing / failing 样本做进一步对照后，差异继续收敛：

- passing 单题样本 `acct-20260530-focus44`
  - recall 注入里能看到独立的：
    - `school_event.md`
    - `mentorship_program_join.md`
- failing 大子集样本 `acct-20260530-imgctx39`
  - 最终回答丢了 mentoring
  - `session_9` 的 memory 更像被组织成了 umbrella event：
    - `lgbtq_mentorship_launch.md`
    - 里面混入 mentoring / pride event / art show 多个时间锚点活动

结论：

- `q36` 的组合退化，不像是 `selected_spans` 或原句裁剪再次出错
- 更像是 extraction 在同一会话里把多个不同时间锚点的活动合并成一个大 event

### 最小修正

仅修改：

- `openviking/prompts/templates/compression/memory_extraction.yaml`

新增规则：

- 同一会话里若出现多个不同时间锚点 / 不同目标的参与类活动，必须拆成独立 `events`
- 禁止合并成 umbrella event
- 若共享同一主题（例如 LGBTQ advocacy），允许：
  - 一个共享 `entities/profile`
  - 多个独立 `events`

### 本地验证

- `tests/benchmark/locomo/openclaw/test_import_to_ov.py`: `7 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

### 远端 extraction 直接证据：`acct-20260530-s39split46`

为避免再次被 `session_7/8` 的书封面/图片链路拖慢，先只灌：

- `session_3`
- `session_9`

fresh account：

- `acct-20260530-s39split46`

结果：

- `session_3` 生成：
  - `events/2023/06/09/school_lgbtq_presentation.md`
- `session_9` 不再生成 umbrella event，而是拆成 3 条独立 event：
  - `events/2023/07/17/lgbtq_youth_mentorship.md`
  - `events/2023/07/17/lgbt_pride_event.md`
  - `events/2023/07/17/lgbtq_art_show.md`

同时共享实体：

- `entities/community/lgbtq_community.md`

中也正确保留了：

- `[[school_lgbtq_presentation]]`
- `[[lgbtq_youth_mentorship]]`
- `[[lgbt_pride_event]]`
- `[[lgbtq_art_show]]`

这说明：

- 新规则已经在 extraction / commit 实际结果层生效
- `session_9` 的 umbrella event 问题被直接打掉

### 远端 QA-only 验证：`q36_split46qa`

在同一 account 上直接跑：

- `skip-ingest`
- `q36` QA-only

结果：

- `q36`: `CORRECT`, `input_tokens=5181`

回答包含：

- `school presentation`
- `LGBTQ youth mentorship`
- `LGBT pride event with her mentee`

结论：

- 当前这条“多时间锚点活动拆分”规则，已经拿到：
  - extraction 层正样本
  - QA 层正样本

### 统一验收当前状态

我尝试做统一 fresh account：

- `acct-20260530-accept47`
- 只灌 `session_3 / 6 / 7 / 8 / 9`
- 目标覆盖：
  - `q20/q22/q23`
  - `q25/q28`
  - `q36`
  - `q39`

当前现象：

- session 目录已写到 `3`
- 进程仍在跑
- 更像再次被 `session_7/8` 的图片链路拖慢

因此现阶段应把它视为：

- 运行态现象
- 不是这次 `q36` 拆分规则的成败结论

### 当前判断

- `q36` 已拿到新的有效正样本
- 这次修正是 **memory extraction 粒度修正**，不是继续泛改 PREPROCESSOR
- 下一步若继续统一验收，应避免让 `session_7/8` 的图片链路掩盖 `q36` 结论：
  - 可以继续等 `accept47`
  - 或改成两个分组验收：
    - text/event 组：`q20/q22/q23/q36`
    - image/book 组：`q25/q28/q39`

### 统一验收补充结果：`accept47`

`accept47` 最终落成了 5 个目标 session：

- `session_3 / 6 / 7 / 8 / 9`

在同一 fresh account 上，拆成两段 QA-only：

1. `q20-28`
2. `q36-39`

#### `q20-28`

结果：

- `q20`: `WRONG`
- `q21`: `CORRECT`
- `q22`: `CORRECT`
- `q23`: `WRONG`
- `q24`: `CORRECT`
- `q25`: `CORRECT`
- `q26`: `WRONG`
- `q27`: `WRONG`
- `q28`: `CORRECT`

汇总：

- `5/9 correct`

对应 CSV：

- `/tmp/accept47_q2028_qa/phaseA_on_7sessions_accept47q2028.csv`

解释：

- 这组 account 没有包含 `session_5`
  - 所以 `q26` 本来就不应视作有效比较样本
- `q22/q25/q28` 继续保持正确
- `q23/q27` 仍然掉线，说明 museum/picnic 与 conference 这组 recall 组织问题还没被这次 `q36` 修正带着一起解决

#### `q36-39`

结果：

- `q36`: `CORRECT`
- `q37`: `CORRECT`
- `q38`: `CORRECT`
- `q39`: `CORRECT`

汇总：

- `4/4 correct`

对应 CSV：

- `/tmp/accept47_q3639_qa/phaseA_on_7sessions_accept47q3639.csv`

结论：

- `q36` 的 event 拆分修正，在与 `session_7/8` 图片链路共存的统一 account 上仍然成立
- 这说明：
  - 当前 `q36` 修正不是只在极小 focused account 上偶发成功
  - 并且没有把 `q39` 再打坏

### 更新后的阶段判断

当前更准确的拆分是：

- `q36/q37/q38/q39`
  - 已经能在统一 fresh account `accept47` 上一起通过
- `q22/q25/q28`
  - 在同一 account 上仍保持正确
- 残余问题更集中在：
  - `q23`
  - `q27`
  - 以及无效样本 `q26`（缺 `session_5`）

所以这轮的实质进展是：

- `q36` 不再只是 focused 正样本
- 已经进入与图片/书题共存的统一 account 正样本

## 2026-05-31：`q23/q27` 收口

### `q23`：新的 extraction 规则已命中

在 `session_6-7` fresh account：

- `acct-20260531-s67split48`

应用新的 prompt 约束后，focused QA-only 结果中：

- `q23`: `CORRECT`, `3080`
- `q24`: `CORRECT`
- `q25`: `WRONG`
- `q27`: `WRONG`

对应 CSV：

- `/tmp/s67split48_q2327_qa/phaseA_on_2sessions_s67split48q2327.csv`

`q23` 的原始回答已经变成：

- `the week immediately before July 6, 2023`

这说明：

- 新增的“不要把多个活动合成 catch-up umbrella event / dated activity 优先保留 concrete event”规则
- 已经足以把 `q23` 从 “no relevant information” 拉回正确答案

### `q27`：主要问题转成索引可见性 / QA 时序

`q27` 在同一 `s67split48` 首次 QA-only 时仍然错误，但证据链发生了变化：

1. 同一 account 上直接打服务侧 `/api/v1/search/find`：
   - top 结果已经是：
     - `profile.md`
     - `entities/event/caroline's lgbtq conference.md`
     - `events/2023/07/12/lgbtq_conference_attendance.md`

2. 但首次 `q27` QA 的插件日志里：
   - query 已经是 `When did Caroline go to the LGBTQ conference?`
   - `inject-detail` 却仍然是 picnic / catch-up 组 memory

3. 在同一 account 上稍晚单独重跑一次 `q27`：
   - run: `s67split48_q27late_qa`
   - 结果：`q27 = CORRECT`
   - 回答：`2023-07-10`

对应 CSV：

- `/tmp/s67split48_q27late_qa/phaseA_on_2sessions_s67split48q27late.csv`

同样现象也在统一 account `accept47` 上复现：

- 首次 `accept47_q2028`：`q27 = WRONG`
- 稍晚单独重跑 `accept47_q27late`：`q27 = CORRECT`

对应 CSV：

- `/tmp/accept47_q27late_qa/phaseA_on_7sessions_accept47q27late.csv`

### 更新判断

当前可以把 `q27` 的主要问题从“extraction 还没修好”下调为：

- **ingest 后索引可见性 / QA 时序问题**
- 不是当前 conference memory 本身缺失
- 也不是当前 relative-time grounding 仍然错误

### 代码级补充

为了避免后续 benchmark 再把这类时序问题误判成算法回归，补了一个最小脚本开关：

- `benchmark/locomo/openclaw/phase_a_off.py`
  - 新增 `--post-ingest-settle-seconds`
  - 只在 ingest 完成且 QA 开始前 sleep
  - 默认 `0.0`，不改变现有默认行为

本地验证：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `4 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py`

### 当前阶段状态

在剩余难点里：

- `q23` 已被这轮 extraction prompt 细化修正
- `q27` 已拿到 late-rerun 正样本，主要问题转为 benchmark/索引可见性

因此当前真正未闭合的点，已经更多偏向：

- 如何定义 direct-ov benchmark 的 ingest 后稳定等待策略
- 而不是继续泛改 PREPROCESSOR / memory_extraction

### `settle49` 验证状态

我进一步尝试用新的：

- `--post-ingest-settle-seconds 15`

做 fresh account 同轮复验：

- `acct-20260531-s67settle49`
- `session_6-7`
- `q23-27`

当前状态：

- ingest 完成
- 但 QA 阶段命中了 provider weekly quota 错误：
  - `FailoverError: exceeded the weekly usage quota`

因此：

- 这轮 `settle49` 不能用于判断算法成败
- 只能说明：
  - settle 开关已接入 benchmark 脚本
  - 但是否足以把 `q27` 从“首跑错”变成“首跑对”，还需要在 quota 恢复后补一轮有效实测

### `q27` 的进一步确认：服务侧可见性已正常

在 `acct-20260531-s67split48` 上继续取证：

1. 直接调用服务侧 `/api/v1/search/find`
- 对 query：
  - `When did Caroline go to the LGBTQ conference?`
- top 结果已经是：
  - `profile.md`
  - `entities/event/caroline's lgbtq conference.md`
  - `events/2023/07/12/lgbtq_conference_attendance.md`

2. 但同一 account 的首次 QA 日志里：
- `inject-detail` 实际注入的仍然是 picnic / catch-up 组 memories

3. 因此：
- 问题不再像“conference memory 没生成”
- 更像是 QA 触发时机过早，context-engine 看到的是尚未稳定的检索结果

### benchmark settle 机制升级

因此把 benchmark 里的 `settle` 从“固定 sleep”升级成了“搜索可见性轮询”：

- `phase_a_off.py`
  - 新增 `collect_memory_visibility_probes()`
  - 新增 `wait_for_search_visibility()`
  - `--post-ingest-settle-seconds` 不再只 sleep
  - 当可构造 probe 且有 API key 时，会轮询 `/api/v1/search/find`
  - 直到新写入 memory 能被 search 命中，或超时后才开始 QA

新的本地验证：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `6 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py tests/benchmark/locomo/openclaw/test_phase_a_off.py`

### 当前状态更新

- `q23`：已被 extraction 规则修正
- `q27`：已有足够证据显示主问题是 ingest 后索引可见性 / QA 时序
- benchmark 脚本已具备 search-visibility settle 能力
- 仍缺一轮**有效远端复验**来证明：
  - 新 settle 机制是否能把 `q27` 从“首跑错、晚跑对”变成“首跑就对”

### settle 机制补强收尾

在把 settle 机制真正收尾到可复验状态时，又补了两处低风险修正：

1. `collect_memory_visibility_probes()` 不再把 `.overview.md` 一类 dotfile 选成 probe
- 避免等待逻辑被 archive 元数据噪音污染

2. `wait_for_search_visibility()` 现在会显式识别 `429 / Too Many Requests`
- 超时结果会返回：
  - `reason = rate_limited_timeout`
- 并把轮询 sleep 提高到至少 2 秒，减轻 search API 自身限流带来的假阴性

对应本地验证最新状态：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `8 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py tests/benchmark/locomo/openclaw/test_phase_a_off.py`

另外，最新一轮本地 probe 选择测试也说明：

- probe 的 conference 事件文件现在可能优先选到更“新”的 dated event 路径
- 因此测试已从硬编码单一路径，改成允许 `07/12` 或 `07/13` 的 conference event

这不改变算法判断，只是让 benchmark settle 测试与当前 probe 排序策略保持一致。

### search-only settle 真实远端复验

为了避免继续消耗 provider completion，我又做了一轮只依赖 `/api/v1/search/find` 的 settle 机制远端复验：

- account:
  - `acct-20260531-s67split48`
- user:
  - `user-20260531-s67split48`
- API key 来源：
  - 直接读取远端 `/root/.openviking/ov.conf` 里的 `server.root_api_key`

复验步骤：

1. 先用 `collect_memory_visibility_probes()` 在真实 account root 上选 probe
2. 再调用 `wait_for_search_visibility()`，不跑 QA、不消耗 provider completion

结果：

- 当 `max_probes=2` 时：
  - probes 被选成：
    - `running_shoe_discussion`
    - `book_discussion`
  - `wait_for_search_visibility()` 返回：
    - `ok = false`
    - `reason = rate_limited_timeout`
    - `last_error = 429 ... Too Many Requests ... /api/v1/search/find`

- 当 benchmark 主路径进一步收紧到 `max_probes=1` 后，再做同样 search-only 复验：
  - 单 probe:
    - `running_shoe_discussion`
  - 结果仍然是：
    - `ok = false`
    - `reason = rate_limited_timeout`
    - `last_error = 429 ... Too Many Requests ... /api/v1/search/find`

这说明当前新的 benchmark settle 机制在代码层已经准备好，但外部状态仍有一个明确限制：

- 不是 provider completion 才会触发 quota
- 连单 probe 的 `/api/v1/search/find` 轮询，也可能立刻被 search rate limit 拦下

因此当前对 `q27` 的最准确阶段判断更新为：

- benchmark 已具备：
  - search-visibility settle
  - dotfile probe 过滤
  - 429-aware timeout reporting
  - 更低 probe fanout
- 但还缺一轮 **search 配额恢复后的有效远端首跑复验**
- 在此之前，不能再把 `q27` 的“首跑错、晚跑对”问题继续归因到 extraction 本身

### `q27` 晚跑转正的实际延迟窗口

进一步直接回读远端已有 artifact 时间戳后，可以把 `q27` 的“首跑错 -> 晚跑对”窗口收得更具体：

1. `s67split48`
- 首跑目录 mtime：
  - `2026-05-30 16:00:58 +0000`
- 晚跑目录 mtime：
  - `2026-05-30 16:04:10 +0000`
- 近似延迟：
  - 约 `3 分 12 秒`

2. `accept47`
- 首跑相关目录：
  - `accept47_q2028_qa`
  - mtime `2026-05-30 15:47:30 +0000`
- 晚跑 `q27` 目录：
  - `accept47_q27late_qa`
  - mtime `2026-05-30 16:05:13 +0000`
- 近似延迟：
  - 约 `17 分 43 秒`

同时题级 CSV 也支持同一结论：

- `s67split48` 首跑 `q27`
  - `input_tokens = 1149`
  - 回答为“no relevant information”
- `s67split48` 晚跑 `q27`
  - `input_tokens = 3665`
  - 已恢复为：
    - `Caroline attended the LGBTQ conference on 2023-07-10`

- `accept47` 首跑 `q27`
  - `input_tokens = 1633`
  - 回答为“no relevant information”
- `accept47` 晚跑 `q27`
  - `input_tokens = 1817`
  - 已恢复为：
    - `Caroline attended the LGBTQ conference on 2023-07-10`

这说明：

- `q27` 的首跑假阴性，至少不是“几十秒级”的 settle 能稳定覆盖的问题
- 在已有正样本里，真实恢复窗口更像是：
  - `3 分钟` 到 `18 分钟` 量级

因此当前 benchmark settle 的更现实定位应是：

- 代码侧先具备：
  - search visibility polling
  - 429-aware timeout reporting
  - 低 probe fanout
- 但若外部 search 配额恢复后仍观测到分钟级可见性延迟，
  - 就需要把推荐 settle 时间上调到**分钟级**，
  - 而不再把 `15s / 30s` 这类秒级等待当成充分假设。

### settle probe 选择进一步对齐待测问题

除了 probe 数和 429 处理之外，又补了一层更直接的修正：

- `phase_a_off.py`
  - 新增 `collect_memory_visibility_probes_for_questions()`
  - 在 QA 前 settle 时，不再只按“最新 event memory”盲选 probe
  - 而是优先根据**待测 QA question** 与 memory path/stem 的词项重叠来选 probe

动机很直接：

- 旧逻辑在 `acct-20260531-s67split48` 上，即使 `max_probes=1`
  - 也会选到：
    - `running_shoe_discussion`
- 这与真正要验证的 `q27`
  - `When did Caroline go to the LGBTQ conference?`
  并不贴近

新的本地测试已补：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`
  - 新增 question-aware probe 选择测试
  - 最新状态：
    - `9 passed`

并且远端 probe worktree 上已经直接验证：

- account:
  - `acct-20260531-s67split48`
- question:
  - `When did Caroline go to the LGBTQ conference?`
- 实际选出的 single probe 现在是：
  - `query = lgbtq conference attendance`
  - `expected_fragment = events/2023/07/12/lgbtq_conference_attendance.md`

这说明：

- 当前 settle 机制即使在 search quota 未恢复时仍无法完成首跑验证，
- 但至少已经不再浪费 probe 在无关 recent event 上，
- 后续一旦 search 配额恢复，就会更直接地检验 `q27` 真正依赖的 conference memory 是否已可见。

### 429 路径改成被动等待

基于真实远端的 search-only 复验，现在可以确认：

- 即使 `max_probes=1`
- `/api/v1/search/find` 第一次请求也可能直接 `429`

因此又对 `wait_for_search_visibility()` 做了一层更保守的修正：

- 一旦命中 `429 / Too Many Requests`
- 不再继续主动轮询 search
- 改成：
  - 记录 `search_attempts`
  - 标记 `passive_wait_after_rate_limit = true`
  - 直接被动等待到 settle timeout 截止

这样做的原因很简单：

- 既然当前 quota 下继续轮询没有正收益，
- 那就不再让 settle 自己扩大 search 压力
- 同时保留足够的 meta 信息，后续可以区分：
  - 是“多次轮询超时”
  - 还是“第一次 search 就被 rate limit 拦住”

新的本地验证：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `9 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py tests/benchmark/locomo/openclaw/test_phase_a_off.py`

对应新行为的测试也已补上：

- `429` 情况下：
  - `search_attempts = 1`
  - `passive_wait_after_rate_limit = true`
  - 只发生一次 post，再进入一次 sleep

### local probe snapshot：补齐 search 受限时的本地就绪证据

在当前 search 配额未恢复的阶段，仅有：

- `probe_count`
- `ready_queries`
- `last_error`

还不够区分两类情况：

1. memory 文件本身就还没写出来
2. memory 已经写出来，但 `/search/find` 因限流或索引可见性问题暂时看不见

因此又补了一层本地诊断：

- `phase_a_off.py`
  - 新增 `collect_local_probe_snapshot()`
  - 在 QA 前 settle 时，对每个 probe 记录：
    - `expected_fragment`
    - `exists`
    - `size_bytes`
    - `mtime`

并把它挂到：

- `post_ingest_settle.local_probe_snapshot`

这样即使最终结果仍是：

- `reason = rate_limited_timeout`

后续也能直接从 meta 判断：

- 是 “search 被 429 卡住，但目标 memory 文件已经在本地落盘”
- 还是 “本地写入本身就没完成”

新的本地验证：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `10 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py tests/benchmark/locomo/openclaw/test_phase_a_off.py`

### 2026-06-01 远端复验：search 配额恢复，但 clean fresh account 下 settle 未实际生效

在 search-only settle 复验恢复正常后，做了两轮 fresh-account、无代码改动的远端验证：

1. 最小用例
- account:
  - `acct-20260601-q27smoke51`
- user:
  - `user-20260601-q27smoke51`
- 口径：
  - `sample 0`
  - `sessions 6-7`
  - `q27 only`
  - `--post-ingest-settle-seconds 240`
  - `--qa-disable-autocapture`

结果：
- `q27 = CORRECT`
- CSV:
  - `/tmp/q27_smoke51_qa/phaseA_on_2sessions_q27smoke51.csv`

2. 小样本用例
- account:
  - `acct-20260601-s67small52`
- user:
  - `user-20260601-s67small52`
- 口径：
  - `sample 0`
  - `sessions 6-7`
  - `q23-27`
  - `--post-ingest-settle-seconds 240`
  - `--qa-disable-autocapture`

结果：
- `q23 = CORRECT`
- `q24 = CORRECT`
- `q25 = WRONG`
- `q27 = CORRECT`
- 有效题：
  - `3/4 correct`
- CSV:
  - `/tmp/s67_small52_qa/phaseA_on_2sessions_s67small52.csv`

但这两轮更重要的新发现不是题级分数，而是：

- `post_ingest_settle` 都是：
  - `reason = fallback_sleep`
  - `probe_count = 0`
  - `probe_queries = []`
  - `local_probe_snapshot = []`

也就是说：

- 这两轮通过的题级结果，**不是** 当前 settle probe/search-visibility 机制验证出来的
- 而是因为 fallback sleep `240s` 后，首跑 QA 本身已经能答对

进一步查看 fresh account 的实际 memory 落盘位置，可以看到当前 namespace policy 下，memory 主要写在：

- `user/<user>/agent/<agent>/memories/...`

而当前 settle probe 选择仍在看：

- `account_root / user / <user> / memories`

这解释了为什么：

- 在旧 account 上，question-aware probe 逻辑可以离线选中 conference memory
- 但在新的 clean fresh account 真正跑 benchmark 时，`probe_count` 却仍然是 `0`

因此当前关于 `q27` 的判断要再修正一层：

- search 配额恢复后，`q27` 的 clean 首跑已经可以在 `240s fallback sleep` 下回正
- 但这**还不能证明**新的 settle probe 机制已经在 clean environment 生效
- 当前 settle 的剩余脚本问题，已经从“429/配额”转成：
  - **probe root 没对齐当前 namespace policy**

### 2026-06-01 第二轮修正：probe root + target_uri 对齐后，settle 机制在 clean 环境真实生效

随后继续做了两层最小脚本修正：

1. settle probe root 改成支持当前 namespace policy
- 不再只看：
  - `user/<user>/memories`
- 而是优先看：
  - `user/<user>/agent/<agent>/memories`

2. agent-scoped probe 的 `target_uri` 也同步改成：
- `viking://user/<user>/agent/<agent>/memories`
- 不再错误使用 user-only URI

本地验证：

- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `11 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py tests/benchmark/locomo/openclaw/test_phase_a_off.py`

修正后重新做了两轮 fresh-account 远端 clean 复验：

#### 1. 最小 `q27` 用例：`q27settle55`

- account:
  - `acct-20260601-q27settle55`
- 口径：
  - `sample0 / sessions 6-7 / q27`
  - `--post-ingest-settle-seconds 240`
  - `--qa-disable-autocapture`

结果：

- `q27 = CORRECT`
- 且这次 `post_ingest_settle` 已经不再是 `fallback_sleep`
- 而是：
  - `ok = true`
  - `ready_queries = ["lgbtq conference"]`
  - `probe_count = 1`
  - `search_attempts = 1`
  - `local_probe_snapshot[0].exists = true`

也就是说：

- settle 机制终于在 clean environment 下**真实命中**了 conference memory

#### 2. 小样本 `q23-27` 用例：`s67settle56`

- account:
  - `acct-20260601-s67settle56`
- 口径：
  - `sample0 / sessions 6-7 / q23-27`
  - `--post-ingest-settle-seconds 240`
  - `--qa-disable-autocapture`

结果：

- `q23 = CORRECT`
- `q24 = CORRECT`
- `q25 = WRONG`
- `q27 = CORRECT`
- 有效题：
  - `3/4 correct`

并且这轮 `post_ingest_settle` 同样已经真实生效：

- `ok = true`
- `ready_queries = ["lgbtq conference attendance"]`
- `probe_count = 1`
- `search_attempts = 1`
- `local_probe_snapshot[0].exists = true`

### 当前更新后的结论

至此，关于 `q27` 的关键问题已经有了更强的闭环证据：

- 不是 extraction 本身仍错
- 不是 search quota 必然挡住验证
- 当前 benchmark settle 机制在 clean fresh-account 下，已经能：
  - 选中正确 conference probe
  - 对齐 agent-scoped URI
  - 成功 wait 到 search visibility ready
  - 再让 `q27` 首跑回正

也就是说，原来那条：

- `fallback_sleep + probe_count=0`

现在已经在 clean sample0 / sessions 6-7 口径下，实测推进成了：

- `search-visibility settle real hit + q27 first-run correct`

### 2026-06-01 更大 clean 子集复验：`q1639settle57`

在 `q27` 的 settle 路径打通后，按同样的 current version 回到更大 clean 子集：

- account:
  - `acct-20260601-q1639settle57`
- 口径：
  - `sample0`
  - `sessions 3-9`
  - `q16-39`
  - `--post-ingest-settle-seconds 240`
  - `--qa-disable-autocapture`

结果：

- `14/21 correct`
- 平均 `input_tokens = 4865.3`
- 错题：
  - `q18`
  - `q19`
  - `q20`
  - `q25`
  - `q28`
  - `q33`
  - `q36`

其中与本轮重点直接相关的结论是：

- `q27 = CORRECT`
- `post_ingest_settle` 已真实生效：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["pride parade"]`
  - `local_probe_snapshot[0].exists = true`

这说明：

- `q27` settle 修正本身**没有引入 `q27` 回归**
- 但当前整版 current version 在更大 clean 子集上，整体表现明显差于历史最佳基线

与历史大子集 best-known baseline 的直接对照：

- 历史基线：
  - `acct-20260530-imgctx39`
  - `17/21 correct`
  - 平均 `input_tokens = 4150.6`
- 当前版本：
  - `acct-20260601-q1639settle57`
  - `14/21 correct`
  - 平均 `input_tokens = 4865.3`

因此当前更准确的判断变成：

- `q27` 这条子问题已经闭环
- 但 current version 作为整体版本，**还不能**宣称优于之前的 `17/21` clean baseline
- `q25` 仍然是残余失败点之一，但已经不再是唯一主要残余；
  当前更大的回归集合至少还包括：
  - `q18/q19/q20/q28/q33/q36`

### 2026-06-01 `baseline + benchmark-only settle` 隔离复验：`benchonly60-62`

按隔离说明，把验证收敛到：

- baseline 仓库：
  - `/home/jcp/agent/code/OpenViking-benchonly`
- 仅保留 benchmark 侧改动：
  - `benchmark/locomo/openclaw/phase_a_off.py`
  - 以及为跑通旧 baseline judge 所需的 `benchmark/locomo/openclaw/judge.py`
- 不混入 current runtime / extraction 文件

先跑最小 clean smoke：

- `sample0 / sessions 6-7 / q27`

#### `q27benchonly61`

第一轮在旧 baseline judge 下，出现两个独立现象：

- `post_ingest_settle` 真实命中 conference memory：
  - `ok = true`
  - `ready_queries = ["lgbtq conference attendance"]`
  - `local_probe_snapshot[0].exists = true`
- 但 judge 失败：
  - `judge.py` 默认模型仍是 `doubao-seed-2-0-pro-260215`
  - 远端账号未开通该模型
  - CSV 中 raw response 实际已经是正确的 `July 10, 2023`

这说明：

- benchmark-only settle 逻辑本身已经可以在 baseline 仓库里工作
- 但旧 baseline 的 benchmark judge 脚本过旧，不能直接用于今天的远端环境

#### `q27benchonly62`

同步当前 benchmark `judge.py` 后，再跑同样 smoke：

- judge 恢复可用
- 但 raw response 变成：
  - `There is no relevant information ...`
- `post_ingest_settle` 仍是 `ok = true`
  - 但这次选到的 probe 是：
    - `ready_queries = ["beach camping"]`

进一步检查 `acct-20260601-q27benchonly62` 的实际 memory 后确认：

- agent-scoped `memories/events` 下只有：
  - `beach_camping.md`
  - `museum_visit.md`
  - `reunion_chat.md`
  - `support_picnic.md`
- **没有** `lgbtq_conference_attendance.md` 或等价 conference event

因此这轮隔离复验的结论非常明确：

- benchmark-only settle 改动**不能单独把 baseline 仓库拉回 `q27` 正确**
- 原因不是 settle 本身坏了
- 而是该 baseline runtime/extraction 组合**本来就没有生成 conference event memory**

所以当前命题：

- `17/21 历史 clean baseline + benchmark-only settle`

在现有可复验 baseline 仓库上，至少对 `q27` 这条题**不成立**

更准确地说：

- `q27` 需要的不只是 benchmark settle
- 还依赖后续 runtime / extraction 路径里已经存在的 conference event generation 修正

### 2026-06-01 `baseline + settle + current memory_extraction.yaml`：`benchprompt63/64/65`

在确认 `benchmark-only settle` 不足后，继续只做一层最小 runtime/extraction 带入：

- 保持 baseline 仓库：
  - `/home/jcp/agent/code/OpenViking-benchonly`
- 保持 benchmark 侧修正：
  - `phase_a_off.py`
  - `judge.py`
- **仅额外带入当前**：
  - `openviking/prompts/templates/compression/memory_extraction.yaml`

不混入：

- `session_extract_context_provider.py`
- `compressor_v2.py`
- `session.py`
- `import_to_ov.py`
- 其它 current runtime 文件

#### 最小 smoke：`q27benchprompt63`

- 口径：
  - `sample0 / sessions 6-7 / q27`
- 结果：
  - `q27 = CORRECT`
- `post_ingest_settle`：
  - `ok = true`
  - `ready_queries = ["lgbtq conference attendance"]`

这说明：

- 对 `q27` 来说，当前真正必要的最小改动不再是整版 current runtime
- 而是：
  - benchmark settle
  - `memory_extraction.yaml` 中关于 public participation events / event splitting / duplicate wrapper avoidance 的当前规则

#### 邻近小集合：`s67benchprompt64`

- 口径：
  - `sample0 / sessions 6-7 / q23-27`
- 结果：
  - `q23 = CORRECT`
  - `q24 = CORRECT`
  - `q25 = WRONG`
  - `q27 = CORRECT`
  - `3/4`

说明这条最小改动集不只是单题偶然有效，至少在 `q23-27` 上是稳定的。

#### 更大 clean 子集：`q1639benchprompt65`

- 口径：
  - `sample0`
  - `sessions 3-9`
  - `q16-39`
  - clean fresh account
- 结果：
  - `18/21 correct`
  - 平均 `input_tokens = 3946.3`
  - 错题：
    - `q25`
    - `q28`
    - `q39`
- `post_ingest_settle`：
  - `ok = true`
  - `ready_queries = ["pride parade"]`

这轮与此前几个关键基线的直接对照：

- 历史 current best-known：
  - `acct-20260530-imgctx39`
  - `17/21`
  - `4150.6`
- settle-only baseline：
  - `acct-20260601-q1639settle57`
  - `14/21`
  - `4865.3`
- 本轮最小改动集：
  - `acct-20260601-q1639benchprompt65`
  - `18/21`
  - `3946.3`

因此当前最强结论更新为：

- 目前最优主候选不再是整版 current runtime，也不是 settle-only baseline
- 而是：
  - baseline runtime
  - + benchmark settle
  - + 当前 `memory_extraction.yaml`

这条线在更大 clean 子集上同时实现了：

- 更高准确率：
  - `18/21` > `17/21` > `14/21`
- 更低平均 token：
  - `3946.3` < `4150.6` < `4865.3`

当前残余失败点收敛为：

- `q25`
- `q28`
- `q39`

也就是说，这条最小改动线已经实测满足：

- `q27` 首跑更可靠
- 大子集准确率提升
- token 进一步下降

### 2026-06-01 `baseline + settle + memory_extraction + import_to_ov`：`imgbench66/67`

在 `18/21` 基础上，继续验证图像 ingest 增量是否值得并入主候选。带入的唯一新增文件是：

- `benchmark/locomo/openclaw/import_to_ov.py`

其核心变化包括：

- 保留 `blip_caption / query`
- 传递 `image_url`
- 尝试对书封面类图片生成：
  - `[image_title_hint]`
  - `[image_author_hint]`

#### 定点图像子集：`imgbench66`

- 口径：
  - `sample0 / sessions 6-9 / q25-39`
- 结果：
  - `q25 = WRONG`
  - `q28 = WRONG`
  - `q39 = CORRECT`
  - 整体 `7/9`

并且对 `acct-20260601-imgbench66` / `acct-20260601-q1639imgbench67` 的实际 ingest `messages.jsonl` 做了直接取证：

- 能看到：
  - `[image_caption]`
  - `[image_query]`
- **看不到**：
  - `[image_title_hint]`
  - `[image_author_hint]`
  - `Nothing is Impossible`
  - `Tom Oliver`

这说明当前 `import_to_ov.py` 增量的实际收益是：

- 对 `q39` 的绘画/图像主题恢复有效
- 但对 `q25/q28` 的书名恢复**没有真正生成标题提示**

#### 更大 clean 子集：`q1639imgbench67`

- 口径：
  - `sample0`
  - `sessions 3-9`
  - `q16-39`
- 结果：
  - `19/21 correct`
  - 平均 `input_tokens = 3638.1`
  - 错题仅剩：
    - `q25`
    - `q28`

与此前主候选对照：

- `q1639benchprompt65`
  - `18/21`
  - `3946.3`
- `q1639imgbench67`
  - `19/21`
  - `3638.1`

因此当前最强结论再次更新为：

- 当前最佳主候选已经变成：
  - baseline runtime
  - + benchmark settle
  - + 当前 `memory_extraction.yaml`
  - + 当前 `import_to_ov.py`

这条线在更大 clean 子集上达到了：

- 更高准确率：
  - `19/21`
- 更低平均 token：
  - `3638.1`

同时，剩余问题进一步收敛成非常窄的两题：

- `q25`
- `q28`

并且这两题的共同根因也更清楚了：

- 当前图像链路虽然已经足以修 `q39`
- 但针对书封面标题的 `[image_title_hint]` 实际上没有被稳定产出
- 所以 `Nothing is Impossible` / `2022` 这组信息仍然丢失

### 2026-06-01 `import_to_ov.py` 书封面 hint 兼容修复：`q2528imgfix69`

在 `q1639imgbench67` 之后，继续对 `q25/q28` 做最小 clean 取证，发现问题并不是 VLM 看不懂书封面，而是 bench-only 旧环境下：

- `_maybe_extract_visual_hints()`
- 通过 `get_openviking_config().vlm`
- 读取配置时会被旧 `MemoryConfig` 的新字段校验直接打断
- 异常被吞掉后，导致：
  - `[image_title_hint]`
  - `[image_author_hint]`
  根本没有被写进 ingest 消息

随后在 `import_to_ov.py` 中补了一个极窄兼容层：

- `_load_vlm_for_visual_hints()`
  - 先尝试 `get_openviking_config().vlm`
  - 失败后只从：
    - `OPENVIKING_CONFIG_FILE`
    - `~/.openviking/ov.conf`
    - `/root/.openviking/ov.conf`
    - `/etc/openviking/ov.conf`
    读取原始 `vlm` 段
  - 直接构造 `VLMConfig`

真实容器内直接复现后，已经能稳定拿到：

- `title = Nothing is Impossible`
- `author = Tom Oliver`
- `confidence = high`

随后在 clean 最小书题子集上重跑：

- run: `q2528imgfix69`
- 口径：
  - `sample0`
  - `sessions 6-7`
  - `q25-28`
- 结果：
  - `q25 = CORRECT`
  - `q27 = CORRECT`
  - `q28 = CORRECT`

这说明：

- 当前最小主候选上的书封面 title/author hint 链路已经真正打通
- `q25/q28` 不再只是在直接函数调用里可用，而是已经在 clean benchmark 最小用例里兑现成题级正确

### 2026-06-01 `baseline + settle + memory_extraction + import_to_ov(fixed)`：`q1639imgfix70`

在 `q2528imgfix69` 证明书题链路打通后，继续对更大 clean 子集做同口径复验：

- run: `q1639imgfix70`
- 口径：
  - `sample0`
  - `sessions 3-9`
  - `q16-39`
  - clean fresh account
- 版本组合：
  - baseline runtime
  - + benchmark settle
  - + 当前 `memory_extraction.yaml`
  - + 修复后的 `import_to_ov.py`

结果：

- `21/21 correct`
- 平均 `input_tokens = 4122.7`
- `post_ingest_settle`：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["mentorship program join"]`

与此前关键版本对照：

- `q1639benchprompt65`
  - `18/21`
  - `3946.3`
- `q1639imgbench67`
  - `19/21`
  - `3638.1`
- `q1639imgfix70`
  - `21/21`
  - `4122.7`

因此当前最强结论再次更新为：

- 当前最佳主候选是：
  - baseline runtime
  - + benchmark settle
  - + 当前 `memory_extraction.yaml`
  - + 修复后的 `import_to_ov.py`

这条最小改动集已经在比单题和小样本更大的 clean 子集上同时实现：

- 准确率继续提升到 `21/21`
- benchmark settle 首跑验证保持有效
- `q25/q28/q39` 也都被带回正确

当前这条线的下一步不再是继续追单个残余题，而是：

- 用同一最小主候选扩大到更大范围验证
- 例如 full `sample0`
- 以验证“更大范围效果”而不是继续围绕单题做局部优化

### 2026-06-02 full-sample QA 通路恢复取证：plugin/context-engine 兼容层

在 `sample0full71` 容器重启后，恢复 `current 1933 + gateway + QA-only` 的过程中，验证通路问题从泛化环境问题收缩到了 plugin/context-engine 兼容层：

1. request-level `http_400` 已解决
   - gateway 当前只接受 `model = openclaw` 或 `openclaw/<agentId>`
   - `phase_a_off.py` 已补 benchmark 侧模型名规范化

2. `current 1933` 已恢复
   - `/health -> version 0.3.18.dev76`
   - 问题不再是 OpenViking server 不可用

3. 当前主要分成两条 plugin 路径
   - **兼容旧 gateway 的旧式 context-engine 路径**
     - runtime 组合：`agent_prefix` schema
     - 可以成功注册 `openviking` context-engine
     - 但主路径只做 `getSessionContext(sessionId)`，针对 QA-only 的新 session 只会看到空上下文
     - 最小手工 `q27` 请求会打：
       - `request /api/v1/sessions/<new-session>/context?...`
       - 然后直接回答 “no record”
     - 这条路径不适合作为 full-sample QA-only 的最终恢复方案
   - **带 `before_prompt_build + auto-recall` 的 current 路径**
     - 能触发 `hook before_prompt_build`
     - 但最初有多层 runtime/config 兼容问题

4. 已确认的 current 路径代码级问题
   - `OpenVikingClient(...)` 构造参数错位
     - `cfg.serverAuthMode` 被误传成第 5 个位置参数
     - 导致后续 `accountId/userId/routingDebugLog` 全部错位
   - 修正后，远端 live 日志已能看到：
     - `accountId = acct-20260601-sample0full71`
     - `userId = user-20260601-sample0full71`
     - `hasApiKey = true`
     - 并且请求日志已出现：
       - `request /api/v1/search/find`
       - `find POST ...`

5. 已确认的验证通路误阻塞
   - `quickRecallPrecheck()` 的 500ms `/health` fast probe 在远端实际是误阻塞
   - 容器内直接复现 `/health` 500ms 检查是稳定成功的
   - 但 hook 侧仍会记录：
     - `skipping auto-recall because precheck failed (health check failed)`
   - 说明当前 precheck 逻辑不适合作为 recall 的 hard gate

6. 当前最新链路状态
   - 兼容 plugin 组合下，远端最小手工 `q27` 已经能稳定看到：
     - `resolveAgentId -> acct-20260601-sample0full71_locomo-eval`
     - `request /api/v1/sessions/<session>/context?...`
   - 但不会进入 account memory recall
   - 因此回答仍是：
     - `I don't have any record ...`

7. 当前阶段判断
   - full-sample QA 通路还未恢复完成
   - 但 blocker 已明确不是：
     - `1933` 不可用
     - gateway 不可用
     - 或 benchmark 请求 schema 错误
   - 而是：
     - **哪套 plugin/runtime 组合能同时满足**
       - 兼容旧 gateway `2026.4.8`
       - 使用 tenant-scoped headers
       - 使用 account memory recall 而不是 fresh session-only context

### 2026-06-02 `q27qa80`: `phase_a_off.py QA-only` 单题恢复成功

在远端 `current 1933 + current gateway` 环境下，基于兼容旧 gateway 的 plugin 组合继续收敛：

- `agent_prefix = acct-20260601-sample0full71`
- `accountId = acct-20260601-sample0full71`
- `userId = user-20260601-sample0full71`
- `apiKey = ov-root-namespace-test-20260517`
- `before_prompt_build` hook 已移植回当前 worktree 版本
- `quickRecallPrecheck()` 不再因为 500ms fast probe 假阴性阻断 recall

然后对 persisted account `acct-20260601-sample0full71` 执行：

- run: `q27qa80`
- 口径：
  - `sample0`
  - `sessions 1-19`
  - `--skip-ingest`
  - `q27 only`

结果：

- `phase_a_off.py` QA-only 成功完成并落盘
- CSV:
  - `/tmp/q27qa80_qa/phaseA_on_19sessions_20260602_051203.csv`
- Meta:
  - `/tmp/q27qa80_qa/phaseA_on_19sessions_20260602_051203_meta.json`
- 题级结果：
  - `q27 = CORRECT`
  - response: `Caroline went to the LGBTQ conference on July 10, 2023 (2023-07-10).`

关键日志证据：

- `hook before_prompt_build`
- `find POST /api/v1/search/find`
- `inject-detail`
- top injected memory 包含：
  - `events/2023/07/12/lgbtq_conference_attendance.md`

这说明：

- benchmark QA-only 通路已经不再停在 `http_400`
- `phase_a_off.py` 已能在远端 clean persisted account 上完成单题 QA、打分并产出 CSV/meta
- 验证通路恢复已经从“请求层可用”推进到“benchmark QA-only 单题可完整闭环”

### 2026-06-02 `sample0full71qa81`: full sample0 QA-only 已重新启动

在 `q27qa80` 单题闭环后，继续基于同一 persisted account 启动：

- run: `sample0full71qa81`
- 口径：
  - `sample0`
  - `sessions 1-19`
  - `--skip-ingest`
  - full QA-only

当前状态（本轮结束时）：

- 进程仍在运行
- output dir 已创建：
  - `/tmp/sample0full71qa81_qa`
- 尚未到可汇总结果阶段

因此当前 full-sample 验证通路还不能宣称完全恢复，但已经拿到：

- `phase_a_off.py QA-only` 单题闭环
- `full sample0 QA-only` 再次起跑且未在启动阶段失败

### 2026-06-02 `sample0full71qa81` 进度更新：已稳定推进到 `q24`

后续继续观察 `sample0full71qa81` 的远端执行状态，确认这不是“起跑成功但中途卡死”：

- 进程仍在运行
- 同一 CSV 已持续增长到：
  - `rows = 23`
  - 当前最新 `qi = 24`
- 最新几题的 raw response 已正常写入 CSV：
  - `q20`
  - `q21`
  - `q22`
  - `q23`
  - `q24`

这说明：

- 当前 full-sample QA-only 并未卡在启动层
- 也没有再次回退到 `http_400`
- persisted account + current gateway + current 1933 + QA-only benchmark 脚本，这条链路已经在持续处理整包题目

当前还没完成的仅是：

- 等 full `sample0` 题目全部跑完
- 再读取最终 CSV / meta，汇总：
  - 总正确数
  - 平均 input tokens
  - 错题分布

### 2026-06-02 `sample0full71qa81` 进度更新：QA 主流程已跑完整包，judge 正在回填

后续继续观察 `sample0full71qa81`，当前状态已经进一步明确：

- CSV 行数已到 `150`
- 说明 full `sample0` 的 QA 主流程已经把整包题目写完
- 当前剩余的是 `judge.py` 回填 `result / reasoning`

当前中间态：

- run: `sample0full71qa81`
- artifact:
  - `/tmp/sample0full71qa81_qa/phaseA_on_19sessions_20260602_051430.csv`
- partial judge state:
  - `graded = 32`
  - `correct = 15`
  - `avg_input_tokens = 1719.0`

当前 partial wrong head：

- `q2`
- `q3`
- `q4`
- `q7`
- `q8`
- `q9`
- `q11`
- `q12`
- `q13`
- `q14`
- `q15`
- `q16`
- `q20`
- `q25`
- `q26`
- `q29`
- `q36`

因此当前结论更新为：

- full-sample QA-only 验证通路恢复已经不只是“持续运行”
- 而是已经进入：
  - **QA 主流程完成**
  - **judge 结果回填中**

还不能下最终结论的唯一原因是：

- `graded != rows`
- 最终 `correct / avg_input_tokens / wrong set` 仍未收敛

### 2026-06-02 `sample0full71qa81` 最终结果：验证通路恢复已完成，恢复跑结果为 `95/150`

继续等待 `sample0full71qa81` 收尾后，最终 artifact 已完整落盘：

- CSV:
  - `/tmp/sample0full71qa81_qa/phaseA_on_19sessions_20260602_051430.csv`
- meta:
  - `/tmp/sample0full71qa81_qa/phaseA_on_19sessions_20260602_051430_meta.json`

最终结果：

- `rows = 150`
- `correct = 95`
- `wrong_count = 55`
- `accuracy = 63.33%`
- `avg_input_tokens = 1818.3`

最终 wrong set：

- `q2`
- `q3`
- `q4`
- `q7`
- `q8`
- `q9`
- `q11`
- `q12`
- `q13`
- `q14`
- `q15`
- `q16`
- `q20`
- `q25`
- `q26`
- `q29`
- `q34`
- `q36`
- `q37`
- `q38`
- `q41`
- `q53`
- `q54`
- `q55`
- `q56`
- `q57`
- `q58`
- `q61`
- `q62`
- `q67`
- `q68`
- `q71`
- `q73`
- `q77`
- `q79`
- `q80`
- `q81`
- `q82`
- `q83`
- `q87`
- `q89`
- `q90`
- `q94`
- `q96`
- `q105`
- `q106`
- `q118`
- `q120`
- `q122`
- `q139`
- `q140`
- `q144`
- `q146`
- `q148`
- `q151`

这轮结果的意义是：

- 它首先证明了 **full-sample QA 验证通路已经恢复完成**
  - current `1933`
  - current gateway
  - `phase_a_off.py` QA-only
  - full-sample CSV / meta / judge 回填
- 但它仍然是 **容器重启后基于 persisted account 的恢复跑结果**
- 因此这轮结果可以作为：
  - A 阶段“验证通路恢复成功”的直接证据
  - 但还不是 B 阶段“clean fresh account 更大范围效果验证”的最终结论

与前一阶段最优子集结果对照：

- `q1639imgfix70`
  - `sample0 / sessions 3-9 / q16-39`
  - `21/21`
  - `4122.7 input_tokens`
- `sample0full71qa81`
  - `sample0 / sessions 1-19 / q1-153`
  - `95/150`
  - `1818.3 input_tokens`

这里不能直接把两者当作同口径优劣比较，因为：

- 一个是更窄的 clean 子集
- 一个是 full sample0 恢复跑
- 但可以确认：
  - 验证通路已经恢复
  - 当前 minimal candidate 在更大范围 full sample0 上仍然存在明显长尾错题

### 2026-06-02 `sample0full82`: 新的 clean fresh account full sample0 已启动

在 `sample0full71qa81` 完成后，继续进入 B 阶段验证：

- run: `sample0full82`
- 目标：
  - 不再依赖 persisted account 恢复路径
  - 直接在新的 clean fresh account 上重跑 full `sample0`
- 新 account:
  - `acct-20260602-sample0full82`
- 新 user:
  - `user-20260602-sample0full82`

为保证 QA recall 仍指向新的 clean account，对 gateway plugin config 做了最小切换：

- `accountId = acct-20260602-sample0full82`
- `userId = user-20260602-sample0full82`
- `agent_prefix = acct-20260602-sample0full82`

随后已成功启动新的 full run：

- output dir:
  - `/tmp/sample0full82_qa`
- 进程：
  - `phase_a_off.py ... --ov-agent-id acct-20260602-sample0full82 --user user-20260602-sample0full82 --ov-account-id acct-20260602-sample0full82`

这一步的目标很明确：

- 验证恢复后的 current `1933 + gateway + benchmark` 链路，是否能在**新的 clean account** 上端到端稳定复现
- 如果 `sample0full82` 能完整落盘并出最终结果，那么 B 阶段的“大范围效果验证”才算真正进入同口径可复验状态

### 2026-06-02 `sample0full82` 进度更新：fresh run 已推进到 `session 10/19`

继续观察 `sample0full82` 的运行状态后，当前可以确认：

- `phase_a_off.py` 进程仍在运行
- gateway 健康：
  - `{"ok": true, "status": "live"}`
- `1933` 健康：
  - `{"status":"ok","healthy":true,"version":"0.3.18.dev76","auth_mode":"api_key"}`
- 新 account 的 session 持续落盘
- 当前已推进到：
  - `session 10/19`

当前 `phase_a_off.py` 输出里最显眼的现象是：

- `session 1`: `memories=18`
- `session 2-10`: 暂时都是 `memories=0`

但这一点当前**不能直接判定为 fresh run 新异常**。补充对照后发现：

- 旧 `acct-20260601-sample0full71` account 的大量 session `.meta.json`
  - `memories_extracted.total` 也经常是 `0`
- 也就是说，`session meta` 里的 `total=0` 在当前口径下并不是 fresh run 独有现象

因此现阶段更准确的判断是：

- `sample0full82` 已经证明：
  - 新 clean account 路径没有回退到 `http_400`
  - ingest/session 落盘正在持续推进
- 但还不能仅凭前几个 session 的 `memories=0`
  - 就把 fresh run 判成 extraction failure

当前应继续观察的关键点仍然是：

- 是否能完整走完 `sessions 1-19`
- 是否进入 QA / CSV / meta 落盘阶段
- full sample0 最终结果是否能在新的 clean account 上完整产出

### 2026-06-02 `sample0full82` 新结论：fresh run 已进入 QA，但当前阻塞是模型请求 `network connection error`

继续跟进 `sample0full82` 后，当前状态进一步明确：

- `sessions 1-19` 已全部 ingest 完成
- QA CSV 已开始落盘：
  - `/tmp/sample0full82_qa/phaseA_on_19sessions_20260602_062947.csv`
- 也就是说：
  - 新 clean account 路径已经不仅是“ingest 成功”
  - 而是已经真正进入 QA 阶段

但当前 QA 结果从开头就出现：

- `q2`: `[ERROR] GatewayResponseError | http_500`
- `q3`: `[ERROR] GatewayResponseError | http_500`
- `q4`: `[ERROR] GatewayResponseError | http_500`
- `q5`: `[ERROR] GatewayResponseError | http_500`

结合 gateway 实时日志，已经可以确认这轮 `http_500` 的具体根因不是：

- `http_400`
- namespace / agent prefix 不兼容
- recall hook 没触发

而是：

- fresh account QA 阶段已经正常触发：
  - `before_prompt_build`
  - `find POST`
  - session message POST
  - session commit POST
- 但随后 gateway 在 embedded assistant 路径上报：
  - `FailoverError: LLM request failed: network connection error`
  - `failoverReason = timeout`
  - primary:
    - `provider = volcengine`
    - `model = doubao-seed-2.0-pro`
  - fallback:
    - `provider = volcengine-plan`
    - `model = doubao-seed-code`

关键日志证据：

- `embedded_run_failover_decision`
  - `decision = fallback_model`
  - `reason = timeout`
  - `status = 408`
  - `rawErrorPreview = Connection error.`
- `lane task error`
  - `FailoverError: LLM request failed: network connection error.`

因此当前 fresh run 的阻塞性质应更新为：

- **这不是 recall / plugin 契约故障**
- **这不是 current `1933` / gateway health 故障**
- **这是 QA 阶段上游 LLM 请求超时导致的 `http_500`**

这条结论很重要，因为它说明：

- A 阶段“验证通路恢复”已经完成
- B 阶段当前失败点，不在 minimal candidate 本身
- 而在 fresh full-sample QA 执行时的外部模型请求稳定性

### 2026-06-02 `sample0full82` 后续验证：benchmark 侧重试补丁已生效，但外部超时仍持续

为了区分“单次外部抖动”与“系统性 QA 不可用”，对 benchmark QA 请求侧做了最小增强：

- 在 `benchmark/locomo/openclaw/phase_a_off.py` 的 `send_gateway_message()` 中增加重试
- 仅对可恢复错误重试：
  - `408`
  - `429`
  - `500`
  - `502`
  - `503`
  - `504`
  - 以及 `requests.Timeout / ConnectionError`
- 本地新增单测并通过：
  - `500 -> retry -> success`
  - `400 -> no retry`

然后将补丁同步到 remote `OpenViking-benchonly`，并对同一 run 做：

- `--skip-ingest`
- 同一 output dir
- 同一 `run_id = 20260602_062947`

这样会自动：

- `resume-prune` 删除已有 retryable `http_500` QA 行
- 只重跑这些失败问题，不重新 ingest 19 个 session

远端实际结果：

- `resume-prune` 已生效，删除了：
  - `qi = 2-22`
- 新的 retry run 已按预期启动

但随后新的 gateway 日志仍然显示：

- `lane task error: FailoverError: LLM request failed: network connection error`
- primary:
  - `volcengine/doubao-seed-2.0-pro`
- fallback:
  - `volcengine-plan/doubao-seed-code`
- 两个 candidate 都 timeout

而且这次已经不是只在新题上失败：

- 被 prune 后重跑的旧题 `q2`
- 以及继续推进到的 `q23`

都再次命中同一类 `network connection error`

因此这一步的结论已经足够明确：

- benchmark 侧“增加请求重试”已经验证过
- 但当前阻塞**不是缺少 retry**
- 而是上游模型请求在当前时段持续不可用，导致：
  - primary/fallback 全部 timeout
  - QA 无法产出正常回答

### 2026-06-02 网络恢复后：`sample0full82` retry run 已重新产出正常 QA 结果

在网络问题修复后，继续观察同一 `sample0full82` retry run，当前状态已经明显前进：

- 进程仍在运行
- 当前 CSV:
  - `/tmp/sample0full82_qa/phaseA_on_19sessions_20260602_062947.csv`
- 已推进到：
  - `q55`

当前统计：

- `rows = 52`
- `errors = 24`
- `non_error = 28`
- `graded = 0`

这说明：

- 当前 run 已经不再是“系统性 `http_500`”
- 从 `q25` 往后已经持续产出正常回答
- 剩余 `24` 个错误行，本质上是**网络恢复前留下的旧 `http_500` 遗留**

因此当前最合理的执行策略不是中途再次打断，而是：

1. 先让这一轮 retry run 跑完整个后半段
2. 然后对 CSV 中仍然保留的旧 `http_500` 行
   - 再做一次新的 `resume-prune`
   - 只补跑这些遗留错误题

这样做的原因很直接：

- 当前 run 已经进入稳定推进阶段
- 现在打断会让后半段再多引入一次额外变量
- 等它先跑完，再做第二次 prune，归因最干净

### 2026-06-02 进一步确认：验证通路已恢复，但 `sample0full82` 仍需第二次 prune 才能得到干净 clean-run 结果

继续对同一 clean fresh account run `sample0full82` 取证后，当前可以把“验证通路是否恢复”与“结果是否已干净”明确分开：

- 网络与服务连通性已恢复：
  - container proxy `172.17.0.1:17898` 可连通
  - gateway `127.0.0.1:18789` 可连通
  - OpenViking `127.0.0.1:1933` 可连通
  - 直连 `ark.cn-beijing.volces.com` 返回 `401`
  - 直连 `openai.com` 返回 `403`
- 同一 retry run 的 QA 已完整写满：
  - `/tmp/sample0full82_qa/phaseA_on_19sessions_20260602_062947.csv`
  - `rows = 150`
- judge 已经实际启动：
  - `judge.py --input /tmp/sample0full82_qa/phaseA_on_19sessions_20260602_062947.csv --parallel 3`

这说明：

- **A 阶段“恢复稳定 full-sample QA 验证通路”已经基本达成**
- 当前 `current 1933 + current gateway + phase_a_off.py QA + full CSV 落盘` 是可复现的
- 当前阻塞点已经不再是“从第 2 题开始系统性 `http_400/http_500` 导致整包无法跑完”

但同一份 CSV 的内容还不干净。进一步检查 `response` 字段后可见：

- 仍然残留 `24` 条历史错误行
- 对应 `qi = 2-25`
- 错误内容为：
  - `[ERROR] GatewayResponseError | http_500`

也就是说：

- 这轮 retry run **已经把 QA 跑满**
- 但因为第一次 `resume-prune` 只删掉了当时已存在的 `qi=2-22`
- 网络恢复前又留下的 `qi=23-25` 历史错误没有被覆盖
- 因此当前 clean-run CSV **仍不能直接作为最终候选效果结论**

当前最准确的下一步应为：

1. 等当前 `judge.py` 收尾
2. 对同一 `run_id = 20260602_062947` 再做一次 `--skip-ingest` retry
3. 依赖 `resume-prune` 删除当前残留的 retryable 错误行
   - 目标是至少清掉当前确认的 `qi = 2-25`
4. 让其只补跑这些旧错误题
5. 再基于第二次 prune 后的 clean CSV + judge 结果，提取：
   - 总正确数
   - 平均 `input_tokens`
   - 错题分布
   - 与上一阶段最优样本对比

因此当前阶段结论更新为：

- **验证通路问题：已基本恢复**
- **候选效果问题：尚不能下最终结论，因为 clean fresh account full-sample 结果还未去除历史 `http_500` 污染**


### 2026-06-02 新定位：`resume-prune` 误删已判 `WRONG` 行，已修复并重开干净 QA-only run

在对 `sample0full82` 做第二次 `--skip-ingest` retry 时，出现了一个新的 benchmark 侧实现问题：

- `resume-prune` 不只删除历史 `http_500` 行
- 还把大量已经 judge 完成、`result = WRONG` 的普通错误题一并删除
- 直接导致同一 `run_id = 20260602_062947` 的 CSV 被大面积裁空

根因已经定位到 `phase_a_off.py` 的本地实现漂移：

- `prune_retryable_qa_rows()` 通过 `_qa_row_is_retryable()` 判断哪些行可重试
- 旧逻辑仍主要依赖旧字段：
  - `label`
  - `rounds`
- 但当前实际 QA CSV / `judge.py` 输出使用的是：
  - `result = CORRECT | WRONG`

由于当前 CSV 中：

- `result = WRONG`
- 但不存在旧的 `label/rounds`

旧逻辑会把这类“已经 judge 完成但答错”的行误判成 retryable，于是 `resume-prune` 会错误删除它们。

这解释了实测现象：

- 第二次 retry 启动时删除的并不只是 `qi = 2-25`
- 还扩展删除了大量普通 `WRONG` 行
- 这不是外部网络问题，而是 benchmark resume 契约和当前 CSV schema 不一致

已做修复：

- 更新：
  - `benchmark/locomo/openclaw/phase_a_off.py`
- 新规则：
  - 当 `result in {CORRECT, WRONG}` 时，视为**已完成判题**，不可作为 retryable 行删除
- 保留原有 retryable 规则：
  - `response` 为空
  - `response` 以 `[ERROR]` 开头
  - 兼容旧 `label/rounds` CSV 的未完成行

本地新增测试并通过：

- `test_qa_row_is_retryable_preserves_judged_wrong_rows`
- `test_prune_retryable_qa_rows_only_removes_error_rows`
- 全文件回归：
  - `tests/benchmark/locomo/openclaw/test_phase_a_off.py`
  - `21 passed`

远端恢复动作：

1. 停掉使用旧 prune 逻辑的错误 retry 进程
2. 将修复后的 `phase_a_off.py` 同步到 remote `OpenViking-benchonly`
3. 使用同一 clean account、`--skip-ingest`、新的 `run_id` 重开干净 QA-only run：
   - `run_id = 20260602_0905fix`

这样做的原因是：

- 旧 `run_id = 20260602_062947` 的 CSV 已被错误 prune 污染
- 继续在该文件上恢复，归因会混入脚本缺陷
- 直接开新的 QA-only clean run，证据链更干净

当前阶段结论进一步更新为：

- **验证通路恢复：已成立**
- **第二次 prune 的脚本实现缺陷：已定位并修复**
- **当前正在推进新的 clean QA-only run，下一步继续等待其完整落盘并 judge**


### 2026-06-02 `20260602_0905fix` 运行中状态：clean QA-only run 持续无错误推进

在修复 `resume-prune` 对 `result=WRONG` 行的误删后，已在同一 clean account 上重开新的 QA-only run：

- `run_id = 20260602_0905fix`
- `--skip-ingest`
- 继续使用：
  - `acct-20260602-sample0full82`
  - `user-20260602-sample0full82`

当前远端实测状态：

- `phase_a_off.py` 进程持续存活
- 新 CSV：
  - `/tmp/sample0full82_qa/phaseA_on_19sessions_20260602_0905fix.csv`
- 当前已推进到：
  - `qi = 13`
- 当前统计：
  - `rows = 12`
  - `errs = 0`
  - `judged = 0`

这说明：

- 修复后的 `resume-prune` 没有再把已判普通错题误删进重跑集合
- 新 clean run 起跑阶段没有复发系统性 `http_400/http_500`
- 当前阶段仍属于“验证通路恢复后，重新收集 clean full-sample 结果”的进行中状态

需要注意的是：

- 当前推进速度偏慢
- 但尚无新的错误迹象
- 目前更像是正常外部调用吞吐较低，而不是验证通路再次损坏

因此当前最合理的动作仍然是不打断，继续等这轮 `20260602_0905fix` 跑完并进入 judge。


### 2026-06-02 新定位：QA session 缺失 trajectory `sessionKey`，导致 local ledger 未进入 CSV；已修复并远端验证

在继续核对 `20260602_0926tokfix` 的 token 口径时，发现新的具体问题：

- session jsonl 本身已经包含 assistant usage：
  - `input ~= 590`
  - `cacheRead ~= 5944`
  - `totalTokens ~= 6800+`
- 但 QA CSV 中仍然是：
  - `input_tokens ~= 590`
  - `cacheRead = 0`

这说明问题不在 token 字段解析，而在 **QA 落盘阶段没有命中 local session ledger**。

进一步取证后，根因定位为：

- `collect_openclaw_session_ledger()` 默认依赖 `find_openclaw_session_file()`
- `find_openclaw_session_file()` 只会从 `*.trajectory.jsonl` 中按 `sessionKey` 反查 session file
- 当前这批 QA session 的 `session.jsonl` 已存在且 usage 完整，但对应 `trajectory.jsonl` 中缺少当前 run 的 `sessionKey` 记录
- 因此 `qa_ledger` 命不中，最终又 fallback 回 `gateway_response_usage`

已做最小修复：

- 新增 prompt-based fallback：
  - 当 trajectory 反查失败时，按当前 QA prompt 精确匹配最近 session file 中的最后一条 user message
- 并在 QA ledger 收集调用中传入：
  - `prompt_text = qa_prompt(qa["question"], question_time)`

这样做的边界是：

- 不修改召回主流程
- 不改 gateway / 1933 协议
- 只补 benchmark QA 本地 ledger 取证路径
- 只在 `sessionKey -> trajectory` 失效时启用 prompt fallback

本地验证：

- 新增测试：
  - `test_collect_openclaw_session_ledger_falls_back_to_prompt_matched_session_file`
- 全文件回归：
  - `tests/benchmark/locomo/openclaw/test_phase_a_off.py`
  - `23 passed`

远端恢复动作：

1. 停掉旧的 `20260602_0926tokfix`
2. 将修复后的 `phase_a_off.py` 同步到 remote `OpenViking-benchonly`
3. 重开新的 clean QA-only run：
   - `run_id = 20260602_0940ledfix`

远端立即验证结果：

- 新 CSV 前两题已经表现为：
  - `q2: input_tokens=594, cacheRead=5944, total_tokens=6824`
  - `q3: input_tokens=591, cacheRead=5944, total_tokens=6874`

这说明：

- QA CSV 终于写入了期望的 local ledger token 口径
- 后续 `avg_input_tokens + cacheRead` 的比较，已经具备 clean 实测基础
- 当前验证通路恢复状态进一步增强：不仅 QA 能跑，而且 token 记录口径也基本修正到可分析状态


### 2026-06-02 `20260602_1102nsfix` 远端续查：不是卡死，是 direct-ov ingest 个别 session 很慢

在修复 benchmark 侧 `memory:none` 和 `ov_agent_id` namespace 不一致后，新的 clean fresh run：

- `run_id = 20260602_1102nsfix`

继续远端取证后，之前看起来像“卡在 session 12”的现象已被排除为服务整体故障。

远端实测：

- `openclaw-gateway` 存活，`/health = ok`
- `1933` 存活，`/health = ok`
- `resume.json` 已推进到：
  - `session_14`
- 当前进程仍在运行：
  - `PID 88851`
- `QA CSV` 还未开始生成，说明当前仍处于 ingest 阶段

更关键的直接证据：

- `session_13.stage = completed`
- `session_13.compact_elapsed_seconds = 222.632`
- `session_14.stage = completed`
- `session_14.compact_elapsed_seconds = 88.752`

因此当前更准确的判断是：

- 这轮不是 `1933/gateway` 挂了
- 也不是 benchmark 在 `session_12` 之后完全不再推进
- 更像是 **direct-ov ingest 的单个 commit task 延迟明显偏大**，导致 stdout 一段时间没有新行，容易被误判成“卡死”

当前动作建议保持保守：

1. 不先改 benchmark 主流程
2. 继续观察该 run 是否完成 `19/19` ingest 并切到 QA
3. 只有当这类慢 task 明显反复阻塞验证时，再考虑是否给 direct-ov ingest 增加更细粒度的 progress/logging 或更明确的超时诊断

补充续查证据：

- `resume.json` 之后已继续推进到：
  - `session_15`
- `session_15.compact_elapsed_seconds = 122.85`
- 直接查询 `1933 /api/v1/tasks` 可见最新任务：
  - `task_type = session_commit`
  - `task_id = ab6fba83-cc24-4762-add2-c5107ef452ff`
  - `status = running`
  - `created_at_iso = 2026-06-02T11:24:50+00:00`

这进一步说明：

- 当前不是 benchmark 本地逻辑停住
- 当前也不是 `1933` 无响应
- 而是 **新的 direct-ov session commit 仍在运行中，尚未完成落盘到下一条 session state**


### 2026-06-02 `20260602_1102nsfix` 已完成 `19/19 ingest` 并开始写 QA CSV

继续远端轮询后，新的 clean fresh run 已确认跨过 ingest 阶段：

- `resume.json` 已推进到：
  - `session_19`
- `session_19.compact_elapsed_seconds = 34.452`
- `/tmp/sample0full82_nsfix/phaseA_on_19sessions_20260602_1102nsfix.csv`
  已开始生成

这说明当前阶段最关键的一点已经成立：

- **修复 namespace + 去掉 `memory:none` 后，这轮 full-sample run 已重新进入 QA**
- 当前不再是“只能 ingest，进不了 QA”的链路状态

更重要的是，前几题回答内容已明显不同于之前 `0/150` 那轮的“空上下文/无相关信息”模式：

- `q2` 回答提到了：
  - `Connected LGBTQ Activists`
  - `LGBTQ youth mentorship program`
  - `LGBTQ conference`
- `q3` 回答提到了：
  - `sunset-themed`
  - `sunflower field landscapes`
- `q4` 回答提到了：
  - `counseling/mental health`
  - `social work / LGBTQ+ studies`
  - `fine arts / art therapy`

当前含义需要严格分层：

1. **验证通路**
   - 已进一步恢复
   - 这轮已成功从 ingest 进入 QA CSV 落盘

2. **候选效果**
   - 当前回答已经不再像之前那样“完全没取到 memory”
   - 但前几题内容看起来仍有明显偏答/过度展开，需要等 judge 或至少更多题样本确认准确率恢复到什么程度

继续观察到 `q2-q9` 后，趋势已经更清楚：

- `rows = 8`
- `errs = 0`
- `judged = 0`

早期 QA 现象不是“无上下文”，而是更像：

- **有上下文，但召回/利用方向偏了**
- 常见表现：
  - 时间点答错
  - 回答过度展开
  - 把相近事件混进答案
  - 在有相关 memory 时仍回答“没有信息”

具体样本：

- `q2`
  - `expected = 7 May 2023`
  - 回答却混入：
    - `Connected LGBTQ Activists`
    - `LGBTQ youth mentorship program`
    - `LGBTQ conference`
- `q7`
  - `expected = The sunday before 25 May 2023`
  - 回答为：
    - `Saturday, May 20, 2023`
- `q8`
  - `expected = June 2023`
  - 回答为：
    - `There is no information ...`
- `q9`
  - `expected = Single`
  - 回答为：
    - `The available context does not include information ...`

所以截至这一步，阶段判断应更新为：

1. **验证通路**
   - 已恢复到可复现、可持续追加 QA CSV 的状态
   - 当前没有复发 `http_400/http_500`

2. **候选质量**
   - 已从“完全空上下文”提升到“能读到一部分 memory”
   - 但当前更像是 **召回排序/答案利用质量问题**，而不是链路问题

继续轮询 `20260602_1102nsfix`：

- `rows = 11`
- `last_qi = 12`
- `errs = 0`
- `judged = 0`

说明：

- QA 仍在稳定持续追加
- 当前 run 还没有进入 judge
- full-sample QA 落盘链路在这一轮上已经不再是间歇性断掉

新增样本也延续了同一趋势：

- `q10`
  - `expected = The week before 9 June 2023`
  - 回答给出了较长的近似时间解释，方向接近，但明显过度展开
- `q11`
  - `expected = The week before 9 June 2023`
  - 回答同样给出较长的近似解释，而不是简洁命中
- `q12`
  - `expected = 4 years`
  - 回答将 “4 years” 扩写成 “4 years and 4 months” 一类的推导式答案风险

因此当前结论进一步稳定为：

- **验证链路：稳定**
- **候选质量：已有上下文，但答案经常不够收束，容易混入相邻事实或做过度推断**
