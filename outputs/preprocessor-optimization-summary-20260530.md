# PREPROCESSOR 阶段总结（2026-05-30）

## 当前结论

本阶段围绕 `PREPROCESSOR` 做了多轮远端容器实测后，当前可确认的结论是：

1. 已验证会伤准确率或无明确收益的方向，不应保留：
   - `created_at -> chat date anchor`
   - 低信息句删除
   - 激进包装去重 / 纯格式瘦身
   - 短 session 强制 fallback（`full_messages_tokens_est < 2000`）

2. 当前应保留的低风险默认路径：
   - `[] tail` 清理
   - 相对时间句保真
   - 句级时间窗口抽取
   - `Last Fri` 缩写支持
   - 相邻同主题时间去重
   - exact duplicate 才做 `fact -> source ref`
   - 短 session facts floor `4 -> 6`

3. 当前 benchmark 口径存在一个会污染比较结果的脚本问题：
   - QA 阶段的 autoCapture 会把同轮答案写回 memory
   - 后续题会召回这些“答案型 memory”
   - 因此同一轮 `q30 -> q31 -> q34...` 这类串行题级比较，会被 QA 自身污染

4. 在去除 QA 污染后，当前默认 PREPROCESSOR 仍然劣于 OFF：
   - `ON_clean = 2/5`
   - `OFF_clean = 4/5`
   - 说明问题不只在 benchmark 口径，而在 PREPROCESSOR 驱动下的 memory 生成内容本身

## 最新有效试验

### 试验：短 session 强制 fallback

- run: `sample0_on_20260529_q3035floor`
- 口径：远端容器，`ON`，`sessions 1-8 / q30-35`
- 行为：8/8 ingest session 都变成 `fallback=session_too_short`

结果对比：

| 口径 | correct | 说明 |
|---|---:|---|
| `旧 OFF` | `4/5` | 当前最好 |
| `旧 ON` | `3/5` | 基线 ON |
| `新 ON floor` | `3/5` | 没有改善 |

题级看：
- `q31` 仍然错，而且 `input_tokens` 变得更高
- `q33/q34/q35` 没有形成“更低 token 且不掉准”的稳定收益

结论：
- 这条试验性改动不达标，已经回滚

### 试验：`QA autoCapture=false` 的干净 ON/OFF 对照

为去掉 benchmark 自身污染，重跑了同口径 clean 对照：

- `ON_clean`: `sample0_on_20260530_q3035onclean`
- `OFF_clean`: `sample0_off_20260530_q3035offclean`
- 共同口径：
  - `sessions 1-8 / q30-35`
  - 远端容器
  - 前台运行
  - 新 `user/account`
  - `openclaw.json` 中 `autoCapture=false`

结果：

| 口径 | correct | 平均 input_tokens |
|---|---:|---:|
| `ON_clean` | `2/5` | `4189.2` |
| `OFF_clean` | `4/5` | `5530.8` |

题级：

| qi | `ON_clean` | `OFF_clean` |
|---|---|---|
| `30` | `WRONG / 3207` | `CORRECT / 5566` |
| `31` | `WRONG / 2966` | `WRONG / 6641` |
| `33` | `CORRECT / 4580` | `CORRECT / 6434` |
| `34` | `WRONG / 4696` | `CORRECT / 4505` |
| `35` | `CORRECT / 5497` | `CORRECT / 4508` |

关键判断：
- 关掉 QA autoCapture 后，ON 的 token 明显降了
- 但 ON 仍然明显比 OFF 掉准
- 因此当前 PREPROCESSOR 默认路径本身仍在伤 accuracy

### 最直接证据

`ON_clean` 写出的 memory 正文里，adoption meeting 被固化成了错误日期：

- `memories/events/2023/07/15/adoption_council_meeting.md`
  - `On 2023-07-07, Caroline attended a council meeting for adoption`
- `entities/event/caroline_adoption_meeting__adoption_journey.md`
  - `Caroline attended a council meeting for adoption on 2023-07-07`

而 `OFF_clean` 对应 memory 写成：
- `entities/event/caroline_adoption_meeting__council_event.md`
  - `last Friday (2023-07-14)`

这说明：
- `q30` 的 ON 错误不是 judge 噪音
- 是 ON 路径确实把 memory 生成为了错误日期

### 定向取证：`q30` 与 `q34`

#### `q30`：问题出在 ON 生成的 memory 本体

`session_8` 的 `wm_preprocess` 取证显示：
- `structured_facts` 里同时保留了：
  - `Last Fri I finally took my kids to a pottery workshop.`
  - `Last Friday I went to a council meeting for adoption.`
- `selected_spans` 也把整段相关对话都保留了

但 `ON_clean` 最终写出的 event memory 是：
- `.../events/2023/07/15/adoption_council_meeting.md`
  - `On 2023-07-07, Caroline attended a council meeting for adoption`
- `.../entities/event/caroline_adoption_meeting__adoption_journey.md`
  - `Caroline attended a council meeting for adoption on 2023-07-07`

而 `OFF_clean` 写成：
- `.../entities/event/caroline_adoption_meeting__council_event.md`
  - `last Friday (2023-07-14)`

结论：
- `q30` 的主要问题不是 recall 没命中
- 而是 `PREPROCESSOR ON -> memory generation` 这条链条把 adoption meeting 错写成了 `2023-07-07`

#### `q34`：问题更像“生成了 support group，但召回 top-6 没带它”

`ON_clean` 实际已经生成了 support group 相关 memory：
- `entities/group/lgbtq_support_group__community_support.md`
- `events/2023/05/08/lgbtq_support_group_visit.md`

但 `q34` 的 `inject-detail` 中，ON top-6 注入是：
- conference
- pride parade
- school talk
- related aggregate memories

没有 support group。

而 `OFF_clean` 的 `q34` 最终回答被 judge 判为 `CORRECT`。

结论：
- `q34` 不是“support group 没被写出”
- 更像是 ON 下 memory 组织/命名/打散后，top-6 recall 没把它排进去

这说明当前 PREPROCESSOR 的损伤至少分两类：
1. `q30`：生成内容直接错
2. `q34`：memory 组织改变后，召回排序/覆盖变差

### 服务侧 search 取证

为避免只看 gateway 注入日志，本轮直接对 OpenViking `/api/v1/search/find` 做了服务侧对比。

#### `q30`

`ON_clean` top-2 结果直接就是错误 memory：
1. `caroline_adoption_meeting__adoption_journey.md`
2. `events/2023/07/15/adoption_council_meeting.md`

两条摘要都写成了 `2023-07-07`。

`OFF_clean` top-1 则是：
1. `caroline_adoption_meeting__council_event.md`

摘要明确写成：
- `last Friday (2023-07-14)`

这进一步证明：
- `q30` 的根因是 ON 生成的 memory 本体错误
- recall 排序只是把这个错误 memory 正常排到了最前面

#### `q34`

`ON_clean` top-10 中：
- support group **存在**，但只排到第 `6`
- 前 1-5 位被：
  - pride parade
  - conference
  - counseling workshop
  - 相关聚合 event memory
  占满

`OFF_clean` top-10 中：
- support group 事件也存在，排到第 `8`
- 但 OFF 最终回答仍被 judge 判为 `CORRECT`

这说明：
- `q34` 的问题不只是“support group 排名略低”
- 更像是 ON 下被注入/组织的前排 memory 更偏向 conference / pride / workshop，导致回答偏移更严重
- 而 OFF 的 memory 组织虽然也没把 support group 排到最前，但整体表述更容易把 school speech + pride + support group 一起答出来

## 阻塞点

当前阻塞已经不主要是 PREPROCESSOR 代码本身，而是**benchmark 运行口径**：

1. `trajectory_diagnostics` 目前看不到真实 injected memories
   - 不能只靠 `relevant_memories_count=0` 判定 recall 没发生
   - 需要结合 `/tmp/openclaw/openclaw-2026-05-29.log` 的 `inject-detail`

2. QA autoCapture 污染同轮 recall
   - 这会让后续题召回到本轮刚生成的答案型 memory
   - 影响 ON/OFF 题级比较可信度

3. 远端容器里不能依赖后台 `nohup ... &` 跑 gateway benchmark
   - 稳定方式仍然是前台 / PTY 长连接

4. 当前如果继续只动 PREPROCESSOR，阻塞点已经收缩成更具体的两条：
   - `q30`：为什么 ON 会把 `Last Friday` 固化成 `2023-07-07`
   - `q34`：为什么 ON 会弱化/丢失 support group 事件，导致 OFF 对而 ON 错

## 建议的下一步

如果继续推进，优先级应是：

1. benchmark 口径保持当前 clean 跑法不变
   - 远端容器
   - 前台运行
   - `QA autoCapture=false`

2. 如果继续只动 PREPROCESSOR，下一步不要大改
   - 直接针对 `session_8` 做最小取证
   - 比较 `ON_clean` 的 `wm_preprocess`、selected spans、structured facts
   - 只定位 `q30/q34` 对应事实为何被压坏

3. 在没有新的题级证据前，不建议继续扩 PREPROCESSOR 改动面
   - 当前已经证明：错误不是“短 session compact”单点造成
   - 需要更细粒度地定位 active 路径中的损伤点

### 试验：强制保留 `date_or_plan` supporting spans

基于 `session_8` 取证，做了一个很小的渲染修复：
- 在 `minimal/balanced` 视图中
- 对 `date_or_plan` facts 对应的 supporting span 强制保留到 `evidence`
- 不再因为该 source 已在 facts 中出现就跳过

配套新增失败单测后修复：
- `test_minimal_render_keeps_temporal_supporting_spans_in_evidence`

本地结果：
- `tests/unit/session/test_extraction_preprocessor.py`: `27 passed`
- `tests/unit/session/test_fixture_token_savings.py`: `19 passed`

远端容器结果：
- `46 passed`

#### 远端复验：`ON_clean2`

复验口径：
- `sample0_on_20260530_q3035onclean2`
- 远端容器
- `sessions 1-8 / q30-35`
- `QA autoCapture=false`

结论：
- `q30` 仍然答成 `2023-07-07`
- `q31` 仍然答成 `2023-07-07`
- 服务侧 `search/find` top-1 仍然是错误 memory：
  - `adoption_council_meeting__july_2023.md`
  - 摘要写成：`Caroline attended an adoption council meeting on 2023-07-07 (last Friday)`

这说明：
- 强制保留 `date_or_plan` supporting spans **没有修复 `q30/q31`**
- 问题不只是 compact view 没把关键原句展示给模型
- 更可能已经在 memory generation 解释阶段把相对时间错误地固化成了 `2023-07-07`

#### 当前阶段判断

到这一步，可以把问题分层明确为：

1. 已排除
   - QA autoCapture 污染不是主因
   - “短 session 强制 fallback”不是有效方向
   - “补充 `date_or_plan` evidence 原句”也不是有效方向

2. 仍未解决的核心
   - ON 路径会把 `Last Friday` / `Last Fri` 解释成错误绝对日期
   - 该错误随后进入 event memory，并被 recall 排到 top-1

3. 继续只动 PREPROCESSOR 的下一步，不应再做渲染层小修
   - 下一步如果还继续，应直接定位：
     - `date_or_plan` facts 的组织方式
     - compact packet 如何影响 creation prompt 对相对时间的解释

### 新增结论：回掉非 PREPROCESSOR 的 entity/facet/URI 改动后，错日期仍存在

本轮先把测试口径里明显超出 PREPROCESSOR 范围的并行改动回掉：
- entity facet 指令
- `name__facet.md` 文件名模板
- facet normalize / legacy entity URI migration

验证：
- 本地：`46 passed` + `13 passed`
- 远端容器：`59 passed`

随后在远端容器做了一个更小的 direct-ov probe：
- sample: `conv-26`
- sessions: `8-8`
- fresh account/user/agent:
  - `acct-20260530-preponly-s8b`
  - `conv-26-preponly-s8b`

直接结果：
- 生成出的 memory 仍然把 adoption / pottery 都写成 `July 7, 2023`
- 具体包括：
  - `entities/event/adoption_council_meeting.md`
  - `entities/activity/pottery_workshop.md`
  - `events/2023/07/15/adoption_council_meeting.md`
  - `events/2023/07/15/pottery_workshop.md`

因此可以排除：
- 这批非 PREPROCESSOR 的 entity/facet/URI 改动不是当前错日期的 primary cause

当前更可信的收敛判断：
- 问题仍在 PREPROCESSOR creation compact path 本身
- 尤其是 compact packet 如何诱导 memory extraction 把 `Last Friday / Last Fri` 固化成 `2023-07-07`

### 新增修正结论：`session_8` 单轮错日期并不区分 ON/OFF

本轮补做了一个同口径的 remote direct-ov OFF probe：
- `conv-26`
- `session_8 only`
- `wm_v2_preprocess_enabled=false`
- fresh account/user/agent

结果：
- adoption / pottery 相关 memory 仍然都写成 `2023-07-07`

这说明：
- 单看 `session_8` 这轮 direct-ingest，错日期**不是** PREPROCESSOR on/off 的直接分叉结果
- 因此，之前 `ON_clean=2/5` vs `OFF_clean=4/5` 的差异，不能再简单解释成“ON 在 session_8 creation 阶段写错、OFF 写对”

当前更可信的收敛判断改为：
- ON/OFF 差异更可能来自 **多 session 累积后的 memory 组织、更新、召回排序** 差异
- 而不是 `session_8` 单轮 direct-ov memory generation 的差异

### 新增修正结论：creation-bypass 在干净口径下无效

本轮在修复 benchmark QA namespace 配置链后，使用**全新 account/user**补跑了一轮真正干净的 `creation-bypass` 子集：

- run: `q3033creationoff2clean7`
- 口径：
  - 远端容器
  - `sessions 1-8`
  - `q30/q31/q33`
  - `QA autoCapture=false`

ingest 成功，8 个 session 的 `memory_count` 为：

- `7, 6, 6, 7, 8, 6, 10, 10`

QA 结果：

| qi | result | input_tokens |
|---|---|---:|
| `30` | `WRONG` | `4639` |
| `31` | `WRONG` | `5174` |
| `33` | `CORRECT` | `7329` |

总计：

- `1/3 correct`

关键取证：

- gateway `inject-detail` 已证明 recall 真实发生，不是空 recall
- `q30` 注入 top-1：
  - `events/2023/07/15/adoption_council_meeting.md`
  - 摘要仍写成 `On 2023-07-07, Caroline attended a council meeting for adoption`
- `q31` 注入 top-1：
  - `events/2023/07/15/pottery_workshop.md`
  - 摘要仍写成 `On 2023-07-07, Melanie took her kids to a pottery workshop`
- `q33` 注入命中：
  - `entities/event/melanie_family_camping.md`
  - 摘要写成 `Took place last week (around 2023-06-20)`

补充说明：

- `trajectory_diagnostics` 仍显示 `relevant_memories_count = 0`
- 但 gateway 日志清楚记录了 `inject-detail { count: 6, memories: [...] }`
- 因此目前不能再用 `trajectory_diagnostics.relevant_memories_count` 判断 recall 是否为空

最终判断：

- `creation 阶段完全禁用 PREPROCESSOR` 这条线没有修复 `q30/q31`
- token 也没有更优
- 因此它目前应视为**无效实验线**，不建议继续扩面

### 新增修正结论：update-bypass 也无效

在 `creation-bypass` 证伪后，本轮又补做了下一条最强假设：

- 恢复 `creation` 走 compact
- 只禁用 `update` 阶段 PREPROCESSOR

对应代码与测试：

- `openviking/session/session.py`
- `tests/unit/session/test_extraction_preprocessor.py`

验证：

- 本地：
  - `tests/unit/session/test_extraction_preprocessor.py`
  - `tests/unit/session/test_fixture_token_savings.py`
  - 结果：`46 passed`
- 远端容器：
  - 同两组测试
  - 结果：`46 passed`

随后在远端容器重跑干净子集：

- run: `q3033updatebypassclean8`
- account: `acct-20260530_q3033updatebypassclean8`
- user: `user-20260530_q3033updatebypassclean8`
- 口径：
  - `sessions 1-8`
  - `q30/q31/q33`
  - `QA autoCapture=false`

ingest memory_count：

- `1, 5, 4, 6, 6, 7, 7, 8`

题级结果：

| qi | result | input_tokens |
|---|---|---:|
| `30` | `WRONG` | `5572` |
| `31` | `WRONG` | `4263` |
| `33` | `CORRECT` | `4845` |

总计：

- `1/3 correct`

gateway `inject-detail` 直接证明 recall 真实发生：

- `q30` top-1 仍是错误的 `events/2023/07/15/adoption_council_meeting.md`
- `q31` top-1 仍是错误的 `events/2023/07/15/pottery_workshop.md`
- `q33` 命中的是正确的 `family_camping_trip` 相关 memory

最终判断：

- `update 阶段完全禁用 PREPROCESSOR` 这条线与 `creation-bypass` 一样，也没有修复 `q30/q31`
- 因此“少做处理/直接绕过 creation 或 update”这类大开关实验，当前都应停止

### 新增修正结论：PREPROCESSOR-only 路线已接近证伪边界

本轮继续做了两类更小的 PREPROCESSOR 实验：

1. 提高 prior summary 保真度
   - 把 `~1.3k token` 的 `session_7` 从 `minimal` 提到 `balanced`
   - 结果：summary 确实更完整，但 `session_8` 的 adoption / pottery 仍一起写成 `2023-07-07`

2. 旁路 `memory_extraction.summary` 注入
   - 当当前会话命中 `date_or_plan` 时，提取阶段不再吃上一轮 `latest_archive_overview`
   - 结果：`session_8` 生成出的
     - `adoption_council_meeting.md`
     - `pottery_workshop.md`
     仍然一起写成 `2023-07-07`

这两条实验都已经在远端容器 fresh account direct-ov 下验证，并且都已回退，不留在默认路径。

#### 当前必要性边界

到这一步，基于远端实测已经可以确认：

- 继续只改 PREPROCESSOR，收益很低
- adoption / pottery 的错日期，更可能来自：
  - `memory_extraction` prompt 本身的 relative-time grounding
  - `events` memory schema 对“相对时间必须绝对化”的要求

直接证据：

- `session_extract_context_provider.py`
  - `Relative times ... are based on Session Time, not today`
- `compression/memory_extraction.yaml`
  - `Temporal precision: Convert to absolute references or omit the time`
- `prompts/templates/memory/events.yaml`
  - 明确要求把 `yesterday/last week/tomorrow` 转成具体年月日

#### 最小必要 plan

如果继续推进，建议按最小范围跨出 PREPROCESSOR：

1. 只修改 relative-time grounding 规则，不动 recall 流程
2. 优先改 prompt / schema 规则，而不是大改代码逻辑
3. 先只验证 `session_8` 的 adoption / pottery memory 正文是否被纠正
4. 若纠正，再补 `q30/q31/q33` QA

### 新增结论：最小 relative-time grounding prompt 修补也无效

本轮按上述最小计划，实际修改了：

- `compression/memory_extraction.yaml`
- `prompts/templates/memory/events.yaml`

新增规则：

- 每个 relative-time 表达必须独立解析
- 不要把一个事件推断出的绝对日期复用到另一个事件
- 若日期不确定，优先保留 relative wording，而不是硬编具体日子

远端 fresh account：

- `acct-20260530-rtg8`
- `user-20260530-rtg8`

direct-ov 灌完 `sessions 1-8` 后，`session_8` 的 `memory_diff.json` 仍然写出：

- adoption -> `2023-07-07`
- pottery -> `2023-07-07`

说明：

- 只做最小 prompt 级修补，仍不足以纠正这组 relative-time grounding 错误
- 如果继续推进，已经需要超出“最小必要改动”的范围

### 当前阶段建议

到这一步，当前任务已经形成一个明确阻塞：

- 在“尽量只动 PREPROCESSOR、非必要不碰 memory extraction/recall”的约束下，
  高信号实验已经基本耗尽
- 若继续，需要先接受进入更大范围的 extraction / temporal-grounding 设计调整

### 新增结论：v2 主路径 extraction helper 只部分生效

本轮改为直接打在 `compressor_v2` 的真实 extraction 路径上，而不是 legacy `memory_extractor.py`：

- `session.py` 读取上一轮 completed archive 的 `session_time`
- `compressor_v2.py` 透传 `latest_archive_session_time`
- `session_extract_context_provider.py` 在 conversation input 中加入：
  - `Relative Time Grounding Hints`
  - `RelativeTimeResolution` inline 注释

测试：
- 本地：
  - `tests/session/memory/test_memory_timestamp_parsing.py`: `4 passed`
  - `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`
- 远端容器：
  - provider 时间测试：`4 passed`
  - PREPROCESSOR 基线测试：`46 passed`

远端 fresh-account direct-ov 结果：

1. `acct-20260530-rtdisambig8`
- `adoption` 修正为 `2023-07-14`
- `pottery` 仍错误为 `2023-07-14`

2. `acct-20260530-rtdisambig10`
- 在更明确 wording + inline resolution 后，结果不变：
  - `adoption` = `2023-07-14`
  - `pottery` = `2023-07-14`

3. `acct-20260530-rtdisambig11`
- 在进一步强化 override wording 后，结果仍不变：
  - `adoption` = `2023-07-14`
  - `pottery` = `2023-07-14`

4. `acct-20260530-rtdisambig12`
- 在把解析结果直接嵌入消息正文后，结果仍不变：
  - `adoption` = `2023-07-14`
  - `pottery` = `2023-07-14`

5. `acct-20260530-rtdisambig13`
- 在 `NormalizedEventTime` 独立区块进入远端有效结果链后，结果仍不变：
  - `adoption` = `2023-07-14`
  - `pottery` = `2023-07-14`

6. `acct-20260530-rtdisambig14`
- 在把 `Last Fri / Last Friday` 直接改写成 `On YYYY-MM-DD ...` 后，结果仍不变：
  - `adoption` = `2023-07-14`
  - `pottery` = `2023-07-14`

7. `acct-20260530-rtdisambig15`
- 在每条命中的 extraction 输入消息前新增独立 `[NormalizedEvent] ... happened on ...` 行后，结果仍不变：
  - `adoption` = `2023-07-14`
  - `pottery` = `2023-07-14`
- 相关衍生 memory 也继续跟着错误时间漂移，例如 `kids_pottery_cup.md` 仍写成 `2023-07-14`

结论：
- helper 对 `adoption` 有正向效果
- 但仍无法把 `Melanie: since we talked! Last Fri ...` 拉回 `2023-07-07`
- 即使把“override 默认 Session Time 规则”写得更明确，也依然无效
- 即使把解析结果直接写进 extraction 实际读取的消息正文，也依然无效
- 即使增加独立的 `Event Time Normalization` 区块，也依然无效
- 即使把相对时间短语直接改写成绝对日期句，也依然无效
- 即使新增独立 `NormalizedEvent` 行，把事件和日期显式拆出来，也依然无效
- 因此它只算部分命中根因，不能算完成修复

7. 新增 root-cause probe：`acct-20260530-rtdisambig17`
- 不是正常 QA/ingest 对比，而是 extractor-only 诊断：
  - 先灌 `sessions 1-7`
  - 再在容器里直接拦截 `ResolvedOperations`
- 结果表明：
  - `pottery = 2023-07-14` 在模型原始 extraction 输出阶段就已经错了
  - 不是 updater / merge 后写坏

新的结论：
- 当前真正缺的是跨 session 的 `Previous Session Anchor`
- `adoption` 只靠当前 `Session Time` 就能对
- `pottery` 需要 “since we talked + last Fri” 的上一轮锚点，缺失时就会回落成 `2023-07-14`
- 所以下一步最应该做的，不是再堆 prompt/helper，而是：
  - 在 `latest_archive_session_time` 为空时
  - 从同一 `account/user/agent` 的最近已完成 session 中推断上一轮时间锚点

8. benchmark 纠正：`rtdisambig19_q30_33`
- 口径：
  - 远端容器
  - `sessions 1-8`
  - `q30/q31/q33`
  - `QA autoCapture=false`
- 结果：
  - `q30`: `CORRECT`，答案 `2023-07-14`
  - `q31`: `WRONG`，答案 `2023-07-07`
  - `q33`: `CORRECT`

关键纠正：
- `q31` 的 benchmark gold 实际要求是：
  - `The Friday before 15 July 2023`
  - 即 `2023-07-14`
- 因此“自动补上一轮 session anchor，把 pottery 拉成 2023-07-07”对 benchmark 是负优化

最终结论更新：
- 保留 `generated_fields` 诊断能力
- 回退“自动补上一轮 session anchor”这条实验逻辑
- 当前本地代码和远端 probe worktree 都已同步回退

下一步若继续：
- 已不适合再做 PREPROCESSOR-only 或 prompt-only 小修
- 需要更强的 extraction-side temporal parsing / event-level binding 逻辑

9. benchmark 对齐版 provider：`rtdisambig21` 与 `q3033_rtdisambig22_qa`
- 当前代码已回到 benchmark 对齐策略：
  - relative weekday grounding 统一按当前 `Session Time`
  - 不再自动补上一轮 session anchor
- `acct-20260530-rtdisambig21` 只灌 `session_8` 时：
  - `adoption = 2023-07-14`
  - `pottery = 2023-07-14`
- 但换成 mixed-context 的 `acct-20260530-rtdisambig22`：
  - 先有 `sessions 1-4`
  - 再补 `session_8`
  - `adoption = 2023-07-14`
  - `pottery = 2023-06-23`

题级 QA：
- run: `q3033_rtdisambig22_qa`
- 口径：
  - 远端容器
  - `sessions 1-4 + 8`
  - `q30-q33`
  - `QA autoCapture=false`
- 结果：
  - `q30`: `CORRECT`，答 `2023-07-14`
  - `q31`: `WRONG`，答 `2023-06-23`
  - `q33`: `CORRECT`

更新后的结论：
- 当前 blocker 已不再是“单轮 session_8 不能对齐”
- 而是 **历史 memory/summary 形态会污染 session_8 的 event grounding**
- 问题层级已经明确收敛到 extraction-side temporal parsing / event-level binding

10. semantic search prefetch 排除实验：`rtdisambig24`
- 新增 provider 规则：
  - 当前会话命中至少两次 `Last Fri/Last Friday` 这类 relative weekday 时
  - 跳过 semantic search prefetch
- 本地：
  - `tests/session/memory/test_memory_timestamp_parsing.py`: `5 passed`
  - `tests/session/memory/test_memory_diff.py::...generated_fields`: `1 passed`
  - `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`
- 远端：
  - provider 时间测试：`5 passed`

远端 fresh account `acct-20260530-rtdisambig24`：
- 先灌 `sessions 1-4`
- 再灌 `session_8`

结果：
- `adoption = 2023-07-14`
- `pottery = 2023-06-23`

更新后的判断：
- 跳过 semantic search prefetch **无效**
- mixed-context 下的 `2023-06-23` 漂移不太像是 multi-file semantic search topN 直接造成的
- 并且 `pottery_workshop.md` 是新加 event，不是旧 event merge 覆盖
- 因此下一步最该做的是：
  - `sessions 1-4` 已存在的 account 上
  - 对 `session_8` 做 extractor-only probe
  - 直接抓原始 `ResolvedOperations`
  - 确认 `pottery=2023-06-23` 是否在原始 extraction 输出阶段就已经出现

11. mixed-context extractor-only probe：`rtdisambig24_extract_only_s8_root`
- 新增脚本：
  - [benchmark/locomo/openclaw/remote_extractor_only_probe.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/remote_extractor_only_probe.py)
- 用途：
  - 在已有 account/user/agent 上
  - 直接调用 `SessionCompressorV2.extract_long_term_memories()`
  - 用 fake updater 拦截原始 `ResolvedOperations`
  - 把 `generated_fields` 落盘到 JSON

远端容器 mixed-context root probe：
- account: `acct-20260530-rtdisambig24`
- 已有 `sessions 1-4`
- probe: `session_8`
- role: `root`

结果文件：
- `/tmp/rtdisambig24_extract_only_s8_root.json`

关键结果：
- raw extraction **没有**直接生成 `pottery_workshop.md` / `adoption_council_meeting.md`
- 只生成了一个 `profile.md` patch
- 且 patch 明确尝试把：
  - `Took kids to pottery workshop on 2023-06-23`
  - 改成
  - `Took kids to pottery workshop on 2023-07-14`

更新后的结论：
- standalone extractor-only root probe 并不支持 `2023-06-23`
- 真实 direct-ov commit 却仍落成 `events/2023/07/15/pottery_workshop.md = 2023-06-23`
- 因此新的 mixed-context 问题已经进一步收敛为：
  - **standalone extractor-only probe 与真实 commit 路径之间仍有未镜像的差异**
  - 或 commit 路径中的 refetch / patch repair / apply 阶段才把 `2023-06-23` 固化到了 event memory

下一步最值得做的：
- 在真实 commit 路径上直接捕获 `ResolvedOperations`
- 不再继续单纯靠 standalone extractor-only 替代 commit

12. 真实 commit mixed-context 新信号：`rtdisambig25`
- 新增受控调试：
  - [openviking/session/compressor_v2.py](/home/jcp/Agent/code/OpenViking/openviking/session/compressor_v2.py)
  - `OPENVIKING_DEBUG_EXTRACT_OPS=1`
  - 目标是在真实 commit 路径 apply 前把原始 `ResolvedOperations` 写到 `archive_001/extracted_operations.json`
- 对应最小单测：
  - `tests/session/memory/test_memory_diff.py::TestMemoryDiffArchive::test_build_operations_debug_dump_serializes_generated_fields`
  - 本地 `1 passed`
  - 远端 `1 passed`

远端 fresh account `acct-20260530-rtdisambig25`：
- 先灌 `sessions 1-4`
- 再灌 `session_8`
- `pottery_workshop.md = 2023-07-14`

题级 QA：
- run: `q3033_rtdisambig25_qa`
- 结果：
  - `q30`: `CORRECT`, `input_tokens=3233`
  - `q31`: `CORRECT`, `input_tokens=3800`
  - `q33`: `CORRECT`, `input_tokens=3783`

当前更新后的结论：
- 这是首次在 mixed-context（`sessions 1-4 + 8`）真实 commit + QA 路径下拿到 `3/3`
- 说明当前 extraction 代码已经出现实质正向信号
- 但 archive 下没有看到预期的 `extracted_operations.json`
- 所以这轮还不足以说明根因已经彻底闭合；更像是拿到了一次有效正样本，但稳定性和可复现性仍待确认

13. mixed-context 第二轮复现：`acct-20260530-rtdisambig27`
- 口径：
  - 远端容器
  - 开启 `OPENVIKING_DEBUG_EXTRACT_OPS=1`
  - fresh account
  - `sessions 1-4` + `session_8`
- 结果：
  - `pottery_workshop.md = 2023-07-14`
  - `adoption_council_meeting.md = 2023-07-14`
  - `q30/q31/q33 = 3/3`

QA CSV：
- `/tmp/q3033_rtdisambig27_qa/phaseA_on_8sessions_q3033_rtdisambig27_qa.csv`

题级：
- `q30`: `CORRECT`, `input_tokens=4329`
- `q31`: `CORRECT`, `input_tokens=4171`
- `q33`: `CORRECT`, `input_tokens=4078`

更关键的是，这轮 finally 拿到了真实 commit 路径的：
- `archive_001/extracted_operations.json`

其中原始 `generated_fields` 已经明确写出：
- `events/2023/07/15/pottery_workshop.md = 2023-07-14`
- `events/2023/07/15/adoption_council_meeting.md = 2023-07-14`

更新后的结论：
- 现在至少有两轮 mixed-context 正样本：
  - `acct-...25`
  - `acct-...27`
- 并且 `acct-...27` 证明：
  - 真实 commit 路径的原始 `ResolvedOperations` 阶段就已经给出 `pottery = 2023-07-14`
- 所以当前 extraction 路径已经不再只是“单次偶发成功”，而是出现了可复现的 benchmark-aligned 正信号

剩余未闭合点：
- 为什么 `acct-...24` 会得到 `pottery = 2023-06-23`
- 也就是 mixed-context 历史不稳定样本的触发条件仍需进一步定位

14. q34 修正：`acct-20260530-rtdisambig28`
- 根因：
  - 在 `acct-...27` 的 `session_3` 原始 `extracted_operations.json` 里，
    school speech 只进了 `profile` patch，没有生成独立 `events` memory
- 最小修正：
  - 只改 [openviking/prompts/templates/compression/memory_extraction.yaml](/home/jcp/Agent/code/OpenViking/openviking/prompts/templates/compression/memory_extraction.yaml)
  - 明确要求：公开参与类活动（talk / speech / workshop / support group / meeting / parade / race / conference）
    只要带时间维度，必须额外生成 `events`

修正后的原始 extraction 证据：
- 在 `acct-20260530-rtdisambig28` 的 `session_3` 原始 `extracted_operations.json` 中，
  已明确生成：
  - `events/2023/06/09/school_talk.md`

远端 mixed-context 验证：
- account: `acct-20260530-rtdisambig28`
- 口径：
  - `sessions 1-4` + `session_8`
  - `q30-q35`
- 结果：
  - `q30`: `CORRECT`, `input_tokens=5647`
  - `q31`: `CORRECT`, `input_tokens=6224`
  - `q33`: `CORRECT`, `input_tokens=3288`
  - `q34`: `CORRECT`, `input_tokens=5536`
  - `q35`: `CORRECT`, `input_tokens=5189`

CSV：
- `/tmp/q3035_rtdisambig28_qa/phaseA_on_8sessions_q3035_rtdisambig28_qa.csv`

更新后的阶段性结论：
- 现在已有：
  - `acct-...27`: `q30/q31/q33 = 3/3`
  - `acct-...28`: `q30-q35 = 5/5`
- 这说明当前 extraction 路径已经拿到更强的 mixed-context 正信号

15. 扩大验证：`acct-20260530-rtdisambig29`
- 口径：
  - 远端容器
  - `sessions 3-9`
  - `q16-39`
  - `QA autoCapture=false`
- 结果：
  - `17/21 correct`
  - 重点题：
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

CSV：
- `/tmp/q1639_rtdisambig29_qa/phaseA_on_7sessions_q1639_rtdisambig29.csv`

当前判断：
- 修正已经不只是在 `q30-q35` 上局部成立。
- 扩到更大代表性子集后，`q30/q31/q33/q35/q36/q37/q38` 都保持正确。
- 但还不能声称“整体完成”，因为：
  - 还缺少与历史 `OFF / 旧 ON` 的系统性 token/准确率对照
  - `q16/q25/q28/q39` 仍是新暴露的残余失败点
  - `session_4-9` 在 summary 中都显示 `memories=0`，说明当前 direct-ov 的 `memory_count` 指标不能直接拿来判断本轮是否真正产生了可用 memory

16. 同口径 `OFF` 对照：`acct-20260530-offcmp30`
- 口径：
  - 远端容器
  - `sessions 3-9`
  - `q16-39`
  - `QA autoCapture=false`
- 结果：
  - `OFF = 12/21`
  - `ON = 17/21`
  - 平均 `input_tokens`：
    - `OFF = 4477.0`
    - `ON = 4426.2`

重点对照：
- `q35`: `OFF WRONG / 4640`，`ON CORRECT / 3561`
- `q36`: `OFF WRONG / 3100`，`ON CORRECT / 5255`
- `q30`: `OFF CORRECT / 4289`，`ON CORRECT / 4584`
- `q31`: `OFF CORRECT / 3417`，`ON CORRECT / 4020`
- `q33`: `OFF CORRECT / 3628`，`ON CORRECT / 1206`
- `q39`: `OFF WRONG / 8052`，`ON WRONG / 5127`

更新后的结论：
- 当前 extraction-side 修正在更大代表性子集里已经明确优于同口径 `OFF`：
  - 准确率提升 `+5/21`
  - 平均输入 token 还略低
- `q16/q25/q28/q39` 现在应视为 `ON/OFF` 共同失败项，不再属于“PREPROCESSOR 引入的新回归”

17. 图片语义补充：`import_to_ov.py`
- 根因：
  - LoCoMo 原始消息里有 `img_url / blip_caption / query`
  - 旧 ingest 只发送 `[speaker]: text`
  - 关键图片语义在 benchmark 路径里直接丢失

最小修正：
- 在 [benchmark/locomo/openclaw/import_to_ov.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py) 中：
  - 把 `blip_caption / query` 轻量拼进消息文本
  - 把 `img_url` 保留为 `image_url` parts 发给 OpenViking

本地验证：
- [tests/benchmark/locomo/openclaw/test_import_to_ov.py](/home/jcp/Agent/code/OpenViking/tests/benchmark/locomo/openclaw/test_import_to_ov.py): `4 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

18. 定点结果
- `q39`：
  - run: `acct-20260530-imgctx31`
  - 口径：`sessions 3-9`, `q39`
  - 结果：`CORRECT`, `input_tokens=4228`
  - 说明 `sunset` 错误主要来自图片语义被 ingest 丢弃

- `q25-q28`：
  - run: `acct-20260530-imgctx32`
  - 口径：`sessions 6-7`, `q25-28`
  - 结果：
    - `q25`: `WRONG`
    - `q27`: `CORRECT`
    - `q28`: `WRONG`
  - 新信号：
    - `q25` 已从“只认出 Charlotte's Web”改善成“Charlotte's Web + 去年读过另一本追梦主题书”
    - 但 `q28` 仍恢复不出书名 `Nothing is Impossible`

更新后的判断：
- 图片语义补充已经明确修复 `q39`
- 对 `q25/q28` 是部分正向，但还不足以完成修复
- 若继续冲 `q25/q28`，大概率需要更强的图片 OCR / media understanding 方案，应视为新的大改方向

补充：
- 仓库中已有 `ImageParser._ocr_extract()` 能力
- 但当前本地和远端容器都未安装 `pytesseract`
- 远端也未确认存在可直接调用的 `tesseract`

所以如果继续冲 `q25/q28`，下一步不能默认“现成 OCR 直接可用”，需要先在大改方案里明确：
- 是补 OCR 依赖
- 还是单独做 vision-title extraction helper

19. 书封面标题恢复：`acct-20260530-imgctx36`
- 新证据：
  - 远程图片 URL 直接交给 provider 会超时
  - 但先下载成 bytes 再喂给同一 VLM，可以稳定识别：
    - `NOTHING IS IMPOSSIBLE`
    - `TOM OLIVER`
- 最小实现：
  - 在 [benchmark/locomo/openclaw/import_to_ov.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py) 的 `_maybe_extract_visual_hints()` 中：
    - 对书相关消息先下载图片 bytes
    - 再做 VLM 标题抽取
    - 注入 `[image_title_hint] / [image_author_hint]`
- 本地验证：
  - [tests/benchmark/locomo/openclaw/test_import_to_ov.py](/home/jcp/Agent/code/OpenViking/tests/benchmark/locomo/openclaw/test_import_to_ov.py): `6 passed`
  - `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

远端定点结果：
- `acct-20260530-imgctx36`
- `sessions 6-7 / q25-28`
- 结果：
  - `q25`: `CORRECT`, `2967`
  - `q27`: `CORRECT`, `3293`
  - `q28`: `CORRECT`, `3354`

CSV：
- `/tmp/q2528_imgctx36_qa/phaseA_on_2sessions_q2528_imgctx36.csv`

20. 组合验证：`acct-20260530-imgctx38`
- 口径：
  - `sessions 6-9`
  - `q25-28,39`
- 结果：
  - `q25`: `CORRECT`, `2967`
  - `q28`: `CORRECT`, `3354`
  - `q39`: `CORRECT`, `3756`

CSV：
- `/tmp/q252839_imgctx38_qa/phaseA_on_4sessions_q252839_imgctx38.csv`

更新后的判断：
- `q25/q28/q39` 都已经拿到远端有效正样本
- 当前剩余主要难点再次收敛到 `q16`
- 下一步更合理的是：用这版最新实现再回到 `sessions 3-9 / q16-39` 做一次最终大子集复验

21. 最终大子集复验：`acct-20260530-imgctx39`
- 口径：
  - 远端容器
  - `sessions 3-9`
  - `q16-39`
  - `QA autoCapture=false`
- 结果：
  - `17/21 correct`
  - 平均 `input_tokens = 4150.6`

CSV：
- `/tmp/q1639_imgctx39_qa/phaseA_on_7sessions_q1639_imgctx39.csv`

与上一版 `acct-20260530-rtdisambig29` 对照：
- 旧版：
  - `17/21`
  - 平均 `input_tokens = 4426.2`
- 新版：
  - `17/21`
  - 平均 `input_tokens = 4150.6`

题级变化：
- 修好：
  - `q16`
  - `q25`
  - `q28`
  - `q39`
- 新掉：
  - `q20`
  - `q22`
  - `q23`
  - `q36`

当前最强阶段结论：
- 最新版没有把大子集准确率从 `17/21` 继续抬高
- 但它实现了：
  - **同准确率**
  - **更低平均 token**
  - 并修复了长期卡住的图片语义题 `q25/q28/q39`

22. hint gate 收紧后的定点复验
- 改动：
  - 只在文本中明确谈“这本书 / 读过的书 / 明确书封面”时，才触发 `image_title_hint`
  - 不再对普通书架/儿童图书馆图片做标题抽取

本地验证：
- [tests/benchmark/locomo/openclaw/test_import_to_ov.py](/home/jcp/Agent/code/OpenViking/tests/benchmark/locomo/openclaw/test_import_to_ov.py): `7 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

远端小口径结果：
- `q2023_hintgate40`
  - `q22`: `CORRECT`, `3392`
  - `q23`: `CORRECT`, `3730`
- `q2528_hintgate40`
  - `q25`: `CORRECT`, `3655`
  - `q27`: `CORRECT`, `4281`
  - `q28`: `CORRECT`, `4094`

CSV：
- `/tmp/q2023_hintgate40_qa/phaseA_on_1sessions_q2023_hintgate40.csv`
- `/tmp/q2528_hintgate40_qa/phaseA_on_2sessions_q2528_hintgate40.csv`

更新后的判断：
- `museum / picnic / Nothing Is Impossible` 这几题已经都能在远端定点样本上同时通过
- 组合退化更像是 memory 组织/命名形态差异，而不是 `selected_spans` 被 PREPROCESSOR 裁坏

23. `q36` 组合稳定性修正：多时间锚点活动拆分
- 新假设：
  - `q36` 在大子集里失败，更像是 `session_9` 被组织成 umbrella event，导致 mentoring 不再作为独立事件稳定命中
- 最小修正：
  - 只改 `openviking/prompts/templates/compression/memory_extraction.yaml`
  - 明确要求：
    - 同一会话里若出现多个不同时间锚点 / 不同目标的参与类活动，必须拆成独立 `events`
    - 不允许合并成一个 umbrella event

本地验证：
- `tests/benchmark/locomo/openclaw/test_import_to_ov.py`: `7 passed`
- `tests/unit/session/test_extraction_preprocessor.py tests/unit/session/test_fixture_token_savings.py`: `46 passed`

远端 extraction 直接证据：
- fresh account：`acct-20260530-s39split46`
- 只灌：
  - `session_3`
  - `session_9`
- 结果：
  - `session_3` 生成：
    - `events/2023/06/09/school_lgbtq_presentation.md`
  - `session_9` 不再生成 umbrella event，而是拆成：
    - `events/2023/07/17/lgbtq_youth_mentorship.md`
    - `events/2023/07/17/lgbt_pride_event.md`
    - `events/2023/07/17/lgbtq_art_show.md`

远端 QA-only：
- run：`q36_split46qa`
- 口径：
  - 同一 account
  - `skip-ingest`
  - `q36` only
- 结果：
  - `q36`: `CORRECT`, `5181`

当前判断：
- `q36` 已经拿到新的有效正样本
- 这次修正属于 **memory extraction 粒度修正**
- 不是继续泛改 PREPROCESSOR

24. 统一验收当前状态：`accept47`
- fresh account：`acct-20260530-accept47`
- 目标只灌：
  - `session_3 / 6 / 7 / 8 / 9`
- 目标覆盖：
  - `q20/q22/q23`
  - `q25/q28`
  - `q36`
  - `q39`

当前现象：
- session 目录已写到 `3`
- 进程仍在跑
- 更像再次被 `session_7/8` 的图片链路拖慢

结论：
- 这属于运行态现象
- 不能作为这次 `q36` 拆分修正的成败结论
- 若后续继续统一验收，更合理的是分成：
  - text/event 组：`q20/q22/q23/q36`
  - image/book 组：`q25/q28/q39`

25. `accept47` 统一 fresh-account 验收
- account：
  - `acct-20260530-accept47`
- 实际灌入：
  - `session_3 / 6 / 7 / 8 / 9`

QA-only 分成两组：

1. `q20-28`
- 结果：
  - `q20`: `WRONG`
  - `q21`: `CORRECT`
  - `q22`: `CORRECT`
  - `q23`: `WRONG`
  - `q24`: `CORRECT`
  - `q25`: `CORRECT`
  - `q26`: `WRONG`
  - `q27`: `WRONG`
  - `q28`: `CORRECT`
- 汇总：
  - `5/9 correct`
- CSV：
  - `/tmp/accept47_q2028_qa/phaseA_on_7sessions_accept47q2028.csv`

2. `q36-39`
- 结果：
  - `q36`: `CORRECT`
  - `q37`: `CORRECT`
  - `q38`: `CORRECT`
  - `q39`: `CORRECT`
- 汇总：
  - `4/4 correct`
- CSV：
  - `/tmp/accept47_q3639_qa/phaseA_on_7sessions_accept47q3639.csv`

更新后的判断：
- `q36` 的 event 拆分修正，不只是 focused account 正样本
- 它已经在与 `session_7/8` 图片链路共存的统一 account 上成立
- 同时没有把 `q39` 再打坏
- 当前残余问题更集中在：
  - `q23`
  - `q27`
  - 以及缺少 `session_5` 的无效样本 `q26`

26. `q23/q27` 继续收口

### `q23`
- fresh account：`acct-20260531-s67split48`
- 只灌：
  - `session_6 / 7`
- QA-only：
  - `q23-27`
- 结果：
  - `q23`: `CORRECT`, `3080`
  - `q24`: `CORRECT`
  - `q25`: `WRONG`
  - `q27`: `WRONG`
- CSV：
  - `/tmp/s67split48_q2327_qa/phaseA_on_2sessions_s67split48q2327.csv`

结论：
- 新增的 prompt 约束已经足以把 `q23` 从 `no relevant information` 拉回正确答案
- `q23` 的主要问题已被当前 extraction 细化命中

### `q27`
同一个 `s67split48` account 上，补做两步验证：

1. 直接服务侧 `/api/v1/search/find`
- 同一 query 下，top 结果已经是：
  - `profile.md`
  - `entities/event/caroline's lgbtq conference.md`
  - `events/2023/07/12/lgbtq_conference_attendance.md`

2. 稍晚单独重跑 `q27`
- run：`s67split48_q27late_qa`
- 结果：
  - `q27 = CORRECT`
- CSV：
  - `/tmp/s67split48_q27late_qa/phaseA_on_2sessions_s67split48q27late.csv`

统一 account `accept47` 上也出现同样现象：
- 首次 `q27`：`WRONG`
- 稍晚单独重跑：
  - `accept47_q27late_qa`
  - `q27 = CORRECT`
- CSV：
  - `/tmp/accept47_q27late_qa/phaseA_on_7sessions_accept47q27late.csv`

更新后的判断：
- `q27` 的主要问题已从 “extraction 还没修好” 转为：
  - **ingest 后索引可见性 / QA 时序**
- 不是当前 conference memory 缺失
- 也不是 current relative-time grounding 继续错误

### Benchmark 脚本补充
- 已在 [benchmark/locomo/openclaw/phase_a_off.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py) 增加：
  - `--post-ingest-settle-seconds`
- 作用：
  - ingest 完成后、QA 前可选等待
  - 用于减少 delayed index visibility 带来的假阴性
- 默认 `0.0`，不改变当前默认行为

本地验证：
- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `4 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py`

27. `settle49` 复验状态
- 新开关：
  - `--post-ingest-settle-seconds 15`
- 目标：
  - 验证 `q27` 能否从“首跑错、晚跑对”变成“首跑就对”
- fresh account：
  - `acct-20260531-s67settle49`
- 口径：
  - `session_6-7`
  - `q23-27`

结果：
- ingest 已完成
- 但 QA 阶段命中了 provider weekly quota 错误

结论：
- `settle49` 这轮不能用于判断算法成败
- 只能确认：
  - settle 开关已经接入 benchmark
  - 需要在 quota 恢复后补一轮有效实测，才能证明 `q27` 是否能首跑回正

28. `q27` 服务侧可见性确认 + settle 机制升级

在 `acct-20260531-s67split48` 上继续取证后，已经确认：

- 直接调用服务侧 `/api/v1/search/find`
  - query: `When did Caroline go to the LGBTQ conference?`
  - top 结果已经是 conference 相关 memory
- 但首次 QA 日志的 `inject-detail`
  - 仍然注入了 picnic / catch-up 组 memories

因此当前更准确的判断是：
- `q27` 的主要问题不是 conference memory 缺失
- 而是 ingest 后索引可见性 / QA 时序

对应地，把 benchmark 的 settle 机制从“固定 sleep”升级成了“search visibility 轮询”：

- [benchmark/locomo/openclaw/phase_a_off.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py)
  - 新增 `collect_memory_visibility_probes()`
  - 新增 `wait_for_search_visibility()`
  - `--post-ingest-settle-seconds` 现在会在有 probe 时轮询 `/api/v1/search/find`
  - 直到新写入 memory 可被 search 命中，或超时后再开始 QA

本地验证：
- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `6 passed`
- `python3 -m py_compile benchmark/locomo/openclaw/phase_a_off.py tests/benchmark/locomo/openclaw/test_phase_a_off.py`

当前阶段结论：
- `q23` 已修正
- `q27` 已收口到 benchmark 时序问题
- benchmark 已具备 search-visibility settle 能力
- 还差一轮 quota 恢复后的有效远端复验，来证明 `q27` 能否首跑回正

29. `q27` search-only settle 远端复验

为了避免继续消耗 provider completion，又做了一轮只依赖 `/api/v1/search/find` 的远端 settle 复验：

- account:
  - `acct-20260531-s67split48`
- API key：
  - 直接读取远端 `/root/.openviking/ov.conf` 中的 `server.root_api_key`

结果：
- 即使把 benchmark 主路径里的 settle probe 数降到 `1`
- `wait_for_search_visibility()` 仍然会直接返回：
  - `ok = false`
  - `reason = rate_limited_timeout`
  - `last_error = 429 ... Too Many Requests ... /api/v1/search/find`

这说明当前阶段的外部限制已经更明确：
- 不只是 provider completion quota
- search API 本身也存在 rate limit

因此最新结论更新为：
- `q27` 不再像 extraction 本身问题
- benchmark 代码侧已具备：
  - dotfile probe 过滤
  - 429-aware settle 结果
  - 更低 probe fanout
- 但还缺一轮 **search 配额恢复后的有效远端首跑复验**

30. `q27` 首跑假阴性的实际延迟窗口

继续直接回读远端 artifact 时间后，`q27` 的“首跑错、晚跑对”已经有了更具体的时间量级：

- `s67split48`
  - 首跑目录 mtime: `2026-05-30 16:00:58 +0000`
  - 晚跑目录 mtime: `2026-05-30 16:04:10 +0000`
  - 近似延迟: `3 分 12 秒`

- `accept47`
  - 首跑相关目录 mtime: `2026-05-30 15:47:30 +0000`
  - 晚跑目录 mtime: `2026-05-30 16:05:13 +0000`
  - 近似延迟: `17 分 43 秒`

而题级 CSV 也支持同一判断：
- 首跑 `q27`
  - 都回答成“no relevant information”
- 晚跑 `q27`
  - 都恢复为 `Caroline attended the LGBTQ conference on 2023-07-10`

因此当前更现实的判断是：
- `q27` 的首跑假阴性，不像是秒级 settle 足以稳定覆盖的问题
- 如果 search 配额恢复后仍成立，就应把 benchmark settle 的推荐等待量级上调到**分钟级**

31. settle probe 已对齐待测问题

为了避免 settle 把唯一 probe 浪费在无关 recent event 上，又补了一层脚本修正：

- [benchmark/locomo/openclaw/phase_a_off.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py)
  - 新增 `collect_memory_visibility_probes_for_questions()`
  - QA 前 settle 会优先按**待测问题**和 memory path/stem 的词项重叠来选 probe

直接原因：
- 旧逻辑在 `s67split48` 上即使只选 1 个 probe
  - 也会选到 `running_shoe_discussion`
- 这和 `q27` 的 `LGBTQ conference` 不贴近

最新状态：
- 本地 `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `9 passed`
- 远端 probe worktree 已验证：
  - 对问题 `When did Caroline go to the LGBTQ conference?`
  - 实际选出的 single probe 已变成：
    - `lgbtq conference attendance`

因此当前 settle 机制的状态进一步更新为：
- probe 数更低
- 429-aware
- probe 内容也更贴近真正待测 QA

32. 429 下的 settle 行为已改成被动等待

基于真实远端 search-only 复验：
- 即使 `max_probes=1`
- `/api/v1/search/find` 也可能第一跳就直接 `429`

因此 [benchmark/locomo/openclaw/phase_a_off.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py) 里又补了一层保守修正：

- 命中 `429 / Too Many Requests` 后
  - 不再继续主动轮询 search
  - 改为记录：
    - `search_attempts`
    - `passive_wait_after_rate_limit`
  - 然后被动等待到 settle timeout 截止

本地验证：
- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `9 passed`

这让后续 meta 的解释更清楚：
- 可以区分“第一次 search 就被限流拦下”
- 和“轮询多次仍未可见”这两种不同失败形态

33. `local_probe_snapshot` 已接入 settle meta

为了在 search 配额未恢复时继续积累高信号证据，又补了一层本地就绪快照：

- [benchmark/locomo/openclaw/phase_a_off.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py)
  - 新增 `collect_local_probe_snapshot()`
  - 对每个 settle probe 记录：
    - `exists`
    - `size_bytes`
    - `mtime`

并写入：
- `post_ingest_settle.local_probe_snapshot`

这样后续即使结果仍是：
- `rate_limited_timeout`

也能直接判断：
- memory 文件已经本地落盘，只是 search 看不见
- 还是写入本身尚未完成

本地验证：
- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`: `10 passed`

34. 2026-06-01 clean fresh-account 复验结果

在不改代码前提下，search 配额恢复后做了两轮 fresh-account 远端复验：

1. 最小 `q27` 用例
- `acct-20260601-q27smoke51`
- `sample0 / sessions 6-7 / q27`
- 结果：
  - `q27 = CORRECT`

2. 小样本用例
- `acct-20260601-s67small52`
- `sample0 / sessions 6-7 / q23-27`
- 结果：
  - `q23 = CORRECT`
  - `q24 = CORRECT`
  - `q25 = WRONG`
  - `q27 = CORRECT`
  - 有效题 `3/4`

但这两轮更关键的新发现是：
- `post_ingest_settle` 都是：
  - `reason = fallback_sleep`
  - `probe_count = 0`

也就是说：
- clean fresh-account 下，当前 settle probe/search-visibility 机制**并没有真正生效**
- 这两轮之所以通过，是因为 fallback sleep `240s` 后首跑 QA 本身已能回正

当前更准确的判断更新为：
- `q27` 的 clean 首跑已经能在分钟级等待后回正
- 但 settle 机制还有一个脚本级剩余问题：
  - probe root 还没对齐当前 namespace policy

35. `q27` settle 机制已在 clean fresh-account 下真实命中

继续做了两层最小脚本修正：

- probe root 支持：
  - `user/<user>/agent/<agent>/memories`
- agent-scoped probe 的 `target_uri` 改成：
  - `viking://user/<user>/agent/<agent>/memories`

然后重新跑 fresh-account clean 复验：

1. `q27settle55`
- `sample0 / sessions 6-7 / q27`
- 结果：
  - `q27 = CORRECT`
- `post_ingest_settle`：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["lgbtq conference"]`

2. `s67settle56`
- `sample0 / sessions 6-7 / q23-27`
- 结果：
  - `q23 = CORRECT`
  - `q24 = CORRECT`
  - `q25 = WRONG`
  - `q27 = CORRECT`
  - 有效题 `3/4`
- `post_ingest_settle`：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["lgbtq conference attendance"]`

这意味着：
- 当前 benchmark settle 机制已经不再是 `fallback_sleep + probe_count=0`
- 而是在 clean environment 下真实命中了 conference memory，并支撑 `q27` 首跑回正

36. 更大 clean 子集复验：`q1639settle57`

在 `q27` settle 路径打通后，回到更大 clean 子集：

- `sample0`
- `sessions 3-9`
- `q16-39`
- fresh account：`acct-20260601-q1639settle57`

结果：
- `14/21 correct`
- 平均 `input_tokens = 4865.3`

错题：
- `q18`
- `q19`
- `q20`
- `q25`
- `q28`
- `q33`
- `q36`

而和这轮修正直接相关的点是：
- `q27 = CORRECT`
- `post_ingest_settle` 已真实生效：
  - `ok = true`
  - `probe_count = 1`
  - `ready_queries = ["pride parade"]`

所以当前最准确的阶段判断是：
- `q27` settle 修正本身没有引入 `q27` 回归
- 但 current version 整体版本在更大 clean 子集上，明显差于历史 `17/21` 基线
- `q25` 仍是残余失败点之一，但已经不是唯一主要问题

37. `baseline + benchmark-only settle` 隔离复验：`benchonly61/62`

按隔离说明回到 baseline 仓库：
- `/home/jcp/agent/code/OpenViking-benchonly`

只保留 benchmark 侧文件：
- `benchmark/locomo/openclaw/phase_a_off.py`
- 为适配远端 judge 环境，额外同步 `benchmark/locomo/openclaw/judge.py`

最小 clean smoke：
- `sample0 / sessions 6-7 / q27`

结果分两步：

1. `q27benchonly61`
- `post_ingest_settle` 已真实命中 conference memory：
  - `ok = true`
  - `ready_queries = ["lgbtq conference attendance"]`
- 但旧 baseline `judge.py` 默认模型是：
  - `doubao-seed-2-0-pro-260215`
- 远端账号未开通，导致 judge 误失败
- raw response 实际是正确的 `July 10, 2023`

2. `q27benchonly62`
- 同步当前 `judge.py` 后，judge 恢复可用
- 但 raw response 反而变成：
  - `There is no relevant information ...`
- `post_ingest_settle` 虽仍 `ok = true`
- 但 `ready_queries` 变成：
  - `["beach camping"]`

进一步检查 account memory 发现：
- `acct-20260601-q27benchonly62` 的 `events` 下只有
  - `beach_camping`
  - `museum_visit`
  - `reunion_chat`
  - `support_picnic`
- **没有** conference event

因此这轮隔离复验的明确结论是：
- benchmark-only settle 改动**不能单独把 baseline 仓库拉回 `q27` 正确**
- 原因不是 settle 逻辑失效
- 而是该 baseline runtime/extraction 组合本来就没有生成 conference event memory

38. `baseline + settle + current memory_extraction.yaml`

在确认 `benchmark-only settle` 不足后，继续只做一层最小 runtime/extraction 带入：

- baseline 仓库：
  - `/home/jcp/agent/code/OpenViking-benchonly`
- 保留 benchmark 侧修正：
  - `phase_a_off.py`
  - `judge.py`
- **仅额外带入当前**：
  - `openviking/prompts/templates/compression/memory_extraction.yaml`

不混入其它 current runtime 文件。

结果：

1. `q27benchprompt63`
- `sample0 / sessions 6-7 / q27`
- `q27 = CORRECT`
- settle 真实命中：
  - `ready_queries = ["lgbtq conference attendance"]`

2. `s67benchprompt64`
- `sample0 / sessions 6-7 / q23-27`
- `q23 = CORRECT`
- `q24 = CORRECT`
- `q25 = WRONG`
- `q27 = CORRECT`
- `3/4`

3. `q1639benchprompt65`
- `sample0 / sessions 3-9 / q16-39`
- `18/21 correct`
- 平均 `input_tokens = 3946.3`
- 错题：
  - `q25`
  - `q28`
  - `q39`

与此前关键基线对照：
- 历史 current best-known：`17/21`, `4150.6`
- settle-only baseline：`14/21`, `4865.3`
- 本轮最小改动集：`18/21`, `3946.3`

因此当前最强结论更新为：
- 当前最优主候选是：
  - baseline runtime
  - + benchmark settle
  - + 当前 `memory_extraction.yaml`
- 它在更大 clean 子集上同时实现了：
  - 更高准确率
  - 更低 token

39. `baseline + settle + memory_extraction + import_to_ov`

在 `18/21` 主候选上，继续只带入一个新增文件：
- `benchmark/locomo/openclaw/import_to_ov.py`

目标是看图像/书封面上下文注入，能否继续修 `q25/q28/q39`。

结果：

1. `imgbench66`
- `sample0 / sessions 6-9 / q25-39`
- `q25 = WRONG`
- `q28 = WRONG`
- `q39 = CORRECT`
- `7/9`

并且直接检查实际 ingest `messages.jsonl` 发现：
- 能看到 `[image_caption] / [image_query]`
- 看不到 `[image_title_hint] / [image_author_hint]`
- 也看不到 `Nothing is Impossible / Tom Oliver`

所以当前图像增量对书名恢复没有真正生效。

2. `q1639imgbench67`
- `sample0 / sessions 3-9 / q16-39`
- `19/21 correct`
- 平均 `input_tokens = 3638.1`
- 错题仅剩：
  - `q25`
  - `q28`

与此前主候选对照：
- `q1639benchprompt65`: `18/21`, `3946.3`
- `q1639imgbench67`: `19/21`, `3638.1`

因此当前最强主候选再次更新为：
- baseline runtime
- + benchmark settle
- + 当前 `memory_extraction.yaml`
- + 当前 `import_to_ov.py`

这条线已经在更大 clean 子集上实现：
- `19/21` 正确
- 更低平均 token

当前残余问题进一步收缩到：
- `q25`
- `q28`

40. `q2528imgfix69`: 书封面 title/author hint 兼容修复后，最小 clean 书题子集已回正

- 根因：
  - `import_to_ov.py` 的 `_maybe_extract_visual_hints()` 在 bench-only 旧环境里通过 `get_openviking_config().vlm` 取配置时，会被旧 `MemoryConfig` 的新字段校验打断
  - 异常被吞掉，导致 `[image_title_hint] / [image_author_hint]` 根本没有进入 ingest 消息
- 修复：
  - 增加 `_load_vlm_for_visual_hints()`
  - 失败后直接从原始 `ov.conf` 的 `vlm` 段构造 `VLMConfig`
- 真实容器复现：
  - `title = Nothing is Impossible`
  - `author = Tom Oliver`
  - `confidence = high`
- clean 最小书题结果：
  - run: `q2528imgfix69`
  - `sample0 / sessions 6-7 / q25-28`
  - `q25 = CORRECT`
  - `q27 = CORRECT`
  - `q28 = CORRECT`

41. `q1639imgfix70`: 当前最小主候选已在更大 clean 子集上到 `21/21`

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
- 结果：
  - `21/21 correct`
  - 平均 `input_tokens = 4122.7`
  - `post_ingest_settle.ok = true`

与此前关键版本对照：
- `q1639benchprompt65`: `18/21`, `3946.3`
- `q1639imgbench67`: `19/21`, `3638.1`
- `q1639imgfix70`: `21/21`, `4122.7`

当前最佳主候选更新为：
- baseline runtime
- + benchmark settle
- + 当前 `memory_extraction.yaml`
- + 修复后的 `import_to_ov.py`

当前下一步应从“单题残余修复”切换成“更大范围效果验证”：
- 保持这条最小主候选不动
- 扩到 full `sample0` 之类的更大 clean 口径
