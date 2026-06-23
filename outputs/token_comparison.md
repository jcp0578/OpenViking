# LoCoMo Sample 0 OFF vs ON Token 全链路对比

## 测试元数据

| 项目 | 值 |
|------|----|
| 数据集 | locomo10_small.json sample 0 (conv-26) |
| Sessions | 4 |
| QA 题数 | 35 (category != 5) |
| GW commitTokenThreshold | 8000 |
| GW keepRecentCount | 0 |
| 注入方式 | eval.py ingest (GW LLM 处理) |
| 测试方法 | 累积 + 手动 compact，1 次 extraction |

## 5 次有效测试结果

| # | 日期 | 轮次 | Test ID | 准确率 | Preprocessor |
|---|------|------|--------|--------|-------------|
| 1 | 05-07 20:30 | OFF | — | 25/35 (71.4%) | 关闭 |
| 2 | 05-07 22:50 | ON | — | 29/35 (82.9%) | CREATION ACTIVE 63% |
| 3 | 05-08 03:50 | OFF | 20260508_035029 | 25/35 (71.4%) | 关闭 |
| 4 | 05-08 04:13 | ON | 20260508_041352 | 29/35 (82.9%) | CREATION ACTIVE 46% |
| 5 | 05-08 06:59 | OFF | 20260508_065921 | 34/35 (97.1%) | 关闭 |

## 全链路 Token 详解

### Test 5 vs Test 4 (最佳对照组，同日期标准化)

| 阶段 | 明细 | Test 5 (OFF) | Test 4 (ON) | 节省 |
|------|------|-------------|-------------|------|
| **GW Ingest** | 4 sessions GW LLM | ~46K | ~46K | 0 |
| **OV VLM** | prompt_tokens | 21,045 | 20,348 | -697 (-3.3%) |
| | completion_tokens | 2,036 | 2,972 | +936 |
| | **VLM 小计** | **23,081** | **23,320** | +239 |
| **OV Embedding** | total_tokens | 5,836 | 8,717 | +2,881 |
| **OV Import 合计** | | **28,917** | **32,037** | +3,120 |
| | | | | |
| **GW QA** | input_tokens | 1,004,602 | 592,005 | **-412,597 (-41%)** |
| | output_tokens | 15,500 | 6,507 | -8,993 |
| | **QA 小计** | **1,020,102** | **598,512** | **-421,590 (-41%)** |
| | | | | |
| **Judge** | 35 题 ARK | ~21,000 | ~21,000 | 0 |
| | | | | |
| **总计** | | **~1,116K** | **~698K** | **~-418K (-37%)** |

### Test 1 vs Test 2 (首轮)

| 阶段 | Test 1 (OFF) | Test 2 (ON) | 节省 |
|------|-------------|-------------|------|
| GW Ingest | ~46K | ~46K | 0 |
| OV VLM | 22,871 | 18,653 | **-4,218 (-18%)** |
| OV Embedding | 8,250 | 1,937 | -6,313 |
| OV Import | 31,121 | 20,590 | -10,531 |
| GW QA | 501,758 | 415,797 | **-85,961 (-17%)** |
| Judge | ~21K | ~21K | 0 |
| **总计** | **~600K** | **~503K** | **~-97K (-16%)** |

## 说明

1. **GW Ingest** (MiniMax)：4 个 session × ~12K/session ≈ 46K tokens。OFF/ON 相同，不受 preprocessor 影响
2. **OV Import** (豆包)：`stat_judge_result.py` 显示的 import token（210K）来自固定数据源，不可信。实际值从 `get_session().llm_token_usage` 获取
3. **GW QA** (MiniMax)：受 LLM 行为方差影响大。Test 3 QA input 异常高（2M，LLM 多轮搜索循环），Test 5 正常（1M）
4. **Judge** (ARK 豆包)：估算值 ~600 tokens/题 × 35 题 = ~21K
5. **核心节省在 QA 侧而非 Import 侧**：preprocessor 提升记忆质量 → recall 注入更少 → QA token 更低
6. **LoCoMo session 太小（~7.5K total）**，VLM prompt 节省绝对值仅 ~4K tokens。大 session 才体现真实价值

## 数据文件

| Test | CSV | Meta |
|------|-----|------|
| 3 | `/tmp/locomo_off_20260508_035029.csv` | `/tmp/locomo_off_20260508_035029_meta.json` |
| 4 | `/tmp/locomo_on_20260508_041352.csv` | `/tmp/locomo_on_20260508_041352_meta.json` |
| 5 | `/tmp/locomo_off_20260508_065921.csv` | `/tmp/locomo_off_20260508_065921_meta.json` |

Test 1/2 无 CSV 保存（仅任务输出中有 judge 摘要）。
