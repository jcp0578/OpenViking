# WM v2 Preprocessor — 操作交接 (2026-05-08)

## 环境

- **远端**: `ssh jcp@123.60.114.206 -p 10008`，密码不写入仓库，容器 `jcp-dev`
- **GW**: `http://127.0.0.1:18789`，auth token 不写入仓库
- **OV**: `http://127.0.0.1:1933`，dev mode (no auth)
- **分支**: `feat/wm-v2-token-distillation`
- **代码路径**: `/home/jcp/agent/code/OpenViking`
- **OV 配置**: `/root/.openviking/ov.conf`
- **GW 配置**: `/root/.openclaw/openclaw.json`

## GW 插件配置

| 参数 | 值 |
|------|----|
| autoCapture | True |
| autoRecall | True |
| commitTokenThreshold | 8000 |
| commitKeepRecentCount | 0 |
| isolateAgentScopeByUser | True |
| recallMaxInjectedChars | 64000 |
| contextInjection | never |
| skipBootstrap | True |

## LLM 配置

| | Model | Provider |
|------|-------|----------|
| OV VLM | doubao-seed-2.0-code | volcengine ARK (coding endpoint) |
| OV Embedding | doubao-embedding-vision-250615 | volcengine |
| GW LLM | MiniMax-M2.7 | minimax (anthropic format) |
| Judge | doubao-seed-2.0-code | volcengine ARK |

## 服务启动

```bash
# GW 启动
docker exec -d jcp-dev openclaw gateway

# OV 启动 (必须 override VLM env)
docker exec -d jcp-dev bash -c '
OPENVIKING_VLM_API_BASE=https://ark.cn-beijing.volces.com/api/coding/v3 \
OPENVIKING_VLM_MODEL=doubao-seed-2.0-code \
PYTHONPATH=/home/jcp/agent/code/OpenViking \
nohup python3 -m openviking_cli.server_bootstrap \
    --config /root/.openviking/ov.conf --host 127.0.0.1 --port 1933 \
    > /tmp/ov-restart.log 2>&1 &
'
```

## 标准化测试流程

### 测试方法

1. **commitTokenThreshold=8000**，`keepRecentCount=0`
2. 注入所有 session（GW 或直连 OV）
3. 条件触发 compact：pending tokens >= 8000 自动触发，或全部注入后手动 `commit_session()`
4. VLM token 用 `get_session().llm_token_usage` 获取（需等 extraction 完成后查询）
5. 结果保存带 timestamp 的 `_meta.json`

### GW 注入（适用于 session 数少 < 10）

```bash
cd /home/jcp/agent/code/OpenViking/benchmark/locomo/openclaw
python3 eval.py ingest LOC_DATA --sample N --sessions 1-100 \
    --base-url http://127.0.0.1:18789 --token GW_TOKEN \
    --user USER --agent-id locomo-eval --clear-ingest-record
```

### 直连 OV 注入（适用于 session 数多 >= 10，避免 GW 上下文溢出）

```python
client = ov.AsyncHTTPClient(url='http://127.0.0.1:1933')
await client.initialize()
res = await client.create_session()
sid = res['session_id']
for session in sessions:
    for msg in session['messages']:
        await client.add_message(session_id=sid, role=role, parts=[...])
await client.commit_session(sid)
```

### Token 统计（全链路 4 段）

| 阶段 | 数据来源 | 提供商 |
|------|---------|--------|
| GW Ingest | eval.py ingest 输出 usage | MiniMax |
| OV Import | `get_session().llm_token_usage` | 豆包 |
| GW QA | `stat_judge_result.py` | MiniMax |
| Judge | ARK API 估算 | 豆包 |

## 已知陷阱

1. **不要用 `pkill -f`**：会误杀 SSH。改用 `ps -eo pid,args | grep ... | awk '{print $1}' | xargs kill`
2. **OV 启动需 override VLM env**：Docker 默认缺 `coding` endpoint
3. **VLM token 查询需等 extraction 完成**：否则返回 0
4. **GW session ID = OV session ID**：从 OV 目录按 mtime 找最新
5. **GW context overflow**：>10 session ingest 时 GW 对话历史累积超模型限制，用直连 OV 注入
6. **`stat_judge_result.py` import token 不可信**：来自固定数据源
7. **新 agent 需复制 workspace**：否则进入 bootstrap 模式

## 关键文件路径（远端容器内）

| 文件 | 路径 |
|------|------|
| OV 配置 | `/root/.openviking/ov.conf` |
| GW 配置 | `/root/.openclaw/openclaw.json` |
| OV 日志 | `/root/.openviking/data/log/openviking.log` |
| Preprocessor 开关 | `/tmp/set_pp.py` |
| LoCoMo small (35Q) | `/home/jcp/agent/code/locomo-eval-kit/data/locomo10_small.json` |
| LoCoMo full (10 samples) | `/home/jcp/agent/code/locomo-eval-kit/data/locomo10.json` |
| LongMemEval small | `/tmp/longmemeval_small_bench.json` |
| Eval 脚本目录 | `/home/jcp/agent/code/OpenViking/benchmark/locomo/openclaw` |
| Preprocessor 代码 | `/home/jcp/agent/code/OpenViking/openviking/session/extraction_preprocessor.py` |
| GW 插件代码 | `/home/jcp/agent/code/OpenViking/examples/openclaw-plugin/context-engine.ts` |
