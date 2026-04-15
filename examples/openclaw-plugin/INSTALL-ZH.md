# 为 OpenClaw 安装 OpenViking 记忆功能

通过 [OpenViking](https://github.com/volcengine/OpenViking) 为 [OpenClaw](https://github.com/openclaw/openclaw) 提供长效记忆能力。安装完成后，OpenClaw 会自动记住对话中的重要信息，并在回复前回忆相关内容。

> 当前文档介绍的是基于 `context-engine` 架构的新版 OpenViking 插件。

## 前置条件

| 组件 | 版本要求 |
| --- | --- |
| Python | >= 3.10 |
| Node.js | >= 22 |
| OpenClaw | >= 2026.3.7 |

快速检查：

```bash
python3 --version
node -v
openclaw --version
```

## 旧版升级说明

如果你之前安装过旧版 `memory-openviking`，先清理旧插件，再执行下面的安装或升级命令。

- 新版 `openviking` 与旧版 `memory-openviking` 不兼容，不能混装。
- 如果你从未安装过旧版插件，可以跳过本节。

```bash
curl -fsSL https://raw.githubusercontent.com/volcengine/OpenViking/main/examples/openclaw-plugin/upgrade_scripts/cleanup-memory-openviking.sh -o cleanup-memory-openviking.sh
bash cleanup-memory-openviking.sh
```

## 安装

推荐使用 `npm` + `ov-install`。macOS、Linux、Windows 的流程相同。

```bash
npm install -g openclaw-openviking-setup-helper

# 安装插件
ov-install

# 安装插件到指定 OpenClaw 实例
ov-install --workdir ~/.openclaw-second
```

## 升级

要把 OpenViking 和插件一起升级到最新版本，执行：

```bash
npm install -g openclaw-openviking-setup-helper@latest && ov-install -y
```

## 安装或升级到指定版本

如果要安装或升级到某个正式发布版本，执行：

```bash
ov-install -y --version 0.2.9
```

## 参数说明

| 参数 | 含义 |
| --- | --- |
| `--workdir PATH` | 指定 OpenClaw 数据目录 |
| `--version VER` | 同时指定插件版本和 OpenViking 版本，例如 `0.2.9` 会对应插件 `v0.2.9` |
| `--current-version` | 查看当前已安装的插件版本和 OpenViking 版本 |
| `--plugin-version REF` | 指定插件版本，支持 tag、分支或 commit |
| `--openviking-version VER` | 指定 PyPI 上的 OpenViking 版本 |
| `--github-repo owner/repo` | 指定插件来源仓库，默认 `volcengine/OpenViking` |
| `--update` | 只升级插件，不升级 OpenViking 服务版本 |
| `-y` | 非交互模式，使用默认配置 |

## OpenClaw 插件参数说明

插件配置写在 `plugins.entries.openviking.config` 下。通常安装助手会自动写好，只有在你需要手动调整时，才需要关注下面这些参数。

查看当前插件整体配置：

```bash
openclaw config get plugins.entries.openviking.config
```

### Local 模式

适用于由 OpenClaw 插件在本机拉起 OpenViking 服务的场景。

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `mode` | `local` | `local` 表示由插件拉起本机 OpenViking；`remote` 表示连接已有远端 OpenViking 服务 |
| `agentId` | `default` | 当前 OpenClaw 实例在 OpenViking 侧使用的标识 |
| `configPath` | `~/.openviking/ov.conf` | 本机 OpenViking 配置文件路径 |
| `port` | `1933` | 本机 OpenViking HTTP 端口 |

`local` 模式下，VLM、Embedding、API Key 等服务端配置写在 `~/.openviking/ov.conf`，不写在 OpenClaw 插件参数里。常见项包括：

| 配置项 | 含义 |
| --- | --- |
| `vlm.api_key` / `vlm.model` / `vlm.api_base` | 记忆抽取使用的 VLM 模型配置 |
| `embedding.dense.api_key` / `embedding.dense.model` / `embedding.dense.api_base` | 向量化使用的 Embedding 模型配置 |
| `server.port` | OpenViking 服务监听端口 |

常见设置：

```bash
openclaw config set plugins.entries.openviking.config.mode local
openclaw config set plugins.entries.openviking.config.configPath ~/.openviking/ov.conf
openclaw config set plugins.entries.openviking.config.port 1933
```

### Remote 模式

适用于连接已有远端 OpenViking 服务的场景。

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `mode` | `remote` | 使用已有远端 OpenViking 服务 |
| `userMode` | `single-user` | `single-user` 保持当前行为；`multi-user` 会把 `senderId` 写入 session `role_id` |
| `baseUrl` | `http://127.0.0.1:1933` | 远端 OpenViking 服务地址 |
| `apiKey` | 空 | `single-user` 下可不填；`multi-user` 下必填，且必须是具备 ADMIN/ROOT 能力的 key |
| `accountId` | 空 | `single-user` 下可作为默认租户头；`userMode=multi-user` 且 `apiKey` 为 ROOT key 时必填 |
| `userId` | 空 | `single-user` 下可作为默认 `X-OpenViking-User`；`multi-user` 下忽略 |
| `agentId` | `default` | 当前 OpenClaw 实例在远端 OpenViking 上的标识 |

常见设置：

```bash
openclaw config set plugins.entries.openviking.config.mode remote
openclaw config set plugins.entries.openviking.config.baseUrl http://your-server:1933
openclaw config set plugins.entries.openviking.config.apiKey your-api-key
openclaw config set plugins.entries.openviking.config.accountId your-account-id
openclaw config set plugins.entries.openviking.config.userId your-user-id
openclaw config set plugins.entries.openviking.config.agentId your-agent-id
```

single-user 说明：

- `accountId` 和 `userId` 会作为默认 `X-OpenViking-Account` / `X-OpenViking-User` 请求头发送。
- 插件仍会调用 `/api/v1/system/status` 做诊断；如果配置的 `userId` 和服务端识别出的 user 不一致，会输出 warning，便于排查 key 或租户配置错误。

multi-user 示例：

```bash
openclaw config set plugins.entries.openviking.config.mode remote
openclaw config set plugins.entries.openviking.config.userMode multi-user
openclaw config set plugins.entries.openviking.config.baseUrl http://your-server:1933
openclaw config set plugins.entries.openviking.config.apiKey your-admin-or-root-key
openclaw config set plugins.entries.openviking.config.accountId your-account-id   # ROOT key 必填
openclaw config set plugins.entries.openviking.config.agentId your-agent-id
```

multi-user 说明：

- 不要在插件配置里设置 `userId`，该字段在 `multi-user` 下不会生效。
- 实际用户身份来自运行时 sender，并在 user message 写入时作为 session `role_id` 传给 OpenViking。

## 启动

安装完成后，运行：

```bash
source ~/.openclaw/openviking.env && openclaw gateway restart
```

Windows PowerShell：

```powershell
. "$HOME/.openclaw/openviking.env.ps1"
openclaw gateway restart
```

## 验证

检查插件是否已接管 `contextEngine`：

```bash
openclaw config get plugins.slots.contextEngine
```

输出 `openviking` 即表示插件已生效。

查看运行日志：

```bash
openclaw logs --follow
```

日志中出现 `openviking: registered context-engine`，表示插件已成功加载。

查看 OpenViking 自身日志：

默认日志文件在你的 `workspace/data/log/openviking.log`。如果使用默认配置，通常对应：

```bash
cat ~/.openviking/data/log/openviking.log
```

查看当前已安装版本：

```bash
ov-install --current-version
```

### 链路检查（可选）

如果上述验证都正常，还想进一步确认从 Gateway 到 OpenViking 的完整链路是否通畅，可以使用插件自带的健康检查脚本：

```bash
python examples/openclaw-plugin/health_check_tools/ov-healthcheck.py
```

该脚本会进行一次真实的对话注入，然后从 OpenViking 侧验证会话是否被正确捕获、提交、归档并提取出记忆。详细说明见 [health_check_tools/HEALTHCHECK-ZH.md](./health_check_tools/HEALTHCHECK-ZH.md)。

## 卸载

只卸载 OpenClaw 插件、保留 OpenViking 运行时：

```bash
curl -fsSL https://raw.githubusercontent.com/volcengine/OpenViking/main/examples/openclaw-plugin/upgrade_scripts/uninstall-openclaw-plugin.sh -o uninstall-openviking.sh
bash uninstall-openviking.sh
```

如果你的 OpenClaw 数据目录不是默认路径：

```bash
curl -fsSL https://raw.githubusercontent.com/volcengine/OpenViking/main/examples/openclaw-plugin/upgrade_scripts/uninstall-openclaw-plugin.sh -o uninstall-openviking.sh
bash uninstall-openviking.sh --workdir ~/.openclaw-second
```

如果还要一并删除本机 OpenViking 运行时和数据，再执行：

```bash
python3 -m pip uninstall openviking -y && rm -rf ~/.openviking
```
