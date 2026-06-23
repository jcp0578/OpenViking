# Remote Small Runbook

## Scope

This runbook captures the stable remote-container workflow for LoCoMo `small`
runs on the known host/container pair:

- host: `123.60.114.206:10008`
- container: `jcp-dev`

It is intended to make the `small` benchmark rerunnable and auditable without
rebuilding ad-hoc `/tmp` scripts each time.

## Scripts

- `benchmark/locomo/openclaw/run_clean_small_in_container.sh`
  - executes *inside* the container
  - performs backup, reset, service restart, and `phase_a_off.py`
- `benchmark/locomo/openclaw/run_remote_clean_small.sh`
  - executes *from local workspace*
  - copies the inner script to the remote host/container and runs it
  - defaults to detached mode for long benchmark runs
- `benchmark/locomo/openclaw/run_remote_judge.sh`
  - executes *from local workspace*
  - runs `judge.py` inside the remote container against an existing QA CSV
- `benchmark/locomo/openclaw/check_remote_small_run.sh`
  - executes *from local workspace*
  - inspects remote process / CSV / resume state / tail log for one `RUN_ID`

## Required environment

Set these locally before running `run_remote_clean_small.sh`:

```bash
export OPENCLAW_GATEWAY_TOKEN='...'
export OPENVIKING_ROOT_API_KEY='...'
```

Optional overrides:

```bash
export REMOTE_HOST='jcp@123.60.114.206'
export REMOTE_PORT='10008'
export REMOTE_CONTAINER='jcp-dev'
export MODE='off'   # or on
export RUN_ID='off_small_manual_20260603_120000'
export DETACH='true'
```

## Default behavior

The container-side script always does the following:

1. backup current OpenViking/OpenClaw data
2. stop benchmark/gateway/openviking processes
3. clear data directories
4. rewrite `/root/.openviking/ov.conf` for the requested `off/on` mode
5. pre-sync `openclaw.json` plugin namespace config
6. sync `/root/.openclaw/agents/main/agent/auth-profiles.json` from the configured primary model provider key
7. restart `openviking` and `openclaw gateway`
8. run `phase_a_off.py` on `sample=0`, `sessions=1-4`

Defaults used by the script:

- `MODE=off`
- `SAMPLE=0`
- `SESSIONS=1-4`
- `SKIP_JUDGE=true`
- `ISOLATE_USER_SCOPE_BY_AGENT=false`
- `ISOLATE_AGENT_SCOPE_BY_USER=false`
- `SYNC_PLUGIN_CONFIG_IN_SCRIPT=true`

The script also rewrites `/root/.openviking/ov.conf` so that:

- `MODE=off` -> `memory.wm_v2_preprocess_enabled = false`
- `MODE=on` -> `memory.wm_v2_preprocess_enabled = true`

When `SYNC_PLUGIN_CONFIG_IN_SCRIPT=true`, the script updates
`/root/.openclaw/openclaw.json` before starting gateway and then passes
`--no-sync-plugin-config` to `phase_a_off.py`. This avoids a mid-run gateway
restart caused by namespace-config mutation during the benchmark itself.

The same container-side script also syncs
`/root/.openclaw/agents/main/agent/auth-profiles.json` so that the provider key
used by OpenClaw's auth profile matches the current primary model provider in
`openclaw.json`. This is required because changing `openclaw.json` alone may not
be enough to switch the effective model key used by the gateway.

## Run examples

### OFF small false/false

```bash
MODE=off \
RUN_ID=off_small_manual_20260603_120000 \
OPENCLAW_GATEWAY_TOKEN="$OPENCLAW_GATEWAY_TOKEN" \
OPENVIKING_ROOT_API_KEY="$OPENVIKING_ROOT_API_KEY" \
bash benchmark/locomo/openclaw/run_remote_clean_small.sh
```

### ON small false/false

```bash
MODE=on \
RUN_ID=on_small_manual_20260603_120000 \
OPENCLAW_GATEWAY_TOKEN="$OPENCLAW_GATEWAY_TOKEN" \
OPENVIKING_ROOT_API_KEY="$OPENVIKING_ROOT_API_KEY" \
bash benchmark/locomo/openclaw/run_remote_clean_small.sh
```

### Check a detached run

```bash
RUN_ID=off_small_manual_20260603_120000 \
MODE=off \
bash benchmark/locomo/openclaw/check_remote_small_run.sh
```

## Judge step

The current benchmark path should keep using `--skip-judge` and then run
`judge.py` explicitly, because the integrated judge path has previously hit
provider/auth mismatch issues.

Example:

```bash
OPENAI_API_KEY="$OPENAI_API_KEY" \
INPUT_CSV="/tmp/<run_id>/phaseA_<mode>_4sessions_<run_id>.csv" \
bash benchmark/locomo/openclaw/run_remote_judge.sh
```

## Known limitations

- `phase_a_off.py` under the current MiniMax setup still shows extraction
  instability on some sessions.
- `on` and `off` both currently rely on manual post-judge execution.
- The "only committed code" run should use a clean committed tree or archive,
  not the current dirty workspace.

## Current reference results

- `off_small_ff_20260603_1445`
  - clean `false/false`
  - judge result: `5 / 35 = 14.29%`
