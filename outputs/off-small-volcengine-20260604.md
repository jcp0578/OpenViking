# Remote Small Progress 2026-06-04

## Scope

- Goal 1: stabilize remote small benchmark scripts so future runs only require config changes
- Goal 2: run clean `off small`

## Configuration baseline

- Remote host: `123.60.114.206:10008`
- Container: `jcp-dev`
- OpenClaw gateway token: `<redacted gateway token>`
- OpenViking root API key: `ov-root-namespace-test-20260517`
- Main LLM provider: `volcengine`
- Main LLM model: `doubao-seed-2.0-pro`
- Main LLM API base: `https://ark.cn-beijing.volces.com/api/coding/v3`
- Embedding provider: `volcengine`
- Embedding model: `doubao-embedding-vision`
- Embedding API base: `https://ark.cn-beijing.volces.com/api/coding/v3`

## Script chain updates

- Updated [run_clean_small_in_container.sh](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/run_clean_small_in_container.sh) to sync `/root/.openclaw/agents/main/agent/auth-profiles.json` from the primary provider key in `openclaw.json` before starting gateway.
- Updated [REMOTE_SMALL_RUNBOOK.md](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/REMOTE_SMALL_RUNBOOK.md) to document the auth-profile sync requirement.
- Updated [check_remote_small_run.sh](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/check_remote_small_run.sh) to fall back to the non-`4sessions` CSV naming pattern actually used by `on` runs and print the resolved `csv_path`.
- Confirmed the committed-only remote tree exists at `/home/jcp/agent/code/OpenViking_head_only`, with committed benchmark entrypoints:
  - `benchmark/locomo/openclaw/eval.py`
  - `benchmark/locomo/openclaw/import_to_ov.py`
  - `benchmark/locomo/openclaw/judge.py`
  - `benchmark/locomo/openclaw/run_full_eval.sh`

## Key findings

- Direct remote Volcengine chat completion with:
  - `api_key=626b6c9a-f0d4-4a05-b8dd-75664219a2a0`
  - `model=doubao-seed-2.0-pro`
  - `api_base=https://ark.cn-beijing.volces.com/api/coding/v3`
  succeeded and returned `smoke-ok`.
- `openclaw` smoke remained unstable until `/root/.openclaw/agents/main/agent/auth-profiles.json` was aligned with the live `volcengine` key.
- After aligning `auth-profiles.json`, minimal `openclaw` smoke succeeded with response `smoke-ok`.

## Smoke results

### Direct OpenViking

- Session create: `200 OK`

### Judge

- `judge.py` smoke: `1/1 CORRECT`

### OpenClaw

- After syncing `auth-profiles.json`, minimal smoke succeeded:
  - elapsed: `3.71s`
  - status: `200`
  - response text: `smoke-ok`

## Clean off small run

- Current run id: `off_small_cfg_20260604_0050`
- Start mode: detached via [run_remote_clean_small.sh](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/run_remote_clean_small.sh)
- Remote master log: `/tmp/off_small_cfg_20260604_0050.master.log`
- Remote output dir: `/tmp/off_small_cfg_20260604_0050`

### Current evidence

- Backup completed
- `wm_v2_preprocess_enabled=false`
- OpenClaw plugin namespace synced
- Auth profile sync log:
  - `profile=volcengine:default`
  - `changed=false`
  - `key_suffix=19a2a0`
- OpenViking health OK
- Gateway health OK
- `phase_a_off.py` process started with:
  - `--mode off`
  - `--sample 0`
  - `--sessions 1-4`
  - `--skip-judge`
  - `--no-sync-plugin-config`
  - `--no-isolate-user-scope-by-agent`
  - `--no-isolate-agent-scope-by-user`

### Current status snapshot

- `PROC`: running
- `CSV`: `no_csv`
- `STATE.updated_at`: `2026-06-03 16:48:57`

### Ingest progress snapshot

- new clean run has started and passed:
  - backup
  - `ov health ok`
  - `gateway health ok`
- ingest for `off_small_cfg_20260604_0050` is in progress

### QA progress snapshot

- `off_small_cfg_20260604_0050` has entered QA
- current CSV rows: `35`
- latest `qi`: `102`
- latest question: `What kind of place does Caroline want to create for people?`
- latest CSV timestamp: `2026-06-03 17:03:24`

### Judge progress snapshot

- final judged rows: `35/35`
- final `CORRECT`: `27`
- final `WRONG`: `8`
- final accuracy: `77.14%`

## Off small final result

- run id: `off_small_cfg_20260604_0050`
- clean `off small`: complete
- final accuracy: `77.14%`
- threshold check: `> 50%`, so objective can advance to clean `off sample`

## Off sample start

- initial `off_sample_cfg_20260604_0105` was invalid for objective 3 because `run_remote_clean_small.sh` did not yet forward `SESSIONS`, so it actually started with `--sessions 1-4`
- fixed [run_remote_clean_small.sh](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/run_remote_clean_small.sh) to forward `SESSIONS` and the related runtime parameters into the container
- fixed [check_remote_small_run.sh](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/check_remote_small_run.sh) to support `SESSIONS_LABEL`, so non-`4sessions` runs are observable without ad-hoc path guessing
- relaunched clean `off sample` as:
  - `run_id = off_sample_cfg_20260604_0112`
  - `--sessions 1-19`
  - `master_log = /tmp/off_sample_cfg_20260604_0112.master.log`
  - `output_dir = /tmp/off_sample_cfg_20260604_0112`
- current state:
  - backup done
  - `ov health ok`
  - `gateway health ok`
  - `CSV = no_csv`
  - ingest has started with `--sessions 1-19`
  - current snapshot:
    - `session_1`: `memories=7`
    - `session_2`: `memories=0`
    - `session_3`: `memories=0`
    - `session_4`: `memories=0`
    - `session_5`: `memories=0`
    - `session_6`: `memories=0`
    - `session_7`: `memories=0`
    - `session_8`: `memories=0`
    - `session_9`: `memories=0`
    - `session_10`: `memories=0`
    - `session_11`: `memories=0`
    - `session_12`: `memories=0`
    - `session_13`: `memories=0`
    - `session_14`: `memories=0`
    - `session_15`: `memories=0`
    - `session_16`: `memories=0`
    - `session_17`: `memories=0`
    - `session_18`: `memories=0`
  - no QA CSV yet
  - observed non-fatal runtime noise:
    - `httpx AsyncClient.aclose(): RuntimeError('Event loop is closed')`

### Off sample QA start

- `off_sample_cfg_20260604_0112` completed ingest `19/19`
- QA CSV started:
  - total filtered QA count for `sample0, sessions 1-19`: `150`
  - current CSV rows: `150`
  - latest `qi`: `153`
  - latest question: `What did Melanie do after the road trip to relax?`
  - latest CSV timestamp: `2026-06-03 18:17:23`
  - process completed

### Off sample judge result

- final judged rows: `150/150`
- final `CORRECT`: `114`
- final `WRONG`: `36`
- final accuracy: `76.00%`
- threshold check: `> 50%`, so objective can advance to clean `on small`

## On small start

- launched clean `on small` as:
  - `run_id = on_small_cfg_20260604_1818`
  - `--sessions 1-4`
  - `master_log = /tmp/on_small_cfg_20260604_1818.master.log`
  - `output_dir = /tmp/on_small_cfg_20260604_1818`
- current state:
  - backup done
  - `wm_v2_preprocess_enabled = true`
  - `ov health ok`
  - `gateway health ok`
  - ingest completed `4/4`
  - QA started
  - current snapshot:
    - `session_1`: `memories=7`
    - `session_2`: `memories=0`
    - `session_3`: `memories=0`
    - `session_4`: `memories=0`
  - QA snapshot:
    - total filtered QA count: `35`
    - `rows = 35`
    - `last_qi = 102`
    - `last_question = What kind of place does Caroline want to create for people?`

### On small judge result

- final judged rows: `35/35`
- final `CORRECT`: `28`
- final `WRONG`: `7`
- final accuracy: `80.00%`
- threshold check: `> 50%`, so objective can advance to committed-only `on small`

## Committed-only on small prep

- committed-only remote tree confirmed at `/home/jcp/agent/code/OpenViking_head_only`
- committed benchmark entrypoints available:
  - `benchmark/locomo/openclaw/eval.py`
  - `benchmark/locomo/openclaw/import_to_ov.py`
  - `benchmark/locomo/openclaw/judge.py`
  - `benchmark/locomo/openclaw/run_full_eval.sh`

## Committed-only on small start

- initial committed-only attempt `on_small_head_20260604_1900` should not be treated as valid; it only reached early clean-reset logging and did not produce benchmark artifacts.
- relaunched committed-only `on small` as:
  - `run_id = on_small_head2_20260604_1910`
  - benchmark code root: `/home/jcp/agent/code/OpenViking_head_only`
  - `master_log = /tmp/on_small_head2_20260604_1910.master.log`
  - `output_dir = /tmp/on_small_head2_20260604_1910`
- current state:
  - backup done
  - `wm_v2_preprocess_enabled = true`
  - plugin config synced
  - auth profile synced

### Committed-only on small auth compatibility

- `on_small_head2_20260604_1910` proved that the committed-only benchmark code still failed at ingest with:
  - `UnauthenticatedError: Missing API Key when resolving identity.`
- Root cause:
  - committed `benchmark/locomo/openclaw/import_to_ov.py` creates `ov.AsyncHTTPClient(url=..., user=..., agent_id=...)`
  - it does not pass `api_key`
  - current remote OV server defaulted to `auth_mode=api_key`
- service-side compatibility validation:
  - when `server.auth_mode` and `server.root_api_key` are removed from `ov.conf` on localhost, OV auto-detects `dev`
  - `/health` then reports `auth_mode: "dev"`

## Committed-only on small auto-dev run

- launched committed-only `on small` with localhost auto-dev OV as:
  - `run_id = on_small_headauto_20260604_1940`
  - benchmark code root: `/home/jcp/agent/code/OpenViking_head_only`
  - `master_log = /tmp/on_small_headauto_20260604_1940.master.log`
  - `output_dir = /tmp/on_small_headauto_20260604_1940`
- current state:
  - backup done
  - OV health passed with `auth_mode=dev`
  - gateway health passed
  - committed `import_to_ov.py` ingest completed `4/4`
  - QA started and is progressing
  - current QA snapshot:
    - `rows = 100`
    - `graded = 0`
    - `last_qi = 100`
    - `last_question = What motivated Caroline to pursue counseling?`

### Committed-only final result

- final committed-only run:
  - `run_id = on_small_headauto_20260604_1940`
  - benchmark code root: `/home/jcp/agent/code/OpenViking_head_only`
- committed-only execution result:
  - ingest completed `4/4`
  - QA rows written: `152/152`
  - final judged rows: `152/152`
  - final `CORRECT`: `0`
  - final `WRONG`: `152`
  - final accuracy: `0.00%`
- interpretation:
  - this run proves the committed benchmark chain can execute end-to-end when OV is made localhost `auth_mode=dev`
  - this is **not** the same benchmark semantics as `phase_a_off.py` small mode
  - committed `eval.py qa` ran `152` QA items rather than the `35`-question `small` path
  - therefore this result is only evidence of committed-only runability and is not directly comparable to:
    - `off small = 27/35 = 77.14%`
    - `on small = 28/35 = 80.00%`

### Invalidated run note

- `off_small_cfg_20260604_0043` should not be treated as a valid clean final run.
- During later diagnosis, `openclaw-gateway` received `SIGTERM` at `2026-06-03T16:46:33+00:00` while `phase_a_off.py` was still active.
- Because the gateway was restarted mid-run, the clean-run contract was broken and the run must be restarted from a fresh clean environment.

## 2026-06-04 follow-up goal

- standard test path locked for follow-up work:
  - `benchmark/locomo/openclaw/run_remote_clean_small.sh`
  - `benchmark/locomo/openclaw/run_clean_small_in_container.sh`
  - `benchmark/locomo/openclaw/phase_a_off.py`
- execution rule:
  - do not modify test code lightly
  - prefer configuration-only changes for new runs

## Three completed benchmark runs

| test | run_id | benchmark shape | result | accuracy | ingest token | QA token | total token | note |
|---|---|---|---|---:|---:|---:|---:|---|
| off small | `off_small_cfg_20260604_0050` | clean `small`, 35 QA | `27 CORRECT / 8 WRONG` | `77.14%` | `67,448` | `290,606` | `358,054` | valid baseline |
| off sample | `off_sample_cfg_20260604_0112` | clean `sample`, 150 QA | `114 CORRECT / 36 WRONG` | `76.00%` | `401,822` | `1,258,566` | `1,660,388` | met continue threshold |
| on small | `on_small_cfg_20260604_1818` | clean `small`, 35 QA | `28 CORRECT / 7 WRONG` | `80.00%` | `66,154` | `288,297` | `354,451` | better than `off small` under same benchmark shape |

### Three-run conclusion

- the three completed, same-path benchmark results are:
  - `off small = 77.14%`
  - `off sample = 76.00%`
  - `on small = 80.00%`
- direct conclusion:
  - under the same clean benchmark path, `on small` outperformed `off small`
  - the test data and derived numbers for these three runs have been recorded locally in this file and remain traceable to remote artifacts by `run_id`

## On sample start

- first `on sample` attempt:
  - `run_id = on_sample_cfg_20260604_0930`
- result:
  - invalid attempt
  - failed before ingest/QA due service config mismatch
- root cause:
  - OV was still left in localhost `auth_mode=dev` from the earlier committed-only compatibility run
  - standard `phase_a_off.py` path calls account bootstrap before ingest
  - account bootstrap requires OV `auth_mode=api_key` with `root_api_key` configured
- error:
  - `ensure_account_namespace failed: http_403`
  - `Admin API requires api_key mode with root_api_key configured`
- resolution:
  - restored `/root/.openviking/ov.conf` server config to:
    - `auth_mode = "api_key"`
    - `root_api_key = "ov-root-namespace-test-20260517"`

## On sample rerun

- relaunched `on sample` as:
  - `run_id = on_sample_cfg_20260604_0942`
  - mode: `on`
  - sessions: `1-19`
  - sample: `0`
  - judge: enabled
- current verified state:
  - OV `/health` reports `auth_mode=api_key`
  - gateway `/health` is live
  - standard `phase_a_off.py` run is active

### On sample sync proof

- runtime + test code sync for this run is verified:
  - local `benchmark/locomo/openclaw/phase_a_off.py`
    - `sha256 = 3c68d2eb49b9f3d6c66b768979494db41c9f0d26b891d39a01e2a955274484b9`
  - remote repo `/home/jcp/agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py`
    - `sha256 = 3c68d2eb49b9f3d6c66b768979494db41c9f0d26b891d39a01e2a955274484b9`
  - local `benchmark/locomo/openclaw/run_clean_small_in_container.sh`
    - `sha256 = 63e26c20762e8eef03544e3307fdaeabf4164df1168640fdf5552bdf60ec0ee9`
  - actual executed container script `/tmp/run_clean_small_in_container_on_sample_cfg_20260604_0942.sh`
    - `sha256 = 63e26c20762e8eef03544e3307fdaeabf4164df1168640fdf5552bdf60ec0ee9`

### On sample invalidation

- `on_sample_cfg_20260604_0942` should not be treated as a valid clean final run.
- observed facts:
  - ingest completed `19/19`
  - extracted memories:
    - `session_1 = 9`
    - `session_2` to `session_19 = 0`
  - QA output file was created but only wrote `4` rows
    - last completed QA row: `qi = 5`
    - no judge rows were written
  - `master.log` and resume state stopped advancing at `2026-06-04 01:57:48+00:00`
  - gateway log shows:
    - `signal SIGTERM received`
    - `received SIGTERM; shutting down`
- implication:
  - the gateway was terminated during the same benchmark attempt
  - therefore the clean-run contract was broken and this run must be restarted from a fresh clean environment

## On sample clean rerun 2

- relaunched `on sample` again as:
  - `run_id = on_sample_cfg_20260604_1024`
  - mode: `on`
  - sessions: `1-19`
  - sample: `0`
  - judge: enabled
- current verified state:
  - OV `/health` reports `auth_mode=api_key`
  - gateway `/health` is live
  - standard `phase_a_off.py` run is active
  - current ingest snapshot:
    - `session_1 = 9`

### On sample final result

- final valid `on sample` run:
  - `run_id = on_sample_cfg_20260604_1024`
  - benchmark shape: clean `sample`, `sessions 1-19`, `150` QA
- runtime + test code sync remained verified for the valid run:
  - local `benchmark/locomo/openclaw/phase_a_off.py`
    - `sha256 = 3c68d2eb49b9f3d6c66b768979494db41c9f0d26b891d39a01e2a955274484b9`
  - remote repo `/home/jcp/agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py`
    - `sha256 = 3c68d2eb49b9f3d6c66b768979494db41c9f0d26b891d39a01e2a955274484b9`
  - local `benchmark/locomo/openclaw/run_clean_small_in_container.sh`
    - `sha256 = 63e26c20762e8eef03544e3307fdaeabf4164df1168640fdf5552bdf60ec0ee9`
  - actual executed container script `/tmp/run_clean_small_in_container_on_sample_cfg_20260604_1024.sh`
    - `sha256 = 63e26c20762e8eef03544e3307fdaeabf4164df1168640fdf5552bdf60ec0ee9`
- final judged rows: `150/150`
- final result:
  - `108 CORRECT / 42 WRONG`
  - `accuracy = 72.00%`
- token summary:
  - ingest token: `477,884`
    - embedding: `109,975`
    - llm/vlm: `367,909`
  - QA token: `1,255,120`
    - input: `324,698`
    - output: `96,489`
  - total token: `1,733,004`

### On sample memory extraction note

- direct task inspection for the valid `on sample` run confirms:
  - `session_1` task returned `memories_extracted = {"memory_write": 9}`
  - `session_2` to `session_19` tasks all returned:
    - `memories_extracted = {}`
    - `active_count_updated = 0`
- this does **not** mean later sessions were not committed:
  - each task is `task_type = session_commit`
  - each task status is `completed`
  - each task returns an `archive_uri`
- observed behavior:
  - later QA sessions still received many relevant memories from later dates/sessions
  - so the issue is specifically that later direct-ingest commits did not promote content into active `memory_write`, not that the content failed to archive entirely

## 8b310ad4 current-framework test attempts

- target:
  - run `8b310ad4` core runtime code with the current benchmark framework / test code
  - order: `small` first, then `sample0`

### 8b310ad4 test root construction

- built remote overlay root:
  - `/home/jcp/agent/code/OpenViking_8b310ad4_test`
- contents:
  - base runtime code: local `git archive 8b310ad4`
  - overlaid current local testing assets:
    - `benchmark/locomo/openclaw/`
    - `tests/`
- current test-code sync inside overlay was verified:
  - `benchmark/locomo/openclaw/phase_a_off.py`
    - `sha256 = 3c68d2eb49b9f3d6c66b768979494db41c9f0d26b891d39a01e2a955274484b9`
  - `benchmark/locomo/openclaw/run_clean_small_in_container.sh`
    - `sha256 = 63e26c20762e8eef03544e3307fdaeabf4164df1168640fdf5552bdf60ec0ee9`

### 8b310ad4 small attempts

- attempted run ids:
  - `on_small_8b310ad4_20260604_1`
  - `on_small_8b310ad4_20260604_2`
  - `on_small_8b310ad4_20260604_3`
  - `on_small_8b310ad4_20260604_4`
- status:
  - no valid `small` benchmark result was produced yet
- common blocker:
  - benchmark bootstrap reaches plugin/auth config stage
  - gateway can become healthy
  - but OV at `127.0.0.1:1933` is not stably healthy when `phase_a_off.py` calls account bootstrap
  - benchmark then fails with:
    - `requests.exceptions.ConnectionError`
    - `Failed to establish a new connection: [Errno 111] Connection refused`
- direct evidence:
  - current root under test: `/home/jcp/agent/code/OpenViking_8b310ad4_test/openviking/server/bootstrap.py`
  - corresponding OV log prints:
    - `OpenViking HTTP Server is running on 127.0.0.1:1933`
  - but immediate health probe still returns:
    - `curl: (7) Failed to connect to 127.0.0.1 port 1933`
  - process table then shows no stable running OV server for that root

### 8b310ad4 blocker conclusion

- under the current framework and without modifying test code, `8b310ad4` small is currently blocked by runtime startup instability of the code under test itself.
- because `small` has not produced a valid result yet, `sample0` has not been started.

## Committed-only on small scope check

- under the current standard test path, committed-only `on small` cannot be expressed by configuration alone.
- evidence:
  - the standard chain executes benchmark code from `${REPO_ROOT}/benchmark/locomo/openclaw/phase_a_off.py`
  - `OpenViking_head_only/benchmark/locomo/openclaw/` only contains:
    - `eval.py`
    - `import_to_ov.py`
    - `judge.py`
    - `run_full_eval.sh`
    - `stat_judge_result.py`
  - it does **not** contain `phase_a_off.py`
- implication:
  - with the current scripts, `REPO_ROOT` couples service/runtime repo and benchmark repo
  - therefore “run with committed code while keeping standard benchmark code” is not currently configurable
- minimal future test-code change request, if this goal must be supported:
  - split the single `REPO_ROOT` into two independently configurable roots:
    - one for service/runtime code
    - one for benchmark/test code

## 8b310ad4 runtime compatibility update

- the earlier `8b310ad4 blocked at startup` conclusion was incomplete.
- root cause is now bounded more precisely:
  - the remote overlay root built from `git archive 8b310ad4` was missing native runtime artifacts that are present in the currently working remote tree
  - missing runtime pieces included:
    - `openviking/lib/ragfs_python.abi3.so`
    - `openviking/lib/libagfsbinding.so`
    - `openviking/storage/vectordb/engine/*.abi3.so`
- this means the primary blocker was not:
  - `1933` port conflict
  - `ov.conf` host/port misconfiguration
  - current benchmark/test code incompatibility
- direct evidence:
  - before补齐 native artifacts:
    - startup failed first on `ragfs_python native library is not available`
    - after adding only `ragfs_python.abi3.so`, startup advanced further but failed on local vectordb engine:
      - `ImportError: Native engine backend is missing from this wheel. Missing symbol: PersistStore`
  - after补齐 the missing native artifacts, direct smoke passed:
    - `/health -> {"status":"ok","healthy":true,"version":"0.3.5","auth_mode":"api_key"}`

## 8b310ad4 on small result

- run id:
  - `on_small_8b310ad4_20260604_1312`
- test path:
  - current standard container script + current benchmark/test code
  - `REPO_ROOT=/home/jcp/agent/code/OpenViking_8b310ad4_test`
- status:
  - complete

### Ingest evidence

- OV health OK
- gateway health OK
- direct-OV ingest completed `4/4`
- extracted memories by session:
  - `session_1 = 9`
  - `session_2 = 8`
  - `session_3 = 7`
  - `session_4 = 8`

### Small final result

- judged rows: `35/35`
- `CORRECT = 26`
- `WRONG = 9`
- accuracy: `74.29%`

### 8b310ad4 small token usage

- ingest token:
  - `70,045`
- QA token:
  - `293,017`
- total token:
  - `363,062`
- QA token breakdown:
  - `input = 95,341`
  - `output = 21,491`

### Interpretation

- compared with current mainline `on small`:
  - current mainline `on small = 28/35 = 80.00%`
  - `8b310ad4 on small = 26/35 = 74.29%`
- compared with current mainline `off small`:
  - current mainline `off small = 27/35 = 77.14%`
  - `8b310ad4 on small = 26/35 = 74.29%`
- therefore on the current standard framework and test code:
  - `8b310ad4` is now runnable after补齐 native runtime artifacts
  - but its `small` result is worse than both current mainline `on small` and current mainline `off small`

## 8b310ad4 on sample0 result

- run id:
  - `on_sample_8b310ad4_20260604_0542`
- test path:
  - current standard container script + current benchmark/test code
  - `REPO_ROOT=/home/jcp/agent/code/OpenViking_8b310ad4_test`
- status:
  - complete

### Ingest evidence

- OV health OK
- gateway health OK
- direct-OV ingest completed `19/19`
- all sessions produced non-zero memory counts
- representative tail sessions:
  - `session_14 = 19`
  - `session_16 = 15`
  - `session_19 = 5`

### Sample0 final result

- judged rows: `150/150`
- `CORRECT = 109`
- `WRONG = 41`
- accuracy: `72.67%`

### 8b310ad4 sample0 token usage

- ingest token:
  - `442,376`
- QA token:
  - `1,278,800`
- total token:
  - `1,721,176`
- QA token breakdown:
  - `input = 313,486`
  - `output = 113,397`

### Round 1 summary

| round | test | run_id | result | accuracy | ingest token | QA token | total token |
|---|---|---|---|---:|---:|---:|---:|
| 1 | `small` | `on_small_8b310ad4_20260604_1312` | `26 CORRECT / 9 WRONG` | `74.29%` | `70,045` | `293,017` | `363,062` |
| 1 | `sample0` | `on_sample_8b310ad4_20260604_0542` | `109 CORRECT / 41 WRONG` | `72.67%` | `442,376` | `1,278,800` | `1,721,176` |

## 8b310ad4 round 2 on small result

- run id:
  - `on_small_8b310ad4_r2_20260604_0650`
- status:
  - complete

### Round 2 small final result

- judged rows: `35/35`
- `CORRECT = 29`
- `WRONG = 6`
- accuracy: `82.86%`

### Round 2 small token usage

- ingest token:
  - `73,598`
- QA token:
  - `287,636`
- total token:
  - `361,234`
- QA token breakdown:
  - `input = 77,024`
  - `output = 16,617`

### Round 2 progress

| round | test | run_id | result | accuracy | ingest token | QA token | total token |
|---|---|---|---|---:|---:|---:|---:|
| 2 | `small` | `on_small_8b310ad4_r2_20260604_0650` | `29 CORRECT / 6 WRONG` | `82.86%` | `73,598` | `287,636` | `361,234` |
| 2 | `sample0` | `on_sample_8b310ad4_r2_20260604_0723` | `94 CORRECT / 56 WRONG` | `62.67%` | `515,389` | `1,252,435` | `1,767,824` |

## 8b310ad4 round 2 on sample0 result

- run id:
  - `on_sample_8b310ad4_r2_20260604_0723`

### Round 2 sample0 final result

- judged rows:
  - `150/150`
- `CORRECT = 94`
- `WRONG = 56`
- accuracy:
  - `62.67%`

### Round 2 sample0 token usage

- ingest token:
  - `515,389`
- QA token:
  - `1,252,435`
- total token:
  - `1,767,824`
- QA token breakdown:
  - `input = 307,581`
  - `output = 88,918`

## 8b310ad4 round 3 on small result

- run id:
  - `on_small_8b310ad4_r3_20260604_1646`
- status:
  - complete

### Round 3 small final result

- judged rows: `35/35`
- `CORRECT = 30`
- `WRONG = 5`
- accuracy: `85.71%`

### Round 3 small token usage

- ingest token:
  - `73,170`
- QA token:
  - `289,643`
- total token:
  - `362,813`
- QA token breakdown:
  - `input = 91,721`
  - `output = 19,602`

### Round 3 progress

| round | test | run_id | result | accuracy | ingest token | QA token | total token |
|---|---|---|---|---:|---:|---:|---:|
| 3 | `small` | `on_small_8b310ad4_r3_20260604_1646` | `30 CORRECT / 5 WRONG` | `85.71%` | `73,170` | `289,643` | `362,813` |
| 3 | `sample0` | `on_sample_8b310ad4_r3_20260604_1702` | `107 CORRECT / 43 WRONG` | `71.33%` | `421,511` | `1,273,676` | `1,695,187` |

## 8b310ad4 round 3 on sample0 result

- run id:
  - `on_sample_8b310ad4_r3_20260604_1702`

### Round 3 sample0 final result

- judged rows:
  - `150/150`
- `CORRECT = 107`
- `WRONG = 43`
- accuracy:
  - `71.33%`

### Round 3 sample0 token usage

- ingest token:
  - `421,511`
- QA token:
  - `1,273,676`
- total token:
  - `1,695,187`
- QA token breakdown:
  - `input = 321,586`
  - `output = 100,050`

## current latest code on small result

### Round 1 progress

| round | test | run_id | result | accuracy | ingest token | QA token | total token |
|---|---|---|---|---:|---:|---:|---:|
| 1 | `current latest on small` | `on_small_latest_r1b_20260604_1840` | `30 CORRECT / 5 WRONG` | `85.71%` | `68,818` | `298,124` | `366,942` |

### Round 1 final result

- judged rows:
  - `35/35`
- `CORRECT = 30`
- `WRONG = 5`
- accuracy:
  - `85.71%`

### Round 1 token usage

- ingest token:
  - `68,818`
- QA token:
  - `298,124`
- total token:
  - `366,942`
- QA token breakdown:
  - `input = 89,209`
  - `output = 23,274`

### Round 2 progress

| round | test | run_id | result | accuracy | ingest token | QA token | total token |
|---|---|---|---|---:|---:|---:|---:|
| 2 | `current latest on small` | `on_small_latest_r2_20260604_1905` | `29 CORRECT / 6 WRONG` | `82.86%` | `67,105` | `293,808` | `360,913` |

### Round 2 final result

- judged rows:
  - `35/35`
- `CORRECT = 29`
- `WRONG = 6`
- accuracy:
  - `82.86%`

### Round 2 token usage

- ingest token:
  - `67,105`
- QA token:
  - `293,808`
- total token:
  - `360,913`
- QA token breakdown:
  - `input = 74,924`
  - `output = 22,732`

## current latest code on sample0 result

### Round 1 progress

| round | test | run_id | result | accuracy | ingest token | QA token | total token |
|---|---|---|---|---:|---:|---:|---:|
| 1 | `current latest sample0` | `on_sample_latest_r1_20260604_1247` | `99 CORRECT / 51 WRONG` | `66.00%` | `334,304` | `1,054,220` | `1,388,524` |

### Round 1 final result

- judged rows:
  - `150/150`
- `CORRECT = 99`
- `WRONG = 51`
- accuracy:
  - `66.00%`

### Round 1 token usage

- ingest token:
  - `334,304`
- QA token:
  - `1,054,220`
- total token:
  - `1,388,524`
- QA token breakdown:
  - `input = 1,029,527`
  - `output = 24,693`

### Round 2 progress

| round | test | run_id | result | accuracy | ingest token | QA token | total token |
|---|---|---|---|---:|---:|---:|---:|
| 2 | `current latest sample0` | `on_sample_latest_r2_20260604_1336` | `92 CORRECT / 58 WRONG` | `61.33%` | `338,812` | `1,053,219` | `1,392,031` |

### Round 2 final result

- judged rows:
  - `150/150`
- `CORRECT = 92`
- `WRONG = 58`
- accuracy:
  - `61.33%`

### Round 2 token usage

- ingest token:
  - `338,812`
- QA token:
  - `1,053,219`
- total token:
  - `1,392,031`
- QA token breakdown:
  - `input = 1,027,895`
  - `output = 25,324`
