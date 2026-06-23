#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-off}"
RUN_ID="${RUN_ID:-${MODE}_small_$(date +%Y%m%d_%H%M%S)}"
LOCOMO_EVAL_MODEL="${LOCOMO_EVAL_MODEL:-}"
LOCOMO_PROVIDER_API_KEY="${LOCOMO_PROVIDER_API_KEY:-}"
OPENCLAW_STATE_DIR="${OPENCLAW_STATE_DIR:-/tmp/openclaw-state-$RUN_ID}"
OPENCLAW_GATEWAY_PORT="${OPENCLAW_GATEWAY_PORT:-28789}"
OPENVIKING_INSTANCE_DIR="${OPENVIKING_INSTANCE_DIR:-/tmp/openviking-$RUN_ID}"
OPENVIKING_PORT="${OPENVIKING_PORT:-21933}"
DATA_PATH="${DATA_PATH:-/home/jcp/agent/code/locomo-test-kit/data/locomo10.json}"
REPO_ROOT="${REPO_ROOT:-/home/jcp/agent/code/OpenViking}"
BASE_OV_CONF_PATH="${BASE_OV_CONF_PATH:-/root/.openviking/ov.conf}"
OV_CONF_PATH="${OV_CONF_PATH:-${OPENVIKING_INSTANCE_DIR}/ov.conf}"
OV_DATA_DIR="${OV_DATA_DIR:-${OPENVIKING_INSTANCE_DIR}/data}"
BASE_OPENCLAW_STATE_DIR="${BASE_OPENCLAW_STATE_DIR:-/root/.openclaw}"
OPENCLAW_AGENT_DIR="${OPENCLAW_AGENT_DIR:-${OPENCLAW_STATE_DIR}/agents/locomo-eval}"
OPENCLAW_MAIN_AGENT_DIR="${OPENCLAW_MAIN_AGENT_DIR:-${OPENCLAW_STATE_DIR}/agents/main/agent}"
OPENCLAW_ENV="${OPENCLAW_ENV:-${OPENCLAW_STATE_DIR}/openviking.env}"
OPENCLAW_CONFIG_PATH="${OPENCLAW_CONFIG_PATH:-${OPENCLAW_STATE_DIR}/openclaw.json}"
OPENVIKING_PYTHON_BIN="${OPENVIKING_PYTHON_BIN:-python3}"
QA_START="${QA_START:-}"
QA_END="${QA_END:-}"
LOCK_FILE="${LOCK_FILE:-/tmp/locomo-openclaw-benchmark.lock}"
export OPENVIKING_CONFIG_FILE="${OV_CONF_PATH}"
export OPENCLAW_STATE_DIR
export OPENCLAW_CONFIG_PATH

OPENCLAW_GATEWAY_TOKEN="${OPENCLAW_GATEWAY_TOKEN:?OPENCLAW_GATEWAY_TOKEN is required}"
OPENVIKING_ROOT_API_KEY="${OPENVIKING_ROOT_API_KEY:?OPENVIKING_ROOT_API_KEY is required}"

OUTPUT_DIR="${OUTPUT_DIR:-/tmp/${RUN_ID}}"
BACKUP_PATH="${BACKUP_PATH:-/tmp/${RUN_ID}_backup.tar.gz}"
MASTER_LOG="${MASTER_LOG:-/tmp/${RUN_ID}.master.log}"
OV_LOG="${OV_LOG:-/tmp/${RUN_ID}_ov.log}"
GW_LOG="${GW_LOG:-/tmp/${RUN_ID}_gw.log}"

OV_ACCOUNT_ID="${OV_ACCOUNT_ID:-acct-${RUN_ID}}"
OV_USER_ID="${OV_USER_ID:-user-${RUN_ID}}"
SESSIONS="${SESSIONS:-1-4}"
SAMPLE="${SAMPLE:-0}"
JUDGE_PARALLEL="${JUDGE_PARALLEL:-3}"
POLL_TIMEOUT="${POLL_TIMEOUT:-1800}"
COMPACT_TIMEOUT="${COMPACT_TIMEOUT:-600}"
ISOLATE_USER_SCOPE_BY_AGENT="${ISOLATE_USER_SCOPE_BY_AGENT:-false}"
ISOLATE_AGENT_SCOPE_BY_USER="${ISOLATE_AGENT_SCOPE_BY_USER:-false}"
SKIP_JUDGE="${SKIP_JUDGE:-true}"
SKIP_OV_CONFIG_CHECK="${SKIP_OV_CONFIG_CHECK:-false}"
SYNC_PLUGIN_CONFIG_IN_SCRIPT="${SYNC_PLUGIN_CONFIG_IN_SCRIPT:-true}"

if [[ "${MODE}" != "off" && "${MODE}" != "on" ]]; then
  echo "MODE must be off or on" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

LOCK_FD=""

acquire_benchmark_lock() {
  exec {LOCK_FD}>"${LOCK_FILE}"
  if ! flock -n "${LOCK_FD}"; then
    echo "[$(date -Is)] benchmark lock busy: ${LOCK_FILE}" >&2
    echo "[$(date -Is)] refusing to start overlapping LoCoMo/OpenClaw run: ${RUN_ID}" >&2
    exit 91
  fi
  printf '%s\n' "${RUN_ID}" 1>&"${LOCK_FD}"
}

release_benchmark_lock() {
  if [[ -n "${LOCK_FD}" ]]; then
    flock -u "${LOCK_FD}" || true
    eval "exec ${LOCK_FD}>&-"
    LOCK_FD=""
  fi
}

trap release_benchmark_lock EXIT

bootstrap_isolated_runtime() {
  mkdir -p "${OPENCLAW_STATE_DIR}" "${OPENCLAW_AGENT_DIR}" "${OPENCLAW_MAIN_AGENT_DIR}" "${OV_DATA_DIR}"
  if [[ -n "${LOCOMO_PROVIDER_API_KEY}" ]]; then
    echo "[$(date -Is)] provider override requested suffix=${LOCOMO_PROVIDER_API_KEY: -6}"
  else
    echo "[$(date -Is)] provider override requested suffix=(empty)"
  fi

  python3 - "${BASE_OPENCLAW_STATE_DIR}" "${OPENCLAW_STATE_DIR}" "${OPENCLAW_CONFIG_PATH}" "${OPENCLAW_GATEWAY_PORT}" "${LOCOMO_EVAL_MODEL}" "${LOCOMO_PROVIDER_API_KEY}" <<'PY'
import json
import shutil
import sys
from pathlib import Path

base_state_dir = Path(sys.argv[1])
state_dir = Path(sys.argv[2])
config_path = Path(sys.argv[3])
gateway_port = int(sys.argv[4])
locomo_model = sys.argv[5].strip()
provider_api_key = sys.argv[6].strip()

state_dir.mkdir(parents=True, exist_ok=True)
config_path.parent.mkdir(parents=True, exist_ok=True)

base_config = json.loads((base_state_dir / "openclaw.json").read_text(encoding="utf-8"))
gateway = base_config.setdefault("gateway", {})
gateway["port"] = gateway_port
if locomo_model:
    base_config.setdefault("agents", {}).setdefault("defaults", {}).setdefault("model", {})["primary"] = locomo_model
    for agent in base_config.get("agents", {}).get("list", []):
        if isinstance(agent, dict) and agent.get("id") == "locomo-eval":
            agent["model"] = locomo_model
if provider_api_key:
    provider_name = (locomo_model or base_config.get("agents", {}).get("defaults", {}).get("model", {}).get("primary", "")).split("/", 1)[0].strip()
    if provider_name:
        providers = base_config.setdefault("models", {}).setdefault("providers", {})
        provider_cfg = providers.setdefault(provider_name, {})
        provider_cfg["apiKey"] = provider_api_key
        result = {
            "provider_override_applied": True,
            "provider_name": provider_name,
            "key_suffix": provider_api_key[-6:],
        }
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(json.dumps({"provider_override_applied": False, "reason": "missing provider_name"}, ensure_ascii=False))
elif locomo_model:
    print(json.dumps({"provider_override_applied": False, "reason": "empty provider_api_key"}, ensure_ascii=False))
control_ui = gateway.setdefault("controlUi", {})
control_ui["allowedOrigins"] = [
    f"http://localhost:{gateway_port}",
    f"http://127.0.0.1:{gateway_port}",
]
config_path.write_text(json.dumps(base_config, ensure_ascii=False, indent=2) + chr(10), encoding="utf-8")

for rel in [
    ("agents/main/agent/auth-profiles.json", "agents/main/agent/auth-profiles.json"),
    ("agents/main/agent/auth-state.json", "agents/main/agent/auth-state.json"),
    ("openviking.env", "openviking.env"),
]:
    src = base_state_dir / rel[0]
    dst = state_dir / rel[1]
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

extensions_src = base_state_dir / "extensions" / "openviking"
extensions_dst = state_dir / "extensions" / "openviking"
if extensions_src.exists():
    if extensions_dst.exists():
        shutil.rmtree(extensions_dst)
    extensions_dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(extensions_src, extensions_dst)
PY

  python3 - "${BASE_OV_CONF_PATH}" "${OV_CONF_PATH}" "${OV_DATA_DIR}" "${OPENVIKING_PORT}" <<'PY'
import json
import sys
from pathlib import Path

base_conf = Path(sys.argv[1])
target_conf = Path(sys.argv[2])
data_dir = Path(sys.argv[3])
ov_port = int(sys.argv[4])

target_conf.parent.mkdir(parents=True, exist_ok=True)
data_dir.mkdir(parents=True, exist_ok=True)

cfg = json.loads(base_conf.read_text(encoding="utf-8"))
cfg.setdefault("server", {})["port"] = ov_port
cfg.setdefault("storage", {})["workspace"] = str(data_dir)
target_conf.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + chr(10), encoding="utf-8")
PY
}

backup_and_reset() {
  echo "[$(date -Is)] backup start -> ${BACKUP_PATH}"
  tar -czf "${BACKUP_PATH}" "${OV_DATA_DIR}" /root/.openclaw/agents 2>/dev/null || true
  echo "[$(date -Is)] backup done"

  if [[ "${OPENCLAW_STATE_DIR}" == "/root/.openclaw" ]]; then
    pkill -f 'phase_a_off.py' || true
    pkill -f 'openclaw-gateway' || true
    pkill -f 'python3 -m openviking.server.bootstrap --host 127.0.0.1 --port 1933 --workers 1' || true
    sleep 2
  fi

  rm -rf "${OV_DATA_DIR:?}/"*
  mkdir -p "${OV_DATA_DIR}"
  rm -rf "${OPENCLAW_AGENT_DIR}/sessions" \
         "${OPENCLAW_AGENT_DIR}"/*.json \
         "${OPENCLAW_AGENT_DIR}"/*.jsonl \
         "${OPENCLAW_AGENT_DIR}"/*.db 2>/dev/null || true
  mkdir -p "${OPENCLAW_AGENT_DIR}/sessions"
}

set_wm_mode() {
  "${OPENVIKING_PYTHON_BIN}" - "${OV_CONF_PATH}" "${MODE}" <<'PY'
import json
import sys
from pathlib import Path

try:
    import openviking
    ov_version = getattr(openviking, "__version__", "")
except Exception:
    ov_version = ""

conf_path = Path(sys.argv[1])
mode = sys.argv[2]
data = json.loads(conf_path.read_text(encoding="utf-8"))
memory = data.setdefault("memory", {})
memory.pop("wm_v2_preprocess_enabled", None)
result = {"wm_v2_preprocess_enabled": None, "skipped_for_version": ov_version, "requested_mode": mode}
conf_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + chr(10), encoding="utf-8")
print(json.dumps(result, ensure_ascii=False))
PY
}

sync_openclaw_plugin_config() {
  python3 - "${OPENCLAW_CONFIG_PATH}" "${OV_USER_ID}" "${OV_ACCOUNT_ID}" "${ISOLATE_USER_SCOPE_BY_AGENT}" "${ISOLATE_AGENT_SCOPE_BY_USER}" "${OPENVIKING_PORT}" <<'PY'
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
user_id = sys.argv[2]
account_id = sys.argv[3]
isolate_user_scope_by_agent = sys.argv[4].lower() == "true"
isolate_agent_scope_by_user = sys.argv[5].lower() == "true"
openviking_port = int(sys.argv[6])

data = json.loads(config_path.read_text(encoding="utf-8"))
plugins = data.setdefault("plugins", {})
entries = plugins.setdefault("entries", {})
openviking = entries.setdefault("openviking", {})
cfg = openviking.setdefault("config", {})

cfg["baseUrl"] = f"http://127.0.0.1:{openviking_port}"
cfg["userId"] = user_id
cfg["accountId"] = account_id
cfg["emitStandardDiagnostics"] = True
cfg["logFindRequests"] = True
cfg.pop("agent_prefix", None)
cfg.pop("isolateUserScopeByAgent", None)
cfg.pop("isolateAgentScopeByUser", None)

slots = plugins.setdefault("slots", {})
slots["contextEngine"] = "openviking"

config_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + chr(10), encoding="utf-8")
print(json.dumps(cfg, ensure_ascii=False))
PY
}

set_openclaw_gateway_port() {
  python3 - "${OPENCLAW_CONFIG_PATH}" "${OPENCLAW_GATEWAY_PORT}" <<'PY'
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
gateway_port = int(sys.argv[2])
data = json.loads(config_path.read_text(encoding="utf-8"))
gateway = data.setdefault("gateway", {})
gateway["port"] = gateway_port
control_ui = gateway.setdefault("controlUi", {})
control_ui["allowedOrigins"] = [
    f"http://localhost:{gateway_port}",
    f"http://127.0.0.1:{gateway_port}",
]
config_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + chr(10), encoding="utf-8")
print(json.dumps({"gateway_port": gateway_port}, ensure_ascii=False))
PY
}

sync_openclaw_auth_profiles() {
  python3 - "${OPENCLAW_CONFIG_PATH}" "${OPENCLAW_MAIN_AGENT_DIR}" <<'PY'
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
main_agent_dir = Path(sys.argv[2])
auth_path = main_agent_dir / "auth-profiles.json"

config = json.loads(config_path.read_text(encoding="utf-8"))
providers = config.get("models", {}).get("providers", {})
default_model = (
    config.get("agents", {})
    .get("defaults", {})
    .get("model", {})
    .get("primary", "")
)
provider_name = str(default_model).split("/", 1)[0].strip()
provider_cfg = providers.get(provider_name, {}) if provider_name else {}
provider_key = str(provider_cfg.get("apiKey") or "").strip()
if not provider_name or not provider_key:
    print(json.dumps({"changed": False, "reason": "missing provider or apiKey"}, ensure_ascii=False))
    raise SystemExit(0)

main_agent_dir.mkdir(parents=True, exist_ok=True)
if auth_path.exists():
    auth = json.loads(auth_path.read_text(encoding="utf-8"))
else:
    auth = {"version": 1, "profiles": {}}

profiles = auth.setdefault("profiles", {})
profile_id = f"{provider_name}:default"
current = profiles.get(profile_id, {})
changed = current.get("key") != provider_key or current.get("provider") != provider_name or current.get("type") != "api_key"
profiles[profile_id] = {
    "type": "api_key",
    "provider": provider_name,
    "key": provider_key,
}
auth_path.write_text(json.dumps(auth, ensure_ascii=False, indent=2) + chr(10), encoding="utf-8")
print(
    json.dumps(
        {
            "changed": changed,
            "profile": profile_id,
            "provider": provider_name,
            "key_suffix": provider_key[-6:] if provider_key else "",
            "path": str(auth_path),
        },
        ensure_ascii=False,
    )
)
PY
}

start_services() {
  cd "${REPO_ROOT}"

  nohup "${OPENVIKING_PYTHON_BIN}" -m openviking.server.bootstrap --config "${OV_CONF_PATH}" --host 127.0.0.1 --port "${OPENVIKING_PORT}" --workers 1 >"${OV_LOG}" 2>&1 &
  for _ in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:${OPENVIKING_PORT}/health" >/tmp/"${RUN_ID}"_ov_health.json 2>/dev/null; then
      echo "[$(date -Is)] ov health ok"
      cat /tmp/"${RUN_ID}"_ov_health.json
      break
    fi
    sleep 1
  done

  # shellcheck disable=SC1090
  source "${OPENCLAW_ENV}"
  nohup env OPENCLAW_STATE_DIR="${OPENCLAW_STATE_DIR}" OPENCLAW_CONFIG_PATH="${OPENCLAW_CONFIG_PATH}" openclaw gateway >"${GW_LOG}" 2>&1 &
  for _ in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:${OPENCLAW_GATEWAY_PORT}/health" >/tmp/"${RUN_ID}"_gw_health.json 2>/dev/null; then
      echo "[$(date -Is)] gateway health ok"
      cat /tmp/"${RUN_ID}"_gw_health.json
      break
    fi
    sleep 1
  done
}

run_phase_a() {
  cd "${REPO_ROOT}"

  local -a args=(
    python3 benchmark/locomo/openclaw/phase_a_off.py
    "${DATA_PATH}"
    --mode "${MODE}"
    --sample "${SAMPLE}"
    --sessions "${SESSIONS}"
    --run-id "${RUN_ID}"
    --output-dir "${OUTPUT_DIR}"
    --base-url "http://127.0.0.1:${OPENCLAW_GATEWAY_PORT}"
    --openviking-url "http://127.0.0.1:${OPENVIKING_PORT}"
    --openclaw-state-dir "${OPENCLAW_STATE_DIR}"
    --token "${OPENCLAW_GATEWAY_TOKEN}"
    --ov-api-key "${OPENVIKING_ROOT_API_KEY}"
    --ov-admin-api-key "${OPENVIKING_ROOT_API_KEY}"
    --ov-account-id "${OV_ACCOUNT_ID}"
    --user "${OV_USER_ID}"
    --poll-timeout "${POLL_TIMEOUT}"
    --compact-timeout "${COMPACT_TIMEOUT}"
    --judge-parallel "${JUDGE_PARALLEL}"
  )

  if [[ -n "${QA_START}" ]]; then
    args+=(--qa-start "${QA_START}")
  fi
  if [[ -n "${QA_END}" ]]; then
    args+=(--qa-end "${QA_END}")
  fi
  if [[ -n "${QA_DISABLE_AUTOCAPTURE:-}" ]]; then
    args+=(--qa-disable-autocapture)
  fi
  if [[ "${SKIP_JUDGE}" == "true" ]]; then
    args+=(--skip-judge)
  fi
  if [[ "${SKIP_OV_CONFIG_CHECK}" == "true" ]]; then
    args+=(--skip-ov-config-check)
  fi
  if [[ "${SYNC_PLUGIN_CONFIG_IN_SCRIPT}" == "true" ]]; then
    args+=(--no-sync-plugin-config)
  fi
  if [[ "${ISOLATE_USER_SCOPE_BY_AGENT}" == "false" ]]; then
    args+=(--no-isolate-user-scope-by-agent)
  fi
  if [[ "${ISOLATE_AGENT_SCOPE_BY_USER}" == "false" ]]; then
    args+=(--no-isolate-agent-scope-by-user)
  fi

  "${args[@]}"
}

{
  acquire_benchmark_lock
  bootstrap_isolated_runtime
  backup_and_reset
  set_wm_mode
  if [[ "${SYNC_PLUGIN_CONFIG_IN_SCRIPT}" == "true" ]]; then
    sync_openclaw_plugin_config
  fi
  set_openclaw_gateway_port
  sync_openclaw_auth_profiles
  start_services
  run_phase_a
} 2>&1 | tee "${MASTER_LOG}"
