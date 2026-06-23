#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-jcp@123.60.114.206}"
REMOTE_PORT="${REMOTE_PORT:-10008}"
REMOTE_CONTAINER="${REMOTE_CONTAINER:-jcp-dev}"
RUN_ID="${RUN_ID:?RUN_ID is required}"
MODE="${MODE:-off}"
SESSIONS_LABEL="${SESSIONS_LABEL:-4sessions}"

CSV="/tmp/${RUN_ID}/phaseA_${MODE}_${SESSIONS_LABEL}_${RUN_ID}.csv"
ALT_CSV="/tmp/${RUN_ID}/phaseA_${MODE}_${RUN_ID}.csv"
STATE="/tmp/phaseA_${MODE}_${SESSIONS_LABEL}_${RUN_ID}_resume.json"
LOG="/tmp/${RUN_ID}.master.log"
TMP_SCRIPT="/tmp/check_remote_small_run_${RUN_ID}.sh"

cat >"${TMP_SCRIPT}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf '==PROC==\n'
ps -ef | grep -F 'run-id ${RUN_ID}' | grep -v grep || true
printf '==CSV==\n'
CSV_PATH=''
if [ -f '${CSV}' ]; then
  CSV_PATH='${CSV}'
elif [ -f '${ALT_CSV}' ]; then
  CSV_PATH='${ALT_CSV}'
fi
if [ -n "\${CSV_PATH}" ]; then
  python3 - "\${CSV_PATH}" <<'PY'
import csv
p=__import__('sys').argv[1]
with open(p,'r',encoding='utf-8',newline='') as f:
    rows=list(csv.DictReader(f))
qis=[int(r['qi']) for r in rows if r.get('qi')]
print('rows=', len(rows))
print('last_qi=', qis[-1] if qis else None)
if rows:
    print('last_question=', rows[-1].get('question'))
    print('last_ts=', rows[-1].get('timestamp'))
print('csv_path=', p)
PY
else
  echo no_csv
fi
printf '==STATE==\n'
if [ -f '${STATE}' ]; then
  python3 - <<'PY'
import json
p='${STATE}'
with open(p,'r',encoding='utf-8') as f:
    data=json.load(f)
print('updated_at=', data.get('updated_at'))
for _, v in (data.get('sessions') or {}).items():
    row=v.get('row') or {}
    print(v.get('index'), v.get('locomo_session_key'), v.get('stage'), row.get('memory_count'))
PY
else
  echo no_state
fi
printf '==LOG==\n'
if [ -f '${LOG}' ]; then
  tail -n 40 '${LOG}'
else
  echo no_log
fi
EOF

chmod +x "${TMP_SCRIPT}"
scp -P "${REMOTE_PORT}" "${TMP_SCRIPT}" "${REMOTE_HOST}:${TMP_SCRIPT}" >/dev/null
ssh -p "${REMOTE_PORT}" "${REMOTE_HOST}" \
  "docker cp ${TMP_SCRIPT} ${REMOTE_CONTAINER}:${TMP_SCRIPT} >/dev/null && \
   docker exec ${REMOTE_CONTAINER} bash ${TMP_SCRIPT}"
rm -f "${TMP_SCRIPT}"
