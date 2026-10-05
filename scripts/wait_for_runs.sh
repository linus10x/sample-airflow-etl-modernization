#!/usr/bin/env bash
# Wait until a DAG has no queued or running runs. Usage: wait_for_runs.sh <dag_id> [timeout_seconds]
set -euo pipefail
dag_id="$1"
timeout="${2:-900}"
deadline=$((SECONDS + timeout))
sleep 4
while (( SECONDS < deadline )); do
  active=$(docker compose exec -T postgres psql -U "${POSTGRES_USER:-warehouse}" -d airflow -Atc \
    "select count(*) from dag_run where dag_id='${dag_id}' and state in ('queued','running')")
  total=$(docker compose exec -T postgres psql -U "${POSTGRES_USER:-warehouse}" -d airflow -Atc \
    "select count(*) from dag_run where dag_id='${dag_id}'")
  if [[ "$active" == "0" && "$total" != "0" ]]; then
    echo "${dag_id}: ${total} run(s), none active"
    exit 0
  fi
  sleep 3
done
echo "timed out waiting for ${dag_id}" >&2
exit 1
