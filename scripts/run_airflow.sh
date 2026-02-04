#!/usr/bin/env bash
set -euo pipefail

export AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-$PWD/dags}"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "Warning: no virtualenv detected. Activate .venv before running."
fi

echo "[airflow] AIRFLOW_HOME=$AIRFLOW_HOME"
echo "[airflow] DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"

nohup airflow scheduler > "$AIRFLOW_HOME/scheduler.out" 2>&1 &
echo $! > "$AIRFLOW_HOME/scheduler.pid"

nohup airflow webserver -p 8080 > "$AIRFLOW_HOME/webserver.out" 2>&1 &
echo $! > "$AIRFLOW_HOME/webserver.pid"

echo "[airflow] started. Web UI: http://localhost:8080"
