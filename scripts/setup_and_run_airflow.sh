#!/usr/bin/env bash
set -euo pipefail

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install -r requirements.txt

export AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-$PWD/dags}"

if [[ ! -f "$AIRFLOW_HOME/airflow.db" ]]; then
  airflow db init
  airflow users create --username admin --password admin \
    --firstname admin --lastname admin --role Admin --email admin@example.com
fi

nohup airflow scheduler > "$AIRFLOW_HOME/scheduler.out" 2>&1 &
echo $! > "$AIRFLOW_HOME/scheduler.pid"

nohup airflow webserver -p 8080 > "$AIRFLOW_HOME/webserver.out" 2>&1 &
echo $! > "$AIRFLOW_HOME/webserver.pid"

echo "[airflow] started. Web UI: http://localhost:8080"
