# Airflow (batch orchestration)

This repo includes a minimal DAG that runs the batch pipeline:
`collect → process → merge → train → predict`.

## Files
- DAG: `dags/tsla_pipeline_dag.py`
- DAG (streaming): `dags/tsla_streaming_dag.py`

## Local setup (quick)
1) Create/activate venv and install deps:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install apache-airflow==2.10.5
```

2) Initialize Airflow (use a local home inside the repo):
```
export AIRFLOW_HOME="$PWD/airflow"
airflow db init
airflow users create \
  --username admin --password admin \
  --firstname admin --lastname admin \
  --role Admin --email admin@example.com
```

3) Point Airflow to this repo’s DAGs:
```
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
```

4) Run scheduler + webserver (two terminals):
```
airflow scheduler
```
```
airflow webserver -p 8080
```

Open the UI: http://localhost:8080

### Single command
```
bash scripts/run_airflow.sh
```

## Streaming control (manual)
The streaming DAG provides two tasks:
- `start_streaming` runs `scripts/run_streaming_all.sh`
- `stop_streaming` runs `scripts/stop_streaming_all.sh`

Trigger the DAG manually when you want to start/stop the live pipeline.

## Notes
- Airflow 2.10.5 supports Python 3.12. If you use an older Airflow, create a Python 3.11 venv.
- The DAG runs daily at 02:00 UTC by default.
- Ensure Kafka/Mongo are running if your collection depends on them.
