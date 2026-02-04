from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_ROOT = Path(__file__).resolve().parents[1]

with DAG(
    dag_id="tsla_streaming_pipeline",
    description="Manual start/stop for Kafka + Spark streaming services",
    start_date=datetime(2025, 1, 1),
    schedule="0 11 * * *",
    catchup=False,
    tags=["bigdata", "tsla", "streaming"],
) as dag:
    start_streaming = BashOperator(
        task_id="start_streaming",
        bash_command=f"cd {REPO_ROOT} && bash scripts/run_streaming_all.sh",
    )

    stop_streaming = BashOperator(
        task_id="stop_streaming",
        bash_command=f"cd {REPO_ROOT} && bash scripts/stop_streaming_all.sh",
    )

    start_streaming >> stop_streaming
