from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_ROOT = Path(__file__).resolve().parents[1]

default_args = {
    "owner": "bigdata-tsla",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tsla_batch_pipeline",
    description="Daily batch pipeline: collect -> process -> merge -> train -> predict",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["bigdata", "tsla", "batch"],
) as dag:
    collect = BashOperator(
        task_id="collect_data",
        bash_command=f"cd {REPO_ROOT} && PYTHONPATH=. python -m scripts.run_collect",
    )

    process = BashOperator(
        task_id="process_social",
        bash_command=f"cd {REPO_ROOT} && PYTHONPATH=. python -m scripts.run_process",
    )

    merge = BashOperator(
        task_id="merge_features",
        bash_command=f"cd {REPO_ROOT} && PYTHONPATH=. python -m scripts.run_merge",
    )

    train = BashOperator(
        task_id="train_models",
        bash_command=f"cd {REPO_ROOT} && PYTHONPATH=. python -m scripts.run_train",
    )

    predict = BashOperator(
        task_id="predict_daily",
        bash_command=f"cd {REPO_ROOT} && PYTHONPATH=. python -m scripts.run_predict",
    )

    collect >> process >> merge >> train >> predict
