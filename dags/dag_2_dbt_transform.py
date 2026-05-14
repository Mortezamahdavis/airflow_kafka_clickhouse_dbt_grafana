"""
DAG 2 — dbt Build Runner
Schedule : every 4 hours
Job      : Run transformation/run_dbt.py inside the dbt container.
           `dbt build` runs all models then all tests in dependency order.
           Schema-level validation is handled upstream by Schema Registry (Avro).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from utils.grafana import post_annotation

with DAG(
    dag_id="dag_2_dbt_build",
    description="Runs dbt build every 4 hours — models + tests",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(hours=4),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "crypto", "retries": 1, "retry_delay": timedelta(minutes=5), "email_on_failure": False},
    tags=["data-quality", "dbt"],
) as dag:

    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="airflow_kafka_clickhouse_dbt_grafana-dbt",  # built by docker-compose
        command="python /dbt/run_dbt.py",
        network_mode="airflow_kafka_clickhouse_dbt_grafana_default",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            "CLICKHOUSE_USER": Variable.get("CLICKHOUSE_USER", default_var="default"),
            "CLICKHOUSE_PASSWORD": Variable.get("CLICKHOUSE_PASSWORD", default_var=""),
        },
    )

    annotate_grafana = PythonOperator(
        task_id="annotate_grafana",
        python_callable=post_annotation,
        op_kwargs={
            "text": "dbt build completed",
            "tags": ["dbt", "transform"],
        },
    )

    dbt_build >> annotate_grafana
