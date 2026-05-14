"""
DAG 1 — Crypto Ingestion
Schedule : every 1 minute
Job      : Poll CoinCap REST API → produce each asset price to Kafka topic 'crypto_prices'
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.coingecko import fetch_prices
from utils.kafka_producer import produce_messages


def ingest(**context) -> None:
    records = fetch_prices()
    produce_messages(records)


with DAG(
    dag_id="dag_1_crypto_ingestion",
    description="Fetches CoinGecko prices every minute and pushes to Kafka",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "crypto", "retries": 2, "retry_delay": timedelta(seconds=15), "email_on_failure": False},
    tags=["ingestion", "kafka", "coingecko"],
) as dag:

    PythonOperator(
        task_id="fetch_prices_produce_to_kafka",
        python_callable=ingest,
    )
