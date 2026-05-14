FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-clickhouse==1.8.0

WORKDIR /dbt
COPY . .
