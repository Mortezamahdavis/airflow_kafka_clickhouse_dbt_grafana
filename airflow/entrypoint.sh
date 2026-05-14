#!/usr/bin/env bash
set -e

# Migrate DB (safe to run on every start — idempotent)
airflow db migrate

# Create admin user if it doesn't exist, otherwise reset the password.
# Both branches use the credentials passed via env vars.
airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin_secret}" \
  --firstname Admin --lastname User \
  --role Admin \
  --email admin@example.com 2>/dev/null \
|| airflow users reset-password \
  -u "${AIRFLOW_ADMIN_USER:-admin}" \
  -p "${AIRFLOW_ADMIN_PASSWORD:-admin_secret}"

# Hand off to the normal Airflow standalone process
exec airflow standalone
