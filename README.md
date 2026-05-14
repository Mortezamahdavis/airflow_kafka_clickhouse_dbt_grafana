# Crypto Analytics Pipeline

![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

I built this to get hands-on with a modern data engineering stack — the kind you'd actually see in production, not just textbook examples. It pulls live crypto prices from CoinGecko every minute, streams them through Kafka, lands them in ClickHouse, then runs dbt transformations on a schedule. Grafana sits on top with a pre-wired dashboard so you can see prices, 24h changes, and OHLC candles right after boot.

Everything runs in Docker Compose — one `docker compose up --build` and you have a full streaming analytics stack on your laptop.

---

## What's in here

```
CoinGecko API
     │
     ▼
Airflow DAG 1 (every 1 min)
     │  Avro + Schema Registry
     ▼
Kafka (3 brokers)  ──►  ClickHouse Kafka Engine  ──►  MergeTree (crypto_raw)
                                                              │
                                              Airflow DAG 2 (every 4 h)
                                                              │
                                                         dbt build
                                                    ┌──────────────────────────┐
                                                    │  stg_crypto_prices (view) │
                                                    │  latest_prices  (table)   │
                                                    │  ohlc_per_minute (table)  │
                                                    └──────────────────────────┘
                                                              │
                                                         Grafana
                                               (auto-provisioned dashboard)
```

The two DAGs are kept intentionally separate — ingestion runs every minute and is lightweight, while the dbt build runs every 4 hours since the transformations don't need to be fresher than that.

**Tech used:**

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9.1 |
| Message broker | Kafka 7.7.0 (3 brokers) + ZooKeeper |
| Schema validation | Confluent Schema Registry (Avro) |
| Storage | ClickHouse |
| Transformation | dbt-clickhouse 1.8.0 |
| Visualisation | Grafana (auto-provisioned) |
| Container runtime | Docker Compose |
| Airflow metadata | PostgreSQL 15 |

---

## Getting started

**Requirements:** Docker Desktop with at least 8 GB RAM allocated (the 3-broker Kafka cluster is the hungry part). Ports 8080, 8081, 8090, 8123, 3000, and 9092–9094 need to be free.

### Clone and configure

```bash
git clone https://github.com/Mortezamahdavis/airflow_kafka_clickhouse_dbt_grafana.git
cd airflow_kafka_clickhouse_dbt_grafana
cp .env.example .env
```

The defaults in `.env` work out of the box for local use — no changes needed unless you want to swap passwords:

```env
CONFLUENT_VERSION=7.7.0
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow_secret
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin_secret
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=grafana_secret
```

> `CLICKHOUSE_PASSWORD` is empty by design — ClickHouse's default user ships with no password. Fine for local dev, but lock it down before putting this anywhere public.

### Start everything

```bash
docker compose up -d --build
```

First boot takes 3–5 minutes. Kafka brokers need to elect a leader and Airflow needs to run its DB migrations before the UI comes up. You can watch progress with `docker compose ps` — wait until the services you care about show as healthy.

### Set up the ClickHouse schema

The `init.sql` is mounted into ClickHouse's `docker-entrypoint-initdb.d/` so it runs automatically on a fresh volume. If you've wiped volumes and restarted and the tables aren't there, run it manually:

```bash
docker exec -i clickhouse clickhouse-client --multiquery < storage/init.sql
```

Quick check:

```bash
docker exec -it clickhouse clickhouse-client --query "SHOW TABLES IN default"
# should print: crypto_raw  crypto_raw_kafka  crypto_raw_mv
```

### Turn on the DAGs

Open Airflow at [http://localhost:8080](http://localhost:8080) and log in with `admin` / `admin_secret`.

Unpause both DAGs:
- **`dag_1_crypto_ingestion`** — hits CoinGecko every minute, serialises to Avro, produces to Kafka
- **`dag_2_dbt_transform`** — runs `dbt build` every 4 hours inside a Docker container, then drops a Grafana annotation

> One thing worth knowing: Airflow's `standalone` mode only creates the admin user on a fresh database — if you restart against an existing volume, the password can drift. I worked around this with a custom entrypoint that runs `users create || reset-password` on every start, so the password from `.env` is always applied regardless of volume state.

### Check data is flowing

After a couple of minutes, rows should be landing in ClickHouse:

```bash
docker exec -it clickhouse clickhouse-client \
  --query "SELECT id, current_price, ingested_at FROM default.crypto_raw LIMIT 5"
```

### Open Grafana

[http://localhost:3000](http://localhost:3000) — `admin` / `grafana_secret`

The **Crypto Analytics** dashboard is pre-provisioned, no setup needed. Use the `asset` dropdown to switch between coins.

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin_secret |
| Grafana | http://localhost:3000 | admin / grafana_secret |
| Kafka UI | http://localhost:8090 | — |
| Schema Registry | http://localhost:8081 | — |
| ClickHouse HTTP | http://localhost:8123 | default / (empty) |

---

## How the pieces fit together

**Ingestion (DAG 1, every minute)**
`coingecko.py` fetches prices for 10 assets — BTC, ETH, SOL, ADA, DOGE, XRP, DOT, LINK, LTC, AVAX. Each record gets an `ingested_at` timestamp and is serialised as Avro using the Schema Registry. `kafka_producer.py` publishes everything to the `crypto_prices` topic across 3 partitions.

**Storage (ClickHouse)**
There are three objects in `default`:
- `crypto_raw_kafka` — a Kafka Engine table that continuously consumes the topic using `AvroConfluent` format
- `crypto_raw_mv` — a Materialized View that pipes rows from the engine into storage
- `crypto_raw` — the actual MergeTree table with native `Float64`/`Int32` types where everything lands

**Transformation (DAG 2, every 4 hours)**
Airflow spins up the `dbt` container via DockerOperator and runs `dbt build --target prod`. Three models get built:
- `stg_crypto_prices` — a view that cleans column names and types
- `latest_prices` — uses ClickHouse's `argMax()` to get the most recent snapshot per asset
- `ohlc_per_minute` — aggregates open/high/low/close per asset per minute

dbt runs 39 tests on every build. If any fail, the DAG task fails and you'll see it in Airflow.

After dbt finishes, `grafana.py` posts an annotation to mark the build time on any time-series panels.

---

## Running dbt locally

If you want to iterate on models without going through Airflow each time:

```bash
cd transformation
pip install -r requirements.txt

export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=

dbt deps
dbt build          # runs models + all tests
dbt docs generate
dbt docs serve     # opens docs in your browser
```

The `dev` profile points at `localhost:8123`, so you need the ClickHouse container running. The `prod` profile (what Airflow uses) points at `clickhouse:8123` inside Docker's network.

---

## Tearing down

Stop containers but keep data:
```bash
docker compose down
```

Stop and wipe all volumes (ClickHouse data, Kafka offsets, Grafana state, Airflow DB):
```bash
docker compose down -v
```

Full reset — removes volumes and locally built images too:
```bash
docker compose down -v --rmi local
docker compose up -d --build
```
