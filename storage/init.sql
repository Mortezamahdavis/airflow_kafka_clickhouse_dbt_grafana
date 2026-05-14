-- =============================================================================
--  ClickHouse Initialization Script
--  Objects:
--   1. crypto_raw_kafka  – Kafka Engine (reads Avro-encoded CoinGecko records)
--   2. crypto_raw        – MergeTree    (raw storage)
--   3. crypto_raw_mv     – Materialized View (Kafka → MergeTree)
-- =============================================================================

-- 1. Kafka Engine table — types match the Avro schema registered in Schema Registry
CREATE TABLE IF NOT EXISTS default.crypto_raw_kafka
(
    id                          String,
    symbol                      String,
    name                        String,
    current_price               Nullable(Float64),
    market_cap                  Nullable(Float64),
    market_cap_rank             Nullable(Int32),
    total_volume                Nullable(Float64),
    high_24h                    Nullable(Float64),
    low_24h                     Nullable(Float64),
    price_change_24h            Nullable(Float64),
    price_change_percentage_24h Nullable(Float64),
    circulating_supply          Nullable(Float64),
    total_supply                Nullable(Float64),
    max_supply                  Nullable(Float64),
    ingested_at                 String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'broker1:9092,broker2:9092,broker3:9092',
    kafka_topic_list           = 'crypto_prices',
    kafka_group_name           = 'clickhouse-consumer',
    kafka_format               = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers        = 3,
    kafka_skip_broken_messages = 10;

-- 2. MergeTree raw storage — numeric fields stored as their native types
CREATE TABLE IF NOT EXISTS default.crypto_raw
(
    id                          String,
    symbol                      String,
    name                        String,
    current_price               Nullable(Float64),
    market_cap                  Nullable(Float64),
    market_cap_rank             Nullable(Int32),
    total_volume                Nullable(Float64),
    high_24h                    Nullable(Float64),
    low_24h                     Nullable(Float64),
    price_change_24h            Nullable(Float64),
    price_change_percentage_24h Nullable(Float64),
    circulating_supply          Nullable(Float64),
    total_supply                Nullable(Float64),
    max_supply                  Nullable(Float64),
    ingested_at                 DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (id, ingested_at)
TTL toDateTime(ingested_at) + INTERVAL 30 DAY;

-- 3. Materialized View: pipes Kafka → MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS default.crypto_raw_mv
TO default.crypto_raw
AS
SELECT
    id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    circulating_supply,
    total_supply,
    max_supply,
    parseDateTimeBestEffortOrZero(ingested_at) AS ingested_at
FROM default.crypto_raw_kafka;
