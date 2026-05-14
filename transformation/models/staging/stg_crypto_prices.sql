-- Staging: rename columns for the mart layer.
-- No casting needed — ClickHouse already stores native Float64/Int32 types
-- thanks to Avro deserialization at the Kafka Engine level.
{{
    config(
        materialized='view'
    )
}}

select
    id                          as asset_id,
    symbol,
    name,
    current_price               as price_usd,
    price_change_percentage_24h as change_pct_24h,
    total_volume                as volume_usd_24h,
    market_cap                  as market_cap_usd,
    high_24h,
    low_24h,
    circulating_supply,
    ingested_at
from {{ source('crypto', 'crypto_raw') }}
where ingested_at >= now() - interval 1 day
