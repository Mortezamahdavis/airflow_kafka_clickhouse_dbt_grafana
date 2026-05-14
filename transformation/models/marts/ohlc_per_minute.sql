-- OHLC per asset per minute (candlestick charts in Grafana)
{{
    config(
        materialized='table'
    )
}}

select
    asset_id,
    symbol,
    toStartOfMinute(ingested_at)             as minute,
    argMin(price_usd, ingested_at)           as open,
    max(price_usd)                           as high,
    min(price_usd)                           as low,
    argMax(price_usd, ingested_at)           as close,
    count()                                  as ticks
from {{ ref('stg_crypto_prices') }}
group by asset_id, symbol, minute
order by asset_id, minute
