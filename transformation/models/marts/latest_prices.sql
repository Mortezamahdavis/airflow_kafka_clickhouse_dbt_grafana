-- Latest snapshot per asset (stat panels / ticker in Grafana)
{{
    config(
        materialized='table'
    )
}}

select
    asset_id,
    symbol,
    name,
    argMax(price_usd,       ingested_at) as latest_price_usd,
    argMax(change_pct_24h,  ingested_at) as change_pct_24h,
    argMax(high_24h,        ingested_at) as high_24h,
    argMax(low_24h,         ingested_at) as low_24h,
    argMax(volume_usd_24h,  ingested_at) as volume_usd_24h,
    argMax(market_cap_usd,  ingested_at) as market_cap_usd,
    max(ingested_at)                     as last_updated
from {{ ref('stg_crypto_prices') }}
group by asset_id, symbol, name
