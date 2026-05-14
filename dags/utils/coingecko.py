"""CoinGecko REST API client — raw fetch only, no type casting."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests

log = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

ASSETS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "dogecoin",
    "ripple",
    "polkadot",
    "chainlink",
    "litecoin",
    "avalanche-2",
]


def fetch_prices() -> list[dict]:
    """
    Fetch market data for ASSETS from CoinGecko.
    Returns the raw API response (list of dicts) with an added ingested_at timestamp.
    All numeric fields are left as-is (no casting).
    """
    response = requests.get(
        COINGECKO_URL,
        params={
            "vs_currency": "usd",
            "ids": ",".join(ASSETS),
            "order": "market_cap_desc",
            "per_page": len(ASSETS),
            "page": 1,
            "sparkline": "false",
        },
        headers={"Accept": "application/json"},
        timeout=10,
    )
    response.raise_for_status()

    ingested_at = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )
    records: list[dict] = response.json()
    for record in records:
        record["ingested_at"] = ingested_at

    log.info("Fetched %d records from CoinGecko", len(records))
    return records
