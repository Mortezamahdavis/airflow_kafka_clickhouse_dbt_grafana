"""Grafana HTTP API helper — posts annotations to mark pipeline events."""

from __future__ import annotations

import logging
import os
import time

import requests

log = logging.getLogger(__name__)

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
GRAFANA_USER = os.environ["GRAFANA_USER"]
GRAFANA_PASSWORD = os.environ["GRAFANA_PASSWORD"]


def post_annotation(text: str, tags: list[str] | None = None) -> None:
    """
    Post a point annotation to Grafana. Shows as a vertical line on all
    time-series panels — useful for marking when a dbt run completed.
    """
    payload = {
        "time": int(time.time() * 1000),  # ms epoch
        "text": text,
        "tags": tags or [],
    }
    response = requests.post(
        f"{GRAFANA_URL}/api/annotations",
        json=payload,
        auth=(GRAFANA_USER, GRAFANA_PASSWORD),
        timeout=5,
    )
    response.raise_for_status()
    log.info("Grafana annotation posted: %s (id=%s)", text, response.json().get("id"))
