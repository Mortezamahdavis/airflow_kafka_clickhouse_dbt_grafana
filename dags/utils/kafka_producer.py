"""Kafka producer helper — Avro serialization via Schema Registry."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "broker1:9092,broker2:9092,broker3:9092"
KAFKA_TOPIC = "crypto_prices"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Avro schema — must match CoinGecko fields written to ClickHouse.
# Any record missing a required field will be rejected here before reaching Kafka.
CRYPTO_PRICE_SCHEMA = """
{
  "type": "record",
  "name": "CryptoPrice",
  "namespace": "com.crypto.analytics",
  "fields": [
    {"name": "id",                          "type": "string"},
    {"name": "symbol",                      "type": "string"},
    {"name": "name",                        "type": "string"},
    {"name": "current_price",               "type": ["null", "double"], "default": null},
    {"name": "market_cap",                  "type": ["null", "double"], "default": null},
    {"name": "market_cap_rank",             "type": ["null", "int"],    "default": null},
    {"name": "total_volume",                "type": ["null", "double"], "default": null},
    {"name": "high_24h",                    "type": ["null", "double"], "default": null},
    {"name": "low_24h",                     "type": ["null", "double"], "default": null},
    {"name": "price_change_24h",            "type": ["null", "double"], "default": null},
    {"name": "price_change_percentage_24h", "type": ["null", "double"], "default": null},
    {"name": "circulating_supply",          "type": ["null", "double"], "default": null},
    {"name": "total_supply",                "type": ["null", "double"], "default": null},
    {"name": "max_supply",                  "type": ["null", "double"], "default": null},
    {"name": "ingested_at",                 "type": "string"}
  ]
}
"""

# Fields the Avro schema cares about — extras from CoinGecko are dropped.
_SCHEMA_FIELDS = {
    "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
    "total_volume", "high_24h", "low_24h", "price_change_24h",
    "price_change_percentage_24h", "circulating_supply", "total_supply",
    "max_supply", "ingested_at",
}


def _on_delivery(err, msg) -> None:
    """Delivery callback — raises on per-message failures."""
    if err:
        raise KafkaException(f"Delivery failed for key={msg.key()}: {err}")
    log.debug("Delivered %s → partition=%d offset=%d", msg.key(), msg.partition(), msg.offset())


def produce_messages(records: list[dict], topic: str = KAFKA_TOPIC) -> None:
    """
    Serialize each record with Avro (validated against Schema Registry) and
    produce to Kafka. Schema Registry rejects messages that don't conform.
    Each record must have an 'id' key (CoinGecko asset id) used as the message key.
    """
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client,
        CRYPTO_PRICE_SCHEMA,
        lambda obj, _ctx: {k: obj.get(k) for k in _SCHEMA_FIELDS},
    )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": 1})
    ctx = SerializationContext(topic, MessageField.VALUE)

    for record in records:
        producer.produce(
            topic=topic,
            key=record["id"].encode(),
            value=avro_serializer(record, ctx),
            on_delivery=_on_delivery,
        )

    remaining = producer.flush(timeout=30)
    if remaining > 0:
        raise RuntimeError(f"{remaining} message(s) were not delivered within the flush timeout")

    log.info("Produced %d Avro messages to Kafka topic '%s'", len(records), topic)
