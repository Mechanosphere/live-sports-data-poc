"""Thin wrapper around confluent_kafka.Producer for publishing raw envelopes.

Publishes raw, provider-native payloads to Kafka *before* normalization
(normalization happens downstream in the consumer via
`app.ingestion.normalizers`). This lets us buffer/replay raw data and
evolve normalization logic without re-polling providers.

Messages are keyed by the provider's own partition key (typically
`{competition}#{game_id}`) so Kafka preserves per-game ordering (all events for
a given game land on the same partition and are delivered in publish order
to a single consumer).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from functools import lru_cache, partial
from typing import Any

from confluent_kafka import Producer

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def _delivery_callback(publish_started_at: float, err, msg) -> None:
    if err is not None:
        logger.error("Kafka delivery failed for %s: %s", msg.key(), err)
        return
    # t3 -> t4: time from producer.produce() call to broker ack. Isolates
    # Kafka client/network/broker latency from everything upstream (polling,
    # normalization) and downstream (consumer, DynamoDB write).
    produce_latency_ms = (time.monotonic() - publish_started_at) * 1000
    logger.info(
        "Kafka produce latency: %.1fms (topic=%s partition=%s offset=%s)",
        produce_latency_ms,
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


class EventProducer:
    def __init__(self, producer: Producer | None = None, topic: str | None = None) -> None:
        """`topic`, if given, pins every publish to that single topic
        (used by tests to isolate runs). If omitted, each message is routed
        to its sport-specific topic (see `Settings.topic_for_sport`) so
        football and basketball raw events land on independent topics.
        """
        settings = get_settings()
        self._producer = producer or Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self._topic = topic

    def publish_raw(
        self, provider_name: str, sport: str, raw: dict[str, Any], key: str
    ) -> None:
        """Publish a raw, un-normalized provider payload.

        `key` should be the provider's partition key (see
        `Provider.partition_key`) so all events for the same game land on
        the same Kafka partition and stay in publish order.
        """
        topic = self._topic or get_settings().topic_for_sport(sport)
        value = json.dumps(
            {
                "provider_name": provider_name,
                "sport": sport,
                "raw": raw,
                # t1: when our poller saw/published this raw payload --
                # the earliest point in our system's own clock, distinct
                # from provider_emitted_at (t0, the provider's own clock).
                "polled_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=partial(_delivery_callback, time.monotonic()),
        )
        # Non-blocking: serve delivery callbacks without waiting for acks.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until all outstanding messages are delivered (or timeout)."""
        return self._producer.flush(timeout)


@lru_cache
def get_event_producer() -> EventProducer:
    return EventProducer()
