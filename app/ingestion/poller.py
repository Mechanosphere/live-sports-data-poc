"""Orchestrates polling registered providers and publishing raw payloads.

Since we currently only poll provider APIs (no webhooks), this runs a
simple interval loop: for each registered provider, fetch any new raw
payloads and publish them to Kafka as-is (raw-first ingestion).
Normalization happens downstream, in the consumer, via
`app.ingestion.normalizers` -- the poller never normalizes.
"""
from __future__ import annotations

import logging
import time

from app.core.config import get_settings
from app.ingestion.kafka_producer import EventProducer
from app.ingestion.providers.base import Provider

logger = logging.getLogger(__name__)


class PollerService:
    def __init__(self, providers: list[Provider], producer: EventProducer) -> None:
        self._providers = providers
        self._producer = producer
        self._running = False

    def poll_once(self) -> int:
        """Poll every registered provider once, publish any new raw payloads.

        Returns the number of raw payloads published.
        """
        published = 0
        for provider in self._providers:
            try:
                raw_events = provider.poll()
            except Exception:
                logger.exception("Provider %s poll() failed", provider.name)
                continue
            for raw in raw_events:
                try:
                    key = provider.partition_key(raw)
                    self._producer.publish_raw(provider.name, provider.sport, raw, key)
                    published += 1
                except Exception:
                    logger.exception(
                        "Failed to publish raw payload from provider %s: %r",
                        provider.name,
                        raw,
                    )
        self._producer.flush(timeout=1.0)
        return published

    def run(self) -> None:
        """Blocking loop: poll on an interval until `stop()` is called."""
        settings = get_settings()
        self._running = True
        while self._running:
            self.poll_once()
            time.sleep(settings.provider_poll_interval_seconds)

    def stop(self) -> None:
        self._running = False
