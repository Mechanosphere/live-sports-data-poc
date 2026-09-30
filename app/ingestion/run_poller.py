"""Entrypoint for the provider-polling ingestion worker.

Runs as its own long-lived process (separate from the FastAPI API process),
polling registered provider adapters (across all sports) on an interval
and publishing their raw payloads to Kafka. `EventProducer` routes each
raw payload to its sport-specific topic (see `Settings.topic_for_sport`),
so one poller process can fan out to multiple sports' topics.

Usage:
    python -m app.ingestion.run_poller
"""
from __future__ import annotations

import logging
import signal

from app.core.logging_config import configure_logging
from app.ingestion.kafka_producer import EventProducer
from app.ingestion.poller import PollerService
from app.ingestion.providers import build_registered_providers, registered_provider_names

configure_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    producer = EventProducer()
    providers = build_registered_providers()
    service = PollerService(providers=providers, producer=producer)

    def _handle_shutdown(signum, frame) -> None:
        logger.info("Received signal %s, stopping poller...", signum)
        service.stop()

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info("Starting provider poller with providers=%s", list(registered_provider_names()))
    service.run()
    producer.flush()
    logger.info("Poller stopped.")


if __name__ == "__main__":
    main()
