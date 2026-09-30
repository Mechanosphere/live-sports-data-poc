"""Entrypoint for the Kafka -> DynamoDB ingestion consumer worker.

Runs as its own long-lived process (separate from the FastAPI API process
and from the poller), consuming raw provider events for a single sport
from that sport's Kafka topic, normalizing them, and persisting to
DynamoDB via the football repository. Run one process per sport (each
with its own topic/consumer group -- see `Settings.topic_for_sport`) so
sports scale and deploy independently.

Usage:
    python -m app.ingestion.run_consumer [--sport football|basketball]
"""
from __future__ import annotations

import argparse
import logging
import signal

from app.core.config import get_settings
from app.core.logging_config import configure_logging
from app.ingestion import canonical
from app.ingestion.kafka_consumer import EventConsumer
from app.models.keys import SPORT_BASKETBALL, SPORT_FOOTBALL
from app.services.dynamodb import create_table_if_not_exists

configure_logging()
logger = logging.getLogger(__name__)

_SPORT_CHOICES = {"football": SPORT_FOOTBALL, "basketball": SPORT_BASKETBALL}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sport",
        choices=sorted(_SPORT_CHOICES),
        default="football",
        help="Sport whose Kafka topic/consumer group this process consumes (default: football).",
    )
    args = parser.parse_args()
    sport = _SPORT_CHOICES[args.sport]

    create_table_if_not_exists()
    settings = get_settings()
    if settings.canonical_backend != "postgres":
        # Mocks the external canonical-data system (Aurora Postgres, in the
        # target design) until it exists for real. Seeds Aurora + does an
        # immediate sync into the local projection. With the real postgres
        # backend, Aurora is populated by the external system instead --
        # only the periodic sync below is needed.
        canonical.seed_mock_canonical_data()
    # Keeps re-syncing from the source of truth (mock or real Aurora,
    # depending on `canonical_backend`) into the local projection on an
    # interval -- see app.ingestion.canonical's module docstring for the
    # two-tier design.
    canonical.start_periodic_sync(settings.canonical_sync_interval_seconds)
    consumer = EventConsumer(sport=sport)

    def _handle_shutdown(signum, frame) -> None:
        logger.info("Received signal %s, stopping consumer...", signum)
        consumer.stop()

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info("Starting %s event consumer...", args.sport)
    try:
        consumer.run()
    finally:
        canonical.stop_periodic_sync()
    logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()
