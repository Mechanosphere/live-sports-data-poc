"""Consumes raw provider payloads from Kafka, normalizes, and persists to DynamoDB.

Normalization happens here (not at publish time) so raw payloads can be
buffered/replayed and normalization logic can evolve independently of
polling. Writes are still idempotent: `app.ingestion.normalizers.normalize`
derives a deterministic (uuid5) `event_id` from the raw payload, so the
DynamoDB item's key (PK/SK) is fully determined by the message contents.
Reprocessing the same message (e.g. after a consumer crash before the
offset was committed) results in the same `put_item` overwrite, not a
duplicate event.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer

from app.core.config import get_settings
from app.ingestion import canonical
from app.ingestion.normalizers import normalize
from app.models.keys import SPORT_FOOTBALL
from app.schemas.football import GameEvent
from app.services import football_repository as repo

logger = logging.getLogger(__name__)


def _deserialize_envelope(raw_value: bytes) -> tuple[str, dict[str, Any]]:
    """Parse a raw Kafka envelope, returning (provider_name, raw_payload)."""
    envelope: dict[str, Any] = json.loads(raw_value.decode("utf-8"))
    return envelope["provider_name"], envelope["raw"]


def _log_latencies(event: GameEvent, received_at: datetime, persisted_at: datetime) -> None:
    # t4 -> t5: time from publish (ingested_at, our best proxy for when the
    # message became available) to this consumer picking it up. A growing
    # value here is the app-level signal that maps to Kafka consumer lag.
    pickup_ms = (received_at - event.ingested_at).total_seconds() * 1000
    # t5 -> t6: time to write the event to DynamoDB.
    persist_ms = (persisted_at - received_at).total_seconds() * 1000
    # t0 -> t6: full end-to-end latency against the provider's own event
    # time -- the number to compare against the 30-45s stream delay budget.
    end_to_end_ms = (persisted_at - event.provider_emitted_at).total_seconds() * 1000
    logger.info(
        "Event latency (event_id=%s type=%s): pickup=%.1fms persist=%.1fms end_to_end=%.1fms",
        event.event_id,
        event.event_type.value,
        pickup_ms,
        persist_ms,
        end_to_end_ms,
    )


class EventConsumer:
    def __init__(
        self,
        consumer: Consumer | None = None,
        topic: str | None = None,
        sport: str = SPORT_FOOTBALL,
        group: str | None = None,
        dead_letter_producer: Any | None = None,
    ) -> None:
        """`topic`/`group`, if given, pin this consumer to explicit values
        (used by tests to isolate runs). Otherwise both default to the
        given `sport`'s topic/consumer group (see
        `Settings.topic_for_sport`/`consumer_group_for_sport`), so a
        consumer process only ever handles one sport's raw events.

        `dead_letter_producer`, if given, overrides the producer used to
        republish events rejected by a strict canonical-data miss (see
        `poll_once`) -- primarily for tests. Defaults to a real Kafka
        `EventProducer` pinned to `Settings.kafka_topic_dead_letter`.
        """
        settings = get_settings()
        self._topic = topic or settings.topic_for_sport(sport)
        self._consumer = consumer or Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": group or settings.consumer_group_for_sport(sport),
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        self._consumer.subscribe([self._topic])
        self._running = False
        # The dead-letter producer is only ever used on a strict-mode
        # canonical miss (see `poll_once`), which never happens in the
        # default permissive mode. Constructing a real Kafka `EventProducer`
        # eagerly here would open an idle producer connection per consumer
        # process for nothing, so it's built lazily on first use instead
        # (an explicitly injected producer, e.g. from tests, is still kept).
        self._dead_letter_producer = dead_letter_producer
        self._dead_letter_topic = settings.kafka_topic_dead_letter

    def _get_dead_letter_producer(self) -> Any:
        """Return the dead-letter producer, constructing it lazily on first use."""
        if self._dead_letter_producer is None:
            from app.ingestion.kafka_producer import EventProducer

            self._dead_letter_producer = EventProducer(topic=self._dead_letter_topic)
        return self._dead_letter_producer

    def poll_once(self, timeout: float = 1.0) -> GameEvent | None:
        """Poll for a single message, persist it, and return it (or None).

        If `Settings.canonical_strict_mode` is enabled and the event's game
        isn't in the canonical projection, `canonical.check_game_exists`
        raises `CanonicalDataMissing`; the raw envelope is republished
        unmodified to the dead-letter topic (`Settings.kafka_topic_dead_letter`)
        instead of being persisted, and this returns `None` (offset still
        advances -- the event isn't retried in place, only via the DLQ).
        """
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            logger.error("Kafka consumer error: %s", msg.error())
            return None
        received_at = datetime.now(timezone.utc)
        provider_name, raw = _deserialize_envelope(msg.value())
        event = normalize(provider_name, raw)
        # Canonical competition/game data is owned by an external system; this is
        # a validation/observability check, not a creation step. See
        # app.ingestion.canonical for the permissive/strict miss policy.
        try:
            canonical.check_game_exists(event.competition_name, event.game_id)
        except canonical.CanonicalDataMissing:
            logger.error(
                "Dead-lettering event_id=%s (competition=%s game_id=%s): canonical data missing",
                event.event_id,
                event.competition_name,
                event.game_id,
            )
            raw_key = msg.key()
            key = raw_key.decode("utf-8") if raw_key else event.game_id
            dead_letter_producer = self._get_dead_letter_producer()
            dead_letter_producer.publish_raw(
                provider_name, event.sport, raw, key=key
            )
            dead_letter_producer.flush(timeout=5.0)
            return None
        repo.put_item(event.to_item())
        _log_latencies(event, received_at, datetime.now(timezone.utc))
        return event

    def run(self) -> None:
        """Blocking loop: poll and persist events until `stop()` is called."""
        self._running = True
        try:
            while self._running:
                self.poll_once(timeout=1.0)
        finally:
            self._consumer.close()

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self._consumer.close()
