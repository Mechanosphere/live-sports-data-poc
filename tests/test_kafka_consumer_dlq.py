"""Unit tests for EventConsumer's strict-mode canonical-miss -> DLQ path.

Uses fake Kafka Consumer/producer stand-ins so these run without a live
Kafka broker (unlike tests/test_ingestion_pipeline.py's integration tests).
"""
from __future__ import annotations

import json

import pytest

from app.core.config import get_settings
from app.ingestion import canonical
from app.ingestion.kafka_consumer import EventConsumer
from app.services.dynamodb import create_table_if_not_exists


@pytest.fixture(scope="module", autouse=True)
def table():
    create_table_if_not_exists()


class _FakeMessage:
    def __init__(self, value: bytes, key: bytes | None = None) -> None:
        self._value = value
        self._key = key

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def error(self):
        return None


class _FakeConsumer:
    def __init__(self, messages: list[_FakeMessage]) -> None:
        self._messages = list(messages)
        self.subscribed: list[str] = []

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float):
        if not self._messages:
            return None
        return self._messages.pop(0)

    def close(self) -> None:
        pass


class _FakeDeadLetterProducer:
    def __init__(self) -> None:
        self.published: list[dict] = []

    def publish_raw(self, provider_name, sport, raw, key) -> None:
        self.published.append(
            {"provider_name": provider_name, "sport": sport, "raw": raw, "key": key}
        )

    def flush(self, timeout: float = 10.0) -> int:
        return 0


def _make_message(competition: str, game_id: str) -> _FakeMessage:
    raw = {
        "competition_name": competition,
        "game_id": game_id,
        "event_type": "GOAL",
        "player": "Muller",
        "team": "Bayern",
        "minute": 10,
    }
    envelope = {"provider_name": "runner", "sport": "FOOTBALL", "raw": raw}
    return _FakeMessage(json.dumps(envelope).encode("utf-8"), key=game_id.encode("utf-8"))


def test_poll_once_dead_letters_event_in_strict_mode_on_canonical_miss(monkeypatch):
    settings = get_settings()
    projection = canonical.get_projection()
    projection.clear()
    original_strict = settings.canonical_strict_mode
    settings.canonical_strict_mode = True
    dlq = _FakeDeadLetterProducer()
    fake_consumer = _FakeConsumer([_make_message("UNKNOWN-COMPETITION", "unknown-game")])
    consumer = EventConsumer(
        consumer=fake_consumer, topic="football.events", dead_letter_producer=dlq
    )
    try:
        result = consumer.poll_once()
        assert result is None
        assert len(dlq.published) == 1
        assert dlq.published[0]["raw"]["game_id"] == "unknown-game"
        assert dlq.published[0]["key"] == "unknown-game"
    finally:
        settings.canonical_strict_mode = original_strict
        projection.clear()


def test_poll_once_persists_normally_when_canonical_data_present(monkeypatch):
    settings = get_settings()
    projection = canonical.get_projection()
    projection.clear()
    original_strict = settings.canonical_strict_mode
    settings.canonical_strict_mode = True
    projection.register_game("KNOWN-COMPETITION", "known-game")
    dlq = _FakeDeadLetterProducer()
    fake_consumer = _FakeConsumer([_make_message("KNOWN-COMPETITION", "known-game")])
    consumer = EventConsumer(
        consumer=fake_consumer, topic="football.events", dead_letter_producer=dlq
    )
    try:
        result = consumer.poll_once()
        assert result is not None
        assert result.game_id == "known-game"
        assert dlq.published == []
    finally:
        settings.canonical_strict_mode = original_strict
        projection.clear()
