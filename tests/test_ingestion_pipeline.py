"""End-to-end integration test: provider -> normalize -> Kafka -> DynamoDB.

Requires local Kafka (docker-compose `kafka` service) and DynamoDB Local
(docker-compose `dynamodb-local` service) to be running.
"""
from __future__ import annotations

import json
import threading
import uuid

import pytest
from fastapi.testclient import TestClient

from app.api.routes import runner as runner_routes
from app.core.config import get_settings
from app.ingestion import canonical
from app.ingestion.kafka_consumer import EventConsumer
from app.ingestion.kafka_producer import EventProducer
from app.ingestion.normalizers import normalize
from app.ingestion.poller import PollerService
from app.ingestion.providers.livescorecore import LiveScoreCoreProvider
from app.ingestion.providers.sportsfeedx import SportsFeedXProvider
from app.main import app
from app.models.keys import EventType
from app.services import football_repository as repo
from app.services.dynamodb import create_table_if_not_exists, get_dynamodb_client

# Unique per test run so repeated runs against persisted local services
# don't collide with (or get confused by) data from previous runs.
RUN_ID = uuid.uuid4().hex[:8]
TOPIC = f"football.events.test.{RUN_ID}"
GROUP = f"football-ingestion-consumer-test.{RUN_ID}"


@pytest.fixture(scope="module", autouse=True)
def table():
    settings = get_settings()
    client = get_dynamodb_client()
    existing = client.list_tables().get("TableNames", [])
    if settings.dynamodb_table_name in existing:
        client.delete_table(TableName=settings.dynamodb_table_name)
        client.get_waiter("table_not_exists").wait(
            TableName=settings.dynamodb_table_name
        )
    create_table_if_not_exists()


def _drain_consumer(consumer: EventConsumer, expected_count: int, timeout_per_poll: float = 5.0):
    events = []
    for _ in range(expected_count):
        event = consumer.poll_once(timeout=timeout_per_poll)
        if event is not None:
            events.append(event)
    return events


def test_producer_to_consumer_to_dynamodb():
    producer = EventProducer(topic=TOPIC)
    consumer = EventConsumer(topic=TOPIC)
    try:
        competition = f"BUNDESLIGA-PIPELINE-{RUN_ID}"
        game_id = f"game-pipeline-{RUN_ID}"

        raw = {
            "matchId": game_id,
            "competition": competition,
            "timestamp": "2026-09-09T14:32:10Z",
            "incident": {
                "type": "GOAL",
                "minute": "10",
                "team": {"id": "T-1", "name": "Bayern"},
                "player": {"id": "P-1", "name": "Muller"},
            },
        }
        expected_event = normalize("sportsfeedx", raw)
        key = f"{competition.upper()}#{game_id}"
        producer.publish_raw("sportsfeedx", expected_event.sport, raw, key)
        producer.flush()

        [consumed] = _drain_consumer(consumer, expected_count=1)
        assert consumed.event_id == expected_event.event_id
        assert consumed.event_type == EventType.GOAL

        # Verify it actually landed in DynamoDB with the same event id.
        # (normalize() upper-cases competition_name, so query with the same casing)
        stored = repo.get_events_by_type(competition.upper(), game_id, EventType.GOAL)
        assert len(stored) == 1
        assert stored[0].event_id == expected_event.event_id
        assert stored[0].player == "Muller"
    finally:
        consumer.close()
        producer.flush()


def test_poller_service_publishes_all_provider_events():
    producer = EventProducer(topic=TOPIC)
    consumer = EventConsumer(topic=TOPIC)
    try:
        providers = [SportsFeedXProvider(), LiveScoreCoreProvider()]
        service = PollerService(providers=providers, producer=producer)

        total_published = 0
        # SportsFeedX has 6 scripted events, LiveScoreCore has 5.
        for _ in range(6):
            total_published += service.poll_once()
        assert total_published == 11

        consumed = _drain_consumer(consumer, expected_count=11)
        assert len(consumed) == 11
        # Consumer wrote every event to DynamoDB idempotently; spot-check one
        # game's events are queryable.
        sfx_events = repo.get_game_events("BUNDESLIGA", "sfx-8841")
        assert len(sfx_events) == 6
    finally:
        consumer.close()
        producer.flush()


def test_runner_endpoint_publishes_scripted_events(monkeypatch):
    producer = EventProducer(topic=TOPIC)
    consumer = EventConsumer(topic=TOPIC)
    # Runner endpoint normally publishes via the process-wide, default-topic
    # producer; redirect it to our isolated test topic so this test doesn't
    # collide with other tests/consumers on the real topic.
    monkeypatch.setattr(runner_routes, "get_event_producer", lambda: producer)
    client = TestClient(app)
    try:
        competition = f"BUNDESLIGA-RUNNER-{RUN_ID}"
        game_id = f"game-runner-{RUN_ID}"

        response = client.post(
            f"/api/runner/competitions/{competition}/games/{game_id}/events",
            json={"delay_seconds": 0},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["competition_name"] == competition.upper()
        assert body["game_id"] == game_id
        assert body["events_scheduled"] == 6
        run_id = body["run_id"]

        # TestClient runs BackgroundTasks synchronously before returning, so
        # all 6 scripted events are already published by this point.
        consumed = _drain_consumer(consumer, expected_count=6)
        assert [e.event_type for e in consumed] == [
            EventType.KICKOFF,
            EventType.GOAL,
            EventType.REDCARD,
            EventType.HALFTIME,
            EventType.GOAL,
            EventType.FULLTIME,
        ]

        stored = repo.get_game_events(competition.upper(), game_id)
        assert len(stored) == 6

        status_response = client.get(f"/api/runner/runs/{run_id}")
        assert status_response.status_code == 200
        status_body = status_response.json()
        assert status_body["status"] == "completed"
        assert status_body["events_published"] == 6

        # Stopping an already-completed run is a no-op that just returns
        # its (unchanged) final status.
        stop_response = client.post(f"/api/runner/runs/{run_id}/stop")
        assert stop_response.status_code == 200
        assert stop_response.json()["status"] == "completed"
    finally:
        consumer.close()
        producer.flush()


def test_runner_stop_endpoint_halts_run_early(monkeypatch):
    producer = EventProducer(topic=TOPIC)
    consumer = EventConsumer(topic=TOPIC)
    monkeypatch.setattr(runner_routes, "get_event_producer", lambda: producer)
    client = TestClient(app)
    try:
        competition = f"BUNDESLIGA-RUNNER-STOP-{RUN_ID}"
        game_id = f"game-runner-stop-{RUN_ID}"

        handle = runner_routes._runner_service.create_run(
            "FOOTBALL", competition.upper(), game_id, delay_seconds=5.0
        )
        run_id = handle.state.run_id

        thread = threading.Thread(
            target=runner_routes._runner_service.publish_script,
            args=(handle, "Bayern", "Dortmund"),
        )
        thread.start()

        # Let the first event publish, then stop before the second.
        [first] = _drain_consumer(consumer, expected_count=1)
        assert first.event_type == EventType.KICKOFF

        stop_response = client.post(f"/api/runner/runs/{run_id}/stop")
        assert stop_response.status_code == 200
        thread.join(timeout=10)

        status_response = client.get(f"/api/runner/runs/{run_id}")
        assert status_response.status_code == 200
        status_body = status_response.json()
        assert status_body["status"] == "stopped"
        assert status_body["events_published"] == 1
    finally:
        consumer.close()
        producer.flush()


def test_strict_mode_dead_letters_unknown_game_over_live_kafka():
    """Live-broker version of tests/test_kafka_consumer_dlq.py: real
    EventProducer/EventConsumer (not fakes) round-tripping through the
    docker-compose Kafka broker, verifying a canonical miss in strict mode
    actually lands on the real dead-letter topic and is never persisted."""
    dlq_topic = f"ingestion.dead-letter.test.{RUN_ID}"
    settings = get_settings()
    original_strict = settings.canonical_strict_mode
    original_dlq_topic = settings.kafka_topic_dead_letter
    settings.canonical_strict_mode = True
    settings.kafka_topic_dead_letter = dlq_topic

    projection = canonical.get_projection()
    projection.clear()

    producer = EventProducer(topic=TOPIC)
    consumer = EventConsumer(topic=TOPIC)
    dlq_consumer = EventConsumer(topic=dlq_topic, group=f"dlq-test.{RUN_ID}")
    try:
        competition = f"UNKNOWN-COMPETITION-{RUN_ID}"
        game_id = f"unknown-game-{RUN_ID}"
        raw = {
            "matchId": game_id,
            "competition": competition,
            "timestamp": "2026-09-09T14:32:10Z",
            "incident": {
                "type": "GOAL",
                "minute": "10",
                "team": {"id": "T-1", "name": "Bayern"},
                "player": {"id": "P-1", "name": "Muller"},
            },
        }
        key = f"{competition.upper()}#{game_id}"
        producer.publish_raw("sportsfeedx", "FOOTBALL", raw, key)
        producer.flush()

        # Strict-mode miss: never persisted, poll_once returns None.
        result = consumer.poll_once(timeout=10.0)
        assert result is None
        assert repo.get_game_events(competition.upper(), game_id) == []

        # ...but republished onto the real dead-letter topic, raw and intact.
        # The DLQ topic is auto-created on first publish, so the consumer's
        # earlier subscribe may race it (transient "unknown topic" errors) --
        # poll in a short retry loop rather than a single call.
        dlq_msg = None
        for _ in range(20):
            candidate = dlq_consumer._consumer.poll(1.0)
            if candidate is not None and candidate.error() is None:
                dlq_msg = candidate
                break
        assert dlq_msg is not None
        dlq_envelope = json.loads(dlq_msg.value().decode("utf-8"))
        assert dlq_envelope["raw"]["matchId"] == game_id
        assert dlq_envelope["raw"]["competition"] == competition
    finally:
        settings.canonical_strict_mode = original_strict
        settings.kafka_topic_dead_letter = original_dlq_topic
        projection.clear()
        consumer.close()
        dlq_consumer.close()
        producer.flush()
