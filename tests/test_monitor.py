import uuid

from fastapi.testclient import TestClient

from app.ingestion import canonical
from app.main import app
from app.models.keys import EventType
from app.schemas.football import GameEvent
from app.services import football_repository as repo
from app.services.dynamodb import create_table_if_not_exists

client = TestClient(app)


def test_get_aurora_games_reflects_aurora_store():
    aurora = canonical.get_aurora_store()
    projection = canonical.get_projection()
    aurora.clear()
    projection.clear()
    try:
        aurora.register_game("MONITOR-COMPETITION", "monitor-game-1", team_names=["A", "B"])

        response = client.get("/api/monitor/canonical/aurora")
        assert response.status_code == 200
        body = response.json()
        assert any(
            g["competition_name"] == "MONITOR-COMPETITION" and g["game_id"] == "monitor-game-1"
            for g in body
        )
        match = next(g for g in body if g["game_id"] == "monitor-game-1")
        assert match["team_names"] == ["A", "B"]
    finally:
        aurora.clear()
        projection.clear()


def test_get_projection_games_reflects_projection_store():
    aurora = canonical.get_aurora_store()
    projection = canonical.get_projection()
    aurora.clear()
    projection.clear()
    try:
        projection.register_game("MONITOR-COMPETITION-2", "monitor-game-2")

        response = client.get("/api/monitor/canonical/projection")
        assert response.status_code == 200
        body = response.json()
        assert any(
            g["competition_name"] == "MONITOR-COMPETITION-2" and g["game_id"] == "monitor-game-2"
            for g in body
        )
    finally:
        aurora.clear()
        projection.clear()


def test_get_recent_events_includes_newly_written_event():
    create_table_if_not_exists()
    competition = f"BUNDESLIGA-MONITOR-{uuid.uuid4().hex[:8]}".upper()
    game_id = "monitor-game-event"
    event = GameEvent(
        competition_name=competition,
        game_id=game_id,
        event_type=EventType.GOAL,
        player="Muller",
        team="Bayern",
        minute=10,
    )
    repo.put_item(event.to_item())

    response = client.get("/api/monitor/events/recent?limit=200")
    assert response.status_code == 200
    body = response.json()
    assert any(
        e["competition_name"] == competition and e["game_id"] == game_id and e["player"] == "Muller"
        for e in body
    )


def test_monitor_dashboard_renders_html():
    response = client.get("/api/monitor")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    body = response.text
    assert "Ingestion monitor" in body
    assert "/api/monitor/canonical/aurora" in body
    assert "/api/monitor/canonical/projection" in body
    assert "/api/monitor/events/recent" in body
