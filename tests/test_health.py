import uuid

from fastapi.testclient import TestClient

from app.models.keys import EventType
from app.schemas.football import GameEvent
from app.services import football_repository as repo
from app.services.dynamodb import create_table_if_not_exists
from app.main import app

client = TestClient(app)


def test_health_check():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_root():
    response = client.get("/")
    assert response.status_code == 200


def test_get_top_game_events_endpoint():
    create_table_if_not_exists()

    competition = f"BUNDESLIGA-API-TEST-{uuid.uuid4().hex[:8]}".upper()
    events = [
        GameEvent(
            competition_name=competition,
            game_id="game-api",
            event_type=EventType.KICKOFF,
            player="",
            team="Bayern",
            minute=0,
        ),
        GameEvent(
            competition_name=competition,
            game_id="game-api",
            event_type=EventType.GOAL,
            player="Muller",
            team="Bayern",
            minute=23,
        ),
    ]
    for event in events:
        repo.put_item(event.to_item())

    response = client.get(f"/api/competitions/{competition}/games/game-api/events/top")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert {e["player"] for e in body} == {"", "Muller"}


def test_get_top_game_events_endpoint_404_when_no_events():
    competition = f"BUNDESLIGA-API-TEST-EMPTY-{uuid.uuid4().hex[:8]}"
    response = client.get(f"/api/competitions/{competition}/games/no-such-game/events/top")
    assert response.status_code == 404


def test_get_game_events_endpoint():
    create_table_if_not_exists()

    competition = f"BUNDESLIGA-API-TEST-{uuid.uuid4().hex[:8]}".upper()
    events = [
        GameEvent(
            competition_name=competition,
            game_id="game-all",
            event_type=EventType.KICKOFF,
            player="",
            team="Bayern",
            minute=0,
        ),
        GameEvent(
            competition_name=competition,
            game_id="game-all",
            event_type=EventType.GOAL,
            player="Muller",
            team="Bayern",
            minute=23,
        ),
    ]
    for event in events:
        repo.put_item(event.to_item())

    response = client.get(f"/api/competitions/{competition}/games/game-all/events")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert {e["player"] for e in body} == {"", "Muller"}


def test_get_game_events_endpoint_404_when_no_events():
    competition = f"BUNDESLIGA-API-TEST-EMPTY-{uuid.uuid4().hex[:8]}"
    response = client.get(f"/api/competitions/{competition}/games/no-such-game/events")
    assert response.status_code == 404


def test_monitor_page_renders_html_with_correct_api_url():
    competition = "BUNDESLIGA"
    game_id = "demo-1"
    response = client.get(f"/api/competitions/{competition}/games/{game_id}/monitor")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    body = response.text
    assert f"{competition} / {game_id}" in body
    assert f"/api/competitions/{competition}/games/{game_id}/events/top?limit=10" in body
    assert "setInterval(refresh, 5000)" in body
