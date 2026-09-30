import uuid

import pytest

from app.core.config import get_settings
from app.models.keys import EventType
from app.schemas.football import Game, GameEvent, GameTeamIndex, Competition, Team
from app.services import football_repository as repo
from app.services.dynamodb import create_table_if_not_exists, get_dynamodb_client

# Unique per test run so repeated runs against a persisted DynamoDB Local
# volume don't see stale data from previous runs.
COMPETITION = f"BUNDESLIGA-TEST-{uuid.uuid4().hex[:8]}"


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


def test_competition_put_and_get():
    competition = Competition(competition_name=COMPETITION, country="DE", current_season="2025/2026")
    repo.put_item(competition.to_item())
    assert repo.get_competition(COMPETITION) == competition


def test_game_put_and_get():
    game = Game(
        competition_name=COMPETITION,
        game_id="game-1",
        kickoff="15:00",
        team_name_1="Bayern",
        team_name_2="Dortmund",
        game_date="2025-11-20",
        score="1-1",
    )
    repo.put_item(game.to_item())
    assert repo.get_game(COMPETITION, "game-1") == game


def test_events_queries():
    kickoff = GameEvent(
        competition_name=COMPETITION,
        game_id="game-2",
        event_type=EventType.KICKOFF,
        player="",
        team="Bayern",
        minute=0,
    )
    goal1 = GameEvent(
        competition_name=COMPETITION,
        game_id="game-2",
        event_type=EventType.GOAL,
        player="Muller",
        team="Bayern",
        minute=10,
    )
    goal2 = GameEvent(
        competition_name=COMPETITION,
        game_id="game-2",
        event_type=EventType.GOAL,
        player="Reus",
        team="Dortmund",
        minute=20,
    )
    redcard = GameEvent(
        competition_name=COMPETITION,
        game_id="game-2",
        event_type=EventType.REDCARD,
        player="Muller",
        team="Bayern",
        minute=30,
    )
    for event in (kickoff, goal1, goal2, redcard):
        repo.put_item(event.to_item())

    # SK = GAMEID#<id>#EVENT#<type>#<uuidv7>, so ordering groups by event
    # type alphabetically first, then chronologically within each type.
    all_events = repo.get_game_events(COMPETITION, "game-2")
    assert len(all_events) == 4
    assert [e.event_type for e in all_events] == [
        EventType.GOAL,
        EventType.GOAL,
        EventType.KICKOFF,
        EventType.REDCARD,
    ]

    goals = repo.get_events_by_type(COMPETITION, "game-2", EventType.GOAL)
    assert [g.player for g in goals] == ["Muller", "Reus"]

    latest_goal = repo.get_latest_event_by_type(COMPETITION, "game-2", EventType.GOAL)
    assert latest_goal.player == "Reus"


def test_get_top_events_returns_most_recent_first_across_types():
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = [
        GameEvent(
            competition_name=COMPETITION,
            game_id="game-top",
            event_type=event_type,
            player=f"Player{i}",
            team="Bayern",
            minute=i,
            ingested_at=base + timedelta(seconds=i),
        )
        for i, event_type in enumerate(
            [
                EventType.KICKOFF,
                EventType.GOAL,
                EventType.REDCARD,
                EventType.GOAL,
                EventType.HALFTIME,
                EventType.FULLTIME,
            ]
        )
    ]
    for event in events:
        repo.put_item(event.to_item())

    top_3 = repo.get_top_events(COMPETITION, "game-top", limit=3)
    assert [e.player for e in top_3] == ["Player5", "Player4", "Player3"]

    top_all = repo.get_top_events(COMPETITION, "game-top", limit=10)
    assert len(top_all) == 6
    assert top_all[0].player == "Player5"
    assert top_all[-1].player == "Player0"


def test_follow_team_access_patterns():
    link = GameTeamIndex(
        competition_name=COMPETITION,
        team_name="Bayern",
        opponent="Dortmund",
        home_away="HOME",
        game_id="game-3",
        game_date="2025-11-21",
        kickoff="18:30",
    )
    repo.put_item(link.to_item())

    goal = GameEvent(
        competition_name=COMPETITION,
        game_id="game-3",
        event_type=EventType.GOAL,
        player="Kane",
        team="Bayern",
        minute=5,
    )
    repo.put_item(goal.to_item())

    games = repo.get_games_for_team("Bayern")
    assert any(g.game_id == "game-3" for g in games)

    events = repo.get_events_for_team("Bayern", EventType.GOAL)
    assert any(e.player == "Kane" for e in events)
