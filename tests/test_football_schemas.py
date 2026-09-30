from datetime import datetime, timezone

from app.models.keys import EventType
from app.schemas.football import Game, GameEvent, GameTeamIndex, Competition, Team


def test_competition_round_trip():
    competition = Competition(competition_name="BUNDESLIGA", country="DE", current_season="2025/2026")
    item = competition.to_item()
    assert item["PK"] == "SPORT#FOOTBALL#BUNDESLIGA"
    assert item["SK"] == "METADATA"
    assert Competition.from_item(item) == competition


def test_game_round_trip():
    game = Game(
        competition_name="BUNDESLIGA",
        game_id="game-1",
        kickoff="15:00",
        team_name_1="Bayern",
        team_name_2="Dortmund",
        game_date="2025-11-20",
        score="1-1",
    )
    item = game.to_item()
    assert item["SK"] == "GAMEID#game-1#METADATA"
    assert Game.from_item(item, "BUNDESLIGA") == game


def test_game_event_to_item_sets_gsi1_and_sk():
    event = GameEvent(
        competition_name="BUNDESLIGA",
        game_id="game-1",
        event_type=EventType.REDCARD,
        player="Player X",
        team="Bayern",
        minute=78,
    )
    item = event.to_item()
    assert item["SK"] == f"GAMEID#game-1#EVENT#REDCARD#{event.event_id}"
    assert item["GSI1PK"] == "TEAM#Bayern"
    assert item["GSI1SK"] == f"EVENT#REDCARD#{event.event_id}"


def test_game_event_round_trip_preserves_identity_and_latency_timestamps():
    provider_emitted_at = datetime(2026, 9, 9, 14, 32, 10, tzinfo=timezone.utc)
    ingested_at = datetime(2026, 9, 9, 14, 32, 12, 500000, tzinfo=timezone.utc)
    event = GameEvent(
        competition_name="BUNDESLIGA",
        game_id="game-1",
        event_type=EventType.GOAL,
        player="Muller",
        team="Bayern",
        minute=23,
        provider_emitted_at=provider_emitted_at,
        ingested_at=ingested_at,
    )
    item = event.to_item()

    # Timestamps are stored as ISO8601 strings, not regenerated on write.
    assert item["ProviderEmittedAt"] == provider_emitted_at.isoformat()
    assert item["IngestedAt"] == ingested_at.isoformat()

    # from_item must round-trip event_id (parsed back out of the SK) and
    # both latency timestamps exactly, not regenerate fresh ones -- this is
    # what makes DynamoDB reads reflect the identity/timing that was
    # actually written, not new random/current values.
    restored = GameEvent.from_item(item, "BUNDESLIGA")
    assert restored == event
    assert restored.event_id == event.event_id
    assert restored.provider_emitted_at == provider_emitted_at
    assert restored.ingested_at == ingested_at


def test_team_round_trip():
    team = Team(competition_name="BUNDESLIGA", team_name="Bayern")
    item = team.to_item()
    assert item["SK"] == "TEAM#Bayern"
    assert Team.from_item(item, "BUNDESLIGA") == team


def test_game_team_index_to_item():
    link = GameTeamIndex(
        competition_name="BUNDESLIGA",
        team_name="Bayern",
        opponent="Dortmund",
        home_away="HOME",
        game_id="game-1",
        game_date="2025-11-20",
        kickoff="15:00",
    )
    item = link.to_item()
    assert item["SK"] == "TEAM#Bayern#GAME#2025-11-20#game-1"
    assert item["GSI1PK"] == "TEAM#Bayern"
    assert item["GSI1SK"] == "GAME#2025-11-20#game-1"
    assert GameTeamIndex.from_item(item, "BUNDESLIGA") == link
