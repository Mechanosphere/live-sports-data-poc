import pytest

from app.ingestion import normalize
from app.ingestion.providers.livescorecore import LiveScoreCoreProvider
from app.ingestion.providers.sportsfeedx import SportsFeedXProvider
from app.models.keys import EventType


def test_normalize_sportsfeedx_goal():
    raw = {
        "matchId": "sfx-8841",
        "competition": "Bundesliga",
        "timestamp": "2026-09-09T14:32:10Z",
        "incident": {
            "type": "GOAL",
            "minute": "23",
            "team": {"id": "T-101", "name": "Bayern"},
            "player": {"id": "P-55", "name": "Muller"},
        },
    }
    event = normalize.normalize_sportsfeedx(raw)
    assert event.competition_name == "BUNDESLIGA"
    assert event.game_id == "sfx-8841"
    assert event.event_type == EventType.GOAL
    assert event.player == "Muller"
    assert event.team == "Bayern"
    assert event.minute == 23


def test_normalize_sportsfeedx_kickoff_no_player():
    raw = {
        "matchId": "sfx-8841",
        "competition": "Bundesliga",
        "timestamp": "2026-09-09T14:00:00Z",
        "incident": {
            "type": "KICKOFF",
            "minute": "0",
            "team": {"id": "T-101", "name": "Bayern"},
            "player": None,
        },
    }
    event = normalize.normalize_sportsfeedx(raw)
    assert event.event_type == EventType.KICKOFF
    assert event.player == ""
    assert event.minute == 0


def test_normalize_livescorecore_goal():
    raw = {
        "fixture_id": "lsc-2291",
        "competition": "BUNDESLIGA",
        "event_code": "GL",
        "min": 23,
        "team_name": "Bayern",
        "player_name": "Muller",
        "ts_epoch_ms": 1757427130000,
    }
    event = normalize.normalize_livescorecore(raw)
    assert event.competition_name == "BUNDESLIGA"
    assert event.game_id == "lsc-2291"
    assert event.event_type == EventType.GOAL
    assert event.player == "Muller"
    assert event.team == "Bayern"
    assert event.minute == 23


def test_normalize_livescorecore_fulltime_no_player():
    raw = {
        "fixture_id": "lsc-2291",
        "competition": "BUNDESLIGA",
        "event_code": "FT",
        "min": 90,
        "team_name": "Bayern",
        "player_name": None,
        "ts_epoch_ms": 1757427130000,
    }
    event = normalize.normalize_livescorecore(raw)
    assert event.event_type == EventType.FULLTIME
    assert event.player == ""


def test_normalize_dispatch_by_provider_name():
    raw = {
        "fixture_id": "lsc-2291",
        "competition": "BUNDESLIGA",
        "event_code": "KO",
        "min": 0,
        "team_name": "Bayern",
        "player_name": None,
        "ts_epoch_ms": 0,
    }
    event = normalize.normalize("livescorecore", raw)
    assert event.event_type == EventType.KICKOFF


def test_normalize_unknown_provider_raises():
    with pytest.raises(ValueError):
        normalize.normalize("unknown-provider", {})


def test_normalize_each_uuid7_is_unique():
    raw = {
        "matchId": "sfx-8841",
        "competition": "Bundesliga",
        "timestamp": "2026-09-09T14:32:10Z",
        "incident": {
            "type": "GOAL",
            "minute": "23",
            "team": {"id": "T-101", "name": "Bayern"},
            "player": {"id": "P-55", "name": "Muller"},
        },
    }
    event1 = normalize.normalize_sportsfeedx(raw)
    event2 = normalize.normalize_sportsfeedx(raw)
    assert event1.event_id != event2.event_id


def test_sportsfeedx_provider_script_ends():
    provider = SportsFeedXProvider()
    all_raw = []
    while True:
        batch = provider.poll()
        if not batch:
            break
        all_raw.extend(batch)
    assert len(all_raw) == 6
    assert all_raw[0]["incident"]["type"] == "KICKOFF"
    assert all_raw[-1]["incident"]["type"] == "FULLTIME"


def test_livescorecore_provider_script_ends():
    provider = LiveScoreCoreProvider()
    all_raw = []
    while True:
        batch = provider.poll()
        if not batch:
            break
        all_raw.extend(batch)
    assert len(all_raw) == 5
    assert all_raw[0]["event_code"] == "KO"
    assert all_raw[-1]["event_code"] == "FT"
