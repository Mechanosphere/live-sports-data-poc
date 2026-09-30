import pytest

from app.ingestion.normalizers import UnknownProviderError, normalize, register
from app.ingestion.providers.basketball_stub import HoopStreamProvider
from app.models.keys import EventType, SPORT_BASKETBALL, SPORT_FOOTBALL


def test_hoopstream_provider_script_ends():
    provider = HoopStreamProvider()
    all_raw = []
    while True:
        batch = provider.poll()
        if not batch:
            break
        all_raw.extend(batch)
    assert len(all_raw) == 7
    assert all_raw[0]["action"] == "START"
    assert all_raw[-1]["action"] == "END"


def test_hoopstream_provider_events_normalize_end_to_end():
    provider = HoopStreamProvider()
    events = []
    while True:
        batch = provider.poll()
        if not batch:
            break
        for raw in batch:
            events.append(normalize("hoopstream", raw))
    assert len(events) == 7
    assert [e.event_type for e in events] == [
        EventType.GAME_START,
        EventType.THREE_POINTER,
        EventType.FOUL,
        EventType.QUARTER_END,
        EventType.TWO_POINTER,
        EventType.FREE_THROW,
        EventType.GAME_END,
    ]
    assert all(e.sport == SPORT_BASKETBALL for e in events)


def test_normalize_hoopstream_three_pointer():
    raw = {
        "game_id": "hps-3301",
        "competition": "NBA",
        "action": "3PT",
        "quarter": 2,
        "team_name": "Lakers",
        "player_name": "James",
        "ts_epoch_ms": 1757427130000,
    }
    event = normalize("hoopstream", raw)
    assert event.sport == SPORT_BASKETBALL
    assert event.competition_name == "NBA"
    assert event.game_id == "hps-3301"
    assert event.event_type == EventType.THREE_POINTER
    assert event.player == "James"
    assert event.team == "Lakers"
    assert event.minute == 2


def test_normalize_hoopstream_timeout_no_player():
    raw = {
        "game_id": "hps-3301",
        "competition": "NBA",
        "action": "TIMEOUT",
        "quarter": 3,
        "team_name": "Lakers",
        "player_name": None,
        "ts_epoch_ms": 1757427130000,
    }
    event = normalize("hoopstream", raw)
    assert event.event_type == EventType.TIMEOUT
    assert event.player == ""


def test_football_normalizers_still_registered_and_default_sport():
    raw = {
        "fixture_id": "lsc-2291",
        "competition": "BUNDESLIGA",
        "event_code": "KO",
        "min": 0,
        "team_name": "Bayern",
        "player_name": None,
        "ts_epoch_ms": 0,
    }
    event = normalize("livescorecore", raw)
    assert event.sport == SPORT_FOOTBALL
    assert event.event_type == EventType.KICKOFF


def test_normalize_unknown_provider_raises_unknown_provider_error():
    with pytest.raises(UnknownProviderError):
        normalize("unknown-provider", {})
    # UnknownProviderError is a ValueError subclass, so old `except ValueError` still works.
    with pytest.raises(ValueError):
        normalize("unknown-provider", {})


def test_register_overwrites_existing_normalizer():
    from app.ingestion.normalizers.football import normalize_livescorecore

    calls = []

    def fake_normalizer(raw):
        calls.append(raw)
        return normalize_livescorecore(raw)

    register("livescorecore", fake_normalizer)
    try:
        raw = {
            "fixture_id": "lsc-2291",
            "competition": "BUNDESLIGA",
            "event_code": "FT",
            "min": 90,
            "team_name": "Bayern",
            "player_name": None,
            "ts_epoch_ms": 0,
        }
        event = normalize("livescorecore", raw)
        assert event.event_type == EventType.FULLTIME
        assert calls == [raw]
    finally:
        register("livescorecore", normalize_livescorecore)
