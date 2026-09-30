from app.models import keys
from app.models.keys import EventType


def test_competition_pk():
    assert keys.competition_pk("BUNDESLIGA") == "SPORT#FOOTBALL#BUNDESLIGA"


def test_game_metadata_sk():
    assert keys.game_metadata_sk("game-1") == "GAMEID#game-1#METADATA"


def test_game_event_sk_and_prefix():
    event_id = keys.uuid7()
    sk = keys.game_event_sk("game-1", EventType.GOAL, event_id)
    assert sk == f"GAMEID#game-1#EVENT#GOAL#{event_id}"
    assert sk.startswith(keys.game_event_sk_prefix("game-1", EventType.GOAL))
    assert sk.startswith(keys.game_event_sk_prefix("game-1"))


def test_uuid7_is_time_ordered():
    import time

    a = keys.uuid7()
    time.sleep(0.002)  # ensure the millisecond timestamp advances
    b = keys.uuid7()
    # UUIDv7 values generated later should sort >= earlier ones lexically.
    assert str(a) <= str(b)
    assert a.version == 7


def test_uuid7_is_monotonic_within_same_millisecond():
    # Even without a sleep, rapid successive calls (e.g. a goal and a card
    # logged in the same millisecond) must still sort chronologically.
    values = [keys.uuid7() for _ in range(50)]
    assert [str(v) for v in values] == sorted(str(v) for v in values)


def test_team_and_player_sk():
    assert keys.team_sk("BAYERN") == "TEAM#BAYERN"
    assert keys.player_sk("BAYERN", "MULLER") == "TEAM#BAYERN#PLAYER#MULLER"


def test_team_game_index_sk():
    sk = keys.team_game_index_sk("BAYERN", "2025-11-20", "game-1")
    assert sk == "TEAM#BAYERN#GAME#2025-11-20#game-1"
    assert sk.startswith(keys.team_game_index_sk_prefix("BAYERN"))


def test_gsi1_keys():
    assert keys.gsi1_team_pk("BAYERN") == "TEAM#BAYERN"
    assert keys.gsi1_game_sk("2025-11-20", "game-1") == "GAME#2025-11-20#game-1"
    event_id = keys.uuid7()
    assert (
        keys.gsi1_event_sk(EventType.REDCARD, event_id)
        == f"EVENT#REDCARD#{event_id}"
    )
