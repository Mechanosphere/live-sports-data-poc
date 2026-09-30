"""PK/SK builders and parsers for the football single-table design.

Table layout (see Match Alerts Solution Design, GSP-9725):

    PK = SPORT#FOOTBALL#{COMPETITION_NAME}
    SK = METADATA                                    (Competition)
       | GAMEID#{GAME_UUID}#METADATA                 (Game)
       | GAMEID#{GAME_UUID}#EVENT#{TYPE}#{UUIDv7}    (Game Event)
       | TEAM#{TEAM_NAME}                            (Team)
       | TEAM#{TEAM_NAME}#PLAYER#{PLAYER_NAME}       (Player)
       | TEAM#{TEAM_NAME}#GAME#{GAME_DATE}#{GAME_UUID} (Game<->Team index, adjacency)

    GSI1PK = TEAM#{TEAM_NAME}
    GSI1SK = GAME#{GAME_DATE}#{GAME_UUID}            (games for a followed team)
           | EVENT#{TYPE}#{UUIDv7}                    (events for a followed team)
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from enum import Enum

SPORT_FOOTBALL = "FOOTBALL"
SPORT_BASKETBALL = "BASKETBALL"
SK_METADATA = "METADATA"

_RAND_BITS = 74  # 80 bits of randomness minus 4 (version) minus 2 (variant)
_RAND_MASK = (1 << _RAND_BITS) - 1

_uuid7_lock = threading.Lock()
_uuid7_last_ts_ms = 0
_uuid7_last_rand = 0


class EventType(str, Enum):
    # Football
    GOAL = "GOAL"
    OWNGOAL = "OWNGOAL"
    REDCARD = "REDCARD"
    PENALTY = "PENALTY"
    KICKOFF = "KICKOFF"
    HALFTIME = "HALFTIME"
    FULLTIME = "FULLTIME"
    # Basketball
    TWO_POINTER = "TWO_POINTER"
    THREE_POINTER = "THREE_POINTER"
    FREE_THROW = "FREE_THROW"
    FOUL = "FOUL"
    TIMEOUT = "TIMEOUT"
    QUARTER_END = "QUARTER_END"
    GAME_START = "GAME_START"
    GAME_END = "GAME_END"


def uuid7() -> uuid.UUID:
    """Generate a time-ordered UUIDv7 (RFC 9562).

    Guarantees strict monotonic ordering even for multiple UUIDs generated
    within the same millisecond, by incrementing the random component
    (RFC 9562 "Monotonic Random" method) instead of re-randomizing it.
    This is required for our event ordering guarantees: bursts of events
    (e.g. goal + card in the same match minute) can easily land in the
    same millisecond.
    """
    global _uuid7_last_ts_ms, _uuid7_last_rand
    with _uuid7_lock:
        ts_ms = int(time.time() * 1000)
        if ts_ms <= _uuid7_last_ts_ms:
            ts_ms = _uuid7_last_ts_ms
            _uuid7_last_rand = (_uuid7_last_rand + 1) & _RAND_MASK
        else:
            _uuid7_last_rand = int.from_bytes(os.urandom(10), "big") & _RAND_MASK
        _uuid7_last_ts_ms = ts_ms

        ts_bytes = ts_ms.to_bytes(6, "big")
        rand_bytes = bytearray(_uuid7_last_rand.to_bytes(10, "big"))
        rand_bytes[0] = (rand_bytes[0] & 0x0F) | 0x70  # version 7
        rand_bytes[2] = (rand_bytes[2] & 0x3F) | 0x80  # variant 10
        return uuid.UUID(bytes=bytes(ts_bytes) + bytes(rand_bytes))


def _event_type_value(event_type: EventType | str) -> str:
    return event_type.value if isinstance(event_type, EventType) else event_type


# --- Partition key ---------------------------------------------------------

def sport_pk(sport: str, competition: str) -> str:
    """Generalized partition key: SPORT#{sport}#{competition}.

    `competition_pk` (football) is now a thin wrapper around this so future
    sports (basketball, etc.) can share the same key builder without
    touching football's existing key shape.
    """
    return f"SPORT#{sport}#{competition}"


def competition_pk(competition_name: str) -> str:
    return sport_pk(SPORT_FOOTBALL, competition_name)


# --- Sort keys ---------------------------------------------------------

def competition_metadata_sk() -> str:
    return SK_METADATA


def game_metadata_sk(game_id: str) -> str:
    return f"GAMEID#{game_id}#METADATA"


def game_event_sk_prefix(game_id: str, event_type: EventType | str | None = None) -> str:
    if event_type is None:
        return f"GAMEID#{game_id}#EVENT#"
    return f"GAMEID#{game_id}#EVENT#{_event_type_value(event_type)}#"


def game_event_sk(
    game_id: str,
    event_type: EventType | str,
    event_uuid: uuid.UUID | str | None = None,
) -> str:
    event_uuid = event_uuid if event_uuid is not None else uuid7()
    return f"{game_event_sk_prefix(game_id, event_type)}{event_uuid}"


def team_sk(team_name: str) -> str:
    return f"TEAM#{team_name}"


def player_sk(team_name: str, player_name: str) -> str:
    return f"TEAM#{team_name}#PLAYER#{player_name}"


def team_game_index_sk_prefix(team_name: str) -> str:
    return f"TEAM#{team_name}#GAME#"


def team_game_index_sk(team_name: str, game_date: str, game_id: str) -> str:
    return f"{team_game_index_sk_prefix(team_name)}{game_date}#{game_id}"


# --- GSI1 (follow-a-team access patterns) ---------------------------------

def gsi1_team_pk(team_name: str) -> str:
    return f"TEAM#{team_name}"


def gsi1_game_sk_prefix() -> str:
    return "GAME#"


def gsi1_game_sk(game_date: str, game_id: str) -> str:
    return f"{gsi1_game_sk_prefix()}{game_date}#{game_id}"


def gsi1_event_sk_prefix(event_type: EventType | str | None = None) -> str:
    if event_type is None:
        return "EVENT#"
    return f"EVENT#{_event_type_value(event_type)}#"


def gsi1_event_sk(event_type: EventType | str, event_uuid: uuid.UUID | str) -> str:
    return f"{gsi1_event_sk_prefix(event_type)}{event_uuid}"


# --- GSI2 (chronological events-per-game access pattern) -------------------
#
# Events are stored in the base table grouped by type first (SK begins with
# GAMEID#{game_id}#EVENT#{TYPE}#...), so a global "most recent N events
# across all types" query isn't possible on the base table without fetching
# every event for the game. GSI2 re-keys events per-game, sorted purely by
# ingestion time, so `get_top_events` can do a direct bounded/descending
# query (`Limit=N`, `ScanIndexForward=False`) instead of a full fetch + sort.

def gsi2_game_pk(competition_name: str, game_id: str) -> str:
    return f"GAME#{competition_name}#{game_id}"


def gsi2_event_sk(ingested_at_iso: str, event_uuid: uuid.UUID | str) -> str:
    return f"INGESTEDAT#{ingested_at_iso}#{event_uuid}"
