"""Query helpers implementing the football single-table access patterns.

Each function maps directly to an access pattern from the Match Alerts
Solution Design (GSP-9725).
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

from boto3.dynamodb.conditions import Attr, Key

from app.models import keys
from app.models.keys import EventType
from app.schemas.football import Game, GameEvent, GameTeamIndex, Competition, Team
from app.services.dynamodb import get_table

logger = logging.getLogger(__name__)


def put_item(item: dict[str, Any]) -> None:
    logger.debug("put_item PK=%s SK=%s", item.get("PK"), item.get("SK"))
    get_table().put_item(Item=item)


# --- Get single items -------------------------------------------------

def get_competition(competition_name: str, sport: str = keys.SPORT_FOOTBALL) -> Optional[Competition]:
    resp = get_table().get_item(
        Key={"PK": keys.sport_pk(sport, competition_name), "SK": keys.competition_metadata_sk()}
    )
    item = resp.get("Item")
    return Competition.from_item(item) if item else None


def get_game(
    competition_name: str, game_id: str, sport: str = keys.SPORT_FOOTBALL
) -> Optional[Game]:
    resp = get_table().get_item(
        Key={"PK": keys.sport_pk(sport, competition_name), "SK": keys.game_metadata_sk(game_id)}
    )
    item = resp.get("Item")
    return Game.from_item(item, competition_name) if item else None


def get_team(
    competition_name: str, team_name: str, sport: str = keys.SPORT_FOOTBALL
) -> Optional[Team]:
    resp = get_table().get_item(
        Key={"PK": keys.sport_pk(sport, competition_name), "SK": keys.team_sk(team_name)}
    )
    item = resp.get("Item")
    return Team.from_item(item, competition_name) if item else None


# --- Game event queries -------------------------------------------------

def get_game_events(
    competition_name: str, game_id: str, sport: str = keys.SPORT_FOOTBALL
) -> list[GameEvent]:
    """All events for a game (any type), ordered chronologically.

    `sport` defaults to football for backward compatibility -- pass
    `keys.SPORT_BASKETBALL` (or any other registered sport) to query games
    stored under that sport's partition (see `keys.sport_pk`).
    """
    resp = get_table().query(
        KeyConditionExpression=Key("PK").eq(keys.sport_pk(sport, competition_name))
        & Key("SK").begins_with(keys.game_event_sk_prefix(game_id))
    )
    items = resp.get("Items", [])
    logger.info("get_game_events competition=%s game=%s -> %d items", competition_name, game_id, len(items))
    return [GameEvent.from_item(i, competition_name) for i in items]


def get_events_by_type(
    competition_name: str, game_id: str, event_type: EventType, sport: str = keys.SPORT_FOOTBALL
) -> list[GameEvent]:
    """All events of a specific type for a game, ordered chronologically."""
    resp = get_table().query(
        KeyConditionExpression=Key("PK").eq(keys.sport_pk(sport, competition_name))
        & Key("SK").begins_with(keys.game_event_sk_prefix(game_id, event_type))
    )
    return [GameEvent.from_item(i, competition_name) for i in resp.get("Items", [])]


def get_latest_event_by_type(
    competition_name: str, game_id: str, event_type: EventType, sport: str = keys.SPORT_FOOTBALL
) -> Optional[GameEvent]:
    """Most recent event of a given type for a game."""
    resp = get_table().query(
        KeyConditionExpression=Key("PK").eq(keys.sport_pk(sport, competition_name))
        & Key("SK").begins_with(keys.game_event_sk_prefix(game_id, event_type)),
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items", [])
    return GameEvent.from_item(items[0], competition_name) if items else None


def get_top_events(competition_name: str, game_id: str, limit: int = 10) -> list[GameEvent]:
    """The `limit` most recent events for a game, across all event types.

    `get_game_events` returns events grouped by event type first (SK begins
    with `EVENT#{type}#...`), then chronologically within each type -- not
    globally chronological. GSI2 re-keys events per-game purely by
    ingestion time (GSI2PK=GAME#{competition}#{game_id}, GSI2SK=INGESTEDAT#...),
    so this queries it directly with `Limit` + descending order instead of
    fetching every event for the game and sorting in Python.
    """
    resp = get_table().query(
        IndexName="GSI2",
        KeyConditionExpression=Key("GSI2PK").eq(keys.gsi2_game_pk(competition_name, game_id)),
        ScanIndexForward=False,
        Limit=limit,
    )
    top = [GameEvent.from_item(i, competition_name) for i in resp.get("Items", [])]
    logger.info(
        "get_top_events competition=%s game=%s limit=%d -> %d items",
        competition_name,
        game_id,
        limit,
        len(top),
    )
    return top


def get_events_in_range(
    competition_name: str,
    game_id: str,
    event_type: EventType,
    start_uuid: str,
    end_uuid: str,
    sport: str = keys.SPORT_FOOTBALL,
) -> list[GameEvent]:
    """Events of a given type within a UUIDv7 range (i.e. a time range)."""
    prefix = keys.game_event_sk_prefix(game_id, event_type)
    resp = get_table().query(
        KeyConditionExpression=Key("PK").eq(keys.sport_pk(sport, competition_name))
        & Key("SK").between(f"{prefix}{start_uuid}", f"{prefix}{end_uuid}")
    )
    return [GameEvent.from_item(i, competition_name) for i in resp.get("Items", [])]


# --- Follow-team access patterns (GSI1) ---------------------------------

def get_games_for_team(team_name: str) -> list[GameTeamIndex]:
    """All games (past/future) involving a followed team, chronologically."""
    resp = get_table().query(
        IndexName="GSI1",
        KeyConditionExpression=Key("GSI1PK").eq(keys.gsi1_team_pk(team_name))
        & Key("GSI1SK").begins_with(keys.gsi1_game_sk_prefix()),
    )
    return [
        GameTeamIndex.from_item(i, i["PK"].split("#", 2)[2])
        for i in resp.get("Items", [])
    ]


def get_events_for_team(
    team_name: str, event_type: Optional[EventType] = None
) -> list[GameEvent]:
    """All events (optionally filtered by type) for a followed team."""
    resp = get_table().query(
        IndexName="GSI1",
        KeyConditionExpression=Key("GSI1PK").eq(keys.gsi1_team_pk(team_name))
        & Key("GSI1SK").begins_with(keys.gsi1_event_sk_prefix(event_type)),
    )
    return [
        GameEvent.from_item(i, i["PK"].split("#", 2)[2]) for i in resp.get("Items", [])
    ]


# --- Monitoring (admin/ops-only, table-wide scans) ----------------------

# Only the attributes GameEvent.from_item actually reads, so the scan pulls
# back just what the monitor view renders instead of every attribute on
# every item. Aliased via ExpressionAttributeNames to sidestep any
# DynamoDB reserved-word collisions (e.g. names that happen to be reserved).
_RECENT_EVENTS_PROJECTED_ATTRS = [
    "PK",
    "SK",
    "GameId",
    "EventType",
    "Player",
    "Team",
    "Minute",
    "ProviderEmittedAt",
    "IngestedAt",
    "Sport",
]
_RECENT_EVENTS_NAME_ALIASES = {
    f"#{name.lower()}": name for name in _RECENT_EVENTS_PROJECTED_ATTRS
}
_RECENT_EVENTS_PROJECTION = ", ".join(_RECENT_EVENTS_NAME_ALIASES.keys())

# Short server-side cache so N dashboard viewers polling every few seconds
# don't each trigger a fresh table `Scan`. Kept intentionally small so the
# monitor view still reflects newly-landed events promptly.
_RECENT_EVENTS_CACHE_TTL_SECONDS = 2.0
_recent_events_cache_lock = threading.Lock()
_recent_events_cache: tuple[float, list[GameEvent]] | None = None


def scan_recent_events(limit: int = 50) -> list[GameEvent]:
    """Most recently ingested events across *all* competitions/games.

    Unlike `get_top_events` (scoped to one game via GSI2), this has no
    single partition key to query against -- it's meant for an ops/
    monitoring view of "what's landing right now" across the whole table,
    not a hot-path access pattern. Implemented as a bounded `Scan` (fine at
    this table's current POC scale); would need a dedicated GSI keyed by a
    coarse time bucket to stay a `Scan`-free `Query` at larger scale.

    A `ProjectionExpression` limits the scan to the attributes the monitor
    view renders, and results are cached for a couple of seconds so
    multiple concurrent dashboard viewers share a single scan rather than
    each issuing their own.
    """
    global _recent_events_cache
    now = time.monotonic()
    with _recent_events_cache_lock:
        if _recent_events_cache is not None and now - _recent_events_cache[0] < _RECENT_EVENTS_CACHE_TTL_SECONDS:
            return _recent_events_cache[1][:limit]

    resp = get_table().scan(
        FilterExpression=Attr("EntityType").begins_with("EVENT#"),
        ProjectionExpression=_RECENT_EVENTS_PROJECTION,
        ExpressionAttributeNames=_RECENT_EVENTS_NAME_ALIASES,
    )
    items = resp.get("Items", [])
    events = []
    for i in items:
        competition_name = i["PK"].split("#", 2)[2] if i["PK"].count("#") >= 2 else i["PK"]
        try:
            events.append(GameEvent.from_item(i, competition_name))
        except Exception:
            logger.exception("Failed to parse event item PK=%s SK=%s", i.get("PK"), i.get("SK"))
    events.sort(key=lambda e: e.ingested_at, reverse=True)

    with _recent_events_cache_lock:
        _recent_events_cache = (time.monotonic(), events)
    return events[:limit]
