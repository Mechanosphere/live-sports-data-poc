"""Football normalizers: SportsFeedX and LiveScoreCore raw payload shapes.

Moved here unchanged from the former `app.ingestion.normalize` module as
part of the normalizer-registry generalization; behavior is identical.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.ingestion.normalizers.registry import register
from app.models.keys import EventType, SPORT_FOOTBALL
from app.schemas.football import GameEvent

# --- SportsFeedX ------------------------------------------------------

_SPORTSFEEDX_EVENT_TYPES = {
    "GOAL": EventType.GOAL,
    "OWNGOAL": EventType.OWNGOAL,
    "REDCARD": EventType.REDCARD,
    "PENALTY": EventType.PENALTY,
    "KICKOFF": EventType.KICKOFF,
    "HALFTIME": EventType.HALFTIME,
    "FULLTIME": EventType.FULLTIME,
}


def normalize_sportsfeedx(raw: dict[str, Any]) -> GameEvent:
    incident = raw["incident"]
    player = incident.get("player") or {}
    return GameEvent(
        sport=SPORT_FOOTBALL,
        competition_name=raw["competition"].upper(),
        game_id=raw["matchId"],
        event_type=_SPORTSFEEDX_EVENT_TYPES[incident["type"]],
        player=player.get("name", ""),
        team=incident["team"]["name"],
        minute=int(incident["minute"]),
        provider_emitted_at=datetime.fromisoformat(raw["timestamp"].replace("Z", "+00:00")),
    )


# --- LiveScoreCore ------------------------------------------------------

_LIVESCORECORE_EVENT_CODES = {
    "GL": EventType.GOAL,
    "OG": EventType.OWNGOAL,
    "RC": EventType.REDCARD,
    "PK": EventType.PENALTY,
    "KO": EventType.KICKOFF,
    "HT": EventType.HALFTIME,
    "FT": EventType.FULLTIME,
}


def normalize_livescorecore(raw: dict[str, Any]) -> GameEvent:
    return GameEvent(
        sport=SPORT_FOOTBALL,
        competition_name=raw["competition"].upper(),
        game_id=raw["fixture_id"],
        event_type=_LIVESCORECORE_EVENT_CODES[raw["event_code"]],
        player=raw.get("player_name") or "",
        team=raw["team_name"],
        minute=int(raw["min"]),
        provider_emitted_at=datetime.fromtimestamp(raw["ts_epoch_ms"] / 1000, tz=timezone.utc),
    )


# --- Runner (manual/demo scripted events, already normalized-shaped) -----


def normalize_runner(raw: dict[str, Any]) -> GameEvent:
    """Pass-through normalizer for the `/api/runner` demo endpoint.

    The runner endpoint synthesizes events directly (no real provider), so
    its raw payload is already shaped like `GameEvent`'s own fields rather
    than a provider-native shape. `sport` defaults to football for backward
    compatibility with raw payloads published before the runner supported
    other sports; `RunnerService` always sets it explicitly (see
    `app.api.routes.runner`).
    """
    return GameEvent(
        sport=raw.get("sport", SPORT_FOOTBALL),
        competition_name=raw["competition_name"],
        game_id=raw["game_id"],
        event_type=EventType(raw["event_type"]),
        player=raw["player"],
        team=raw["team"],
        minute=int(raw["minute"]),
    )


register("sportsfeedx", normalize_sportsfeedx)
register("livescorecore", normalize_livescorecore)
register("runner", normalize_runner)
