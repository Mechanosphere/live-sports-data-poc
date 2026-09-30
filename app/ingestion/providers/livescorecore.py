"""Stub adapter for a fictional provider: "LiveScoreCore".

Payload shape (flat, minute as int, event represented by a short code, and
epoch-millisecond timestamp instead of ISO8601):

    {
        "fixture_id": "lsc-2291",
        "competition": "BUNDESLIGA",
        "event_code": "GL",
        "min": 23,
        "team_name": "Bayern",
        "player_name": "Muller",
        "ts_epoch_ms": 1757427130000
    }

Deliberately structured differently from SportsFeedX (flat vs nested,
event codes vs full names, int vs string minute, epoch ms vs ISO8601) to
exercise the normalization layer against real-world provider heterogeneity.

In production this would poll a real HTTP API. For this POC, `poll()`
replays a fixed script of events for a single demo match.
"""
from __future__ import annotations

import time
from typing import Any

from app.ingestion.providers.base import Provider
from app.ingestion.providers.registry import register
from app.models.keys import SPORT_FOOTBALL

_SCRIPT: list[dict[str, Any]] = [
    {"event_code": "KO", "min": 0, "team_name": "Bayern", "player_name": None},
    {"event_code": "GL", "min": 30, "team_name": "Bayern", "player_name": "Kimmich"},
    {"event_code": "OG", "min": 55, "team_name": "Dortmund", "player_name": "Hummels"},
    {"event_code": "PK", "min": 61, "team_name": "Bayern", "player_name": "Kane"},
    {"event_code": "FT", "min": 90, "team_name": "Bayern", "player_name": None},
]


class LiveScoreCoreProvider(Provider):
    name = "livescorecore"
    sport = SPORT_FOOTBALL

    def __init__(self, fixture_id: str = "lsc-2291", competition: str = "BUNDESLIGA") -> None:
        self._fixture_id = fixture_id
        self._competition = competition
        self._next_index = 0

    def partition_key(self, raw: dict[str, Any]) -> str:
        return f"{raw['competition'].upper()}#{raw['fixture_id']}"

    def poll(self) -> list[dict[str, Any]]:
        if self._next_index >= len(_SCRIPT):
            return []
        event = _SCRIPT[self._next_index]
        self._next_index += 1
        return [
            {
                "fixture_id": self._fixture_id,
                "competition": self._competition,
                "ts_epoch_ms": int(time.time() * 1000),
                **event,
            }
        ]


register(LiveScoreCoreProvider.name, LiveScoreCoreProvider)
