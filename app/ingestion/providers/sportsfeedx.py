"""Stub adapter for a fictional provider: "SportsFeedX".

Payload shape (verbose/nested, minute as string, event type as an uppercase
name matching our own EventType names):

    {
        "matchId": "sfx-8841",
        "competition": "Bundesliga",
        "timestamp": "2026-09-09T14:32:10Z",
        "incident": {
            "type": "GOAL",
            "minute": "23",
            "team": {"id": "T-101", "name": "Bayern"},
            "player": {"id": "P-55", "name": "Muller"}
        }
    }

In production this would poll a real HTTP API (e.g. `GET /v2/matches/{id}/incidents?since=...`).
For this POC, `poll()` replays a fixed script of incidents for a single demo
match, one incident per call, to simulate events trickling in over time.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.ingestion.providers.base import Provider
from app.ingestion.providers.registry import register
from app.models.keys import SPORT_FOOTBALL

_SCRIPT: list[dict[str, Any]] = [
    {"type": "KICKOFF", "minute": "0", "team": {"id": "T-101", "name": "Bayern"}, "player": None},
    {"type": "GOAL", "minute": "23", "team": {"id": "T-101", "name": "Bayern"}, "player": {"id": "P-55", "name": "Muller"}},
    {"type": "REDCARD", "minute": "41", "team": {"id": "T-102", "name": "Dortmund"}, "player": {"id": "P-77", "name": "Reus"}},
    {"type": "HALFTIME", "minute": "45", "team": {"id": "T-101", "name": "Bayern"}, "player": None},
    {"type": "GOAL", "minute": "68", "team": {"id": "T-102", "name": "Dortmund"}, "player": {"id": "P-77", "name": "Reus"}},
    {"type": "FULLTIME", "minute": "90", "team": {"id": "T-101", "name": "Bayern"}, "player": None},
]


class SportsFeedXProvider(Provider):
    name = "sportsfeedx"
    sport = SPORT_FOOTBALL

    def __init__(self, match_id: str = "sfx-8841", competition: str = "Bundesliga") -> None:
        self._match_id = match_id
        self._competition = competition
        self._next_index = 0

    def partition_key(self, raw: dict[str, Any]) -> str:
        return f"{raw['competition'].upper()}#{raw['matchId']}"

    def poll(self) -> list[dict[str, Any]]:
        if self._next_index >= len(_SCRIPT):
            return []
        incident = _SCRIPT[self._next_index]
        self._next_index += 1
        return [
            {
                "matchId": self._match_id,
                "competition": self._competition,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "incident": incident,
            }
        ]


register(SportsFeedXProvider.name, SportsFeedXProvider)
