"""Stub adapter for a fictional provider: "HoopStream" (basketball).

Payload shape (flat, action code, quarter instead of minute, epoch-ms
timestamp -- see app.ingestion.normalizers.basketball for the matching
normalizer):

    {
        "game_id": "hps-3301",
        "competition": "NBA",
        "action": "3PT",
        "quarter": 2,
        "team_name": "Lakers",
        "player_name": "James",
        "ts_epoch_ms": 1757427130000
    }

In production this would poll a real HTTP API. For this POC, `poll()`
replays a fixed script of actions for a single demo game, one action per
call, to simulate events trickling in over time (mirrors the football
stub providers' pattern).
"""
from __future__ import annotations

import time
from typing import Any

from app.ingestion.providers.base import Provider
from app.ingestion.providers.registry import register
from app.models.keys import SPORT_BASKETBALL

_SCRIPT: list[dict[str, Any]] = [
    {"action": "START", "quarter": 1, "team_name": "Lakers", "player_name": None},
    {"action": "3PT", "quarter": 1, "team_name": "Lakers", "player_name": "James"},
    {"action": "FOUL", "quarter": 1, "team_name": "Celtics", "player_name": "Tatum"},
    {"action": "QEND", "quarter": 1, "team_name": "Lakers", "player_name": None},
    {"action": "2PT", "quarter": 2, "team_name": "Celtics", "player_name": "Tatum"},
    {"action": "FT", "quarter": 4, "team_name": "Lakers", "player_name": "James"},
    {"action": "END", "quarter": 4, "team_name": "Lakers", "player_name": None},
]


class HoopStreamProvider(Provider):
    name = "hoopstream"
    sport = SPORT_BASKETBALL

    def __init__(self, game_id: str = "hps-3301", competition: str = "NBA") -> None:
        self._game_id = game_id
        self._competition = competition
        self._next_index = 0

    def partition_key(self, raw: dict[str, Any]) -> str:
        return f"{raw['competition'].upper()}#{raw['game_id']}"

    def poll(self) -> list[dict[str, Any]]:
        if self._next_index >= len(_SCRIPT):
            return []
        action = _SCRIPT[self._next_index]
        self._next_index += 1
        return [
            {
                "game_id": self._game_id,
                "competition": self._competition,
                "ts_epoch_ms": int(time.time() * 1000),
                **action,
            }
        ]


register(HoopStreamProvider.name, HoopStreamProvider)
