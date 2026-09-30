"""Basketball normalizer for the "HoopStream" stub provider.

Payload shape (flat, action code + quarter/game-clock instead of minute):

    {
        "game_id": "hps-3301",
        "competition": "NBA",
        "action": "3PT",
        "quarter": 2,
        "team_name": "Lakers",
        "player_name": "James",
        "ts_epoch_ms": 1757427130000
    }

NOTE: `GameEvent.minute` is a football-shaped field (minutes-into-match);
basketball doesn't have a direct equivalent, so we store `quarter` in it
for now. This is a known approximation to avoid a broader schema change
here -- a proper generic `Occurrence` schema (see design discussion) would
replace `minute` with a sport-agnostic clock/period representation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.ingestion.normalizers.registry import register
from app.models.keys import EventType, SPORT_BASKETBALL
from app.schemas.football import GameEvent

_HOOPSTREAM_ACTION_CODES = {
    "2PT": EventType.TWO_POINTER,
    "3PT": EventType.THREE_POINTER,
    "FT": EventType.FREE_THROW,
    "FOUL": EventType.FOUL,
    "TIMEOUT": EventType.TIMEOUT,
    "QEND": EventType.QUARTER_END,
    "START": EventType.GAME_START,
    "END": EventType.GAME_END,
}


def normalize_hoopstream(raw: dict[str, Any]) -> GameEvent:
    return GameEvent(
        sport=SPORT_BASKETBALL,
        competition_name=raw["competition"].upper(),
        game_id=raw["game_id"],
        event_type=_HOOPSTREAM_ACTION_CODES[raw["action"]],
        player=raw.get("player_name") or "",
        team=raw["team_name"],
        minute=int(raw["quarter"]),
        provider_emitted_at=datetime.fromtimestamp(raw["ts_epoch_ms"] / 1000, tz=timezone.utc),
    )


register("hoopstream", normalize_hoopstream)
