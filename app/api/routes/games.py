"""Read endpoints for football game data stored in DynamoDB."""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

from app.models.keys import SPORT_FOOTBALL
from app.schemas.football import GameEvent
from app.services import football_repository as repo

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/competitions/{competition_name}/games/{game_id}/events",
    response_model=list[GameEvent],
    summary="All events for a game, queried by competition name and game ID",
)
def get_game_events(
    competition_name: str, game_id: str, sport: str = SPORT_FOOTBALL
) -> list[GameEvent]:
    """Return every event recorded for a given competition/game, ordered
    chronologically (oldest first). This is the direct DynamoDB query
    keyed on `PK=SPORT#{sport}#{competition_name}` and `SK` begins_with
    `GAME#{game_id}#EVENT#`, with no truncation -- use `/events/top` if
    you only want the most recent N. `sport` defaults to football;
    pass `?sport=BASKETBALL` for basketball games.
    """
    events = repo.get_game_events(competition_name.upper(), game_id, sport=sport)
    if not events:
        logger.warning("No events found for competition=%s game=%s", competition_name, game_id)
        raise HTTPException(status_code=404, detail="No events found for this game")
    return events


@router.get(
    "/competitions/{competition_name}/games/{game_id}/events/top",
    response_model=list[GameEvent],
    summary="Top N most recent events for a game",
)
def get_top_game_events(
    competition_name: str,
    game_id: str,
    limit: int = Query(10, ge=1, le=100, description="Max number of events to return"),
) -> list[GameEvent]:
    """Return the `limit` most recent events for a game, newest first.

    Queries all events for the game/competition from DynamoDB and returns the
    top N by ingestion time. 404s if the game has no events (e.g. unknown
    competition/game_id).
    """
    events = repo.get_top_events(competition_name.upper(), game_id, limit=limit)
    if not events:
        logger.warning("No events found for competition=%s game=%s", competition_name, game_id)
        raise HTTPException(status_code=404, detail="No events found for this game")
    return events


@router.get(
    "/competitions/{competition_name}/games/{game_id}/monitor",
    response_class=HTMLResponse,
    summary="Live-updating HTML page showing a game's top 10 events",
)
def monitor_game_events(competition_name: str, game_id: str) -> HTMLResponse:
    """A small self-contained HTML/JS page for watching events land in near
    real time -- polls the `/events/top` JSON endpoint every 5 seconds and
    re-renders the table, without a full page reload. Handy for watching
    the runner endpoint (`POST /runner/competitions/.../games/.../events`) fill
    in a game live.
    """
    api_url = f"/api/competitions/{competition_name}/games/{game_id}/events/top?limit=10"
    html = _MONITOR_PAGE_TEMPLATE.format(
        competition_name=competition_name,
        game_id=game_id,
        api_url=api_url,
    )
    return HTMLResponse(content=html)


_MONITOR_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Live events: {competition_name} / {game_id}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #222; }}
    h1 {{ font-size: 1.25rem; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
    th, td {{ text-align: left; padding: 0.4rem 0.75rem; border-bottom: 1px solid #ddd; }}
    th {{ background: #f5f5f5; }}
    #status {{ color: #666; font-size: 0.85rem; }}
    .error {{ color: #b00020; }}
  </style>
</head>
<body>
  <h1>Top events &mdash; {competition_name} / {game_id}</h1>
  <div id="status">Loading&hellip;</div>
  <table>
    <thead>
      <tr><th>Type</th><th>Minute</th><th>Team</th><th>Player</th><th>Ingested At</th></tr>
    </thead>
    <tbody id="events-body"></tbody>
  </table>

  <script>
    const apiUrl = {api_url!r};
    const statusEl = document.getElementById("status");
    const bodyEl = document.getElementById("events-body");

    async function refresh() {{
      try {{
        const resp = await fetch(apiUrl, {{ cache: "no-store" }});
        if (resp.status === 404) {{
          bodyEl.innerHTML = "";
          statusEl.textContent = "No events yet for this game.";
          statusEl.className = "";
          return;
        }}
        if (!resp.ok) {{
          throw new Error("HTTP " + resp.status);
        }}
        const events = await resp.json();
        bodyEl.innerHTML = events.map(e => `
          <tr>
            <td>${{e.event_type}}</td>
            <td>${{e.minute}}</td>
            <td>${{e.team}}</td>
            <td>${{e.player || ""}}</td>
            <td>${{e.ingested_at}}</td>
          </tr>
        `).join("");
        statusEl.textContent = "Last updated: " + new Date().toLocaleTimeString();
        statusEl.className = "";
      }} catch (err) {{
        statusEl.textContent = "Failed to refresh: " + err;
        statusEl.className = "error";
      }}
    }}

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""
