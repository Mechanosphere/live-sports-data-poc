"""Ops/monitoring endpoints: canonical data (Aurora + projection) and the
DynamoDB event store, for watching data land as it's written.

Not part of the consumer's hot path -- these back a small dashboard for
observing ingestion in near real time (see `GET /api/monitor`).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.core.config import get_settings
from app.ingestion import canonical
from app.services import football_repository as repo

logger = logging.getLogger(__name__)

router = APIRouter()


class CanonicalGameOut(BaseModel):
    competition_name: str
    game_id: str
    team_names: list[str]


def _to_out(game: canonical.CanonicalGame) -> CanonicalGameOut:
    return CanonicalGameOut(
        competition_name=game.competition_name,
        game_id=game.game_id,
        team_names=sorted(game.team_names),
    )


@router.get(
    "/monitor/canonical/aurora",
    response_model=list[CanonicalGameOut],
    summary="Canonical games currently known to Aurora (source of truth)",
)
def get_aurora_games() -> list[CanonicalGameOut]:
    # Mirrors the sync job's own window (see `sync_canonical_projection`) so
    # this view matches what the projection would pick up on its next run.
    window_hours = get_settings().canonical_sync_window_hours
    games = canonical.get_aurora_store().all_games(window_hours=window_hours)
    return [_to_out(g) for g in games]


@router.get(
    "/monitor/canonical/projection",
    response_model=list[CanonicalGameOut],
    summary="Canonical games currently in the DynamoDB projection (consumer's hot-path read)",
)
def get_projection_games() -> list[CanonicalGameOut]:
    games = canonical.get_projection().all_games()
    return [_to_out(g) for g in games]


@router.get(
    "/monitor/events/recent",
    summary="Most recently ingested events across all competitions/games (DynamoDB event store)",
)
def get_recent_events(
    limit: int = Query(50, ge=1, le=200, description="Max number of events to return"),
):
    events = repo.scan_recent_events(limit=limit)
    return [e.model_dump(mode="json") for e in events]


@router.get(
    "/monitor",
    response_class=HTMLResponse,
    summary="Live-updating dashboard: Aurora canonical data, projection, and recent events",
)
def monitor_dashboard() -> HTMLResponse:
    return HTMLResponse(content=_DASHBOARD_TEMPLATE)


_DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Ingestion monitor</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; color: #222; }
    h1 { font-size: 1.4rem; }
    h2 { font-size: 1.05rem; margin-top: 2rem; }
    table { border-collapse: collapse; width: 100%; margin-top: 0.5rem; }
    th, td { text-align: left; padding: 0.4rem 0.75rem; border-bottom: 1px solid #ddd; font-size: 0.9rem; }
    th { background: #f5f5f5; }
    .status { color: #666; font-size: 0.85rem; }
    .error { color: #b00020; }
    .cols { display: flex; gap: 2rem; flex-wrap: wrap; }
    .cols > div { flex: 1 1 320px; }
  </style>
</head>
<body>
  <h1>Ingestion monitor</h1>
  <div class="status" id="status">Loading&hellip;</div>

  <div class="cols">
    <div>
      <h2>Aurora (source of truth)</h2>
      <table>
        <thead><tr><th>Competition</th><th>Game ID</th><th>Teams</th></tr></thead>
        <tbody id="aurora-body"></tbody>
      </table>
    </div>
    <div>
      <h2>Canonical projection (consumer reads this)</h2>
      <table>
        <thead><tr><th>Competition</th><th>Game ID</th><th>Teams</th></tr></thead>
        <tbody id="projection-body"></tbody>
      </table>
    </div>
  </div>

  <h2>Recent events (DynamoDB event store)</h2>
  <table>
    <thead><tr><th>Competition</th><th>Game ID</th><th>Type</th><th>Minute</th><th>Team</th><th>Player</th><th>Ingested At</th></tr></thead>
    <tbody id="events-body"></tbody>
  </table>

  <script>
    const statusEl = document.getElementById("status");

    function rows(items, mapper) {
      return items.map(mapper).join("");
    }

    async function refresh() {
      try {
        const [aurora, projection, events] = await Promise.all([
          fetch("/api/monitor/canonical/aurora", { cache: "no-store" }).then(r => r.json()),
          fetch("/api/monitor/canonical/projection", { cache: "no-store" }).then(r => r.json()),
          fetch("/api/monitor/events/recent?limit=50", { cache: "no-store" }).then(r => r.json()),
        ]);

        document.getElementById("aurora-body").innerHTML = rows(aurora, g => `
          <tr><td>${g.competition_name}</td><td>${g.game_id}</td><td>${g.team_names.join(", ")}</td></tr>
        `);
        document.getElementById("projection-body").innerHTML = rows(projection, g => `
          <tr><td>${g.competition_name}</td><td>${g.game_id}</td><td>${g.team_names.join(", ")}</td></tr>
        `);
        document.getElementById("events-body").innerHTML = rows(events, e => `
          <tr>
            <td>${e.competition_name}</td><td>${e.game_id}</td><td>${e.event_type}</td>
            <td>${e.minute}</td><td>${e.team}</td><td>${e.player || ""}</td>
            <td>${e.ingested_at}</td>
          </tr>
        `);
        statusEl.textContent = "Last updated: " + new Date().toLocaleTimeString();
        statusEl.className = "status";
      } catch (err) {
        statusEl.textContent = "Failed to refresh: " + err;
        statusEl.className = "status error";
      }
    }

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""
