"""Runner endpoint: manually trigger a scripted sequence of game events.

Lets you simulate a live match for any `competition_name`/`game_id` without
waiting for the poller's hardcoded stub providers, by publishing a canned
sequence of raw, already-normalized-shaped events (provider_name="runner",
see `app.ingestion.normalizers.football.normalize_runner`) directly to
Kafka. Useful for exercising the full ingestion pipeline (Kafka -> consumer
-> DynamoDB) and the `/events/top` endpoint on demand, e.g. during a demo
or manual latency check. Supports football and basketball scripts,
selected via `RunnerRequest.sport` (see `_SCRIPTS`).
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from enum import Enum
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from app.ingestion.kafka_producer import get_event_producer
from app.models.keys import SPORT_BASKETBALL, SPORT_FOOTBALL, EventType

logger = logging.getLogger(__name__)

router = APIRouter()

# (event_type, minute, is_home_team, player)
#
# Two scripts, one per sport, keyed by the same `sport` string accepted by
# `RunnerRequest.sport`/`RunnerService.create_run` -- `minute` is reused for
# basketball's quarter number too (see `app.ingestion.normalizers.basketball`
# on why `GameEvent.minute` doubles as "quarter" for basketball events).
_FOOTBALL_SCRIPT: list[tuple[EventType, int, bool, str]] = [
    (EventType.KICKOFF, 0, True, ""),
    (EventType.GOAL, 23, True, "Muller"),
    (EventType.REDCARD, 41, False, "Reus"),
    (EventType.HALFTIME, 45, True, ""),
    (EventType.GOAL, 68, False, "Reus"),
    (EventType.FULLTIME, 90, True, ""),
]

_BASKETBALL_SCRIPT: list[tuple[EventType, int, bool, str]] = [
    (EventType.GAME_START, 1, True, ""),
    (EventType.THREE_POINTER, 1, True, "James"),
    (EventType.FOUL, 1, False, "Tatum"),
    (EventType.QUARTER_END, 1, True, ""),
    (EventType.TWO_POINTER, 2, False, "Tatum"),
    (EventType.FREE_THROW, 4, True, "James"),
    (EventType.GAME_END, 4, True, ""),
]

_SCRIPTS: dict[str, list[tuple[EventType, int, bool, str]]] = {
    SPORT_FOOTBALL: _FOOTBALL_SCRIPT,
    SPORT_BASKETBALL: _BASKETBALL_SCRIPT,
}
_DEFAULT_TEAMS: dict[str, tuple[str, str]] = {
    SPORT_FOOTBALL: ("Bayern", "Dortmund"),
    SPORT_BASKETBALL: ("Lakers", "Celtics"),
}


class RunStatus(str, Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"


class RunnerRequest(BaseModel):
    sport: str = SPORT_FOOTBALL
    #: Defaults to a sport-appropriate matchup (`_DEFAULT_TEAMS`) when left
    #: unset, so basketball runs don't default to football team names.
    team_home: Optional[str] = None
    team_away: Optional[str] = None
    delay_seconds: float = 2.0


class RunState(BaseModel):
    run_id: str
    sport: str
    competition_name: str
    game_id: str
    status: RunStatus
    events_published: int
    events_scheduled: int
    delay_seconds: float
    error: Optional[str] = None


class _RunHandle:
    """Mutable, thread-safe state for one in-flight/completed runner run.

    The background task (running in FastAPI's threadpool) mutates `state`
    and checks `stop_event` between publishes; the stop/status endpoints
    read `state` and set `stop_event` from the request-handling thread.
    """

    def __init__(
        self, run_id: str, sport: str, competition_name: str, game_id: str, delay_seconds: float
    ) -> None:
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.state = RunState(
            run_id=run_id,
            sport=sport,
            competition_name=competition_name,
            game_id=game_id,
            status=RunStatus.RUNNING,
            events_published=0,
            events_scheduled=len(_SCRIPTS[sport]),
            delay_seconds=delay_seconds,
        )


class RunnerService:
    """Owns the in-memory registry of runner runs and the publish worker.

    Encapsulating this in a class (rather than module-level globals/
    functions) makes the run registry/lock an explicit, injectable
    dependency -- e.g. tests can construct an isolated `RunnerService()`
    instead of mutating shared module state. Still process-local/in-memory,
    fine for this single-process POC; a multi-worker deployment would need
    shared storage (e.g. a DynamoDB item or Redis key) instead.
    """

    def __init__(self) -> None:
        self._runs: dict[str, _RunHandle] = {}
        self._lock = threading.Lock()

    def create_run(
        self, sport: str, competition_name: str, game_id: str, delay_seconds: float
    ) -> _RunHandle:
        """Register a new run and return its handle (not yet publishing)."""
        run_id = uuid.uuid4().hex
        handle = _RunHandle(run_id, sport, competition_name, game_id, delay_seconds)
        with self._lock:
            self._runs[run_id] = handle
        return handle

    def publish_script(self, handle: _RunHandle, team_home: str, team_away: str) -> None:
        """Publish the sport's script to Kafka for `handle`, honoring stop requests.

        Intended to run in a background task/thread; mutates `handle.state`
        as it progresses so `get_run`/`list_runs` reflect live progress.
        """
        producer = get_event_producer()
        script = _SCRIPTS[handle.state.sport]
        try:
            for event_type, minute, is_home, player in script:
                if handle.stop_event.is_set():
                    with handle.lock:
                        handle.state.status = RunStatus.STOPPED
                    logger.info("Runner run %s stopped early", handle.state.run_id)
                    return

                raw = {
                    "sport": handle.state.sport,
                    "competition_name": handle.state.competition_name,
                    "game_id": handle.state.game_id,
                    "event_type": event_type.value,
                    "player": player,
                    "team": team_home if is_home else team_away,
                    "minute": minute,
                }
                key = f"{handle.state.competition_name}#{handle.state.game_id}"
                producer.publish_raw("runner", handle.state.sport, raw, key)
                producer.flush(timeout=1.0)
                logger.info(
                    "Runner run %s published %s for %s/%s",
                    handle.state.run_id,
                    event_type.value,
                    handle.state.competition_name,
                    handle.state.game_id,
                )
                with handle.lock:
                    handle.state.events_published += 1

                if handle.state.delay_seconds > 0:
                    # Sleep in short slices so a stop request is honored
                    # quickly instead of waiting out the full delay.
                    remaining = handle.state.delay_seconds
                    while remaining > 0 and not handle.stop_event.is_set():
                        step = min(0.2, remaining)
                        time.sleep(step)
                        remaining -= step
            with handle.lock:
                if handle.state.status == RunStatus.RUNNING:
                    handle.state.status = RunStatus.COMPLETED
        except Exception as exc:  # noqa: BLE001 - surfaced via run status, not raised
            logger.exception("Runner run %s failed", handle.state.run_id)
            with handle.lock:
                handle.state.status = RunStatus.FAILED
                handle.state.error = str(exc)

    def list_runs(self, status: Optional[RunStatus] = None) -> list[RunState]:
        with self._lock:
            handles = list(self._runs.values())
        states = []
        for handle in handles:
            with handle.lock:
                states.append(handle.state.model_copy())
        if status is not None:
            states = [s for s in states if s.status == status]
        return states

    def get_run(self, run_id: str) -> _RunHandle | None:
        return self._runs.get(run_id)

    def stop_run(self, run_id: str) -> _RunHandle | None:
        handle = self._runs.get(run_id)
        if handle is not None:
            handle.stop_event.set()
        return handle


# Process-wide runner registry backing the endpoints below.
_runner_service = RunnerService()


@router.post(
    "/runner/competitions/{competition_name}/games/{game_id}/events",
    response_model=RunState,
    summary="Start a scripted sequence of game events for a competition/game",
)
def start_runner(
    competition_name: str,
    game_id: str,
    background_tasks: BackgroundTasks,
    request: Optional[RunnerRequest] = None,
) -> RunState:
    """Publish a canned KICKOFF->GOAL->REDCARD->HALFTIME->GOAL->FULLTIME
    sequence of events to Kafka for the given competition/game, spaced
    `delay_seconds` apart.

    Runs in a background task so the request returns immediately with a
    `run_id`; events land in Kafka (and then DynamoDB, via the running
    consumer) over the following `delay_seconds * len(script)` seconds --
    approximating a live match trickling in. Use `GET /runner/runs/{run_id}`
    to check progress and `POST /runner/runs/{run_id}/stop` to cancel it
    before it finishes.
    """
    body = request or RunnerRequest()
    if body.sport not in _SCRIPTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported sport {body.sport!r}; expected one of {sorted(_SCRIPTS)}",
        )
    default_home, default_away = _DEFAULT_TEAMS[body.sport]
    team_home = body.team_home or default_home
    team_away = body.team_away or default_away
    handle = _runner_service.create_run(
        body.sport, competition_name.upper(), game_id, body.delay_seconds
    )
    background_tasks.add_task(_runner_service.publish_script, handle, team_home, team_away)
    return handle.state


@router.get(
    "/runner/runs",
    response_model=list[RunState],
    summary="List runner runs, optionally filtered by status",
)
def list_runner_runs(status: Optional[RunStatus] = None) -> list[RunState]:
    """Return all known runner runs (in-memory, this process only).

    Pass `?status=running` to list only currently in-flight runs, or omit
    it to see every run (running/stopped/completed/failed) since process
    start.
    """
    return _runner_service.list_runs(status)


@router.get(
    "/runner/runs/{run_id}",
    response_model=RunState,
    summary="Check the status/progress of a runner run",
)
def get_runner_status(run_id: str) -> RunState:
    """Return the current status of a runner run started via `POST
    /runner/competitions/{competition_name}/games/{game_id}/events`."""
    handle = _runner_service.get_run(run_id)
    if handle is None:
        raise HTTPException(status_code=404, detail="No such runner run")
    with handle.lock:
        return handle.state.model_copy()


@router.post(
    "/runner/runs/{run_id}/stop",
    response_model=RunState,
    summary="Stop an in-progress runner run",
)
def stop_runner(run_id: str) -> RunState:
    """Request cancellation of a running runner run.

    Idempotent: stopping an already-stopped/completed/failed run just
    returns its current (unchanged) status.
    """
    handle = _runner_service.stop_run(run_id)
    if handle is None:
        raise HTTPException(status_code=404, detail="No such runner run")
    with handle.lock:
        return handle.state.model_copy()
