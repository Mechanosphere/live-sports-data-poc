"""Canonical metadata lookup (competitions/games/teams).

In the target architecture, canonical entities (Competition, Game, Team) are
owned and published by a separate external system (Aurora Postgres RDS,
in the intended design) that knows about a game 12h+ before it starts.
This pipeline is only ever a *consumer* of that data -- it looks up/
validates against canonical IDs, it does not create them.

Two-tier lookup design (mirrors the target architecture, mocked here):

    Aurora Postgres (source of truth)
            |  scheduled sync job (see `sync_canonical_projection`),
            |  pulls games in the upcoming window ahead of time
            v
    Canonical projection (fast local read; a DynamoDB table in
    production, an in-memory `CanonicalProjection` here)
            |  read by `check_game_exists()` in the consumer's hot path
            v
    Kafka consumer's per-event canonical check

The live event path (`check_game_exists`) never queries the source of
truth directly -- only the sync job does, on a schedule, well ahead of
when events for a game actually start flowing. This keeps the per-event
hot path a fast local read regardless of event volume. Cancelled games
need no special handling: a cancelled game simply never produces events,
so it's never looked up; stale projection entries can be left to expire
(e.g. via a DynamoDB TTL keyed off the game's start time) rather than
requiring active reconciliation/deletion.

That external system doesn't exist yet, so this module mocks both tiers
with in-memory stores. Swap `get_aurora_store()`'s backing implementation
for a real Aurora client and `sync_canonical_projection()` for a real
scheduled job writing into a real DynamoDB projection table once the real
canonical system is available -- the store interfaces below are meant to
stay stable across that swap.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from threading import Lock
from typing import Protocol

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class CanonicalDataMissing(Exception):
    """Raised when a provider event references canonical data we don't know about.

    Raised by `check_game_exists` only when `Settings.canonical_strict_mode`
    is enabled (see its docstring) -- appropriate once a real sync job is
    seeding the projection well ahead of game time, so a miss becomes a
    real signal rather than expected POC/dev-mode noise.
    """


@dataclass
class CanonicalGame:
    competition_name: str
    game_id: str
    team_names: set[str] = field(default_factory=set)


class CanonicalSourceStore(Protocol):
    """Interface the sync job needs from a canonical source of truth.

    Implemented by `MockCanonicalStore` (POC/dev/test) and
    `PostgresCanonicalStore` (real Aurora Postgres RDS client) so
    `sync_canonical_projection()` works against either without change.
    """

    def has_game(self, competition_name: str, game_id: str) -> bool: ...

    def get_game(self, competition_name: str, game_id: str) -> CanonicalGame | None: ...

    def all_games(self, window_hours: float | None = None) -> list[CanonicalGame]: ...


class MockCanonicalStore:
    """In-memory stand-in for a canonical-data store.

    Thread-safe, process-local, non-persistent. Used for both tiers of the
    two-tier design: as the "Aurora" source-of-truth mock (`get_aurora_store`)
    and as the "DynamoDB projection" mock (`get_projection`) -- in production
    these would be two different technologies, but the read/write shape each
    tier needs from this POC is identical. Seed the Aurora-side store via
    `register_game()` or `seed_mock_canonical_data()`; populate the
    projection from it via `sync_canonical_projection()`.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._games: dict[tuple[str, str], CanonicalGame] = {}

    def register_game(
        self, competition_name: str, game_id: str, team_names: list[str] | None = None
    ) -> None:
        key = (competition_name.upper(), game_id)
        with self._lock:
            self._games[key] = CanonicalGame(
                competition_name=competition_name.upper(),
                game_id=game_id,
                team_names=set(team_names or []),
            )

    def get_game(self, competition_name: str, game_id: str) -> CanonicalGame | None:
        with self._lock:
            return self._games.get((competition_name.upper(), game_id))

    def has_game(self, competition_name: str, game_id: str) -> bool:
        return self.get_game(competition_name, game_id) is not None

    def all_games(self, window_hours: float | None = None) -> list[CanonicalGame]:
        # `window_hours` is accepted for interface parity with
        # `PostgresCanonicalStore` but ignored here -- the mock has no
        # notion of a start_time to filter on.
        with self._lock:
            return list(self._games.values())

    def clear(self) -> None:
        with self._lock:
            self._games.clear()


class PostgresCanonicalStore:
    """Real Aurora Postgres RDS-backed canonical source-of-truth store.

    Talks to a `games` table (owned/populated by the external canonical
    system) expected to have at least: `competition_name`, `game_id`,
    `team_names` (array or joinable), and `start_time`/`updated_at`
    columns. Uses `psycopg2`, imported lazily so the rest of the app
    (and the mock backend) works without that dependency installed --
    it's only required when `Settings.canonical_backend == "postgres"`.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def _connect(self):
        import psycopg2  # lazy import: only required for the postgres backend

        return psycopg2.connect(self._dsn)

    def get_game(self, competition_name: str, game_id: str) -> CanonicalGame | None:
        query = (
            "SELECT competition_name, game_id, team_names FROM games "
            "WHERE competition_name = %s AND game_id = %s"
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, (competition_name.upper(), game_id))
            row = cur.fetchone()
        if row is None:
            return None
        return CanonicalGame(
            competition_name=row[0], game_id=row[1], team_names=set(row[2] or [])
        )

    def has_game(self, competition_name: str, game_id: str) -> bool:
        return self.get_game(competition_name, game_id) is not None

    def all_games(self, window_hours: float | None = None) -> list[CanonicalGame]:
        """Fetch games starting within `window_hours`, plus any recently updated.

        Mirrors the sync job's query described in this module's docstring:
        games in the upcoming window (new games) unioned with games updated
        in the last day regardless of `start_time` (to catch reschedules
        that moved a game into or out of the window).
        """
        query = (
            "SELECT competition_name, game_id, team_names FROM games "
            "WHERE start_time BETWEEN now() AND now() + (%s || ' hours')::interval "
            "   OR updated_at > now() - interval '1 day'"
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, (window_hours,))
            rows = cur.fetchall()
        return [
            CanonicalGame(competition_name=row[0], game_id=row[1], team_names=set(row[2] or []))
            for row in rows
        ]


# Tier 1: the Aurora Postgres source of truth (or its in-memory mock, for
# POC/dev/test -- selected by `Settings.canonical_backend`).
_aurora_store = MockCanonicalStore()
_postgres_store: PostgresCanonicalStore | None = None
# Tier 2: mocks the DynamoDB canonical projection the consumer actually
# reads from on the per-event hot path (kept in sync from tier 1 by
# `sync_canonical_projection`, on a schedule in production).
_projection = MockCanonicalStore()

_sync_timer: threading.Timer | None = None


def get_aurora_store() -> CanonicalSourceStore:
    """Return the process-wide Aurora (source-of-truth) store.

    Backed by `MockCanonicalStore` (default) or a real
    `PostgresCanonicalStore`, selected by `Settings.canonical_backend`.
    The Postgres client is constructed lazily and cached so importing this
    module never requires `psycopg2` unless the postgres backend is
    actually selected.
    """
    global _postgres_store
    settings = get_settings()
    if settings.canonical_backend == "postgres":
        if _postgres_store is None:
            if not settings.canonical_aurora_dsn:
                raise RuntimeError(
                    "canonical_backend='postgres' requires canonical_aurora_dsn to be set"
                )
            _postgres_store = PostgresCanonicalStore(settings.canonical_aurora_dsn)
        return _postgres_store
    return _aurora_store


def get_store() -> CanonicalSourceStore:
    """Deprecated alias for `get_aurora_store()`, kept for backward compatibility."""
    return get_aurora_store()


def get_projection() -> MockCanonicalStore:
    """Return the process-wide mock canonical projection (consumer reads this)."""
    return _projection


def sync_canonical_projection() -> int:
    """Copy all games from the Aurora mock into the projection.

    Simulates the scheduled sync job: in production this would run in its
    own process/task on an interval (e.g. every 15-30 min), querying Aurora
    for games starting in the next 12-48h (plus any recently updated, to
    catch reschedules) and upserting them into a DynamoDB projection table
    with a TTL so entries expire naturally after the game's start time --
    no explicit reconciliation is needed for cancelled games, since a
    cancelled game simply never produces events to look up.

    Returns the number of games synced.
    """
    settings = get_settings()
    games = get_aurora_store().all_games(window_hours=settings.canonical_sync_window_hours)
    for game in games:
        _projection.register_game(
            game.competition_name, game.game_id, team_names=sorted(game.team_names)
        )
    logger.info("Synced %d canonical game(s) from Aurora into the projection.", len(games))
    return len(games)


def start_periodic_sync(interval_seconds: float) -> threading.Timer:
    """Start a background timer that re-runs `sync_canonical_projection` on an interval.

    Stand-in, within a single process, for a real standalone scheduled sync
    job/service. Returns the `threading.Timer` so the caller can `.cancel()`
    it on shutdown.
    """
    global _sync_timer

    def _tick() -> None:
        global _sync_timer
        sync_canonical_projection()
        _sync_timer = threading.Timer(interval_seconds, _tick)
        _sync_timer.daemon = True
        _sync_timer.start()

    _sync_timer = threading.Timer(interval_seconds, _tick)
    _sync_timer.daemon = True
    _sync_timer.start()
    return _sync_timer


def stop_periodic_sync() -> None:
    global _sync_timer
    if _sync_timer is not None:
        _sync_timer.cancel()
        _sync_timer = None


def seed_mock_canonical_data() -> None:
    """Populate the Aurora mock with canonical data matching the POC's stub providers.

    Mirrors the scripted matches in `app.ingestion.providers.sportsfeedx`,
    `app.ingestion.providers.livescorecore`, and
    `app.ingestion.providers.basketball_stub` (HoopStream) so the consumer
    has real canonical data to validate the default demo scenario against
    for both sports. Real deployments get this data from the external
    canonical system instead. Also runs an immediate sync so the projection
    is populated right away (in production, the sync job's own schedule --
    run well ahead of game time -- would already have populated it before
    any events arrive).

    Kept in sync with `db/aurora-postgres-init/002_seed_demo_games.sql`,
    which seeds the same games into a real Postgres backend.
    """
    _aurora_store.register_game("BUNDESLIGA", "sfx-8841", team_names=["Bayern", "Dortmund"])
    _aurora_store.register_game("BUNDESLIGA", "lsc-2291", team_names=["Bayern", "Dortmund"])
    _aurora_store.register_game("NBA", "hps-3301", team_names=["Lakers", "Celtics"])
    sync_canonical_projection()


def check_game_exists(competition_name: str, game_id: str) -> None:
    """Validate that canonical data exists for (competition, game).

    Reads the projection (tier 2), not the Aurora source of truth directly
    -- this is the per-event hot path and must stay a fast local read
    regardless of event volume; see the sync-job design in this module's
    docstring.

    Behavior on a miss depends on `Settings.canonical_strict_mode`:

    - Permissive (default, `canonical_strict_mode=False`): logs a warning
      and proceeds. Appropriate for POC/dev/test, where ad-hoc/dynamic
      competitions aren't pre-seeded into the mock Aurora store.
    - Strict (`canonical_strict_mode=True`): raises `CanonicalDataMissing`.
      Appropriate once a real sync job is proactively seeding the
      projection well ahead of game time (see module docstring) -- a miss
      then means the event's game genuinely isn't known yet, which is a
      real signal the caller should act on (e.g. dead-letter the event)
      rather than a mock-data gap.
    """
    if not _projection.has_game(competition_name, game_id):
        if get_settings().canonical_strict_mode:
            raise CanonicalDataMissing(
                f"No canonical data found for competition={competition_name} game_id={game_id} "
                "in canonical projection"
            )
        logger.warning(
            "No canonical data found for competition=%s game_id=%s in canonical "
            "projection; proceeding anyway (mock canonical data / dev mode)",
            competition_name,
            game_id,
        )
