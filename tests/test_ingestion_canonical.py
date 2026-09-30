import logging
import time
import uuid

import pytest

from app.core.config import get_settings
from app.ingestion import canonical

try:
    import psycopg2

    _AURORA_TEST_DSN = "postgresql://canonical:canonical@localhost:5432/canonical"
    with psycopg2.connect(_AURORA_TEST_DSN):
        pass
    _AURORA_AVAILABLE = True
except Exception:
    _AURORA_AVAILABLE = False

requires_aurora = pytest.mark.skipif(
    not _AURORA_AVAILABLE,
    reason="local aurora-postgres container (docker compose) not reachable",
)


def test_register_and_get_game():
    store = canonical.MockCanonicalStore()
    store.register_game("BUNDESLIGA", "sfx-8841", team_names=["Bayern", "Dortmund"])

    game = store.get_game("bundesliga", "sfx-8841")
    assert game is not None
    assert game.competition_name == "BUNDESLIGA"
    assert game.game_id == "sfx-8841"
    assert game.team_names == {"Bayern", "Dortmund"}


def test_has_game_false_when_unregistered():
    store = canonical.MockCanonicalStore()
    assert store.has_game("BUNDESLIGA", "unknown-game") is False


def test_clear_removes_all_games():
    store = canonical.MockCanonicalStore()
    store.register_game("BUNDESLIGA", "sfx-8841")
    store.clear()
    assert store.has_game("BUNDESLIGA", "sfx-8841") is False


def test_seed_mock_canonical_data_populates_known_games():
    aurora = canonical.get_aurora_store()
    projection = canonical.get_projection()
    aurora.clear()
    projection.clear()
    try:
        canonical.seed_mock_canonical_data()
        # Seeding writes to Aurora...
        assert aurora.has_game("BUNDESLIGA", "sfx-8841") is True
        assert aurora.has_game("BUNDESLIGA", "lsc-2291") is True
        # ...and syncs immediately into the projection the consumer reads.
        assert projection.has_game("BUNDESLIGA", "sfx-8841") is True
        assert projection.has_game("BUNDESLIGA", "lsc-2291") is True
    finally:
        aurora.clear()
        projection.clear()


def test_check_game_exists_warns_but_does_not_raise_when_missing(caplog):
    projection = canonical.get_projection()
    projection.clear()
    try:
        with caplog.at_level(logging.WARNING):
            canonical.check_game_exists("UNKNOWN-COMPETITION", "unknown-game")
        assert any(
            "No canonical data found" in record.message for record in caplog.records
        )
    finally:
        projection.clear()


def test_check_game_exists_silent_when_present(caplog):
    projection = canonical.get_projection()
    projection.clear()
    try:
        projection.register_game("BUNDESLIGA", "sfx-8841")
        with caplog.at_level(logging.WARNING):
            canonical.check_game_exists("BUNDESLIGA", "sfx-8841")
        assert not any(
            "No canonical data found" in record.message for record in caplog.records
        )
    finally:
        projection.clear()


def test_sync_canonical_projection_copies_aurora_games_only():
    aurora = canonical.get_aurora_store()
    projection = canonical.get_projection()
    aurora.clear()
    projection.clear()
    try:
        aurora.register_game("PREMIER COMPETITION", "epl-101", team_names=["Arsenal", "Chelsea"])
        assert projection.has_game("PREMIER COMPETITION", "epl-101") is False

        synced = canonical.sync_canonical_projection()

        assert synced == 1
        assert projection.has_game("PREMIER COMPETITION", "epl-101") is True
    finally:
        aurora.clear()
        projection.clear()


def test_check_game_exists_does_not_query_aurora_directly(caplog):
    """The per-event hot path reads the projection, not Aurora -- a game
    registered only in Aurora (not yet synced) should still be reported
    missing until a sync runs."""
    aurora = canonical.get_aurora_store()
    projection = canonical.get_projection()
    aurora.clear()
    projection.clear()
    try:
        aurora.register_game("SERIE A", "sa-77")
        with caplog.at_level(logging.WARNING):
            canonical.check_game_exists("SERIE A", "sa-77")
        assert any(
            "No canonical data found" in record.message for record in caplog.records
        )
    finally:
        aurora.clear()
        projection.clear()


def test_check_game_exists_raises_in_strict_mode_when_missing():
    settings = get_settings()
    projection = canonical.get_projection()
    projection.clear()
    original = settings.canonical_strict_mode
    settings.canonical_strict_mode = True
    try:
        with pytest.raises(canonical.CanonicalDataMissing):
            canonical.check_game_exists("UNKNOWN-COMPETITION", "unknown-game")
    finally:
        settings.canonical_strict_mode = original
        projection.clear()


def test_check_game_exists_strict_mode_does_not_raise_when_present():
    settings = get_settings()
    projection = canonical.get_projection()
    projection.clear()
    original = settings.canonical_strict_mode
    settings.canonical_strict_mode = True
    try:
        projection.register_game("BUNDESLIGA", "sfx-8841")
        canonical.check_game_exists("BUNDESLIGA", "sfx-8841")  # should not raise
    finally:
        settings.canonical_strict_mode = original
        projection.clear()


def test_get_aurora_store_returns_mock_by_default():
    settings = get_settings()
    assert settings.canonical_backend == "mock"
    assert canonical.get_aurora_store() is canonical.get_store()


def test_get_aurora_store_postgres_backend_requires_dsn():
    settings = get_settings()
    original_backend = settings.canonical_backend
    original_dsn = settings.canonical_aurora_dsn
    settings.canonical_backend = "postgres"
    settings.canonical_aurora_dsn = None
    try:
        with pytest.raises(RuntimeError):
            canonical.get_aurora_store()
    finally:
        settings.canonical_backend = original_backend
        settings.canonical_aurora_dsn = original_dsn


def test_get_aurora_store_postgres_backend_returns_postgres_store():
    settings = get_settings()
    original_backend = settings.canonical_backend
    original_dsn = settings.canonical_aurora_dsn
    original_postgres_store = canonical._postgres_store
    settings.canonical_backend = "postgres"
    settings.canonical_aurora_dsn = "postgresql://user:pass@localhost/db"
    canonical._postgres_store = None
    try:
        store = canonical.get_aurora_store()
        assert isinstance(store, canonical.PostgresCanonicalStore)
    finally:
        settings.canonical_backend = original_backend
        settings.canonical_aurora_dsn = original_dsn
        canonical._postgres_store = original_postgres_store


@requires_aurora
def test_postgres_canonical_store_against_real_aurora_postgres():
    """End-to-end check of `PostgresCanonicalStore` against a real local
    Postgres (docker compose `aurora-postgres` service), not just a mocked
    DSN/connection like the tests above."""
    store = canonical.PostgresCanonicalStore(_AURORA_TEST_DSN)
    competition = f"POSTGRES-IT-{uuid.uuid4().hex[:8]}".upper()
    game_id = "pg-game-1"

    conn = psycopg2.connect(_AURORA_TEST_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO games (competition_name, game_id, team_names, start_time, updated_at) "
                "VALUES (%s, %s, %s, now() + interval '2 hours', now())",
                (competition, game_id, ["Arsenal", "Chelsea"]),
            )

        assert store.has_game(competition, game_id) is True
        game = store.get_game(competition, game_id)
        assert game is not None
        assert game.team_names == {"Arsenal", "Chelsea"}

        window_games = store.all_games(window_hours=48)
        assert any(g.competition_name == competition and g.game_id == game_id for g in window_games)

        assert store.has_game(competition, "no-such-game") is False
    finally:
        with conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM games WHERE competition_name = %s AND game_id = %s",
                (competition, game_id),
            )
        conn.close()


@requires_aurora
def test_get_aurora_store_postgres_backend_reads_real_data():
    """`get_aurora_store()` with the real postgres backend selected talks
    to the real database end-to-end (not a mocked connection)."""
    settings = get_settings()
    original_backend = settings.canonical_backend
    original_dsn = settings.canonical_aurora_dsn
    original_postgres_store = canonical._postgres_store
    settings.canonical_backend = "postgres"
    settings.canonical_aurora_dsn = _AURORA_TEST_DSN
    canonical._postgres_store = None

    competition = f"POSTGRES-IT2-{uuid.uuid4().hex[:8]}".upper()
    game_id = "pg-game-2"
    conn = psycopg2.connect(_AURORA_TEST_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO games (competition_name, game_id, team_names, start_time, updated_at) "
                "VALUES (%s, %s, %s, now() + interval '1 hour', now())",
                (competition, game_id, ["Real", "Madrid"]),
            )

        store = canonical.get_aurora_store()
        assert isinstance(store, canonical.PostgresCanonicalStore)
        assert store.has_game(competition, game_id) is True
    finally:
        with conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM games WHERE competition_name = %s AND game_id = %s",
                (competition, game_id),
            )
        conn.close()
        settings.canonical_backend = original_backend
        settings.canonical_aurora_dsn = original_dsn
        canonical._postgres_store = original_postgres_store


def test_start_periodic_sync_runs_on_interval():
    aurora = canonical.get_aurora_store()
    projection = canonical.get_projection()
    aurora.clear()
    projection.clear()
    try:
        aurora.register_game("LA LIGA", "ll-9")
        assert projection.has_game("LA LIGA", "ll-9") is False

        canonical.start_periodic_sync(interval_seconds=0.05)
        time.sleep(0.2)

        assert projection.has_game("LA LIGA", "ll-9") is True
    finally:
        canonical.stop_periodic_sync()
        aurora.clear()
        projection.clear()
