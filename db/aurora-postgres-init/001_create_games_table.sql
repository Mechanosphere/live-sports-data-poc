-- Schema for the canonical "games" table, as expected by
-- app.ingestion.canonical.PostgresCanonicalStore.
--
-- Auto-run by the official postgres image on first container startup
-- (files in /docker-entrypoint-initdb.d/, mounted read-only from this
-- directory -- see the `aurora-postgres` service in docker-compose.yml).
-- Only runs against an empty data directory/volume; it will NOT re-run
-- (or apply migrations) on a container that already has data.
CREATE TABLE IF NOT EXISTS games (
    competition_name TEXT NOT NULL,
    game_id TEXT NOT NULL,
    team_names TEXT[] NOT NULL DEFAULT '{}',
    start_time TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (competition_name, game_id)
);
