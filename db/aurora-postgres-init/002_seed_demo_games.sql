-- Seeds the real Aurora Postgres backend with the same canonical games the
-- mock backend seeds via app.ingestion.canonical.seed_mock_canonical_data()
-- (football) plus the HoopStream basketball demo game, so CANONICAL_BACKEND
-- =postgres has real canonical data to validate the POC's stub providers
-- against without any manual `psql` steps.
--
-- Auto-run by the official postgres image on first container startup, after
-- 001_create_games_table.sql (files run in filename order). Only runs
-- against an empty data directory/volume, like 001.
--
-- start_time is set to now() so every row falls inside the sync job's
-- default lookahead window (CANONICAL_SYNC_WINDOW_HOURS=48h, see
-- app.ingestion.canonical) as soon as the container starts.
INSERT INTO games (competition_name, game_id, team_names, start_time, updated_at) VALUES
    ('BUNDESLIGA', 'sfx-8841', ARRAY['Bayern', 'Dortmund'], now(), now()),
    ('BUNDESLIGA', 'lsc-2291', ARRAY['Bayern', 'Dortmund'], now(), now()),
    ('NBA', 'hps-3301', ARRAY['Lakers', 'Celtics'], now(), now())
ON CONFLICT (competition_name, game_id) DO NOTHING;
