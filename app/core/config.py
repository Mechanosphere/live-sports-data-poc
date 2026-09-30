from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    project_name: str = "LiveSportsData"
    api_v1_prefix: str = "/api/v1"
    environment: str = "development"

    # DynamoDB
    dynamodb_endpoint_url: str | None = "http://localhost:8000"
    dynamodb_table_name: str = "live-sports-data"
    aws_region: str = "us-east-1"
    aws_access_key_id: str = "fakeMyKeyId"
    aws_secret_access_key: str = "fakeSecretAccessKey"

    # Kafka
    # Use the IPv4 loopback rather than "localhost": on hosts where
    # "localhost" resolves to IPv6 (::1) first (e.g. Docker Desktop/colima,
    # which only publish the broker port on IPv4 127.0.0.1), a "localhost"
    # bootstrap address makes librdkafka try ::1:9092 first and hit
    # "Connection refused", producing flaky client connections. Override via
    # KAFKA_BOOTSTRAP_SERVERS in real deployments.
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    # Per-sport topics/consumer groups so each sport's ingestion can run as
    # an independent process (different throughput, deploy cadence, etc.)
    # without one sport's backlog affecting another's.
    kafka_topic_football_events: str = "football.events"
    kafka_topic_basketball_events: str = "basketball.events"
    kafka_consumer_group_football: str = "football-ingestion-consumer"
    kafka_consumer_group_basketball: str = "basketball-ingestion-consumer"
    # Dead-letter topic for events rejected by a strict canonical-data miss
    # (see `canonical_strict_mode`) -- the raw envelope is republished here
    # unmodified so it can be inspected/replayed once canonical data catches up.
    kafka_topic_dead_letter: str = "ingestion.dead-letter"

    # Ingestion
    provider_poll_interval_seconds: float = 5.0
    # How often the (mocked) canonical sync job re-pulls from the Aurora
    # source of truth into the local canonical projection the consumer's
    # per-event hot path reads from. In production this job would query
    # Aurora for games starting in the next 12-48h (well ahead of kickoff)
    # so the projection is already warm before any events arrive.
    canonical_sync_interval_seconds: float = 900.0

    # Canonical data backend: "mock" (default, in-memory, POC/dev/test) or
    # "postgres" (real Aurora Postgres RDS client -- requires psycopg2 and
    # `canonical_aurora_dsn` to be set).
    canonical_backend: str = "mock"
    canonical_aurora_dsn: str | None = None
    # How far ahead of kickoff the sync job pulls games from Aurora (also
    # matches any row with updated_at in the last day, to catch reschedules
    # of games outside this window).
    canonical_sync_window_hours: float = 48.0
    # Whether a canonical miss should raise `CanonicalDataMissing` (strict --
    # appropriate once the real Aurora sync job is seeding the projection
    # well ahead of game time, so a miss is a real signal) instead of just
    # warning and proceeding (permissive -- current mock/dev-mode default,
    # since ad-hoc/dynamic test competitions aren't pre-seeded).
    canonical_strict_mode: bool = False

    # Logging
    log_level: str = "INFO"

    def topic_for_sport(self, sport: str) -> str:
        return {
            "FOOTBALL": self.kafka_topic_football_events,
            "BASKETBALL": self.kafka_topic_basketball_events,
        }.get(sport.upper(), self.kafka_topic_football_events)

    def consumer_group_for_sport(self, sport: str) -> str:
        return {
            "FOOTBALL": self.kafka_consumer_group_football,
            "BASKETBALL": self.kafka_consumer_group_basketball,
        }.get(sport.upper(), self.kafka_consumer_group_football)


@lru_cache
def get_settings() -> Settings:
    return Settings()
