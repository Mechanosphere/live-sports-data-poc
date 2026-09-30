import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.api.routes import games, health, monitor, runner
from app.core.config import get_settings
from app.core.logging_config import configure_logging
from app.ingestion import canonical

configure_logging()
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Seed/sync this process's own canonical mock stores.
    #
    # `app.ingestion.canonical`'s mock Aurora/projection stores are
    # process-local (see its module docstring) -- they're normally
    # seeded/synced by the consumer worker process(es)
    # (`app.ingestion.run_consumer`), not by the API process. Without this,
    # `GET /api/monitor/canonical/{aurora,projection}` would always show
    # empty here even while the consumer's own copies are populated and
    # validating real events. Mirrors `run_consumer.main()`'s startup so
    # the monitor dashboard reflects the same (mock) canonical data.
    if settings.canonical_backend != "postgres":
        canonical.seed_mock_canonical_data()
    canonical.start_periodic_sync(settings.canonical_sync_interval_seconds)
    try:
        yield
    finally:
        canonical.stop_periodic_sync()


app = FastAPI(title=settings.project_name, lifespan=_lifespan)

app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(games.router, prefix="/api", tags=["games"])
app.include_router(runner.router, prefix="/api", tags=["runner"])
app.include_router(monitor.router, prefix="/api", tags=["monitor"])


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log every request/response with method, path, status, and latency.

    Gives full visibility into API traffic (which endpoints are hit, how
    often, how slowly) alongside the ingestion-pipeline latency logs
    already emitted by the Kafka producer/consumer.
    """
    started_at = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - started_at) * 1000
    logger.info(
        "%s %s -> %s (%.1fms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.get("/")
def root():
    return {"message": "Welcome to LiveSportsData API"}
