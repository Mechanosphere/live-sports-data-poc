"""Central logging configuration, shared by the API process and the
standalone ingestion worker processes (poller, consumer).

Having one place that calls `logging.basicConfig` (idempotently) means all
three processes -- and every module's `logging.getLogger(__name__)` logger
-- share the same format and level, controlled by `LOG_LEVEL` in settings.
"""
from __future__ import annotations

import logging

from app.core.config import get_settings

_configured = False


def configure_logging() -> None:
    """Configure the root logger. Safe to call multiple times (no-op after
    the first call), so every entrypoint (`app.main`, `run_poller`,
    `run_consumer`) can call it unconditionally on import/startup.
    """
    global _configured
    if _configured:
        return

    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    _configured = True
