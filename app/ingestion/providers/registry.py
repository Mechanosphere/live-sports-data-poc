"""Provider factory registry for the poller entrypoint.

Importing `app.ingestion.providers` registers all built-in provider
factories as a side effect, so `run_poller` can instantiate every
available provider without keeping a hardcoded list in sync.
"""
from __future__ import annotations

import logging
from typing import Callable

from app.ingestion.providers.base import Provider

logger = logging.getLogger(__name__)

ProviderFactory = Callable[[], Provider]

_PROVIDER_FACTORIES: dict[str, ProviderFactory] = {}


def register(provider_name: str, factory: ProviderFactory) -> None:
    """Register a zero-argument provider factory under `provider_name`."""
    if provider_name in _PROVIDER_FACTORIES:
        logger.warning("Overwriting existing provider factory for %r", provider_name)
    _PROVIDER_FACTORIES[provider_name] = factory


def registered_provider_names() -> tuple[str, ...]:
    """Return provider names in registration order."""
    return tuple(_PROVIDER_FACTORIES)


def build_registered_providers() -> list[Provider]:
    """Instantiate all registered providers in registration order."""
    return [factory() for factory in _PROVIDER_FACTORIES.values()]
