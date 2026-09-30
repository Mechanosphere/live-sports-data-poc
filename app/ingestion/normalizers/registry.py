"""Provider -> normalizer function registry.

Normalization happens as early as possible (right after polling, before
publishing to Kafka) so that every downstream consumer only ever deals
with one canonical shape, and the event's UUIDv7 is generated exactly
once, at ingestion time (see `app.schemas.football.GameEvent`).

Adding a new sport/provider means adding a new module under
`app.ingestion.normalizers` that calls `register()` at import time --
no changes needed here or in existing normalizer modules.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Callable

from app.schemas.football import GameEvent

logger = logging.getLogger(__name__)

NormalizerFn = Callable[[dict[str, Any]], GameEvent]

_NORMALIZERS: dict[str, NormalizerFn] = {}

# Fixed namespace for deriving deterministic event_ids (uuid5) from raw
# payloads. Using uuid5 (instead of the uuid7 the schema defaults to)
# makes Kafka -> DynamoDB idempotent even when normalization happens in
# the consumer: reprocessing the exact same raw Kafka message always
# recomputes the same event_id/DynamoDB key, so a retry overwrites the
# same item instead of creating a duplicate with a fresh random id.
_EVENT_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "livesportsdata.ingestion.event")


def _deterministic_event_id(provider_name: str, raw: dict[str, Any]) -> uuid.UUID:
    # Raw payloads are reprocessed byte-for-byte from the same Kafka message,
    # so hashing the full raw payload (sorted for stable key order) is a
    # simple, sufficient dedupe key -- no per-provider field allowlist needed.
    payload = json.dumps(raw, sort_keys=True, default=str)
    return uuid.uuid5(_EVENT_ID_NAMESPACE, f"{provider_name}:{payload}")


class UnknownProviderError(ValueError):
    """Raised when normalize() is called for a provider with no registered normalizer."""


def register(provider_name: str, normalizer: NormalizerFn) -> None:
    """Register a normalizer function for a provider name.

    Re-registering the same `provider_name` overwrites the previous
    normalizer (last import wins) -- intended for tests, not production use.
    """
    if provider_name in _NORMALIZERS:
        logger.warning("Overwriting existing normalizer for provider %r", provider_name)
    _NORMALIZERS[provider_name] = normalizer


def normalize(provider_name: str, raw: dict[str, Any]) -> GameEvent:
    """Dispatch to the registered normalizer for `provider_name`."""
    try:
        normalizer = _NORMALIZERS[provider_name]
    except KeyError as exc:
        logger.error("No normalizer registered for provider %r", provider_name)
        raise UnknownProviderError(
            f"No normalizer registered for provider {provider_name!r}"
        ) from exc

    try:
        event = normalizer(raw)
        # Assign the deterministic id in place rather than rebuilding the
        # model via `model_copy(update=...)`, which would re-construct and
        # re-validate the whole `GameEvent` a second time per event on the
        # ingestion hot path. `GameEvent` has no `validate_assignment`, so a
        # plain attribute set is equivalent here and avoids the extra copy.
        event.event_id = _deterministic_event_id(provider_name, raw)
    except Exception:
        logger.exception("Failed to normalize raw payload from provider %s: %r", provider_name, raw)
        raise

    logger.debug(
        "Normalized %s event_id=%s sport=%s competition=%s game=%s type=%s",
        provider_name,
        event.event_id,
        event.sport,
        event.competition_name,
        event.game_id,
        event.event_type.value,
    )
    return event
