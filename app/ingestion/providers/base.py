"""Common interface for polling-based provider adapters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Provider(ABC):
    """A sports data provider we poll for new events.

    Each concrete provider is responsible only for fetching raw payloads in
    its own native shape (from a real API in production; simulated here for
    the POC) -- it publishes those raw payloads to Kafka as-is. Normalization
    into our internal schema happens downstream, in the consumer, via
    `app.ingestion.normalizers`, keeping provider-specific quirks isolated
    and allowing reprocessing/schema evolution from the raw payload.
    """

    #: Identifier used as the "provider" tag on raw payloads and for
    #: dispatching to the right normalizer in the registry.
    name: str

    #: Sport this provider emits events for (e.g. keys.SPORT_FOOTBALL).
    #: Carried in the Kafka envelope so consumers/topic-routing can use it
    #: without having to normalize the raw payload first.
    sport: str

    @abstractmethod
    def poll(self) -> list[dict[str, Any]]:
        """Fetch any new raw event payloads since the last poll.

        Returns a list of raw, provider-native payload dicts. Should return
        an empty list if there is nothing new (not raise).
        """
        raise NotImplementedError

    @abstractmethod
    def partition_key(self, raw: dict[str, Any]) -> str:
        """Return the Kafka partition key for a raw payload from this provider.

        Should identify "the game/match this event belongs to" (e.g.
        `{competition}#{game_id}`) so Kafka preserves per-game ordering, without
        requiring normalization first.
        """
        raise NotImplementedError
