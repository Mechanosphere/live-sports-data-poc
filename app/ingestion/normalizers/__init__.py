"""Pluggable per-provider normalizer registry.

Each sport/provider combination registers a normalizer function that turns
a provider-native raw payload into our internal `GameEvent` schema. This
package replaces the single flat `app.ingestion.normalize` module so new
sports (basketball, ...) can be added as independent modules without
touching existing football normalizers.

Importing this package registers all built-in normalizers (football,
basketball) as a side effect, via the submodule imports below.
"""
from __future__ import annotations

from app.ingestion.normalizers.registry import (
    UnknownProviderError,
    normalize,
    register,
)

# Side-effecting imports: each submodule calls `register(...)` for its
# provider(s) at import time.
from app.ingestion.normalizers import basketball, football  # noqa: E402,F401

__all__ = ["UnknownProviderError", "normalize", "register"]
