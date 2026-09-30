"""Thin backward-compatible facade over `app.ingestion.normalizers`.

Normalization logic has moved to the pluggable registry under
`app.ingestion.normalizers` (`registry.py` + one module per sport/provider,
e.g. `football.py`, `basketball.py`) so new sports can be added without
touching existing normalizers. This module re-exports the same names that
used to live here directly (`normalize`, `normalize_sportsfeedx`,
`normalize_livescorecore`) so existing call sites/imports
(`from app.ingestion.normalize import normalize` / `normalize.normalize(...)`
/ `normalize.normalize_sportsfeedx(...)`) keep working unchanged.

`normalize()` now raises `app.ingestion.normalizers.UnknownProviderError`
(a `ValueError` subclass) for unregistered providers, so callers that
caught `ValueError` still work.
"""
from __future__ import annotations

from app.ingestion.normalizers import UnknownProviderError, normalize  # noqa: F401
from app.ingestion.normalizers.football import (  # noqa: F401
    normalize_livescorecore,
    normalize_sportsfeedx,
)

__all__ = [
    "normalize",
    "normalize_sportsfeedx",
    "normalize_livescorecore",
    "UnknownProviderError",
]
