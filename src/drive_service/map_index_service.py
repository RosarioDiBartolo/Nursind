"""Deprecated compatibility module.

Use `src.drive_service.index` for canonical imports.
"""

from __future__ import annotations

import warnings

from .index.map_index import MapIndex

warnings.warn(
    "src.drive_service.map_index_service is deprecated; import MapIndex from src.drive_service.index",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["MapIndex"]
