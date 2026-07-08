from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


class MissingColumnsError(ValueError):
    """Raised when an input table is missing required columns."""


def require_columns(
    df: pd.DataFrame,
    required: Iterable[str],
    *,
    source: str | Path,
    stage: str,
) -> None:
    missing = [column for column in required if column not in df.columns]
    if not missing:
        return
    available = ", ".join(str(column) for column in df.columns) or "<none>"
    required_text = ", ".join(required)
    missing_text = ", ".join(missing)
    raise MissingColumnsError(
        f"{stage}: {source} is missing required column(s): {missing_text}. "
        f"Required: {required_text}. Available: {available}."
    )


def require_columns_in_header(
    columns: Iterable[str] | None,
    required: Iterable[str],
    *,
    source: str | Path,
    stage: str,
) -> None:
    header = list(columns or [])
    missing = [column for column in required if column not in header]
    if not missing:
        return
    available = ", ".join(header) or "<none>"
    required_text = ", ".join(required)
    missing_text = ", ".join(missing)
    raise MissingColumnsError(
        f"{stage}: {source} is missing required column(s): {missing_text}. "
        f"Required: {required_text}. Available: {available}."
    )


__all__ = [
    "MissingColumnsError",
    "require_columns",
    "require_columns_in_header",
]
