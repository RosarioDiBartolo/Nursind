from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd


def build_year_range(start: int | None, end: int | None) -> list[int]:
    if start is None and end is None:
        return []
    if start is None:
        assert end is not None
        resolved_start = end
    else:
        resolved_start = start
    resolved_end = resolved_start if end is None else end
    if resolved_start > resolved_end:
        resolved_start, resolved_end = resolved_end, resolved_start
    return list(range(resolved_start, resolved_end + 1))


def years_from_frame(df: pd.DataFrame, column: str = "year") -> list[int]:
    if column not in df.columns:
        return []
    values = pd.to_numeric(df[column], errors="coerce").dropna()
    return sorted({int(year) for year in values})


def select_year_columns(
    found_years: Iterable[int],
    *,
    year_start: int | None,
    year_end: int | None,
) -> list[int]:
    configured_years = build_year_range(year_start, year_end)
    if configured_years:
        return configured_years
    return sorted({int(year) for year in found_years})


def rows_with_year_columns(
    rows: Iterable[dict[str, Any]],
    years: Iterable[int],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        out = {"employee": row.get("employee"), "turno": row.get("turno")}
        for year in years:
            out[str(year)] = int(row.get(str(year), 0) or 0)
        normalized.append(out)
    return normalized


__all__ = [
    "build_year_range",
    "rows_with_year_columns",
    "select_year_columns",
    "years_from_frame",
]
