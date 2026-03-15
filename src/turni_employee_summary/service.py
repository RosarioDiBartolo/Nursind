from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.shift_services import assign_turno_bucket, to_datetime_series

from .options import (
    DEFAULT_YEAR_END,
    DEFAULT_YEAR_START,
    default_enriched_dir,
)

logger = logging.getLogger(__name__)

TURNI = ("N", "P", "F", "M", "S")


def _employee_from_path(path: Path) -> str:
    name = path.stem
    if name.lower().endswith(".enriched"):
        name = name[: -len(".enriched")]
    return name or "unknown"


def _years_range(start: int | None, end: int | None) -> list[int]:
    if start is None and end is None:
        return []
    if start is None:
        start = end
    if end is None:
        end = start
    if start > end:
        start, end = end, start
    return list(range(int(start), int(end) + 1))


def _rows_for_employee(
    employee: str,
    counts: dict[tuple[str, int], int],
    years: Iterable[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code in TURNI:
        row: dict[str, Any] = {
            "employee": employee,
            "turno": code,
        }
        for year in years:
            row[str(year)] = int(counts.get((code, int(year)), 0))
        rows.append(row)
    return rows


def _ensure_year_column(df: pd.DataFrame) -> pd.DataFrame:
    if "year" in df.columns:
        return df
    if "entry_ts" not in df.columns:
        return df
    working = df.copy()
    working["entry_ts"] = to_datetime_series(working["entry_ts"])
    working["year"] = working["entry_ts"].dt.year
    return working


def _ensure_turno_bucket(df: pd.DataFrame, *, min_hours: float | None) -> pd.DataFrame:
    if "turno_bucket" in df.columns:
        return df

    threshold = 6.0 if min_hours is None else float(min_hours)
    working = df.copy()
    working["turno_bucket"] = assign_turno_bucket(working, min_hours=threshold)
    return working


def process_one_enriched_file(
    enriched_file: str | Path,
    *,
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
) -> dict[str, Any]:
    source_path = Path(enriched_file)
    employee = _employee_from_path(source_path)
    years = _years_range(year_start, year_end)

    result: dict[str, Any] = {
        "status": "error",
        "source_enriched_csv": str(source_path),
        "employee": employee,
        "rows_total": 0,
        "rows_classified": 0,
        "summary_rows": [],
        "years": years,
        "error_code": None,
        "error": None,
    }

    try:
        df = pd.read_csv(source_path)
        result["rows_total"] = int(len(df))
        if df.empty:
            result["summary_rows"] = _rows_for_employee(employee, {}, years)
            result["status"] = "ok"
            return result

        working = df.copy()
        working = _ensure_year_column(working)
        working = _ensure_turno_bucket(working, min_hours=min_hours)
        if "year" not in working.columns or "turno_bucket" not in working.columns:
            result["summary_rows"] = _rows_for_employee(employee, {}, years)
            result["status"] = "ok"
            return result

        if years:
            working = working.loc[working["year"].isin(years)].copy()

        working = working.loc[working["turno_bucket"].isin(TURNI)].copy()
        result["rows_classified"] = int(len(working))

        counts_series = working.groupby(["turno_bucket", "year"]).size()
        counts = {(code, int(year)): int(count) for (code, year), count in counts_series.items()}
        result["summary_rows"] = _rows_for_employee(employee, counts, years)
        result["status"] = "ok"
        return result
    except Exception as exc:
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def process_many_enriched_files(
    enriched_files: Iterable[str | Path],
    *,
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
    enriched_dir: str | None = None,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    normalized_files = sorted(Path(path) for path in enriched_files)
    years = _years_range(year_start, year_end)

    totals: dict[str, Any] = {
        "files_total": len(normalized_files),
        "files_processed": 0,
        "files_error": 0,
        "dipendenti": len(normalized_files),
        "file_totali": 0,
        "file_mancanti": 0,
        "file_errori": 0,
        "righe_totali": 0,
        "righe_classificate": 0,
        "enriched_dir": os.path.abspath(enriched_dir) if enriched_dir else None,
        "year_start": year_start,
        "year_end": year_end,
    }

    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    rows: list[dict[str, Any]] = []

    for file_index, enriched_file in enumerate(normalized_files, start=1):
        employee_name = _employee_from_path(enriched_file)
        logger.info(
            "Dipendente %s/%s: %s",
            file_index,
            len(normalized_files),
            employee_name,
        )

        item = process_one_enriched_file(
            enriched_file,
            min_hours=min_hours,
            year_start=year_start,
            year_end=year_end,
        )
        items.append(item)

        if item["status"] != "ok":
            totals["files_error"] += 1
            totals["file_errori"] += 1
            errors.append(
                {
                    "enriched_csv": str(item["source_enriched_csv"]),
                    "error": str(item["error"]),
                }
            )
            continue

        totals["files_processed"] += 1
        totals["file_totali"] += 1
        totals["righe_totali"] += int(item["rows_total"])
        totals["righe_classificate"] += int(item["rows_classified"])
        rows.extend(list(item["summary_rows"]))

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "rows": rows,
        "years": years,
        "file_errors": errors,
    }
