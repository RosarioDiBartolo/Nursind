from __future__ import annotations

"""Aggregate enriched shifts per employee into turno counts by year.

Input: enriched CSVs (from turni_enrichment).
Output: CSV with five rows per employee (N, P, F, M, S) and year columns.
Rules:
- Prefer `turno_bucket` from enrichment when present.
- Fallback: compute `turno_bucket` from duration + holiday + turno flags.
"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.logging_utils import setup_logging
from src.drive_service.output_paths import build_output_paths
from src.shift_services import assign_turno_bucket, to_datetime_series

logger = logging.getLogger(__name__)

TURNI = ("N", "P", "F", "M", "S")
DEFAULT_YEAR_START = 2016
DEFAULT_YEAR_END = 2025
DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_ENRICHED_DIR = str(DEFAULT_OUTPUTS.enrichment_output)
DEFAULT_SUMMARY_CSV = str(DEFAULT_OUTPUTS.aggregation_output / "turni_employee_summary.csv")


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


def build_employee_turni_summary(
    *,
    enriched_dir: str = DEFAULT_ENRICHED_DIR,
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    enriched_path = Path(enriched_dir)
    enriched_files = sorted(enriched_path.glob("*.enriched.csv"))

    stats = {
        "dipendenti": len(enriched_files),
        "file_totali": 0,
        "file_mancanti": 0,
        "file_errori": 0,
        "righe_totali": 0,
        "righe_classificate": 0,
    }

    years = _years_range(year_start, year_end)
    rows: list[dict[str, Any]] = []

    for emp_index, enriched_file in enumerate(enriched_files, start=1):
        emp_name = _employee_from_path(enriched_file)
        logger.info(
            "Dipendente %s/%s: %s",
            emp_index,
            len(enriched_files),
            emp_name,
        )

        try:
            df = pd.read_csv(enriched_file)
        except Exception:
            stats["file_errori"] += 1
            rows.extend(_rows_for_employee(emp_name, {}, years))
            continue

        stats["file_totali"] += 1
        stats["righe_totali"] += len(df)
        if df.empty:
            rows.extend(_rows_for_employee(emp_name, {}, years))
            continue

        working = df.copy()
        working = _ensure_year_column(working)
        working = _ensure_turno_bucket(working, min_hours=min_hours)
        if "year" not in working.columns or "turno_bucket" not in working.columns:
            rows.extend(_rows_for_employee(emp_name, {}, years))
            continue

        if years:
            working = working.loc[working["year"].isin(years)].copy()

        working = working.loc[working["turno_bucket"].isin(TURNI)].copy()
        stats["righe_classificate"] += len(working)

        counts_series = working.groupby(["turno_bucket", "year"]).size()
        counts = {
            (code, int(year)): int(count)
            for (code, year), count in counts_series.items()
        }
        rows.extend(_rows_for_employee(emp_name, counts, years))

    return rows, stats


def _write_csv(out_path: str, rows: list[dict[str, Any]]) -> None:
    ensure_parent_dir(out_path)
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)


def _write_json(out_path: str, rows: list[dict[str, Any]], stats: dict[str, int]) -> None:
    ensure_parent_dir(out_path)
    payload = {"rows": rows, "stats": stats}
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Aggrega i turni (N/P/F/M/S) per dipendente dai CSV arricchiti."
    )
    parser.add_argument(
        "--enriched-dir",
        default=DEFAULT_ENRICHED_DIR,
        help=f"Directory dei CSV arricchiti (default: {DEFAULT_ENRICHED_DIR})",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_SUMMARY_CSV,
        help=f"Path di output (default: {DEFAULT_SUMMARY_CSV})",
    )
    parser.add_argument(
        "--year-start",
        type=int,
        default=DEFAULT_YEAR_START,
        help="Anno iniziale (default: 2016)",
    )
    parser.add_argument(
        "--year-end",
        type=int,
        default=DEFAULT_YEAR_END,
        help="Anno finale (default: 2025)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Formato output (default: csv)",
    )
    parser.add_argument(
        "--min-hours",
        type=float,
        help="Soglia ore per fallback classificazione turno_bucket (durata > soglia).",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    rows, stats = build_employee_turni_summary(
        enriched_dir=args.enriched_dir,
        min_hours=args.min_hours,
        year_start=args.year_start,
        year_end=args.year_end,
    )

    if args.format == "csv":
        _write_csv(args.out, rows)
    else:
        _write_json(args.out, rows, stats)

    logger.info(
        "Completato: dipendenti=%s file_totali=%s mancanti=%s errori=%s righe=%s classificate=%s",
        stats["dipendenti"],
        stats["file_totali"],
        stats["file_mancanti"],
        stats["file_errori"],
        stats["righe_totali"],
        stats["righe_classificate"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
