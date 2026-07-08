from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from core.csv_validation import MissingColumnsError, require_columns
from core.drive.fs_utils import ensure_dir
from core.reporting import (
    build_stage_report,
    compact_stage_report,
    resolve_output_path,
    write_json_report,
)
from core.shift_logic import ShiftClassifier, to_bool_series, to_datetime_series
from core.shifts.year_columns import rows_with_year_columns, select_year_columns, years_from_frame
from core.table_outputs import write_csv_and_excel

from .options import (
    DEFAULT_REPORT_JSON,
    DEFAULT_SUMMARY_CSV,
    TurniCustomCountsOptions,
    default_enriched_dir,
    default_output_dir,
)

logger = logging.getLogger(__name__)

COUNT_BUCKETS = ("P", "N", "M", "MF")
SUMMARY_COLUMNS = ("employee", "turno")
REQUIRED_ENRICHED_COLUMNS = (
    "entry_ts",
    "is_holiday",
    "is_afternoon",
    "is_night",
    "is_long",
    "year",
)


def _employee_from_path(path: Path) -> str:
    name = path.stem
    if name.lower().endswith(".enriched"):
        name = name[: -len(".enriched")]
    return name or "unknown"


def _rows_for_employee(
    employee: str,
    counts: dict[tuple[str, int], int],
    years: Iterable[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code in COUNT_BUCKETS:
        row: dict[str, Any] = {"employee": employee, "turno": code}
        for year in years:
            row[str(year)] = int(counts.get((code, int(year)), 0))
        rows.append(row)
    return rows


def _ensure_year_column(df: pd.DataFrame) -> pd.DataFrame:
    if "year" in df.columns or "entry_ts" not in df.columns:
        return df
    working = df.copy()
    working["entry_ts"] = to_datetime_series(working["entry_ts"])
    working["year"] = working["entry_ts"].dt.year
    return working


def _ensure_turno_flags(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"is_holiday", "is_afternoon", "is_night"}
    if required_cols.issubset(set(df.columns)):
        return df
    if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
        return df
    return ShiftClassifier(include_holidays=False).classify(df)


def _count_custom_categories(df: pd.DataFrame) -> dict[tuple[str, int], int]:
    required_cols = {"entry_ts", "is_holiday", "is_afternoon", "is_night", "is_long"}
    if not required_cols.issubset(set(df.columns)):
        return {}

    entry_ts = to_datetime_series(df["entry_ts"])
    is_holiday = to_bool_series(df["is_holiday"])
    is_afternoon = to_bool_series(df["is_afternoon"])
    is_night = to_bool_series(df["is_night"])
    is_long = to_bool_series(df["is_long"])
    is_morning = ~is_afternoon & ~is_night
    is_saturday = entry_ts.dt.dayofweek == 5

    masks = {
        "P": is_long & is_afternoon,
        "N": is_long & is_night,
        "M": is_long & is_morning & is_saturday,
        "MF": is_long & is_morning & is_holiday,
    }

    counts: dict[tuple[str, int], int] = {}
    years = pd.to_numeric(df["year"], errors="coerce")
    for code, mask in masks.items():
        selected = years.loc[mask.fillna(False)]
        for year, count in selected.value_counts().items():
            if pd.notna(year):
                counts[(code, int(year))] = int(count)
    return counts


def process_one_enriched_file(
    enriched_file: str | Path,
    *,
    year_start: int | None = None,
    year_end: int | None = None,
) -> dict[str, Any]:
    source_path = Path(enriched_file)
    employee = _employee_from_path(source_path)
    years = select_year_columns([], year_start=year_start, year_end=year_end)
    result: dict[str, Any] = {
        "status": "error",
        "source_enriched_csv": str(source_path),
        "employee": employee,
        "rows_total": 0,
        "rows_counted": 0,
        "summary_rows": [],
        "years": years,
        "error_code": None,
        "error": None,
    }

    try:
        df = pd.read_csv(source_path)
        require_columns(
            df,
            REQUIRED_ENRICHED_COLUMNS,
            source=source_path,
            stage="turni_custom_counts",
        )
        result["rows_total"] = int(len(df))
        if df.empty:
            result["summary_rows"] = _rows_for_employee(employee, {}, years)
            result["status"] = "ok"
            return result

        working = _ensure_year_column(df.copy())
        working = _ensure_turno_flags(working)
        if "year" not in working.columns:
            result["summary_rows"] = _rows_for_employee(employee, {}, years)
            result["status"] = "ok"
            return result

        years = select_year_columns(
            years_from_frame(working),
            year_start=year_start,
            year_end=year_end,
        )
        result["years"] = years
        if years:
            working = working.loc[working["year"].isin(years)].copy()
        counts = _count_custom_categories(working)
        result["rows_counted"] = int(sum(counts.values()))
        result["summary_rows"] = _rows_for_employee(employee, counts, years)
        result["status"] = "ok"
        return result
    except MissingColumnsError:
        raise
    except Exception as exc:
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def process_many_enriched_files(
    enriched_files: Iterable[str | Path],
    *,
    year_start: int | None = None,
    year_end: int | None = None,
    enriched_dir: str | None = None,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    normalized_files = sorted(Path(path) for path in enriched_files)
    rows: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    stats: dict[str, Any] = {
        "files_total": len(normalized_files),
        "files_processed": 0,
        "files_error": 0,
        "employees_total": len(normalized_files),
        "rows_total": 0,
        "rows_counted": 0,
        "year_start": year_start,
        "year_end": year_end,
    }

    for file_index, enriched_file in enumerate(normalized_files, start=1):
        employee_name = _employee_from_path(enriched_file)
        logger.info("Employee %s/%s: %s", file_index, len(normalized_files), employee_name)
        item = process_one_enriched_file(
            enriched_file,
            year_start=year_start,
            year_end=year_end,
        )
        items.append(item)
        if item["status"] != "ok":
            stats["files_error"] += 1
            issues.append(
                {
                    "code": str(item.get("error_code") or "processing_error"),
                    "enriched_csv": str(item.get("source_enriched_csv") or ""),
                    "message": str(item.get("error") or "processing_error"),
                }
            )
            continue

        stats["files_processed"] += 1
        stats["rows_total"] += int(item["rows_total"])
        stats["rows_counted"] += int(item["rows_counted"])

    years = select_year_columns(
        (
            int(year)
            for item in items
            if item["status"] == "ok"
            for year in item.get("years", [])
        ),
        year_start=year_start,
        year_end=year_end,
    )
    for item in items:
        if item["status"] == "ok":
            rows.extend(rows_with_year_columns(item["summary_rows"], years))

    report = build_stage_report(
        stage="turni_custom_counts",
        inputs={
            "enriched_dir": os.path.abspath(enriched_dir) if enriched_dir else None,
            "year_start": year_start,
            "year_end": year_end,
            "buckets": list(COUNT_BUCKETS),
        },
        outputs={},
        stats=stats,
        row_totals={"items": len(items), "issues": len(issues), "summary_rows": len(rows)},
        items=items,
        issues=issues,
    )
    report["rows"] = rows
    report["years"] = years
    return report


def build_turni_custom_counts_from_dir(
    *,
    enriched_dir: str | None = None,
    output_dir: str | None = None,
    summary_csv: str | None = None,
    report_json: str | None = None,
    year_start: int | None = None,
    year_end: int | None = None,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    output_dir = output_dir or default_output_dir()
    summary_csv = summary_csv or DEFAULT_SUMMARY_CSV
    report_json = report_json or DEFAULT_REPORT_JSON
    ensure_dir(output_dir)

    output_path = resolve_output_path(output_dir, summary_csv)
    report_path = resolve_output_path(output_dir, report_json)
    enriched_path = Path(enriched_dir)
    enriched_files = sorted(enriched_path.glob("*.enriched.csv"))
    report = process_many_enriched_files(
        enriched_files,
        year_start=year_start,
        year_end=year_end,
        enriched_dir=enriched_dir,
    )

    columns = list(SUMMARY_COLUMNS) + [str(year) for year in report["years"]]
    output_path, excel_path = write_csv_and_excel(
        report["rows"],
        output_path,
        columns=columns,
        sheet_name="Custom Counts",
    )

    report["outputs"]["summary_csv"] = str(output_path.resolve())
    report["outputs"]["summary_xlsx"] = str(excel_path.resolve())
    report["outputs"]["report_json"] = str(report_path.resolve())
    write_json_report(report_path, compact_stage_report(report))
    return report


def run_from_options(options: TurniCustomCountsOptions) -> dict[str, Any]:
    return build_turni_custom_counts_from_dir(
        enriched_dir=options.enriched_dir,
        output_dir=options.output_dir,
        summary_csv=options.summary_csv,
        report_json=options.report_json,
        year_start=options.year_start,
        year_end=options.year_end,
    )


__all__ = [
    "build_turni_custom_counts_from_dir",
    "process_many_enriched_files",
    "process_one_enriched_file",
    "run_from_options",
]
