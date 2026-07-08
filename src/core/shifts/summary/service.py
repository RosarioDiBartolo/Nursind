from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable, Literal

import pandas as pd

from core.csv_validation import MissingColumnsError, require_columns
from core.reporting import build_stage_report, compact_stage_report, write_json_report
from core.shift_logic import ShiftClassifier, assign_turno_bucket, to_datetime_series
from core.shifts.year_columns import rows_with_year_columns, select_year_columns, years_from_frame
from core.table_outputs import write_csv_and_excel

from .options import (
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_YEAR_END,
    DEFAULT_YEAR_START,
    TurniEmployeeSummaryOptions,
    default_enriched_dir,
    default_report_json_path,
    default_summary_csv_path,
)

logger = logging.getLogger(__name__)

TURNI = ("N", "P", "F", "M", "S")
REQUIRED_ENRICHED_COLUMNS = (
    "entry_ts",
    "exit_ts",
    "duration_hours",
    "is_holiday",
    "is_afternoon",
    "is_night",
    "turno_bucket",
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
    for code in TURNI:
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


def _duration_hhmm_to_hours(values: pd.Series) -> pd.Series:
    parts = values.astype(str).str.strip().str.extract(r"^(?P<hours>\d+):(?P<minutes>\d{1,2})$")
    hours = pd.to_numeric(parts["hours"], errors="coerce")
    minutes = pd.to_numeric(parts["minutes"], errors="coerce")
    return hours + (minutes / 60.0)


def _ensure_duration_hours(df: pd.DataFrame) -> pd.DataFrame:
    if "duration_hours" in df.columns:
        return df
    working = df.copy()
    if "entry_ts" in working.columns and "exit_ts" in working.columns:
        working = working.assign(
            entry_ts=to_datetime_series(working["entry_ts"]),
            exit_ts=to_datetime_series(working["exit_ts"]),
        )
        overnight = working["exit_ts"] < working["entry_ts"]
        if overnight.any():
            working.loc[overnight, "exit_ts"] = working.loc[overnight, "exit_ts"] + pd.Timedelta(
                days=1
            )
        duration_hours = (working["exit_ts"] - working["entry_ts"]).dt.total_seconds() / 3600.0
        working = working.assign(duration_hours=duration_hours)
        return working
    if "duration_hhmm" in working.columns:
        working = working.assign(duration_hours=_duration_hhmm_to_hours(working["duration_hhmm"]))
    return working


def _ensure_turno_flags(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"is_holiday", "is_afternoon", "is_night"}
    if required_cols.issubset(set(df.columns)):
        return df
    if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
        return df
    return ShiftClassifier(include_holidays=False).classify(df)


def _ensure_turno_bucket(df: pd.DataFrame, *, min_hours: float | None) -> pd.DataFrame:
    if "turno_bucket" in df.columns and df["turno_bucket"].notna().any():
        return df
    threshold = 6.0 if min_hours is None else float(min_hours)
    working = _ensure_turno_flags(_ensure_duration_hours(df.copy()))
    return working.assign(turno_bucket=assign_turno_bucket(working, min_hours=threshold))


def _write_json(out_path: str, rows: list[dict[str, Any]], stats: dict[str, Any]) -> None:
    write_json_report(out_path, {"rows": rows, "stats": stats})


def process_one_enriched_file(
    enriched_file: str | Path,
    *,
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
) -> dict[str, Any]:
    source_path = Path(enriched_file)
    employee = _employee_from_path(source_path)
    years = select_year_columns([], year_start=year_start, year_end=year_end)

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
        require_columns(
            df,
            REQUIRED_ENRICHED_COLUMNS,
            source=source_path,
            stage="turni_employee_summary",
        )
        result["rows_total"] = int(len(df))
        if df.empty:
            result["summary_rows"] = _rows_for_employee(employee, {}, years)
            result["status"] = "ok"
            return result

        working = _ensure_year_column(df.copy())
        working = _ensure_turno_bucket(working, min_hours=min_hours)
        if "year" not in working.columns or "turno_bucket" not in working.columns:
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

        working = working.loc[working["turno_bucket"].isin(TURNI)].copy()
        result["rows_classified"] = int(len(working))

        counts_series = working.groupby(["turno_bucket", "year"]).size()
        counts = {(code, int(year)): int(count) for (code, year), count in counts_series.items()}
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
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
    enriched_dir: str | None = None,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    normalized_files = sorted(Path(path) for path in enriched_files)

    stats: dict[str, Any] = {
        "files_total": len(normalized_files),
        "files_processed": 0,
        "files_error": 0,
        "employees_total": len(normalized_files),
        "rows_total": 0,
        "rows_classified": 0,
        "year_start": year_start,
        "year_end": year_end,
    }
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for file_index, enriched_file in enumerate(normalized_files, start=1):
        employee_name = _employee_from_path(enriched_file)
        logger.info("Employee %s/%s: %s", file_index, len(normalized_files), employee_name)

        item = process_one_enriched_file(
            enriched_file,
            min_hours=min_hours,
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
        stats["rows_classified"] += int(item["rows_classified"])

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
        stage="turni_employee_summary",
        inputs={
            "enriched_dir": os.path.abspath(enriched_dir) if enriched_dir else None,
            "year_start": year_start,
            "year_end": year_end,
            "min_hours": min_hours,
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


def build_turni_employee_summary_from_dir(
    *,
    enriched_dir: str | None = None,
    out: str | None = None,
    report_json: str | None = None,
    output_format: Literal["csv", "json"] = DEFAULT_OUTPUT_FORMAT,
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    out = out or default_summary_csv_path()
    report_json = report_json or default_report_json_path()
    enriched_path = Path(enriched_dir)
    enriched_files = sorted(enriched_path.glob("*.enriched.csv"))
    report = process_many_enriched_files(
        enriched_files,
        min_hours=min_hours,
        year_start=year_start,
        year_end=year_end,
        enriched_dir=enriched_dir,
    )

    if output_format == "csv":
        csv_path, excel_path = write_csv_and_excel(report["rows"], out, sheet_name="Turni")
        report["outputs"]["excel_path"] = str(excel_path.resolve())
    else:
        _write_json(out, report["rows"], report["stats"])
        csv_path = None

    report["outputs"]["output_path"] = os.path.abspath(str(csv_path or out))
    report["outputs"]["output_format"] = output_format
    report["outputs"]["report_json"] = str(Path(report_json).resolve())
    write_json_report(report_json, compact_stage_report(report))
    return report


def run_from_options(options: TurniEmployeeSummaryOptions) -> dict[str, Any]:
    return build_turni_employee_summary_from_dir(
        enriched_dir=options.enriched_dir,
        out=options.out,
        report_json=options.report_json,
        output_format=options.output_format,
        min_hours=options.min_hours,
        year_start=options.year_start,
        year_end=options.year_end,
    )


__all__ = [
    "build_turni_employee_summary_from_dir",
    "process_many_enriched_files",
    "process_one_enriched_file",
    "run_from_options",
]

