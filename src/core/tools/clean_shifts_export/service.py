from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from core.csv_validation import MissingColumnsError, require_columns
from core.drive.fs_utils import ensure_dir, ensure_parent_dir
from core.drive.names import safe_name
from core.reporting import (
    build_stage_report,
    compact_stage_report,
    resolve_output_path,
    write_json_report,
)
from core.shift_logic import to_datetime_series

from .options import (
    DEFAULT_REPORT_JSON,
    CleanShiftsExportOptions,
    default_enriched_dir,
    default_output_dir,
)

logger = logging.getLogger(__name__)

STAGE = "clean_shifts_export"
OUTPUT_COLUMNS = ("Data", "Giorno", "Entrata", "Uscita", "Durata", "Festivo", "Turno")
REQUIRED_ENRICHED_COLUMNS = (
    "entry_ts",
    "exit_ts",
    "duration_hours",
    "is_holiday",
    "turno",
    "turno_bucket",
)
ITALIAN_WEEKDAYS = (
    "Lunedi",
    "Martedi",
    "Mercoledi",
    "Giovedi",
    "Venerdi",
    "Sabato",
    "Domenica",
)


def _employee_from_path(path: Path) -> str:
    name = path.stem
    if name.lower().endswith(".enriched"):
        name = name[: -len(".enriched")]
    return name or "unknown"


def _output_workbook_path(*, output_dir: str | Path, employee: str) -> Path:
    return Path(output_dir) / f"{safe_name(employee)}.xlsx"


def _format_duration_hmm(values: pd.Series) -> pd.Series:
    duration_hours = pd.to_numeric(values, errors="coerce")
    total_minutes = (duration_hours * 60.0).round()
    return total_minutes.map(_minutes_to_hmm)


def _minutes_to_hmm(total_minutes: object) -> str:
    if pd.isna(total_minutes):
        return ""
    total_minutes = int(total_minutes)
    sign = "-" if total_minutes < 0 else ""
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    return f"{sign}{hours}:{minutes:02d}"


def _format_holiday(values: pd.Series) -> pd.Series:
    normalized = values.fillna("").astype(str).str.strip().str.lower()
    is_holiday = normalized.isin({"true", "1", "yes", "y", "t"})
    return is_holiday.map(lambda value: "festivo" if value else "non festivo")


def format_clean_shift_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    turno_bucket = df["turno_bucket"].fillna("").astype(str).str.strip().str.upper()
    working = df.loc[turno_bucket.ne("M")].copy()
    if working.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    entry_ts = to_datetime_series(working["entry_ts"])
    exit_ts = to_datetime_series(working["exit_ts"])
    overnight = entry_ts.notna() & exit_ts.notna() & (exit_ts < entry_ts)
    if overnight.any():
        exit_ts.loc[overnight] = exit_ts.loc[overnight] + pd.Timedelta(days=1)

    output = pd.DataFrame(
        {
            "Data": entry_ts.dt.strftime("%Y-%m-%d").fillna(""),
            "Giorno": entry_ts.dt.dayofweek.map(
                {index: name for index, name in enumerate(ITALIAN_WEEKDAYS)}
            ).fillna(""),
            "Entrata": entry_ts.dt.strftime("%H:%M").fillna(""),
            "Uscita": exit_ts.dt.strftime("%H:%M").fillna(""),
            "Durata": _format_duration_hmm(working["duration_hours"]),
            "Festivo": _format_holiday(working["is_holiday"]),
            "Turno": working["turno"].fillna(""),
        }
    )
    output = output.sort_values(by=["Data", "Entrata", "Uscita"], kind="stable")
    return output.loc[:, OUTPUT_COLUMNS].reset_index(drop=True)


def process_one_enriched_file(
    enriched_file: str | Path,
    *,
    output_dir: str | None = None,
) -> dict[str, Any]:
    output_dir = output_dir or default_output_dir()
    source_path = Path(enriched_file)
    employee = _employee_from_path(source_path)
    output_path = _output_workbook_path(output_dir=output_dir, employee=employee)
    result: dict[str, Any] = {
        "status": "error",
        "source_enriched_csv": str(source_path),
        "output_xlsx": str(output_path.resolve()),
        "employee": employee,
        "rows_total": 0,
        "rows_written": 0,
        "error_code": None,
        "error": None,
    }

    try:
        df = pd.read_csv(source_path)
        require_columns(df, REQUIRED_ENRICHED_COLUMNS, source=source_path, stage=STAGE)
    except MissingColumnsError:
        raise
    except Exception as exc:
        result["error_code"] = "read_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["rows_total"] = int(len(df))
    formatted = format_clean_shift_rows(df)
    ensure_parent_dir(str(output_path))
    formatted.to_excel(output_path, index=False, sheet_name="Turni")
    result["rows_written"] = int(len(formatted))
    result["status"] = "ok"
    return result


def process_many_enriched_files(
    enriched_files: Iterable[str | Path],
    *,
    enriched_dir: str | None = None,
    output_dir: str | None = None,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    output_dir = output_dir or default_output_dir()
    normalized_files = sorted(Path(path) for path in enriched_files)
    stats: dict[str, Any] = {
        "files_total": len(normalized_files),
        "files_processed": 0,
        "files_error": 0,
        "employees_total": len(normalized_files),
        "rows_total": 0,
        "rows_written": 0,
    }
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []

    for file_index, enriched_file in enumerate(normalized_files, start=1):
        employee_name = _employee_from_path(enriched_file)
        logger.info("Employee %s/%s: %s", file_index, len(normalized_files), employee_name)
        item = process_one_enriched_file(enriched_file, output_dir=output_dir)
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
        stats["rows_written"] += int(item["rows_written"])

    return build_stage_report(
        stage=STAGE,
        inputs={"enriched_dir": os.path.abspath(enriched_dir)},
        outputs={"output_dir": os.path.abspath(output_dir)},
        stats=stats,
        row_totals={
            "items": len(items),
            "issues": len(issues),
            "clean_shift_workbooks": stats["files_processed"],
        },
        items=items,
        issues=issues,
    )


def export_clean_shifts_from_dir(
    *,
    enriched_dir: str | None = None,
    output_dir: str | None = None,
    report_json: str | None = None,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    output_dir = output_dir or default_output_dir()
    report_json = report_json or DEFAULT_REPORT_JSON
    ensure_dir(output_dir)

    enriched_path = Path(enriched_dir)
    enriched_files = sorted(enriched_path.glob("*.enriched.csv"))
    report_path = resolve_output_path(output_dir, report_json)
    if not enriched_files:
        message = (
            f"No enriched CSV files found in {enriched_path.resolve()}. "
            "Run src/scripts/enrich_shifts.py before the clean shifts export."
        )
        report = build_stage_report(
            stage=STAGE,
            status="error",
            inputs={"enriched_dir": str(enriched_path.resolve())},
            outputs={
                "output_dir": str(Path(output_dir).resolve()),
                "report_json": str(report_path.resolve()),
            },
            stats={
                "files_total": 0,
                "files_processed": 0,
                "files_error": 0,
                "employees_total": 0,
                "rows_total": 0,
                "rows_written": 0,
            },
            row_totals={"items": 0, "issues": 1, "clean_shift_workbooks": 0},
            issues=[
                {
                    "code": "missing_enriched_inputs",
                    "enriched_dir": str(enriched_path.resolve()),
                    "message": message,
                }
            ],
        )
        write_json_report(report_path, compact_stage_report(report))
        return report

    report = process_many_enriched_files(
        enriched_files,
        enriched_dir=enriched_dir,
        output_dir=output_dir,
    )
    report["outputs"]["report_json"] = str(report_path.resolve())
    write_json_report(report_path, compact_stage_report(report))
    return report


def run_from_options(options: CleanShiftsExportOptions) -> dict[str, Any]:
    return export_clean_shifts_from_dir(
        enriched_dir=options.enriched_dir,
        output_dir=options.output_dir,
        report_json=options.report_json,
    )


__all__ = [
    "export_clean_shifts_from_dir",
    "format_clean_shift_rows",
    "process_many_enriched_files",
    "process_one_enriched_file",
    "run_from_options",
]
