from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from cartellino_parser.drive_service.fs_utils import ensure_dir, ensure_parent_dir
from cartellino_parser.drive_service.names import safe_name
from cartellino_parser.reporting import build_stage_report, compact_stage_report, write_json_report
from cartellino_parser.shift_services import to_bool_series

from .artifacts import TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS
from .options import (
    TurniAfternoonLongExportOptions,
    default_enriched_dir,
    default_output_dir,
    default_pairs_dir,
    default_report_json_path,
)

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "dipendente",
    "entrata",
    "uscita",
    "durata turno",
    "Festivo",
    "Turno",
    "Data",
]


def _employee_from_path(path: Path) -> str:
    name = path.stem
    if name.lower().endswith(".enriched"):
        name = name[: -len(".enriched")]
    return name or "unknown"


def _output_csv_path(*, output_dir: str | Path, employee: str) -> str:
    safe_employee = safe_name(employee)
    filename = f"{safe_employee}{TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS.filtered_file_suffix}"
    return str(Path(output_dir) / safe_employee / filename)


def _output_pairs_csv_path(*, output_dir: str | Path, employee: str) -> str:
    safe_employee = safe_name(employee)
    filename = f"{safe_employee}{TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS.pairs_file_suffix}"
    return str(Path(output_dir) / safe_employee / filename)


def _source_pairs_csv_path(*, pairs_dir: str | Path, employee: str) -> Path:
    return Path(pairs_dir) / f"{safe_name(employee)}.pairs.csv"


def _empty_like(df: pd.DataFrame) -> pd.DataFrame:
    del df
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _filter_afternoon_long(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"is_afternoon", "is_long", "entry_ts"}
    if not required_cols.issubset(df.columns):
        return _empty_like(df)

    try:
        entry_dt = pd.to_datetime(df["entry_ts"], errors="coerce", format="mixed")
    except TypeError:
        entry_dt = pd.to_datetime(df["entry_ts"], errors="coerce")

    rounded_entry = _nearest_target_entry_label(entry_dt)
    mask = (
        to_bool_series(df["is_afternoon"])
        & to_bool_series(df["is_long"])
        & rounded_entry.eq("14:00")
    )
    filtered = df.loc[mask].copy()
    filtered["_rounded_entry_target"] = rounded_entry.loc[mask]
    return filtered


def _nearest_target_entry_label(entry_dt: pd.Series) -> pd.Series:
    def _resolve(value: pd.Timestamp) -> str:
        if pd.isna(value):
            return ""
        minutes = int(value.hour) * 60 + int(value.minute)
        diff_1300 = abs(minutes - (13 * 60))
        diff_1400 = abs(minutes - (14 * 60))
        return "13:00" if diff_1300 < diff_1400 else "14:00"

    return entry_dt.map(_resolve)


def _format_output_rows(df: pd.DataFrame, *, fallback_employee: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    employee_values = (
        df["employee"] if "employee" in df.columns else pd.Series(fallback_employee, index=df.index)
    )
    entry_values = df["entry_ts"] if "entry_ts" in df.columns else pd.Series("", index=df.index)
    exit_values = df["exit_ts"] if "exit_ts" in df.columns else pd.Series("", index=df.index)
    duration_values = (
        df["duration_hours"] if "duration_hours" in df.columns else pd.Series("", index=df.index)
    )
    turno_values = df["turno"] if "turno" in df.columns else pd.Series("", index=df.index)
    holiday_values = (
        to_bool_series(df["is_holiday"])
        if "is_holiday" in df.columns
        else pd.Series(False, index=df.index)
    )
    try:
        entry_dt = pd.to_datetime(entry_values, errors="coerce", format="mixed")
    except TypeError:
        entry_dt = pd.to_datetime(entry_values, errors="coerce")

    output = pd.DataFrame(
        {
            "dipendente": employee_values.fillna(fallback_employee),
            "entrata": entry_values,
            "uscita": exit_values,
            "durata turno": _format_duration_hours(duration_values),
            "Festivo": holiday_values.map(lambda value: "Festivo" if bool(value) else "Non Festivo"),
            "Turno": turno_values,
            "Data": entry_dt.dt.strftime("%Y-%m-%d").fillna(""),
        }
    )
    return output[OUTPUT_COLUMNS]


def _format_duration_hours(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")

    def _to_hhmm(value: float) -> str:
        if pd.isna(value):
            return ""
        total_minutes = int(round(float(value) * 60))
        sign = "-" if total_minutes < 0 else ""
        total_minutes = abs(total_minutes)
        hours, minutes = divmod(total_minutes, 60)
        return f"{sign}{hours:02d}:{minutes:02d}"

    return numeric.map(_to_hhmm)


def process_one_enriched_file(
    enriched_file: str | Path,
    *,
    output_dir: str | None = None,
    pairs_dir: str | None = None,
) -> dict[str, Any]:
    output_dir = output_dir or default_output_dir()
    pairs_dir = pairs_dir or default_pairs_dir()
    source_path = Path(enriched_file)
    employee = _employee_from_path(source_path)
    out_path = _output_csv_path(output_dir=output_dir, employee=employee)
    source_pairs_path = _source_pairs_csv_path(pairs_dir=pairs_dir, employee=employee)
    output_pairs_path = _output_pairs_csv_path(output_dir=output_dir, employee=employee)
    result: dict[str, Any] = {
        "status": "error",
        "source_enriched_csv": str(source_path),
        "source_pairs_csv": str(source_pairs_path),
        "output_filtered_csv": os.path.abspath(out_path),
        "output_pairs_csv": os.path.abspath(output_pairs_path),
        "employee": employee,
        "rows_total": 0,
        "rows_selected": 0,
        "pairs_rows": 0,
        "error_code": None,
        "error": None,
    }

    if not source_path.exists():
        result["error_code"] = "missing_input"
        result["error"] = f"Missing enriched file: {source_path}"
        return result

    try:
        df = pd.read_csv(source_path)
    except Exception as exc:
        result["error_code"] = "read_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["rows_total"] = int(len(df))
    filtered = _filter_afternoon_long(df)
    result["rows_selected"] = int(len(filtered))
    formatted = _format_output_rows(filtered, fallback_employee=employee)

    ensure_parent_dir(out_path)
    if formatted.empty:
        _empty_like(df).to_csv(out_path, index=False)
    else:
        formatted.to_csv(out_path, index=False)

    if not source_pairs_path.exists():
        result["error_code"] = "missing_pairs"
        result["error"] = f"Missing pairs file: {source_pairs_path}"
        return result

    try:
        pairs_df = pd.read_csv(source_pairs_path)
    except Exception as exc:
        result["error_code"] = "pairs_read_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["pairs_rows"] = int(len(pairs_df))
    ensure_parent_dir(output_pairs_path)
    shutil.copyfile(source_pairs_path, output_pairs_path)
    result["status"] = "ok"
    return result


def process_many_enriched_files(
    enriched_files: Iterable[str | Path],
    *,
    output_dir: str | None = None,
    enriched_dir: str | None = None,
    pairs_dir: str | None = None,
) -> dict[str, Any]:
    output_dir = output_dir or default_output_dir()
    enriched_dir = enriched_dir or default_enriched_dir()
    pairs_dir = pairs_dir or default_pairs_dir()
    normalized_files = sorted(Path(path) for path in enriched_files)

    stats: dict[str, Any] = {
        "files_total": len(normalized_files),
        "files_processed": 0,
        "files_error": 0,
        "employees_total": len(normalized_files),
        "rows_total": 0,
        "rows_selected": 0,
        "pairs_rows": 0,
        "files_with_selected_rows": 0,
        "files_without_selected_rows": 0,
    }
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []

    for file_index, enriched_file in enumerate(normalized_files, start=1):
        employee_name = _employee_from_path(enriched_file)
        logger.info("Employee %s/%s: %s", file_index, len(normalized_files), employee_name)

        item = process_one_enriched_file(
            enriched_file,
            output_dir=output_dir,
            pairs_dir=pairs_dir,
        )
        items.append(item)

        if item["status"] != "ok":
            stats["files_error"] += 1
            issues.append(
                {
                    "code": str(item.get("error_code") or "processing_error"),
                    "enriched_csv": str(item.get("source_enriched_csv") or ""),
                    "pairs_csv": str(item.get("source_pairs_csv") or ""),
                    "message": str(item.get("error") or "processing_error"),
                }
            )
            continue

        stats["files_processed"] += 1
        stats["rows_total"] += int(item["rows_total"])
        stats["rows_selected"] += int(item["rows_selected"])
        stats["pairs_rows"] += int(item["pairs_rows"])
        if int(item["rows_selected"]) > 0:
            stats["files_with_selected_rows"] += 1
        else:
            stats["files_without_selected_rows"] += 1

    return build_stage_report(
        stage=TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS.step,
        inputs={
            "enriched_dir": os.path.abspath(enriched_dir) if enriched_dir else None,
            "pairs_dir": os.path.abspath(pairs_dir) if pairs_dir else None,
            "output_dir": os.path.abspath(output_dir),
            "filter": {"is_afternoon": True, "is_long": True},
        },
        outputs={"output_dir": os.path.abspath(output_dir)},
        stats=stats,
        row_totals={
            "items": len(items),
            "issues": len(issues),
            "filtered_files": stats["files_processed"],
        },
        items=items,
        issues=issues,
    )


def export_afternoon_long_from_dir(
    *,
    enriched_dir: str | None = None,
    pairs_dir: str | None = None,
    output_dir: str | None = None,
    report_json: str | None = None,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    pairs_dir = pairs_dir or default_pairs_dir()
    output_dir = output_dir or default_output_dir()
    report_json = report_json or default_report_json_path()
    ensure_dir(output_dir)
    enriched_path = Path(enriched_dir)
    enriched_files = sorted(enriched_path.glob("*.enriched.csv"))
    report = process_many_enriched_files(
        enriched_files,
        output_dir=output_dir,
        enriched_dir=enriched_dir,
        pairs_dir=pairs_dir,
    )
    report["outputs"]["report_json"] = str(Path(report_json).resolve())
    write_json_report(report_json, compact_stage_report(report))
    return report


def run_from_options(options: TurniAfternoonLongExportOptions) -> dict[str, Any]:
    return export_afternoon_long_from_dir(
        enriched_dir=options.enriched_dir,
        pairs_dir=options.pairs_dir,
        output_dir=options.output_dir,
        report_json=options.report_json,
    )


__all__ = [
    "export_afternoon_long_from_dir",
    "process_many_enriched_files",
    "process_one_enriched_file",
    "run_from_options",
]
