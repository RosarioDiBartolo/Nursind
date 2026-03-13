from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.names import safe_name

from .options import (
    DEFAULT_EMPLOYEE_SUMMARY_CSV,
    DEFAULT_ISSUES_CSV,
    DEFAULT_PIPELINE_DIR,
    DEFAULT_REPORT_JSON,
    TimbratureMissingReportOptions,
)
from .service import (
    EMPLOYEE_SUMMARY_COLUMNS,
    ISSUE_COLUMNS,
    audit_missing_timbrature_pipeline,
)

DEFAULT_NON_OCR_FILES_DIR = "missing_timbrature.non_ocr_files"
DEFAULT_MISSING_MONTHS_DIR = "missing_timbrature.missing_months"
MONTH_NAMES_IT = {
    1: "gennaio",
    2: "febbraio",
    3: "marzo",
    4: "aprile",
    5: "maggio",
    6: "giugno",
    7: "luglio",
    8: "agosto",
    9: "settembre",
    10: "ottobre",
    11: "novembre",
    12: "dicembre",
}
NON_OCR_FILE_COLUMNS = [
    "employee",
    "employee_id",
    "file_id",
    "file_link",
    "file_name",
    "year",
    "month",
    "month_name",
    "detail",
]
MISSING_MONTH_COLUMNS = [
    "employee",
    "employee_id",
    "year",
    "month",
    "month_name",
]


def _resolve_output_path(pipeline_dir: str | Path, path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return Path(pipeline_dir) / candidate


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    ensure_parent_dir(str(path))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def _write_grouped_employee_csvs(
    *,
    folder: Path,
    rows_by_employee: dict[str, list[dict[str, Any]]],
    columns: list[str],
) -> list[str]:
    folder.mkdir(parents=True, exist_ok=True)
    written_paths: list[str] = []
    written_names: set[str] = set()

    for employee in sorted(rows_by_employee):
        file_name = f"{safe_name(employee)}.csv"
        output_path = folder / file_name
        _write_csv(output_path, rows_by_employee[employee], columns)
        written_paths.append(str(output_path.resolve()))
        written_names.add(file_name)

    for stale_path in folder.glob("*.csv"):
        if stale_path.name not in written_names:
            stale_path.unlink(missing_ok=True)

    return written_paths


def _month_name(month_value: Any) -> str | None:
    try:
        month = int(month_value)
    except (TypeError, ValueError):
        return None
    return MONTH_NAMES_IT.get(month)


def _build_non_ocr_rows_by_employee(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    rows_by_employee: dict[str, list[dict[str, Any]]] = {}
    employees = report.get("employees") or []
    issues = report.get("issues") or []

    for employee_row in employees:
        if not isinstance(employee_row, dict):
            continue
        employee_name = str(employee_row.get("employee") or "unknown")
        rows_by_employee.setdefault(employee_name, [])

    for row in issues:
        if not isinstance(row, dict) or row.get("issue_type") != "missing_text_layer":
            continue
        employee_name = str(row.get("employee") or "unknown")
        rows_by_employee.setdefault(employee_name, []).append(
            {
                "employee": employee_name,
                "employee_id": row.get("employee_id"),
                "file_id": row.get("file_id"),
                "file_link": row.get("file_link"),
                "file_name": row.get("file_name"),
                "year": row.get("year"),
                "month": row.get("month"),
                "month_name": _month_name(row.get("month")),
                "detail": row.get("detail"),
            }
        )

    return rows_by_employee


def _build_missing_month_rows_by_employee(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    rows_by_employee: dict[str, list[dict[str, Any]]] = {}
    employees = report.get("employees") or []

    for employee_row in employees:
        if not isinstance(employee_row, dict):
            continue
        employee_name = str(employee_row.get("employee") or "unknown")
        employee_id = employee_row.get("employee_id")
        missing_months = employee_row.get("missing_required_months") or []
        rows_by_employee.setdefault(employee_name, [])
        for year_month in missing_months:
            label = str(year_month or "")
            year = None
            month = None
            if len(label) == 7 and label[4] == "-":
                try:
                    year = int(label[:4])
                    month = int(label[5:7])
                except ValueError:
                    year = None
                    month = None
            rows_by_employee[employee_name].append(
                {
                    "employee": employee_name,
                    "employee_id": employee_id,
                    "year": year,
                    "month": month,
                    "month_name": _month_name(month),
                }
            )

    return rows_by_employee


def build_missing_timbrature_report(
    *,
    pipeline_dir: str = DEFAULT_PIPELINE_DIR,
    report_json: str = DEFAULT_REPORT_JSON,
    employee_summary_csv: str = DEFAULT_EMPLOYEE_SUMMARY_CSV,
    issues_csv: str = DEFAULT_ISSUES_CSV,
) -> dict[str, Any]:
    report = audit_missing_timbrature_pipeline(pipeline_dir)

    report_path = _resolve_output_path(pipeline_dir, report_json)
    employee_csv_path = _resolve_output_path(pipeline_dir, employee_summary_csv)
    issues_csv_path = _resolve_output_path(pipeline_dir, issues_csv)
    non_ocr_dir_path = _resolve_output_path(pipeline_dir, DEFAULT_NON_OCR_FILES_DIR)
    missing_months_dir_path = _resolve_output_path(pipeline_dir, DEFAULT_MISSING_MONTHS_DIR)

    non_ocr_rows_by_employee = _build_non_ocr_rows_by_employee(report)
    missing_month_rows_by_employee = _build_missing_month_rows_by_employee(report)

    outputs = report.setdefault("outputs", {})
    outputs["report_json"] = str(report_path.resolve())
    outputs["employee_summary_csv"] = str(employee_csv_path.resolve())
    outputs["issues_csv"] = str(issues_csv_path.resolve())
    outputs["non_ocr_files_dir"] = str(non_ocr_dir_path.resolve())
    outputs["missing_months_dir"] = str(missing_months_dir_path.resolve())

    _write_csv(
        employee_csv_path,
        report["employee_summary_rows"],
        EMPLOYEE_SUMMARY_COLUMNS,
    )
    _write_csv(
        issues_csv_path,
        report["issues"],
        ISSUE_COLUMNS,
    )
    outputs["non_ocr_employee_csvs"] = _write_grouped_employee_csvs(
        folder=non_ocr_dir_path,
        rows_by_employee=non_ocr_rows_by_employee,
        columns=NON_OCR_FILE_COLUMNS,
    )
    outputs["missing_month_employee_csvs"] = _write_grouped_employee_csvs(
        folder=missing_months_dir_path,
        rows_by_employee=missing_month_rows_by_employee,
        columns=MISSING_MONTH_COLUMNS,
    )

    ensure_parent_dir(str(report_path))
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def run_from_options(options: TimbratureMissingReportOptions) -> dict[str, Any]:
    return build_missing_timbrature_report(
        pipeline_dir=options.pipeline_dir,
        report_json=options.report_json,
        employee_summary_csv=options.employee_summary_csv,
        issues_csv=options.issues_csv,
    )


__all__ = [
    "build_missing_timbrature_report",
    "run_from_options",
]
