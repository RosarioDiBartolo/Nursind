from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir

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

    outputs = report.setdefault("outputs", {})
    outputs["report_json"] = str(report_path.resolve())
    outputs["employee_summary_csv"] = str(employee_csv_path.resolve())
    outputs["issues_csv"] = str(issues_csv_path.resolve())

    ensure_parent_dir(str(report_path))
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)

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
