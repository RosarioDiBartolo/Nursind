from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir

from .options import (
    TimbratureMissingReportOptions,
    default_coverage_csv_path,
    default_findings_csv_path,
    default_pipeline_dir,
    default_report_json_path,
    default_summary_csv_path,
)
from .service import (
    COVERAGE_COLUMNS,
    FINDING_COLUMNS,
    SUMMARY_COLUMNS,
    audit_missing_timbrature_pipeline,
)

LEGACY_OUTPUT_FILES = (
    "missing_timbrature.employees.csv",
    "missing_timbrature.issues.csv",
)
LEGACY_OUTPUT_DIRS = (
    "missing_timbrature.non_ocr_files",
    "missing_timbrature.missing_months",
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


def _cleanup_legacy_outputs(
    *,
    pipeline_dir: str | Path,
    active_output_paths: set[Path],
) -> None:
    for relative_path in LEGACY_OUTPUT_FILES:
        legacy_path = _resolve_output_path(pipeline_dir, relative_path).resolve()
        if legacy_path in active_output_paths:
            continue
        legacy_path.unlink(missing_ok=True)

    for relative_path in LEGACY_OUTPUT_DIRS:
        legacy_dir = _resolve_output_path(pipeline_dir, relative_path).resolve()
        if not legacy_dir.exists():
            continue
        if any(legacy_dir == active_path or legacy_dir in active_path.parents for active_path in active_output_paths):
            continue
        shutil.rmtree(legacy_dir, ignore_errors=True)


def _build_json_payload(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "stats": report["stats"],
        "artifacts": report["artifacts"],
        "outputs": report["outputs"],
        "finding_counts_by_type": report["finding_counts_by_type"],
        "coverage_counts_by_type": report["coverage_counts_by_type"],
        "row_totals": {
            "summary_rows": len(report["summary_rows"]),
            "finding_rows": len(report["finding_rows"]),
            "coverage_rows": len(report["coverage_rows"]),
        },
    }


def build_missing_timbrature_report(
    *,
    pipeline_dir: str | None = None,
    report_json: str | None = None,
    summary_csv: str | None = None,
    findings_csv: str | None = None,
    coverage_csv: str | None = None,
) -> dict[str, Any]:
    pipeline_dir = pipeline_dir or default_pipeline_dir()
    report_json = report_json or default_report_json_path()
    summary_csv = summary_csv or default_summary_csv_path()
    findings_csv = findings_csv or default_findings_csv_path()
    coverage_csv = coverage_csv or default_coverage_csv_path()
    report = audit_missing_timbrature_pipeline(pipeline_dir)

    report_path = _resolve_output_path(pipeline_dir, report_json)
    summary_csv_path = _resolve_output_path(pipeline_dir, summary_csv)
    findings_csv_path = _resolve_output_path(pipeline_dir, findings_csv)
    coverage_csv_path = _resolve_output_path(pipeline_dir, coverage_csv)

    outputs = report.setdefault("outputs", {})
    outputs["report_json"] = str(report_path.resolve())
    outputs["summary_csv"] = str(summary_csv_path.resolve())
    outputs["findings_csv"] = str(findings_csv_path.resolve())
    outputs["coverage_csv"] = str(coverage_csv_path.resolve())

    _write_csv(summary_csv_path, report["summary_rows"], SUMMARY_COLUMNS)
    _write_csv(findings_csv_path, report["finding_rows"], FINDING_COLUMNS)
    _write_csv(coverage_csv_path, report["coverage_rows"], COVERAGE_COLUMNS)

    active_output_paths = {
        report_path.resolve(),
        summary_csv_path.resolve(),
        findings_csv_path.resolve(),
        coverage_csv_path.resolve(),
    }
    _cleanup_legacy_outputs(pipeline_dir=pipeline_dir, active_output_paths=active_output_paths)

    json_payload = _build_json_payload(report)
    ensure_parent_dir(str(report_path))
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(json_payload, handle, ensure_ascii=False, indent=2)
    return report


def run_from_options(options: TimbratureMissingReportOptions) -> dict[str, Any]:
    return build_missing_timbrature_report(
        pipeline_dir=options.pipeline_dir,
        report_json=options.report_json,
        summary_csv=options.summary_csv,
        findings_csv=options.findings_csv,
        coverage_csv=options.coverage_csv,
    )


__all__ = [
    "build_missing_timbrature_report",
    "run_from_options",
]
