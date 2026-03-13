from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

try:
    from src.drive_service.output_paths import build_pipelines_paths
except Exception:  # pragma: no cover - defensive fallback
    build_pipelines_paths = None

if build_pipelines_paths is not None:
    DEFAULT_OUTPUTS = build_pipelines_paths()
    DEFAULT_PIPELINE_DIR = str(DEFAULT_OUTPUTS.root_output)
else:
    DEFAULT_OUTPUTS = None
    DEFAULT_PIPELINE_DIR = str(Path("output") / "default")

DEFAULT_REPORT_JSON = "missing_timbrature.report.json"
DEFAULT_EMPLOYEE_SUMMARY_CSV = "missing_timbrature.employees.csv"
DEFAULT_ISSUES_CSV = "missing_timbrature.issues.csv"


@dataclass(slots=True)
class TimbratureMissingReportOptions:
    pipeline_dir: str = DEFAULT_PIPELINE_DIR
    report_json: str = DEFAULT_REPORT_JSON
    employee_summary_csv: str = DEFAULT_EMPLOYEE_SUMMARY_CSV
    issues_csv: str = DEFAULT_ISSUES_CSV
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit a pipeline folder and report employees with missing timbrature, "
            "missing text-layer documents, unresolved month/year pages, and months "
            "missing after pairing."
        )
    )
    parser.add_argument(
        "--pipeline-dir",
        default=DEFAULT_PIPELINE_DIR,
        help="Root pipeline folder. Supports current and legacy layouts.",
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help="JSON report output path (relative paths are resolved from --pipeline-dir).",
    )
    parser.add_argument(
        "--employee-summary-csv",
        default=DEFAULT_EMPLOYEE_SUMMARY_CSV,
        help="Employee summary CSV output path (relative paths are resolved from --pipeline-dir).",
    )
    parser.add_argument(
        "--issues-csv",
        default=DEFAULT_ISSUES_CSV,
        help="Issue detail CSV output path (relative paths are resolved from --pipeline-dir).",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> TimbratureMissingReportOptions:
    args = build_parser().parse_args(argv)
    return TimbratureMissingReportOptions(
        pipeline_dir=args.pipeline_dir,
        report_json=args.report_json,
        employee_summary_csv=args.employee_summary_csv,
        issues_csv=args.issues_csv,
        verbose=bool(args.verbose),
    )


__all__ = [
    "DEFAULT_EMPLOYEE_SUMMARY_CSV",
    "DEFAULT_ISSUES_CSV",
    "DEFAULT_OUTPUTS",
    "DEFAULT_PIPELINE_DIR",
    "DEFAULT_REPORT_JSON",
    "TimbratureMissingReportOptions",
    "build_parser",
    "parse_options",
]
