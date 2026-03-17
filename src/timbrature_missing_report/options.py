from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Sequence

from cartellino_parser.pipeline_paths import build_pipeline_paths, with_timbrature_missing_report_overrides


def _default_paths():
    return build_pipeline_paths().timbrature_missing_report


def default_pipeline_dir() -> str:
    return str(_default_paths().pipeline_dir)


def default_report_json_path() -> str:
    return str(_default_paths().report_json)


def default_summary_csv_path() -> str:
    return str(_default_paths().summary_csv)


def default_findings_csv_path() -> str:
    return str(_default_paths().findings_csv)


def default_coverage_csv_path() -> str:
    return str(_default_paths().coverage_csv)


DEFAULT_REPORT_JSON = "missing_timbrature.report.json"
DEFAULT_SUMMARY_CSV = "missing_timbrature.summary.csv"
DEFAULT_FINDINGS_CSV = "missing_timbrature.findings.csv"
DEFAULT_COVERAGE_CSV = "missing_timbrature.coverage.csv"


@dataclass(slots=True)
class TimbratureMissingReportOptions:
    pipeline_dir: str = field(default_factory=default_pipeline_dir)
    report_json: str = field(default_factory=default_report_json_path)
    summary_csv: str = field(default_factory=default_summary_csv_path)
    findings_csv: str = field(default_factory=default_findings_csv_path)
    coverage_csv: str = field(default_factory=default_coverage_csv_path)
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(
        description=(
            "Audit a pipeline folder and report employees with missing timbrature, "
            "missing text-layer documents, unresolved month/year pages, and missing "
            "coverage months from relevant pages."
        )
    )
    parser.add_argument(
        "--pipeline-dir",
        default=str(defaults.pipeline_dir),
        help="Root pipeline folder using the canonical layout.",
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help="JSON report output path (relative paths are resolved from --pipeline-dir).",
    )
    parser.add_argument(
        "--summary-csv",
        default=DEFAULT_SUMMARY_CSV,
        help="Summary CSV output path (relative paths are resolved from --pipeline-dir).",
    )
    parser.add_argument(
        "--findings-csv",
        default=DEFAULT_FINDINGS_CSV,
        help="Findings CSV output path (relative paths are resolved from --pipeline-dir).",
    )
    parser.add_argument(
        "--coverage-csv",
        default=DEFAULT_COVERAGE_CSV,
        help="Coverage CSV output path (relative paths are resolved from --pipeline-dir).",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> TimbratureMissingReportOptions:
    args = build_parser().parse_args(argv)
    paths = with_timbrature_missing_report_overrides(
        build_pipeline_paths(),
        pipeline_dir=args.pipeline_dir,
        report_json=args.report_json,
        summary_csv=args.summary_csv,
        findings_csv=args.findings_csv,
        coverage_csv=args.coverage_csv,
    )
    resolved = paths.timbrature_missing_report
    return TimbratureMissingReportOptions(
        pipeline_dir=str(resolved.pipeline_dir),
        report_json=str(resolved.report_json),
        summary_csv=str(resolved.summary_csv),
        findings_csv=str(resolved.findings_csv),
        coverage_csv=str(resolved.coverage_csv),
        verbose=bool(args.verbose),
    )


__all__ = [
    "DEFAULT_COVERAGE_CSV",
    "DEFAULT_FINDINGS_CSV",
    "DEFAULT_REPORT_JSON",
    "DEFAULT_SUMMARY_CSV",
    "TimbratureMissingReportOptions",
    "build_parser",
    "default_coverage_csv_path",
    "default_findings_csv_path",
    "default_pipeline_dir",
    "default_report_json_path",
    "default_summary_csv_path",
    "parse_options",
]

