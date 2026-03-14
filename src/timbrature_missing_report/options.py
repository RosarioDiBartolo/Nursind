from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

try:
    from src.pipeline_paths import build_pipelines_paths
except Exception:  # pragma: no cover - defensive fallback
    build_pipelines_paths = None

if build_pipelines_paths is not None:
    DEFAULT_OUTPUTS = build_pipelines_paths()
    DEFAULT_PIPELINE_DIR = str(DEFAULT_OUTPUTS.root_output)
else:
    DEFAULT_OUTPUTS = None
    DEFAULT_PIPELINE_DIR = str(Path("output") / "default")

DEFAULT_REPORT_JSON = "missing_timbrature.report.json"
DEFAULT_SUMMARY_CSV = "missing_timbrature.summary.csv"
DEFAULT_FINDINGS_CSV = "missing_timbrature.findings.csv"
DEFAULT_COVERAGE_CSV = "missing_timbrature.coverage.csv"


@dataclass(slots=True)
class TimbratureMissingReportOptions:
    pipeline_dir: str = DEFAULT_PIPELINE_DIR
    report_json: str = DEFAULT_REPORT_JSON
    summary_csv: str = DEFAULT_SUMMARY_CSV
    findings_csv: str = DEFAULT_FINDINGS_CSV
    coverage_csv: str = DEFAULT_COVERAGE_CSV
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
    return TimbratureMissingReportOptions(
        pipeline_dir=args.pipeline_dir,
        report_json=args.report_json,
        summary_csv=args.summary_csv,
        findings_csv=args.findings_csv,
        coverage_csv=args.coverage_csv,
        verbose=bool(args.verbose),
    )


__all__ = [
    "DEFAULT_COVERAGE_CSV",
    "DEFAULT_FINDINGS_CSV",
    "DEFAULT_OUTPUTS",
    "DEFAULT_PIPELINE_DIR",
    "DEFAULT_REPORT_JSON",
    "DEFAULT_SUMMARY_CSV",
    "TimbratureMissingReportOptions",
    "build_parser",
    "parse_options",
]
