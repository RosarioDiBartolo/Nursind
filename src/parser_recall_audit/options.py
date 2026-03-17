from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Sequence

from cartellino_parser.pipeline_paths import build_pipeline_paths, with_parser_recall_audit_overrides

DEFAULT_MAX_TINY_ROWS = 3
DEFAULT_MIN_LARGE_ROWS = 10
DEFAULT_LOW_COVERAGE_THRESHOLD = 0.25


def _default_paths():
    return build_pipeline_paths().parser_recall_audit


def default_root_dir() -> str:
    return str(_default_paths().root_dir)


def default_report_json_path() -> str:
    return str(_default_paths().report_json)


def default_suspicious_csv_path() -> str:
    return str(_default_paths().suspicious_csv)


@dataclass(slots=True)
class ParserRecallAuditOptions:
    root_dir: str = field(default_factory=default_root_dir)
    report_json: str = field(default_factory=default_report_json_path)
    suspicious_csv: str = field(default_factory=default_suspicious_csv_path)
    max_tiny_rows: int = DEFAULT_MAX_TINY_ROWS
    min_large_rows: int = DEFAULT_MIN_LARGE_ROWS
    low_coverage_threshold: float = DEFAULT_LOW_COVERAGE_THRESHOLD
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(
        description=(
            "Scan output/* pipeline folders, rank suspicious pages from events/pages.csv, "
            "and emit a review queue with direct Drive PDF links."
        )
    )
    parser.add_argument(
        "--root-dir",
        default=str(defaults.root_dir),
        help=(
            "Root folder containing one or more canonical pipeline folders. "
            "A single pipeline dir is also supported."
        ),
    )
    parser.add_argument(
        "--report-json",
        default="parser_recall_audit.report.json",
        help="JSON report output path relative to --root-dir.",
    )
    parser.add_argument(
        "--suspicious-csv",
        default="suspicious_pages.csv",
        help="Suspicious-pages CSV output path relative to --root-dir.",
    )
    parser.add_argument(
        "--max-tiny-rows",
        type=int,
        default=DEFAULT_MAX_TINY_ROWS,
        help="Maximum rows_considered value treated as a tiny-page anomaly.",
    )
    parser.add_argument(
        "--min-large-rows",
        type=int,
        default=DEFAULT_MIN_LARGE_ROWS,
        help="Minimum rows_considered value treated as a large page for zero/low-coverage checks.",
    )
    parser.add_argument(
        "--low-coverage-threshold",
        type=float,
        default=DEFAULT_LOW_COVERAGE_THRESHOLD,
        help="Coverage ratio threshold used for low-coverage page anomalies.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> ParserRecallAuditOptions:
    args = build_parser().parse_args(argv)
    paths = with_parser_recall_audit_overrides(
        build_pipeline_paths(),
        root_dir=args.root_dir,
        report_json=args.report_json,
        suspicious_csv=args.suspicious_csv,
    )
    resolved = paths.parser_recall_audit
    return ParserRecallAuditOptions(
        root_dir=str(resolved.root_dir),
        report_json=str(resolved.report_json),
        suspicious_csv=str(resolved.suspicious_csv),
        max_tiny_rows=int(args.max_tiny_rows),
        min_large_rows=int(args.min_large_rows),
        low_coverage_threshold=float(args.low_coverage_threshold),
        verbose=bool(args.verbose),
    )


__all__ = [
    "DEFAULT_LOW_COVERAGE_THRESHOLD",
    "DEFAULT_MAX_TINY_ROWS",
    "DEFAULT_MIN_LARGE_ROWS",
    "ParserRecallAuditOptions",
    "build_parser",
    "default_report_json_path",
    "default_root_dir",
    "default_suspicious_csv_path",
    "parse_options",
]

