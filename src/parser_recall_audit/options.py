from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

DEFAULT_ROOT_DIR = str(Path("output"))
DEFAULT_REPORT_JSON = "parser_recall_audit.report.json"
DEFAULT_SUSPICIOUS_CSV = "suspicious_pages.csv"
DEFAULT_MAX_TINY_ROWS = 3
DEFAULT_MIN_LARGE_ROWS = 10
DEFAULT_LOW_COVERAGE_THRESHOLD = 0.25


@dataclass(slots=True)
class ParserRecallAuditOptions:
    root_dir: str = DEFAULT_ROOT_DIR
    report_json: str = DEFAULT_REPORT_JSON
    suspicious_csv: str = DEFAULT_SUSPICIOUS_CSV
    max_tiny_rows: int = DEFAULT_MAX_TINY_ROWS
    min_large_rows: int = DEFAULT_MIN_LARGE_ROWS
    low_coverage_threshold: float = DEFAULT_LOW_COVERAGE_THRESHOLD
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Scan output/* pipeline folders, rank suspicious pages from events/pages.csv, "
            "and emit a review queue with direct Drive PDF links."
        )
    )
    parser.add_argument(
        "--root-dir",
        default=DEFAULT_ROOT_DIR,
        help=(
            "Root folder containing one or more canonical pipeline folders. "
            "A single pipeline dir is also supported."
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help="JSON report output path (relative paths are resolved from --root-dir).",
    )
    parser.add_argument(
        "--suspicious-csv",
        default=DEFAULT_SUSPICIOUS_CSV,
        help="Suspicious-pages CSV output path (relative paths are resolved from --root-dir).",
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
    return ParserRecallAuditOptions(
        root_dir=args.root_dir,
        report_json=args.report_json,
        suspicious_csv=args.suspicious_csv,
        max_tiny_rows=int(args.max_tiny_rows),
        min_large_rows=int(args.min_large_rows),
        low_coverage_threshold=float(args.low_coverage_threshold),
        verbose=bool(args.verbose),
    )


__all__ = [
    "DEFAULT_LOW_COVERAGE_THRESHOLD",
    "DEFAULT_MAX_TINY_ROWS",
    "DEFAULT_MIN_LARGE_ROWS",
    "DEFAULT_REPORT_JSON",
    "DEFAULT_ROOT_DIR",
    "DEFAULT_SUSPICIOUS_CSV",
    "ParserRecallAuditOptions",
    "build_parser",
    "parse_options",
]
