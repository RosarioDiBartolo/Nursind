from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MAX_TINY_ROWS = 3
DEFAULT_MIN_LARGE_ROWS = 10
DEFAULT_LOW_COVERAGE_THRESHOLD = 0.25


def default_root_dir() -> str:
    return "output"


def default_report_json_path() -> str:
    return "output/parser_recall_audit.report.json"


def default_suspicious_csv_path() -> str:
    return "output/suspicious_pages.csv"


@dataclass(slots=True)
class ParserRecallAuditOptions:
    root_dir: str
    report_json: str
    suspicious_csv: str
    max_tiny_rows: int = DEFAULT_MAX_TINY_ROWS
    min_large_rows: int = DEFAULT_MIN_LARGE_ROWS
    low_coverage_threshold: float = DEFAULT_LOW_COVERAGE_THRESHOLD
    verbose: bool = False
