from __future__ import annotations

from dataclasses import dataclass

DEFAULT_REPORT_JSON = "missing_timbrature.report.json"
DEFAULT_SUMMARY_CSV = "missing_timbrature.summary.csv"
DEFAULT_FINDINGS_CSV = "missing_timbrature.findings.csv"
DEFAULT_COVERAGE_CSV = "missing_timbrature.coverage.csv"


def default_pipeline_dir() -> str:
    return "output/default"


def default_report_json_path() -> str:
    return f"{default_pipeline_dir()}/{DEFAULT_REPORT_JSON}"


def default_summary_csv_path() -> str:
    return f"{default_pipeline_dir()}/{DEFAULT_SUMMARY_CSV}"


def default_findings_csv_path() -> str:
    return f"{default_pipeline_dir()}/{DEFAULT_FINDINGS_CSV}"


def default_coverage_csv_path() -> str:
    return f"{default_pipeline_dir()}/{DEFAULT_COVERAGE_CSV}"


@dataclass(slots=True)
class TimbratureMissingReportOptions:
    pipeline_dir: str
    report_json: str = DEFAULT_REPORT_JSON
    summary_csv: str = DEFAULT_SUMMARY_CSV
    findings_csv: str = DEFAULT_FINDINGS_CSV
    coverage_csv: str = DEFAULT_COVERAGE_CSV
    verbose: bool = False
