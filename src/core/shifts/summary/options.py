from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

DEFAULT_YEAR_START: int | None = None
DEFAULT_YEAR_END: int | None = None
DEFAULT_OUTPUT_FORMAT: Literal["csv", "json"] = "csv"


def default_enriched_dir() -> str:
    return "output/default/enrichment"


def default_summary_csv_path() -> str:
    return "output/default/aggregation/turni_employee_summary.csv"


def default_report_json_path() -> str:
    return "output/default/aggregation/turni_employee_summary.report.json"


@dataclass(slots=True)
class TurniEmployeeSummaryOptions:
    enriched_dir: str
    out: str
    report_json: str
    year_start: int | None = DEFAULT_YEAR_START
    year_end: int | None = DEFAULT_YEAR_END
    output_format: Literal["csv", "json"] = DEFAULT_OUTPUT_FORMAT
    min_hours: float | None = None
    verbose: bool = False
