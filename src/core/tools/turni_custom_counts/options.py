from __future__ import annotations

from dataclasses import dataclass

DEFAULT_REPORT_JSON = "turni_custom_counts.report.json"
DEFAULT_SUMMARY_CSV = "turni_custom_counts.csv"
DEFAULT_YEAR_START: int | None = None
DEFAULT_YEAR_END: int | None = None


def default_enriched_dir() -> str:
    return "output/default/enrichment"


def default_output_dir() -> str:
    return "output/default/turni_custom_counts"


@dataclass(slots=True)
class TurniCustomCountsOptions:
    enriched_dir: str = default_enriched_dir()
    output_dir: str = default_output_dir()
    summary_csv: str = DEFAULT_SUMMARY_CSV
    report_json: str = DEFAULT_REPORT_JSON
    year_start: int | None = DEFAULT_YEAR_START
    year_end: int | None = DEFAULT_YEAR_END
    verbose: bool = False
