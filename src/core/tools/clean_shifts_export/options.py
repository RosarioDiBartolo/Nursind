from __future__ import annotations

from dataclasses import dataclass

DEFAULT_REPORT_JSON = "turni_clean_shifts.report.json"


def default_enriched_dir() -> str:
    return "output/default/enrichment"


def default_output_dir() -> str:
    return "output/default/clean_shifts"


@dataclass(slots=True)
class CleanShiftsExportOptions:
    enriched_dir: str = default_enriched_dir()
    output_dir: str = default_output_dir()
    report_json: str = DEFAULT_REPORT_JSON
    verbose: bool = False
