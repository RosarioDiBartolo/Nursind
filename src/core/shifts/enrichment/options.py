from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MIN_HOURS = 6.0


def default_output_dir() -> str:
    return "output/default/enrichment"


def default_input_dir() -> str:
    return "output/default/shifts"


def default_report_json_path() -> str:
    return "output/default/enrichment/turni_enrichment.stats.json"


@dataclass(slots=True)
class TurniEnrichmentOptions:
    input_dir: str
    output_dir: str
    min_hours: float = DEFAULT_MIN_HOURS
    include_holidays: bool = True
    report_json: str = "turni_enrichment.stats.json"
    verbose: bool = False
