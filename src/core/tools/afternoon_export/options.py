from __future__ import annotations

from dataclasses import dataclass


def default_enriched_dir() -> str:
    return "output/default/enrichment"


def default_pairs_dir() -> str:
    return "output/default/shifts"


def default_output_dir() -> str:
    return "output/afternoon_long_export"


def default_report_json_path() -> str:
    return f"{default_output_dir()}/turni_afternoon_long_export.report.json"


@dataclass(slots=True)
class TurniAfternoonLongExportOptions:
    enriched_dir: str
    pairs_dir: str
    output_dir: str
    report_json: str
    verbose: bool = False
