from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_dir, ensure_parent_dir

from .options import (
    DEFAULT_MIN_HOURS,
    TurniEnrichmentOptions,
    default_input_dir,
    default_output_dir,
    default_report_json_path,
)
from .service import process_many_pairs_files


def _write_json(out_path: str, payload: dict[str, Any]) -> None:
    ensure_parent_dir(out_path)
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def build_turni_enrichment_from_dir(
    *,
    input_dir: str | None = None,
    output_dir: str | None = None,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
    report_json: str | None = None,
) -> dict[str, Any]:
    input_dir = input_dir or default_input_dir()
    output_dir = output_dir or default_output_dir()
    report_json = report_json or default_report_json_path()
    ensure_dir(output_dir)
    input_path = Path(input_dir)
    pairs_files = sorted(input_path.glob("*.pairs.csv"))
    report = process_many_pairs_files(
        pairs_files,
        output_dir=output_dir,
        min_hours=min_hours,
        include_holidays=include_holidays,
        input_dir=input_dir,
    )
    _write_json(report_json, report)
    return report


def enrich_pairs_by_employee(
    *,
    input_dir: str | None = None,
    output_dir: str | None = None,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
) -> dict[str, int]:
    input_dir = input_dir or default_input_dir()
    output_dir = output_dir or default_output_dir()
    ensure_dir(output_dir)
    input_path = Path(input_dir)
    pairs_files = sorted(input_path.glob("*.pairs.csv"))
    report = process_many_pairs_files(
        pairs_files,
        output_dir=output_dir,
        min_hours=min_hours,
        include_holidays=include_holidays,
        input_dir=input_dir,
    )
    stats = report["stats"]
    return {
        "dipendenti": int(stats["dipendenti"]),
        "file_totali": int(stats["file_totali"]),
        "file_mancanti": int(stats["file_mancanti"]),
        "file_errori": int(stats["file_errori"]),
        "righe_totali": int(stats["righe_totali"]),
        "righe_completate": int(stats["righe_completate"]),
        "righe_enriched": int(stats["righe_enriched"]),
        "overnight_fix": int(stats["overnight_fix"]),
    }


def run_from_options(options: TurniEnrichmentOptions) -> dict[str, Any]:
    return build_turni_enrichment_from_dir(
        input_dir=options.input_dir,
        output_dir=options.output_dir,
        min_hours=options.min_hours,
        include_holidays=options.include_holidays,
        report_json=options.report_json,
    )
