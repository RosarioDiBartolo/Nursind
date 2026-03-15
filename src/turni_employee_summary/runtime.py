from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir

from .options import (
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_YEAR_END,
    DEFAULT_YEAR_START,
    TurniEmployeeSummaryOptions,
    default_enriched_dir,
    default_report_json_path,
    default_summary_csv_path,
)
from .service import process_many_enriched_files


def _write_csv(out_path: str, rows: list[dict[str, Any]]) -> None:
    ensure_parent_dir(out_path)
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)


def _write_json(out_path: str, rows: list[dict[str, Any]], stats: dict[str, Any]) -> None:
    ensure_parent_dir(out_path)
    payload = {"rows": rows, "stats": stats}
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def build_turni_employee_summary_from_dir(
    *,
    enriched_dir: str | None = None,
    out: str | None = None,
    report_json: str | None = None,
    output_format: Literal["csv", "json"] = DEFAULT_OUTPUT_FORMAT,
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
) -> dict[str, Any]:
    enriched_dir = enriched_dir or default_enriched_dir()
    out = out or default_summary_csv_path()
    report_json = report_json or default_report_json_path()
    enriched_path = Path(enriched_dir)
    enriched_files = sorted(enriched_path.glob("*.enriched.csv"))
    report = process_many_enriched_files(
        enriched_files,
        min_hours=min_hours,
        year_start=year_start,
        year_end=year_end,
        enriched_dir=enriched_dir,
    )

    if output_format == "csv":
        _write_csv(out, report["rows"])
    else:
        _write_json(out, report["rows"], report["stats"])

    report["output_path"] = os.path.abspath(out)
    report["output_format"] = output_format
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def build_employee_turni_summary(
    *,
    enriched_dir: str | None = None,
    min_hours: float | None = None,
    year_start: int | None = DEFAULT_YEAR_START,
    year_end: int | None = DEFAULT_YEAR_END,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    enriched_dir = enriched_dir or default_enriched_dir()
    enriched_path = Path(enriched_dir)
    enriched_files = sorted(enriched_path.glob("*.enriched.csv"))
    report = process_many_enriched_files(
        enriched_files,
        min_hours=min_hours,
        year_start=year_start,
        year_end=year_end,
        enriched_dir=enriched_dir,
    )
    stats = report["stats"]
    legacy_stats = {
        "dipendenti": int(stats["dipendenti"]),
        "file_totali": int(stats["file_totali"]),
        "file_mancanti": int(stats["file_mancanti"]),
        "file_errori": int(stats["file_errori"]),
        "righe_totali": int(stats["righe_totali"]),
        "righe_classificate": int(stats["righe_classificate"]),
    }
    return list(report["rows"]), legacy_stats


def run_from_options(options: TurniEmployeeSummaryOptions) -> dict[str, Any]:
    return build_turni_employee_summary_from_dir(
        enriched_dir=options.enriched_dir,
        out=options.out,
        report_json=options.report_json,
        output_format=options.output_format,
        min_hours=options.min_hours,
        year_start=options.year_start,
        year_end=options.year_end,
    )
