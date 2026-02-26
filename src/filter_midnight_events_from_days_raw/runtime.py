from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir

from .options import (
    DEFAULT_EVENTS_NAME,
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_REMOVED_CSV,
    DEFAULT_REPORT_JSON,
    FilterMidnightEventsOptions,
)
from .service import process_many_events_files


def _build_removed_rows_df(removed_rows_records: list[dict[str, Any]]) -> pd.DataFrame:
    if removed_rows_records:
        return pd.DataFrame(removed_rows_records)
    return pd.DataFrame(
        columns=[
            "source_events_csv",
            "source_events_row_index",
            "filter_reason",
        ]
    )


def build_filter_midnight_events_from_days_raw_from_dir(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    events_name: str = DEFAULT_EVENTS_NAME,
    out_name: str = DEFAULT_OUT_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    removed_csv: str = DEFAULT_REMOVED_CSV,
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
    in_place: bool = False,
) -> dict[str, Any]:
    base = Path(input_dir)
    event_files = sorted(base.rglob(events_name))
    report = process_many_events_files(
        event_files,
        output_dir=input_dir,
        out_name=out_name,
        input_base=base,
        events_name=events_name,
        input_dir=input_dir,
        max_removed_examples_per_file=max_removed_examples_per_file,
        in_place=in_place,
        include_removed_rows_records=True,
    )

    removed_rows_records = list(report.pop("_removed_rows_records", []))
    removed_rows_df = _build_removed_rows_df(removed_rows_records)
    ensure_parent_dir(removed_csv)
    removed_rows_df.to_csv(removed_csv, index=False)

    report["removed_rows_csv"] = os.path.abspath(removed_csv)

    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def filter_midnight_events_dir(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    events_name: str = DEFAULT_EVENTS_NAME,
    out_name: str = DEFAULT_OUT_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    removed_csv: str = DEFAULT_REMOVED_CSV,
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
    in_place: bool = False,
) -> dict[str, Any]:
    return build_filter_midnight_events_from_days_raw_from_dir(
        input_dir=input_dir,
        events_name=events_name,
        out_name=out_name,
        report_json=report_json,
        removed_csv=removed_csv,
        max_removed_examples_per_file=max_removed_examples_per_file,
        in_place=in_place,
    )


def run_from_options(options: FilterMidnightEventsOptions) -> dict[str, Any]:
    return build_filter_midnight_events_from_days_raw_from_dir(
        input_dir=options.input_dir,
        events_name=options.events_name,
        out_name=options.out_name,
        report_json=options.report_json,
        removed_csv=options.removed_csv,
        max_removed_examples_per_file=options.max_removed_examples_per_file,
        in_place=options.in_place,
    )
