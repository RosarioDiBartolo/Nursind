from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir

EVENT_COLUMNS = [
    "event_id",
    "event_ts",
    "event_kind",
    "event_time_hhmm",
    "event_raw",
    "parser_id",
    "source_origin",
    "source_doc_json",
    "source_file_id",
    "source_file_name",
    "source_employee",
    "source_drive_path",
    "source_file_link",
    "source_page_no",
    "source_line_id",
    "source_line_no",
    "source_slot",
    "source_event_ref",
]

PAGE_COLUMNS = [
    "page_ref",
    "source_doc_json",
    "source_file_id",
    "source_file_name",
    "source_employee",
    "source_drive_path",
    "source_file_link",
    "page_no",
    "page_kind",
    "decision",
    "decision_reason",
    "parser_id",
    "page_year",
    "page_month",
    "year_month_source",
    "relevant_for_coverage",
    "rows_considered",
    "rows_with_events",
    "rows_without_events",
    "events_extracted",
    "events_dropped_missing_year_month",
    "coverage_ratio_page",
    "header_preview",
    "parse_error",
]


def write_rows_csv(
    *,
    rows: list[dict[str, Any]],
    out_csv: Path,
    columns: list[str],
) -> None:
    ensure_parent_dir(str(out_csv))
    with open(out_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


__all__ = [
    "EVENT_COLUMNS",
    "PAGE_COLUMNS",
    "write_rows_csv",
]
