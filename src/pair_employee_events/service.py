from __future__ import annotations

import logging
import os
from typing import Any, Iterable

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.names import safe_name
from src.reporting import build_stage_report
from src.shift_services import PairsCloser

from .event_normalization import (
    dedupe_events,
    events_to_partial_pairs,
    normalize_employee,
    normalize_events_file,
)
from .options import DEFAULT_MAX_GAP_HOURS, default_output_dir
from .output_formatting import dedupe_closed_pairs, format_output_pairs

logger = logging.getLogger(__name__)


def process_one_employee_events(
    employee: dict[str, Any],
    *,
    output_dir: str | None = None,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    keep_inferred_column: bool = False,
) -> dict[str, Any]:
    output_dir = output_dir or default_output_dir()
    employee_name = str(employee.get("employee") or "unknown")
    employee_id = employee.get("employee_id")
    employee_files = list(employee.get("files") or [])
    result: dict[str, Any] = {
        "status": "error",
        "source_employee": employee_name,
        "employee_id": employee_id,
        "files_total": len(employee_files),
        "files_loaded": 0,
        "files_missing": 0,
        "files_error": 0,
        "events_rows_in": 0,
        "events_valid": 0,
        "events_invalid_kind": 0,
        "events_invalid_ts": 0,
        "events_deduped": 0,
        "partial_rows": 0,
        "pairs_out": 0,
        "pairs_deduped": 0,
        "inferred_pairs": 0,
        "rows_unmatched_after_close": 0,
        "output_csv": None,
        "error_code": None,
        "error": None,
    }

    try:
        closer = PairsCloser(
            max_gap_hours=max_gap_hours,
            mark_inferred=True,
            preserve_exit_raw=True,
            clear_duration_hhmm=True,
        )
        events_frames: list[pd.DataFrame] = []

        for file_desc in employee_files:
            source_events_csv = str(file_desc.get("events_csv") or "")
            file_id = file_desc.get("file_id")
            file_name = file_desc.get("file_name")

            if not source_events_csv or not os.path.exists(source_events_csv):
                result["files_missing"] += 1
                continue

            try:
                raw_df = pd.read_csv(source_events_csv)
                normalized, load_stats = normalize_events_file(
                    raw_df,
                    source_events_csv=source_events_csv,
                    file_id=file_id,
                    file_name=file_name,
                    source_employee=employee_name,
                )
            except Exception as exc:
                result["files_error"] += 1
                logger.exception("Error reading %s", source_events_csv)
                result["error"] = str(exc)
                continue

            result["files_loaded"] += 1
            for key in ("events_rows_in", "events_valid", "events_invalid_kind", "events_invalid_ts"):
                result[key] += int(load_stats[key])

            if not normalized.empty:
                events_frames.append(normalized)

        if int(result["files_loaded"]) <= 0:
            result["error_code"] = "no_events_loaded"
            result["error"] = (
                "No readable events files for employee "
                f"{employee_name}: missing={result['files_missing']} read_errors={result['files_error']}"
            )
            return result

        if events_frames:
            events_merged = pd.concat(events_frames, ignore_index=True)
            events_merged = events_merged.sort_values(
                by=["event_ts", "source_events_csv", "source_row_index", "event_index"],
                kind="stable",
            )
            events_merged, events_deduped = dedupe_events(events_merged)
            result["events_deduped"] = int(events_deduped)
            partial_pairs = events_to_partial_pairs(events_merged)
        else:
            partial_pairs = events_to_partial_pairs(pd.DataFrame())

        result["partial_rows"] = int(len(partial_pairs))
        closed = partial_pairs.copy() if partial_pairs.empty else closer.close(partial_pairs)

        inferred_pairs = 0
        if "closed_inferred" in closed.columns:
            inferred_pairs = int(closed["closed_inferred"].fillna(False).sum())
        result["inferred_pairs"] = inferred_pairs
        result["rows_unmatched_after_close"] = max(
            0,
            int(result["partial_rows"]) - int(len(closed)) - inferred_pairs,
        )

        closed, pairs_deduped = dedupe_closed_pairs(closed)
        result["pairs_deduped"] = int(pairs_deduped)

        out_df = format_output_pairs(closed, keep_inferred_column=keep_inferred_column)
        result["pairs_out"] = int(len(out_df))

        out_path = os.path.abspath(os.path.join(output_dir, f"{safe_name(employee_name)}.pairs.csv"))
        ensure_parent_dir(out_path)
        out_df.to_csv(out_path, index=False)
        result["output_csv"] = out_path
        result["status"] = "ok"
        result["error"] = None
        return result
    except Exception as exc:
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def process_many_employee_events(
    employees: Iterable[dict[str, Any]],
    *,
    output_dir: str | None = None,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    keep_inferred_column: bool = False,
    input_mode: str | None = None,
    input_dir: str | None = None,
    index_path: str | None = None,
    events_name: str | None = None,
    discovered_event_files_total: int = 0,
) -> dict[str, Any]:
    output_dir = output_dir or default_output_dir()
    normalized_employees = list(employees)

    stats: dict[str, Any] = {
        "files_total": len(normalized_employees),
        "files_processed": 0,
        "files_error": 0,
        "employees_total": len(normalized_employees),
        "employees_processed": 0,
        "employees_with_pairs": 0,
        "event_files_total": 0,
        "event_files_loaded": 0,
        "event_files_missing": 0,
        "event_files_error": 0,
        "events_rows_in": 0,
        "events_valid": 0,
        "events_invalid_kind": 0,
        "events_invalid_ts": 0,
        "events_deduped": 0,
        "partial_rows": 0,
        "pairs_out": 0,
        "pairs_deduped": 0,
        "inferred_pairs": 0,
        "rows_unmatched_after_close": 0,
        "discovered_event_files_total": int(discovered_event_files_total),
        "max_gap_hours": float(max_gap_hours),
    }
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []

    for employee_index, employee in enumerate(normalized_employees, start=1):
        employee_name = str(employee.get("employee") or "unknown")
        logger.info(
            "Employee %s/%s: %s (files=%s)",
            employee_index,
            len(normalized_employees),
            employee_name,
            len(list(employee.get("files") or [])),
        )

        result = process_one_employee_events(
            employee,
            output_dir=output_dir,
            max_gap_hours=max_gap_hours,
            keep_inferred_column=keep_inferred_column,
        )
        items.append(result)

        stats["employees_processed"] += 1
        if result["status"] == "ok":
            stats["files_processed"] += 1
        else:
            stats["files_error"] += 1
            issues.append(
                {
                    "code": str(result.get("error_code") or "processing_error"),
                    "employee": str(result.get("source_employee") or ""),
                    "message": str(result.get("error") or "processing_error"),
                }
            )

        if int(result["pairs_out"]) > 0:
            stats["employees_with_pairs"] += 1

        stats["event_files_total"] += int(result["files_total"])
        stats["event_files_loaded"] += int(result["files_loaded"])
        stats["event_files_missing"] += int(result["files_missing"])
        stats["event_files_error"] += int(result["files_error"])
        for key in (
            "events_rows_in",
            "events_valid",
            "events_invalid_kind",
            "events_invalid_ts",
            "events_deduped",
            "partial_rows",
            "pairs_out",
            "pairs_deduped",
            "inferred_pairs",
            "rows_unmatched_after_close",
        ):
            stats[key] += int(result[key])

    return build_stage_report(
        stage="pair_employee_events",
        inputs={
            "input_mode": input_mode,
            "input_dir": os.path.abspath(input_dir) if input_dir else None,
            "index_path": os.path.abspath(index_path) if index_path else None,
            "output_dir": os.path.abspath(output_dir),
            "events_name": events_name,
            "max_gap_hours": float(max_gap_hours),
        },
        outputs={"output_dir": os.path.abspath(output_dir)},
        stats=stats,
        row_totals={"items": len(items), "issues": len(issues)},
        items=items,
        issues=issues,
    )


__all__ = [
    "normalize_employee",
    "process_many_employee_events",
    "process_one_employee_events",
]
