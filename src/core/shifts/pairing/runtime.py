from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from core.csv_validation import MissingColumnsError
from core.drive.fs_utils import ensure_dir, ensure_parent_dir

from .options import (
    DEFAULT_EVENTS_NAME,
    DEFAULT_MAX_GAP_HOURS,
    PairEmployeeEventsOptions,
    default_input_dir,
    default_output_dir,
    default_report_json_path,
)
from .service import normalize_employee, process_many_employee_events

logger = logging.getLogger(__name__)


def _source_name_from_events_csv(event_path: Path) -> str:
    name = event_path.name
    if name in {"events.cleaned.csv", "events.csv"}:
        return "events"
    for marker in (
        ".events.cleaned.csv",
        ".events.csv",
    ):
        if name.endswith(marker):
            return name[: -len(marker)] or "unknown"
    return event_path.stem or "unknown"


def _register_grouped_file(
    *,
    grouped: dict[str, dict[str, Any]],
    employee_name: str,
    event_path: Path,
) -> None:
    key = normalize_employee(employee_name)
    if key not in grouped:
        grouped[key] = {
            "employee": employee_name,
            "employee_id": None,
            "files": [],
            "key": f"name:{key}",
        }
    grouped[key]["files"].append(
        {
            "events_csv": str(event_path.resolve()),
            "file_id": None,
            "file_name": _source_name_from_events_csv(event_path),
        }
    )


def _discover_employees_from_aggregated_file(event_path: Path) -> list[str]:
    try:
        frame = pd.read_csv(event_path, usecols=["source_employee"])
    except ValueError as exc:
        if "Usecols do not match columns" in str(exc):
            raise MissingColumnsError(
                "pair_employee_events: "
                f"{event_path} is missing required column(s): source_employee. "
                "Required for root-level aggregated event files."
            ) from exc
        raise
    except Exception:
        return []
    if "source_employee" not in frame.columns:
        return []

    employees: list[str] = []
    seen: set[str] = set()
    for raw in frame["source_employee"].fillna("").astype(str).tolist():
        employee_name = " ".join(raw.strip().split()) or "unknown"
        key = normalize_employee(employee_name)
        if key in seen:
            continue
        seen.add(key)
        employees.append(employee_name)
    return employees


def _discover_employees_from_events_dir(
    *,
    input_dir: str,
    events_name: str,
) -> tuple[list[dict[str, Any]], int]:
    base = Path(input_dir)
    event_files = sorted(base.rglob(events_name))
    grouped: dict[str, dict[str, Any]] = {}

    for event_path in event_files:
        rel = event_path.relative_to(base)
        if len(rel.parts) == 1 and event_path.name in {"events.cleaned.csv", "events.csv"}:
            aggregated_employees = _discover_employees_from_aggregated_file(event_path)
            if aggregated_employees:
                for employee_name in aggregated_employees:
                    _register_grouped_file(
                        grouped=grouped,
                        employee_name=employee_name,
                        event_path=event_path,
                    )
                continue

        if len(rel.parts) >= 2:
            employee_name = rel.parts[0]
        else:
            employee_name = "unknown"
        _register_grouped_file(
            grouped=grouped,
            employee_name=employee_name,
            event_path=event_path,
        )

    return list(grouped.values()), len(event_files)


def build_pair_employee_events_from_dir(
    *,
    input_dir: str | None = None,
    output_dir: str | None = None,
    events_name: str = DEFAULT_EVENTS_NAME,
    report_json: str | None = None,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    employee_filter: str | None = None,
    keep_inferred_column: bool = False,
) -> dict[str, Any]:
    input_dir = input_dir or default_input_dir()
    output_dir = output_dir or default_output_dir()
    report_json = report_json or default_report_json_path()
    if not input_dir:
        raise ValueError("--input-dir is required")
    ensure_dir(output_dir)
    employees, discovered_event_files_total = _discover_employees_from_events_dir(
        input_dir=str(input_dir),
        events_name=events_name,
    )

    if employee_filter:
        token = normalize_employee(employee_filter)
        employees = [
            employee
            for employee in employees
            if normalize_employee(employee.get("employee")) == token
        ]

    report = process_many_employee_events(
        employees,
        output_dir=output_dir,
        max_gap_hours=max_gap_hours,
        keep_inferred_column=keep_inferred_column,
        input_mode="folder",
        input_dir=str(input_dir),
        events_name=events_name,
        discovered_event_files_total=discovered_event_files_total,
    )
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def pair_employee_events(
    *,
    input_dir: str | None = None,
    output_dir: str | None = None,
    events_name: str = DEFAULT_EVENTS_NAME,
    report_json: str | None = None,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    employee_filter: str | None = None,
    keep_inferred_column: bool = False,
) -> dict[str, Any]:
    return build_pair_employee_events_from_dir(
        input_dir=input_dir,
        output_dir=output_dir,
        events_name=events_name,
        report_json=report_json,
        max_gap_hours=max_gap_hours,
        employee_filter=employee_filter,
        keep_inferred_column=keep_inferred_column,
    )


def run_from_options(options: PairEmployeeEventsOptions) -> dict[str, Any]:
    return build_pair_employee_events_from_dir(
        input_dir=options.input_dir,
        output_dir=options.output_dir,
        events_name=options.events_name,
        report_json=options.report_json,
        max_gap_hours=options.max_gap_hours,
        employee_filter=options.employee_filter,
        keep_inferred_column=options.keep_inferred_column,
    )

