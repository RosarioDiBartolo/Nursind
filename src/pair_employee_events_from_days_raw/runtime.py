from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_dir, ensure_parent_dir
from src.drive_service.index import MapIndex
from src.drive_service.index_runtime import doc_attr
from src.shift_services import EmployeeGrouper, PairsPathResolver

from .options import (
    DEFAULT_EVENTS_NAME,
    DEFAULT_INDEX,
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_GAP_HOURS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_JSON,
    PairEmployeeEventsOptions,
)
from .service import normalize_employee, process_many_employee_events

logger = logging.getLogger(__name__)


def _source_name_from_events_csv(event_path: Path) -> str:
    name = event_path.name
    for marker in (
        ".events_from_text_raw.cleaned.csv",
        ".events_from_text_raw.csv",
    ):
        if name.endswith(marker):
            return name[: -len(marker)] or "unknown"
    return event_path.stem or "unknown"


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
        if len(rel.parts) >= 2:
            employee_name = rel.parts[0]
        else:
            employee_name = "unknown"
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

    return list(grouped.values()), len(event_files)


def _resolve_cleaned_events_path(
    *,
    resolver: PairsPathResolver,
    index_dir: str,
    inc: Any,
    emp_name: str,
    events_name: str,
) -> str:
    outputs = doc_attr(inc, "outputs")
    events_rel = doc_attr(outputs, "events_csv") if outputs else None
    if events_rel:
        event_abs = os.path.abspath(os.path.join(index_dir, events_rel))
        event_name = os.path.basename(event_abs)
        return event_abs

    expected_pairs = resolver.expected_pairs_path(
        emp_name,
        doc_attr(inc, "file_name"),
        doc_attr(inc, "file_id"),
    )
    source_name = os.path.basename(expected_pairs)
    if source_name.endswith(".pairs.csv"):
        source_name = source_name[: -len(".pairs.csv")] + ".events_from_text_raw.cleaned.csv"
    else:
        source_name = "events_from_text_raw.cleaned.csv"
    if "*" in events_name:
        suffix = events_name.replace("*", "").lstrip(".")
        if source_name.endswith(".events_from_text_raw.cleaned.csv"):
            prefix = source_name[: -len(".events_from_text_raw.cleaned.csv")]
            event_name = f"{prefix}.{suffix}" if prefix else suffix
        elif source_name.endswith(".events_from_text_raw.csv"):
            prefix = source_name[: -len(".events_from_text_raw.csv")]
            event_name = f"{prefix}.{suffix}" if prefix else suffix
        else:
            event_name = suffix
    else:
        event_name = events_name
    return os.path.abspath(os.path.join(os.path.dirname(expected_pairs), event_name))


def _discover_employees_from_index(
    *,
    index_path: str,
    events_name: str,
) -> tuple[list[dict[str, Any]], str]:
    index_abs = os.path.abspath(index_path)
    index_dir = os.path.dirname(index_abs)
    report = MapIndex.load_index(index_abs, strict=True)
    resolver = PairsPathResolver(index_abs)
    grouper = EmployeeGrouper(normalize_employee)
    grouped = grouper.group(list(report.files.values()))

    employees: list[dict[str, Any]] = []
    for grouped_employee in grouped:
        employee_name = str(grouped_employee.get("employee") or "unknown")
        employee_id = grouped_employee.get("employee_id")
        files: list[dict[str, Any]] = []
        for inc in grouped_employee.get("files", []):
            files.append(
                {
                    "events_csv": _resolve_cleaned_events_path(
                        resolver=resolver,
                        index_dir=index_dir,
                        inc=inc,
                        emp_name=employee_name,
                        events_name=events_name,
                    ),
                    "file_id": doc_attr(inc, "file_id"),
                    "file_name": doc_attr(inc, "file_name"),
                }
            )
        employees.append(
            {
                "employee": employee_name,
                "employee_id": employee_id,
                "files": files,
            }
        )

    return employees, index_abs


def build_pair_employee_events_from_days_raw_from_dir(
    *,
    input_dir: str | None = DEFAULT_INPUT_DIR,
    index_path: str | None = DEFAULT_INDEX,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    events_name: str = DEFAULT_EVENTS_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    employee_filter: str | None = None,
    keep_inferred_column: bool = False,
) -> dict[str, Any]:
    ensure_dir(output_dir)
    use_folder_mode = bool(input_dir)
    if use_folder_mode and index_path:
        logger.warning(
            "--index is deprecated and ignored when --input-dir is provided; using folder mode."
        )

    employees: list[dict[str, Any]]
    discovered_event_files_total = 0
    index_abs: str | None = None
    if use_folder_mode:
        employees, discovered_event_files_total = _discover_employees_from_events_dir(
            input_dir=str(input_dir),
            events_name=events_name,
        )
    else:
        if not index_path:
            raise ValueError("Either --input-dir or --index must be provided")
        logger.warning(
            "--index mode is deprecated and will be removed; use --input-dir folder mode."
        )
        employees, index_abs = _discover_employees_from_index(
            index_path=index_path,
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
        input_mode="folder" if use_folder_mode else "index",
        input_dir=str(input_dir) if use_folder_mode else None,
        index_path=index_abs,
        events_name=events_name,
        discovered_event_files_total=discovered_event_files_total,
    )
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def pair_employee_events(
    *,
    input_dir: str | None = DEFAULT_INPUT_DIR,
    index_path: str | None = DEFAULT_INDEX,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    events_name: str = DEFAULT_EVENTS_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    employee_filter: str | None = None,
    keep_inferred_column: bool = False,
) -> dict[str, Any]:
    return build_pair_employee_events_from_days_raw_from_dir(
        input_dir=input_dir,
        index_path=index_path,
        output_dir=output_dir,
        events_name=events_name,
        report_json=report_json,
        max_gap_hours=max_gap_hours,
        employee_filter=employee_filter,
        keep_inferred_column=keep_inferred_column,
    )


def run_from_options(options: PairEmployeeEventsOptions) -> dict[str, Any]:
    return build_pair_employee_events_from_days_raw_from_dir(
        input_dir=options.input_dir,
        index_path=options.index_path,
        output_dir=options.output_dir,
        events_name=options.events_name,
        report_json=options.report_json,
        max_gap_hours=options.max_gap_hours,
        employee_filter=options.employee_filter,
        keep_inferred_column=options.keep_inferred_column,
    )
