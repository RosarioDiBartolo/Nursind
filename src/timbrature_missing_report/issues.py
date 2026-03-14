from __future__ import annotations

from typing import Any

from src.drive_service.text_extraction_csv import build_google_drive_file_link

from .accumulator import EmployeeAccumulator
from .inputs import YearMonth, format_year_month

SUMMARY_COLUMNS = [
    "employee",
    "employee_id",
    "source_files_total",
    "coverage_month_range",
    "coverage_months_count",
    "missing_coverage_months_count",
    "scan_without_included_files",
    "missing_text_layer_files",
    "pages_missing_year_month",
    "finding_count",
    "coverage_gap_count",
]

FINDING_COLUMNS = [
    "employee",
    "employee_id",
    "finding_type",
    "stage",
    "file_id",
    "file_link",
    "file_name",
    "source_doc_json",
    "page_no",
    "year",
    "month",
    "year_month",
    "detail",
    "events_dropped",
]

COVERAGE_COLUMNS = [
    "employee",
    "employee_id",
    "gap_type",
    "stage",
    "year",
    "month",
    "year_month",
    "detail",
]


def append_finding(
    record: EmployeeAccumulator,
    finding_rows: list[dict[str, Any]],
    *,
    finding_type: str,
    stage: str,
    file_id: str | None = None,
    file_name: str | None = None,
    source_doc_json: str | None = None,
    page_no: int | None = None,
    year_month: YearMonth | None = None,
    detail: str | None = None,
    events_dropped: int | None = None,
) -> None:
    row = {
        "employee": record.employee,
        "employee_id": record.employee_id,
        "finding_type": finding_type,
        "stage": stage,
        "file_id": file_id,
        "file_link": build_google_drive_file_link(file_id=file_id),
        "file_name": file_name,
        "source_doc_json": source_doc_json,
        "page_no": page_no,
        "year": year_month[0] if year_month is not None else None,
        "month": year_month[1] if year_month is not None else None,
        "year_month": format_year_month(year_month) if year_month is not None else None,
        "detail": detail,
        "events_dropped": events_dropped,
    }
    finding_rows.append(row)


def append_coverage_gap(
    record: EmployeeAccumulator,
    coverage_rows: list[dict[str, Any]],
    *,
    gap_type: str,
    stage: str,
    year_month: YearMonth,
    detail: str,
) -> None:
    coverage_rows.append(
        {
            "employee": record.employee,
            "employee_id": record.employee_id,
            "gap_type": gap_type,
            "stage": stage,
            "year": year_month[0],
            "month": year_month[1],
            "year_month": format_year_month(year_month),
            "detail": detail,
        }
    )


__all__ = [
    "COVERAGE_COLUMNS",
    "FINDING_COLUMNS",
    "SUMMARY_COLUMNS",
    "append_coverage_gap",
    "append_finding",
]
