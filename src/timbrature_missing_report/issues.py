from __future__ import annotations

from typing import Any

from src.drive_service.text_extraction_csv import build_google_drive_file_link

from .accumulator import EmployeeAccumulator
from .inputs import YearMonth, format_year_month

EMPLOYEE_SUMMARY_COLUMNS = [
    "employee",
    "employee_id",
    "source_files_total",
    "scan_without_included_files",
    "missing_text_layer_files",
    "pages_missing_year_month",
    "required_month_range",
    "found_event_months",
    "found_event_months_count",
    "missing_required_months",
    "missing_required_months_count",
    "expected_months",
    "paired_months",
    "missing_months_after_pairing",
    "complete_pairing_absence",
    "pair_rows",
    "pair_status",
    "pair_error_code",
    "pair_output_csv",
    "issues_total",
]

ISSUE_COLUMNS = [
    "employee",
    "employee_id",
    "issue_type",
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
    "pair_status",
    "pair_error_code",
    "pair_output_csv",
]


def append_issue(
    record: EmployeeAccumulator,
    issues: list[dict[str, Any]],
    *,
    issue_type: str,
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
        "issue_type": issue_type,
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
        "pair_status": record.pair_status,
        "pair_error_code": record.pair_error_code,
        "pair_output_csv": record.pair_output_csv,
    }
    record.issues.append(row)
    issues.append(row)


__all__ = [
    "EMPLOYEE_SUMMARY_COLUMNS",
    "ISSUE_COLUMNS",
    "append_issue",
]
