from __future__ import annotations

from dataclasses import dataclass, field

from src.drive_service.names import normalize_name

from .inputs import YearMonth, format_year_month


@dataclass(slots=True)
class EmployeeAccumulator:
    employee: str
    employee_id: str | None = None
    source_files_total: int = 0
    scan_without_included_files: bool = False
    missing_text_layer_files: int = 0
    pages_missing_year_month: int = 0
    pairs_rows: int = 0
    pair_status: str | None = None
    pair_error_code: str | None = None
    pair_error: str | None = None
    pair_output_csv: str | None = None
    found_event_months: set[YearMonth] = field(default_factory=set)
    expected_months: set[YearMonth] = field(default_factory=set)
    paired_months: set[YearMonth] = field(default_factory=set)
    expected_sources: dict[YearMonth, list[dict[str, str | None]]] = field(default_factory=dict)
    upstream_causes_by_month: dict[YearMonth, set[str]] = field(default_factory=dict)
    _source_tokens: set[str] = field(default_factory=set)


def register_source_file(record: EmployeeAccumulator, token: str) -> None:
    if token in record._source_tokens:
        return
    record._source_tokens.add(token)
    record.source_files_total += 1


def register_expected_month(
    record: EmployeeAccumulator,
    *,
    year_month: YearMonth,
    source: dict[str, str | None],
) -> None:
    record.expected_months.add(year_month)
    record.expected_sources.setdefault(year_month, []).append(source)


def mark_upstream_cause(
    record: EmployeeAccumulator,
    *,
    year_months: set[YearMonth],
    cause: str,
) -> None:
    for year_month in year_months:
        record.upstream_causes_by_month.setdefault(year_month, set()).add(cause)


def build_expected_month_detail(
    *,
    record: EmployeeAccumulator,
    year_month: YearMonth,
) -> str:
    labels: list[str] = []
    source_names = []
    for source in record.expected_sources.get(year_month, []):
        name = source.get("file_name") or source.get("source_doc_json") or source.get("file_id")
        if name and name not in source_names:
            source_names.append(str(name))
    if source_names:
        labels.append(f"sources={', '.join(sorted(source_names))}")

    if not labels:
        return f"Expected month {format_year_month(year_month)} not present in pairs"
    return (
        f"Expected month {format_year_month(year_month)} not present in pairs; "
        + "; ".join(labels)
    )


def _new_employee(employee_name: str | None, employee_id: str | None) -> EmployeeAccumulator:
    return EmployeeAccumulator(
        employee=employee_name or "unknown",
        employee_id=employee_id,
    )


def ensure_employee(
    employees: list[EmployeeAccumulator],
    by_id: dict[str, EmployeeAccumulator],
    by_name: dict[str, EmployeeAccumulator],
    *,
    employee_name: str | None,
    employee_id: str | None,
) -> EmployeeAccumulator:
    normalized_name = normalize_name(employee_name)
    record: EmployeeAccumulator | None = None
    if employee_id:
        record = by_id.get(employee_id)
    if record is None:
        record = by_name.get(normalized_name)
    if record is None:
        record = _new_employee(employee_name, employee_id)
        employees.append(record)

    if employee_id and not record.employee_id:
        record.employee_id = employee_id
    if employee_name and normalize_name(record.employee) == "unknown":
        record.employee = employee_name

    if record.employee_id:
        by_id[record.employee_id] = record
    by_name[normalize_name(record.employee)] = record
    if employee_name:
        by_name[normalized_name] = record
    return record


__all__ = [
    "EmployeeAccumulator",
    "build_expected_month_detail",
    "ensure_employee",
    "mark_upstream_cause",
    "register_expected_month",
    "register_source_file",
]
