from __future__ import annotations

from dataclasses import dataclass, field

from core.drive.names import normalize_name

from .inputs import YearMonth


@dataclass(slots=True)
class EmployeeAccumulator:
    employee: str
    employee_id: str | None = None
    source_files_total: int = 0
    scan_without_included_files: bool = False
    missing_text_layer_files: int = 0
    pages_missing_year_month: int = 0
    coverage_months: set[YearMonth] = field(default_factory=set)
    _source_tokens: set[str] = field(default_factory=set)


def register_source_file(record: EmployeeAccumulator, token: str) -> None:
    if token in record._source_tokens:
        return
    record._source_tokens.add(token)
    record.source_files_total += 1


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
    "ensure_employee",
    "register_source_file",
]

