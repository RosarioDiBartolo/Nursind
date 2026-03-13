from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.drive_service.index import MapIndex
from src.drive_service.io_json import load_json
from src.drive_service.names import normalize_name
from src.drive_service.text_extraction_csv import (
    build_google_drive_file_link,
    find_text_extraction_csvs,
    load_text_extraction_doc,
    read_text_extraction_rows,
)
from src.raw_text_parsing import infer_year_month_from_filename, resolve_year_month

logger = logging.getLogger(__name__)

YearMonth = tuple[int, int]

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

REQUIRED_YEAR_START = 2014
REQUIRED_YEAR_END = 2026
REQUIRED_MONTH_RANGE_LABEL = f"{REQUIRED_YEAR_START:04d}-01..{REQUIRED_YEAR_END:04d}-12"
REQUIRED_MONTHS: tuple[YearMonth, ...] = tuple(
    (year, month)
    for year in range(REQUIRED_YEAR_START, REQUIRED_YEAR_END + 1)
    for month in range(1, 13)
)
REQUIRED_MONTHS_SET = set(REQUIRED_MONTHS)


@dataclass(slots=True)
class ResolvedAuditInputs:
    pipeline_dir: Path
    scan_dir: Path
    documents_dir: Path
    events_dir: Path
    shifts_dir: Path
    scan_report_path: Path
    excluded_index_path: Path
    found_events_csv_path: Path
    pages_csv_path: Path
    pair_report_path: Path


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
    issues: list[dict[str, Any]] = field(default_factory=list)
    _source_tokens: set[str] = field(default_factory=set)


def _clean_str(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.lower() in {"nan", "none", "<na>", "null"}:
        return None
    return text


def _parse_int(value: object) -> int | None:
    text = _clean_str(value)
    if text is None:
        return None
    try:
        return int(text)
    except Exception:
        try:
            return int(float(text))
        except Exception:
            return None


def _parse_year_month(year_value: object, month_value: object) -> YearMonth | None:
    year = _parse_int(year_value)
    month = _parse_int(month_value)
    if year is None or month is None:
        return None
    if not (1900 <= year <= 2100 and 1 <= month <= 12):
        return None
    return year, month


def _format_year_month(value: YearMonth) -> str:
    return f"{value[0]:04d}-{value[1]:02d}"


def _sorted_month_labels(values: set[YearMonth]) -> list[str]:
    return [_format_year_month(item) for item in sorted(values)]


def _pick_existing_dir(base: Path, candidates: list[str]) -> Path:
    for candidate in candidates:
        path = base / candidate
        if path.exists():
            return path
    return base / candidates[0]


def _pick_existing_file(base: Path, candidates: list[str]) -> Path:
    for candidate in candidates:
        path = base / candidate
        if path.exists():
            return path
    return base / candidates[0]


def resolve_audit_inputs(pipeline_dir: str | Path) -> ResolvedAuditInputs:
    base = Path(pipeline_dir).resolve()
    scan_dir = _pick_existing_dir(base, ["scan"])
    documents_dir = _pick_existing_dir(base, ["documents", "text_extracted"])
    events_dir = _pick_existing_dir(base, ["events"])
    shifts_dir = _pick_existing_dir(base, ["shifts", "employee_shifts_from_raw"])
    return ResolvedAuditInputs(
        pipeline_dir=base,
        scan_dir=scan_dir,
        documents_dir=documents_dir,
        events_dir=events_dir,
        shifts_dir=shifts_dir,
        scan_report_path=scan_dir / "scan_directory.report.json",
        excluded_index_path=documents_dir / "excluded_documents.index.json",
        found_events_csv_path=_pick_existing_file(events_dir, ["events.cleaned.csv", "events.csv"]),
        pages_csv_path=events_dir / "pages.csv",
        pair_report_path=shifts_dir / "pair_employee_events.report.json",
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _iter_csv_rows(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", newline="") as handle:
        yield from csv.DictReader(handle)


def _source_path_hint(
    *,
    file_name: str | None,
    source_text_ref: str | None,
    drive_path: str | None,
    fallback: str,
) -> Path:
    return Path(file_name or source_text_ref or drive_path or fallback)


def _load_document_full_text(documents_dir: Path, row: dict[str, Any]) -> str:
    payload = load_text_extraction_doc(documents_dir, _clean_str(row.get("doc_json")))
    if not isinstance(payload, dict):
        return ""
    document = payload.get("document")
    if not isinstance(document, dict):
        return ""
    full_text = document.get("full_text")
    return str(full_text or "")


def _derive_manifest_expected_months(
    *,
    documents_dir: Path,
    row: dict[str, Any],
    page_rows: list[dict[str, str]],
) -> tuple[set[YearMonth], str | None]:
    page_months = {
        ym
        for ym in (
            _parse_year_month(item.get("page_year"), item.get("page_month")) for item in page_rows
        )
        if ym is not None
    }
    if page_months:
        return page_months, "pages"

    path_hint = _source_path_hint(
        file_name=_clean_str(row.get("file_name")),
        source_text_ref=_clean_str(row.get("source_text_ref")),
        drive_path=_clean_str(row.get("drive_path")),
        fallback=_clean_str(row.get("doc_json")) or "unknown.pdf",
    )
    full_text = _load_document_full_text(documents_dir, row)
    year, month = resolve_year_month(full_text, path_hint)
    if year is not None and month is not None:
        return {(int(year), int(month))}, "document"
    return set(), None


def _infer_expected_month_from_file(
    *,
    file_name: str | None,
    drive_path: str | None,
    source_text_ref: str | None = None,
) -> YearMonth | None:
    for candidate in (file_name, source_text_ref, drive_path):
        text = _clean_str(candidate)
        if text is None:
            continue
        year, month = infer_year_month_from_filename(Path(text))
        if year is not None and month is not None:
            return int(year), int(month)
    return None


def _read_pair_months(path: Path) -> tuple[set[YearMonth], int]:
    rows = _read_csv_rows(path)
    months = {
        ym
        for ym in (_parse_year_month(row.get("year"), row.get("month")) for row in rows)
        if ym is not None
    }
    return months, len(rows)


def _parse_event_year_month(row: dict[str, str]) -> YearMonth | None:
    event_ts = _clean_str(row.get("event_ts"))
    if event_ts is None:
        return None
    event_day = event_ts.split(" ", 1)[0]
    parts = event_day.split("-")
    if len(parts) < 2:
        return None
    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError:
        return None
    return _parse_year_month(year, month)


def _register_source_file(record: EmployeeAccumulator, token: str) -> None:
    if token in record._source_tokens:
        return
    record._source_tokens.add(token)
    record.source_files_total += 1


def _register_expected_month(
    record: EmployeeAccumulator,
    *,
    year_month: YearMonth,
    source: dict[str, str | None],
) -> None:
    record.expected_months.add(year_month)
    record.expected_sources.setdefault(year_month, []).append(source)


def _mark_upstream_cause(
    record: EmployeeAccumulator,
    *,
    year_months: set[YearMonth],
    cause: str,
) -> None:
    for year_month in year_months:
        record.upstream_causes_by_month.setdefault(year_month, set()).add(cause)


def _append_issue(
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
        "year_month": _format_year_month(year_month) if year_month is not None else None,
        "detail": detail,
        "events_dropped": events_dropped,
        "pair_status": record.pair_status,
        "pair_error_code": record.pair_error_code,
        "pair_output_csv": record.pair_output_csv,
    }
    record.issues.append(row)
    issues.append(row)


def _build_expected_month_detail(
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

    upstream_causes = sorted(record.upstream_causes_by_month.get(year_month, set()))
    if upstream_causes:
        labels.append(f"upstream_causes={', '.join(upstream_causes)}")

    if not labels:
        return f"Expected month {_format_year_month(year_month)} not present in pairs"
    return (
        f"Expected month {_format_year_month(year_month)} not present in pairs; "
        + "; ".join(labels)
    )


def _new_employee(employee_name: str | None, employee_id: str | None) -> EmployeeAccumulator:
    return EmployeeAccumulator(
        employee=employee_name or "unknown",
        employee_id=employee_id,
    )


def _ensure_employee(
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


def audit_missing_timbrature_pipeline(
    pipeline_dir: str | Path,
) -> dict[str, Any]:
    resolved = resolve_audit_inputs(pipeline_dir)

    artifact_errors: list[dict[str, str]] = []
    issues: list[dict[str, Any]] = []
    employees: list[EmployeeAccumulator] = []
    employees_by_id: dict[str, EmployeeAccumulator] = {}
    employees_by_name: dict[str, EmployeeAccumulator] = {}

    manifest_rows: list[dict[str, str]] = []
    manifest_csvs = find_text_extraction_csvs(resolved.documents_dir)
    manifest_by_file_id: dict[str, dict[str, str]] = {}
    manifest_by_doc_json: dict[str, dict[str, str]] = {}

    pages_rows: list[dict[str, str]] = []
    pages_by_file_id: dict[str, list[dict[str, str]]] = {}
    pages_by_doc_json: dict[str, list[dict[str, str]]] = {}
    expected_months_by_file_id: dict[str, set[YearMonth]] = {}
    expected_months_by_doc_json: dict[str, set[YearMonth]] = {}
    scan_report_payload: dict[str, Any] | None = None
    scan_report_has_zero_included_data = False

    if resolved.scan_report_path.exists():
        try:
            loaded = load_json(str(resolved.scan_report_path))
            if isinstance(loaded, dict):
                scan_report_payload = loaded
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "scan_report", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception("Failed reading scan report from %s", resolved.scan_report_path)

    if manifest_csvs:
        try:
            manifest_rows = read_text_extraction_rows(manifest_csvs, hydrate_text=False)
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "manifest_csvs", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception("Failed reading manifest CSVs from %s", resolved.documents_dir)

    if resolved.pages_csv_path.exists():
        try:
            pages_rows = _read_csv_rows(resolved.pages_csv_path)
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "pages_csv", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception("Failed reading pages CSV from %s", resolved.pages_csv_path)

    for row in pages_rows:
        file_id = _clean_str(row.get("source_file_id"))
        doc_json = _clean_str(row.get("source_doc_json"))
        if file_id:
            pages_by_file_id.setdefault(file_id, []).append(row)
        if doc_json:
            pages_by_doc_json.setdefault(doc_json, []).append(row)

    if scan_report_payload is not None:
        raw_employees_found = scan_report_payload.get("employees_found")
        if isinstance(raw_employees_found, list):
            for row in raw_employees_found:
                if not isinstance(row, dict):
                    continue
                _ensure_employee(
                    employees,
                    employees_by_id,
                    employees_by_name,
                    employee_name=_clean_str(row.get("employee")) or "unknown",
                    employee_id=_clean_str(row.get("employee_id")),
                )

        raw_zero_included = scan_report_payload.get("employees_without_included_files")
        if isinstance(raw_zero_included, list):
            scan_report_has_zero_included_data = True
            for row in raw_zero_included:
                if not isinstance(row, dict):
                    continue
                record = _ensure_employee(
                    employees,
                    employees_by_id,
                    employees_by_name,
                    employee_name=_clean_str(row.get("employee")) or "unknown",
                    employee_id=_clean_str(row.get("employee_id")),
                )
                record.scan_without_included_files = True
                detail = (
                    "Direct employee folder exists in scan root but produced 0 included files"
                )
                filtered_files = _parse_int(row.get("filtered_files"))
                filtered_folders = _parse_int(row.get("filtered_folders"))
                detail_parts = [detail]
                if filtered_files is not None:
                    detail_parts.append(f"filtered_files={filtered_files}")
                if filtered_folders is not None:
                    detail_parts.append(f"filtered_folders={filtered_folders}")
                _append_issue(
                    record,
                    issues,
                    issue_type="scan_without_included_files",
                    stage="scan",
                    detail="; ".join(detail_parts),
                )

    for row in manifest_rows:
        employee_name = _clean_str(row.get("employee")) or "unknown"
        employee_id = _clean_str(row.get("employee_id"))
        record = _ensure_employee(
            employees,
            employees_by_id,
            employees_by_name,
            employee_name=employee_name,
            employee_id=employee_id,
        )

        file_id = _clean_str(row.get("file_id"))
        file_name = _clean_str(row.get("file_name"))
        doc_json = _clean_str(row.get("doc_json"))
        drive_path = _clean_str(row.get("drive_path"))
        source_text_ref = _clean_str(row.get("source_text_ref"))
        source_token = file_id or doc_json or file_name or drive_path or source_text_ref or employee_name
        _register_source_file(record, source_token)

        if file_id:
            manifest_by_file_id[file_id] = row
        if doc_json:
            manifest_by_doc_json[doc_json] = row

        file_page_rows = []
        if file_id:
            file_page_rows.extend(pages_by_file_id.get(file_id, []))
        if doc_json:
            file_page_rows.extend(pages_by_doc_json.get(doc_json, []))

        expected_months, source_kind = _derive_manifest_expected_months(
            documents_dir=resolved.documents_dir,
            row=row,
            page_rows=file_page_rows,
        )
        for year_month in expected_months:
            _register_expected_month(
                record,
                year_month=year_month,
                source={
                    "file_id": file_id,
                    "file_name": file_name,
                    "source_doc_json": doc_json,
                    "source_kind": source_kind,
                },
            )
        if file_id and expected_months:
            expected_months_by_file_id[file_id] = set(expected_months)
        if doc_json and expected_months:
            expected_months_by_doc_json[doc_json] = set(expected_months)

    if resolved.excluded_index_path.exists():
        try:
            excluded_index = MapIndex.load_index(str(resolved.excluded_index_path), strict=True)
            for entry in excluded_index.files.values():
                if _clean_str(entry.reason) != "missing_text_layer":
                    continue
                record = _ensure_employee(
                    employees,
                    employees_by_id,
                    employees_by_name,
                    employee_name=_clean_str(entry.employee) or "unknown",
                    employee_id=_clean_str(entry.employee_id),
                )
                file_id = _clean_str(entry.file_id)
                file_name = _clean_str(entry.file_name)
                drive_path = _clean_str(entry.drive_path)
                source_token = file_id or file_name or drive_path or record.employee
                _register_source_file(record, source_token)
                record.missing_text_layer_files += 1

                inferred_month = _infer_expected_month_from_file(
                    file_name=file_name,
                    drive_path=drive_path,
                )
                inferred_months = {inferred_month} if inferred_month is not None else set()
                if inferred_month is not None:
                    _register_expected_month(
                        record,
                        year_month=inferred_month,
                        source={
                            "file_id": file_id,
                            "file_name": file_name,
                            "source_doc_json": None,
                            "source_kind": "missing_text_layer",
                        },
                    )
                    _mark_upstream_cause(
                        record,
                        year_months=inferred_months,
                        cause="missing_text_layer",
                    )

                _append_issue(
                    record,
                    issues,
                    issue_type="missing_text_layer",
                    stage="documents",
                    file_id=file_id,
                    file_name=file_name,
                    year_month=inferred_month,
                    detail="Document was excluded because the PDF had no text layer",
                )
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "excluded_index", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception("Failed reading excluded index from %s", resolved.excluded_index_path)

    for row in pages_rows:
        decision_reason = _clean_str(row.get("decision_reason"))
        dropped = _parse_int(row.get("events_dropped_missing_year_month")) or 0
        if decision_reason != "missing_page_year_month" and dropped <= 0:
            continue

        file_id = _clean_str(row.get("source_file_id"))
        doc_json = _clean_str(row.get("source_doc_json"))
        manifest_row = None
        if file_id:
            manifest_row = manifest_by_file_id.get(file_id)
        if manifest_row is None and doc_json:
            manifest_row = manifest_by_doc_json.get(doc_json)

        employee_name = (
            _clean_str(manifest_row.get("employee")) if manifest_row is not None else None
        ) or _clean_str(row.get("source_employee")) or "unknown"
        employee_id = (
            _clean_str(manifest_row.get("employee_id")) if manifest_row is not None else None
        )
        record = _ensure_employee(
            employees,
            employees_by_id,
            employees_by_name,
            employee_name=employee_name,
            employee_id=employee_id,
        )
        record.pages_missing_year_month += 1

        inferred_months = set()
        if file_id:
            inferred_months.update(expected_months_by_file_id.get(file_id, set()))
        if doc_json:
            inferred_months.update(expected_months_by_doc_json.get(doc_json, set()))
        if not inferred_months:
            inferred = _infer_expected_month_from_file(
                file_name=(
                    _clean_str(manifest_row.get("file_name"))
                    if manifest_row is not None
                    else _clean_str(row.get("source_file_name"))
                ),
                drive_path=_clean_str(manifest_row.get("drive_path")) if manifest_row else None,
                source_text_ref=(
                    _clean_str(manifest_row.get("source_text_ref")) if manifest_row else None
                ),
            )
            if inferred is not None:
                inferred_months.add(inferred)

        if inferred_months:
            _mark_upstream_cause(
                record,
                year_months=inferred_months,
                cause="missing_page_year_month",
            )

        issue_month = sorted(inferred_months)[0] if inferred_months else None
        _append_issue(
            record,
            issues,
            issue_type="missing_page_year_month",
            stage="events",
            file_id=file_id,
            file_name=(
                _clean_str(manifest_row.get("file_name"))
                if manifest_row is not None
                else _clean_str(row.get("source_file_name"))
            ),
            source_doc_json=doc_json,
            page_no=_parse_int(row.get("page_no")),
            year_month=issue_month,
            detail="Events were detected on the page but month/year could not be resolved",
            events_dropped=dropped,
        )

    if resolved.found_events_csv_path.exists():
        try:
            for row in _iter_csv_rows(resolved.found_events_csv_path):
                employee_name = _clean_str(row.get("source_employee"))
                if employee_name is None or normalize_name(employee_name) == "unknown":
                    continue
                year_month = _parse_event_year_month(row)
                if year_month is None or year_month not in REQUIRED_MONTHS_SET:
                    continue
                record = _ensure_employee(
                    employees,
                    employees_by_id,
                    employees_by_name,
                    employee_name=employee_name,
                    employee_id=None,
                )
                record.found_event_months.add(year_month)
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "found_events_csv", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception(
                "Failed reading found events CSV from %s", resolved.found_events_csv_path
            )

    pair_report_payload: dict[str, Any] | None = None
    if resolved.pair_report_path.exists():
        try:
            loaded = load_json(str(resolved.pair_report_path))
            if isinstance(loaded, dict):
                pair_report_payload = loaded
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "pair_report", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception("Failed reading pair report from %s", resolved.pair_report_path)

    pair_report_rows = []
    if pair_report_payload is not None:
        raw_rows = pair_report_payload.get("by_employee")
        if isinstance(raw_rows, list):
            pair_report_rows = [row for row in raw_rows if isinstance(row, dict)]

    if pair_report_rows:
        for row in pair_report_rows:
            record = _ensure_employee(
                employees,
                employees_by_id,
                employees_by_name,
                employee_name=_clean_str(row.get("employee")) or "unknown",
                employee_id=_clean_str(row.get("employee_id")),
            )
            record.pair_status = _clean_str(row.get("status"))
            record.pair_error_code = _clean_str(row.get("error_code"))
            record.pair_error = _clean_str(row.get("error"))
            record.pair_output_csv = _clean_str(row.get("output_csv"))
            record.pairs_rows = max(record.pairs_rows, _parse_int(row.get("pairs_out")) or 0)

            if record.pair_status != "ok":
                _append_issue(
                    record,
                    issues,
                    issue_type="pairing_failed",
                    stage="shifts",
                    detail=record.pair_error or "Pairing step reported an error",
                )

            output_csv = _clean_str(row.get("output_csv"))
            if output_csv:
                output_path = Path(output_csv)
                if output_path.exists():
                    months, row_count = _read_pair_months(output_path)
                    record.paired_months.update(months)
                    record.pairs_rows = max(record.pairs_rows, row_count)
                else:
                    _append_issue(
                        record,
                        issues,
                        issue_type="pair_output_missing",
                        stage="shifts",
                        detail=f"Pair report points to a missing CSV: {output_csv}",
                    )
    else:
        for pair_csv in sorted(resolved.shifts_dir.glob("*.pairs.csv")):
            employee_name = pair_csv.name[: -len(".pairs.csv")] or "unknown"
            record = _ensure_employee(
                employees,
                employees_by_id,
                employees_by_name,
                employee_name=employee_name,
                employee_id=None,
            )
            months, row_count = _read_pair_months(pair_csv)
            record.paired_months.update(months)
            record.pairs_rows = max(record.pairs_rows, row_count)
            record.pair_output_csv = str(pair_csv.resolve())
            if record.pair_status is None:
                record.pair_status = "ok"

    employee_summary_rows: list[dict[str, Any]] = []
    employees_nested: list[dict[str, Any]] = []
    employees_with_issues = 0
    employees_missing_required_months = 0
    employees_missing_months_after_pairing = 0
    employees_complete_pairing_absence = 0
    missing_required_months_total = 0
    months_missing_after_pairing_total = 0

    for record in sorted(
        employees,
        key=lambda item: (normalize_name(item.employee), item.employee_id or ""),
    ):
        missing_required_months = REQUIRED_MONTHS_SET - record.found_event_months
        if missing_required_months:
            employees_missing_required_months += 1
            missing_required_months_total += len(missing_required_months)

        missing_months = record.expected_months - record.paired_months
        if missing_months:
            employees_missing_months_after_pairing += 1
            months_missing_after_pairing_total += len(missing_months)
            for year_month in sorted(missing_months):
                _append_issue(
                    record,
                    issues,
                    issue_type="missing_month_after_pairing",
                    stage="shifts",
                    year_month=year_month,
                    detail=_build_expected_month_detail(record=record, year_month=year_month),
                )

        complete_pairing_absence = bool(record.expected_months and not record.paired_months)
        if complete_pairing_absence:
            employees_complete_pairing_absence += 1
            _append_issue(
                record,
                issues,
                issue_type="complete_pairing_absence",
                stage="shifts",
                detail="No paired months found for employee despite having source documents",
            )

        if record.issues:
            employees_with_issues += 1

        found_event_labels = _sorted_month_labels(record.found_event_months)
        missing_required_labels = _sorted_month_labels(missing_required_months)
        expected_labels = _sorted_month_labels(record.expected_months)
        paired_labels = _sorted_month_labels(record.paired_months)
        missing_labels = _sorted_month_labels(missing_months)
        summary_row = {
            "employee": record.employee,
            "employee_id": record.employee_id,
            "source_files_total": record.source_files_total,
            "scan_without_included_files": record.scan_without_included_files,
            "missing_text_layer_files": record.missing_text_layer_files,
            "pages_missing_year_month": record.pages_missing_year_month,
            "required_month_range": REQUIRED_MONTH_RANGE_LABEL,
            "found_event_months": ";".join(found_event_labels),
            "found_event_months_count": len(found_event_labels),
            "missing_required_months": ";".join(missing_required_labels),
            "missing_required_months_count": len(missing_required_labels),
            "expected_months": ";".join(expected_labels),
            "paired_months": ";".join(paired_labels),
            "missing_months_after_pairing": ";".join(missing_labels),
            "complete_pairing_absence": complete_pairing_absence,
            "pair_rows": record.pairs_rows,
            "pair_status": record.pair_status,
            "pair_error_code": record.pair_error_code,
            "pair_output_csv": record.pair_output_csv,
            "issues_total": len(record.issues),
        }
        employee_summary_rows.append(summary_row)
        employees_nested.append(
            {
                **summary_row,
                "found_event_months": found_event_labels,
                "missing_required_months": missing_required_labels,
                "expected_months": expected_labels,
                "paired_months": paired_labels,
                "missing_months_after_pairing": missing_labels,
                "pair_error": record.pair_error,
                "issues": list(record.issues),
            }
        )

    stats = {
        "employees_total": len(employee_summary_rows),
        "employees_with_issues": employees_with_issues,
        "issues_total": len(issues),
        "required_month_range": REQUIRED_MONTH_RANGE_LABEL,
        "required_months_total": len(REQUIRED_MONTHS),
        "employees_missing_required_months": employees_missing_required_months,
        "missing_required_months_total": missing_required_months_total,
        "scan_without_included_files": sum(
            1 for row in employee_summary_rows if bool(row["scan_without_included_files"])
        ),
        "source_manifest_files": len(manifest_rows),
        "missing_text_layer_files": sum(
            int(row["missing_text_layer_files"]) for row in employee_summary_rows
        ),
        "pages_missing_year_month": sum(
            int(row["pages_missing_year_month"]) for row in employee_summary_rows
        ),
        "employees_missing_months_after_pairing": employees_missing_months_after_pairing,
        "months_missing_after_pairing_total": months_missing_after_pairing_total,
        "employees_complete_pairing_absence": employees_complete_pairing_absence,
        "artifact_errors_total": len(artifact_errors),
    }

    artifacts = {
        "pipeline_dir": str(resolved.pipeline_dir),
        "scan_dir": str(resolved.scan_dir),
        "documents_dir": str(resolved.documents_dir),
        "events_dir": str(resolved.events_dir),
        "shifts_dir": str(resolved.shifts_dir),
        "scan_report_found": resolved.scan_report_path.exists(),
        "scan_report_has_zero_included_data": scan_report_has_zero_included_data,
        "found_events_csv": str(resolved.found_events_csv_path),
        "found_events_csv_found": resolved.found_events_csv_path.exists(),
        "manifest_csv_count": len(manifest_csvs),
        "excluded_index_found": resolved.excluded_index_path.exists(),
        "pages_csv_found": resolved.pages_csv_path.exists(),
        "pair_report_found": resolved.pair_report_path.exists(),
        "layout_mode": (
            "legacy"
            if resolved.documents_dir.name == "text_extracted"
            or resolved.shifts_dir.name == "employee_shifts_from_raw"
            else "current"
        ),
        "errors": artifact_errors,
    }

    return {
        "stats": stats,
        "artifacts": artifacts,
        "employee_summary_rows": employee_summary_rows,
        "employees": employees_nested,
        "issues": issues,
    }


__all__ = [
    "EMPLOYEE_SUMMARY_COLUMNS",
    "ISSUE_COLUMNS",
    "ResolvedAuditInputs",
    "audit_missing_timbrature_pipeline",
    "resolve_audit_inputs",
]
