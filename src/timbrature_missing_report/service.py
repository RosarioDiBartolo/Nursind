from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.drive_service.index import MapIndex
from src.drive_service.io_json import load_json
from src.drive_service.names import normalize_name
from src.drive_service.text_extraction_csv import (
    find_text_extraction_csvs,
    read_text_extraction_rows,
)

from .accumulator import (
    EmployeeAccumulator,
    build_expected_month_detail,
    ensure_employee,
    mark_upstream_cause,
    register_expected_month,
    register_source_file,
    sorted_month_labels,
)
from .inputs import (
    ResolvedAuditInputs,
    YearMonth,
    clean_str,
    derive_manifest_expected_months,
    infer_expected_month_from_file,
    iter_csv_rows,
    parse_event_year_month,
    parse_int,
    resolve_audit_inputs,
    read_csv_rows,
    read_pair_months,
)
from .issues import EMPLOYEE_SUMMARY_COLUMNS, ISSUE_COLUMNS, append_issue

logger = logging.getLogger(__name__)

REQUIRED_YEAR_START = 2014
REQUIRED_YEAR_END = 2026
REQUIRED_MONTH_RANGE_LABEL = f"{REQUIRED_YEAR_START:04d}-01..{REQUIRED_YEAR_END:04d}-12"
REQUIRED_MONTHS: tuple[YearMonth, ...] = tuple(
    (year, month)
    for year in range(REQUIRED_YEAR_START, REQUIRED_YEAR_END + 1)
    for month in range(1, 13)
)
REQUIRED_MONTHS_SET = set(REQUIRED_MONTHS)


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
            pages_rows = read_csv_rows(resolved.pages_csv_path)
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "pages_csv", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception("Failed reading pages CSV from %s", resolved.pages_csv_path)

    for row in pages_rows:
        file_id = clean_str(row.get("source_file_id"))
        doc_json = clean_str(row.get("source_doc_json"))
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
                ensure_employee(
                    employees,
                    employees_by_id,
                    employees_by_name,
                    employee_name=clean_str(row.get("employee")) or "unknown",
                    employee_id=clean_str(row.get("employee_id")),
                )

        raw_zero_included = scan_report_payload.get("employees_without_included_files")
        if isinstance(raw_zero_included, list):
            scan_report_has_zero_included_data = True
            for row in raw_zero_included:
                if not isinstance(row, dict):
                    continue
                record = ensure_employee(
                    employees,
                    employees_by_id,
                    employees_by_name,
                    employee_name=clean_str(row.get("employee")) or "unknown",
                    employee_id=clean_str(row.get("employee_id")),
                )
                record.scan_without_included_files = True
                detail = "Direct employee folder exists in scan root but produced 0 included files"
                filtered_files = parse_int(row.get("filtered_files"))
                filtered_folders = parse_int(row.get("filtered_folders"))
                detail_parts = [detail]
                if filtered_files is not None:
                    detail_parts.append(f"filtered_files={filtered_files}")
                if filtered_folders is not None:
                    detail_parts.append(f"filtered_folders={filtered_folders}")
                append_issue(
                    record,
                    issues,
                    issue_type="scan_without_included_files",
                    stage="scan",
                    detail="; ".join(detail_parts),
                )

    for row in manifest_rows:
        employee_name = clean_str(row.get("employee")) or "unknown"
        employee_id = clean_str(row.get("employee_id"))
        record = ensure_employee(
            employees,
            employees_by_id,
            employees_by_name,
            employee_name=employee_name,
            employee_id=employee_id,
        )

        file_id = clean_str(row.get("file_id"))
        file_name = clean_str(row.get("file_name"))
        doc_json = clean_str(row.get("doc_json"))
        drive_path = clean_str(row.get("drive_path"))
        source_text_ref = clean_str(row.get("source_text_ref"))
        source_token = file_id or doc_json or file_name or drive_path or source_text_ref or employee_name
        register_source_file(record, source_token)

        if file_id:
            manifest_by_file_id[file_id] = row
        if doc_json:
            manifest_by_doc_json[doc_json] = row

        file_page_rows = []
        if file_id:
            file_page_rows.extend(pages_by_file_id.get(file_id, []))
        if doc_json:
            file_page_rows.extend(pages_by_doc_json.get(doc_json, []))

        expected_months, source_kind = derive_manifest_expected_months(
            documents_dir=resolved.documents_dir,
            row=row,
            page_rows=file_page_rows,
        )
        for year_month in expected_months:
            register_expected_month(
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
                if clean_str(entry.reason) != "missing_text_layer":
                    continue
                record = ensure_employee(
                    employees,
                    employees_by_id,
                    employees_by_name,
                    employee_name=clean_str(entry.employee) or "unknown",
                    employee_id=clean_str(entry.employee_id),
                )
                file_id = clean_str(entry.file_id)
                file_name = clean_str(entry.file_name)
                drive_path = clean_str(entry.drive_path)
                source_token = file_id or file_name or drive_path or record.employee
                register_source_file(record, source_token)
                record.missing_text_layer_files += 1

                inferred_month = infer_expected_month_from_file(
                    file_name=file_name,
                    drive_path=drive_path,
                )
                inferred_months = {inferred_month} if inferred_month is not None else set()
                if inferred_month is not None:
                    register_expected_month(
                        record,
                        year_month=inferred_month,
                        source={
                            "file_id": file_id,
                            "file_name": file_name,
                            "source_doc_json": None,
                            "source_kind": "missing_text_layer",
                        },
                    )
                    mark_upstream_cause(
                        record,
                        year_months=inferred_months,
                        cause="missing_text_layer",
                    )

                append_issue(
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
        decision_reason = clean_str(row.get("decision_reason"))
        dropped = parse_int(row.get("events_dropped_missing_year_month")) or 0
        if decision_reason != "missing_page_year_month" and dropped <= 0:
            continue

        file_id = clean_str(row.get("source_file_id"))
        doc_json = clean_str(row.get("source_doc_json"))
        manifest_row = None
        if file_id:
            manifest_row = manifest_by_file_id.get(file_id)
        if manifest_row is None and doc_json:
            manifest_row = manifest_by_doc_json.get(doc_json)

        employee_name = (
            clean_str(manifest_row.get("employee")) if manifest_row is not None else None
        ) or clean_str(row.get("source_employee")) or "unknown"
        employee_id = (
            clean_str(manifest_row.get("employee_id")) if manifest_row is not None else None
        )
        record = ensure_employee(
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
            inferred = infer_expected_month_from_file(
                file_name=(
                    clean_str(manifest_row.get("file_name"))
                    if manifest_row is not None
                    else clean_str(row.get("source_file_name"))
                ),
                drive_path=clean_str(manifest_row.get("drive_path")) if manifest_row else None,
                source_text_ref=(
                    clean_str(manifest_row.get("source_text_ref")) if manifest_row else None
                ),
            )
            if inferred is not None:
                inferred_months.add(inferred)

        if inferred_months:
            mark_upstream_cause(
                record,
                year_months=inferred_months,
                cause="missing_page_year_month",
            )

        issue_month = sorted(inferred_months)[0] if inferred_months else None
        append_issue(
            record,
            issues,
            issue_type="missing_page_year_month",
            stage="events",
            file_id=file_id,
            file_name=(
                clean_str(manifest_row.get("file_name"))
                if manifest_row is not None
                else clean_str(row.get("source_file_name"))
            ),
            source_doc_json=doc_json,
            page_no=parse_int(row.get("page_no")),
            year_month=issue_month,
            detail="Events were detected on the page but month/year could not be resolved",
            events_dropped=dropped,
        )

    if resolved.found_events_csv_path.exists():
        try:
            for row in iter_csv_rows(resolved.found_events_csv_path):
                employee_name = clean_str(row.get("source_employee"))
                if employee_name is None or normalize_name(employee_name) == "unknown":
                    continue
                year_month = parse_event_year_month(row)
                if year_month is None or year_month not in REQUIRED_MONTHS_SET:
                    continue
                record = ensure_employee(
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
            record = ensure_employee(
                employees,
                employees_by_id,
                employees_by_name,
                employee_name=clean_str(row.get("employee")) or "unknown",
                employee_id=clean_str(row.get("employee_id")),
            )
            record.pair_status = clean_str(row.get("status"))
            record.pair_error_code = clean_str(row.get("error_code"))
            record.pair_error = clean_str(row.get("error"))
            record.pair_output_csv = clean_str(row.get("output_csv"))
            record.pairs_rows = max(record.pairs_rows, parse_int(row.get("pairs_out")) or 0)

            if record.pair_status != "ok":
                append_issue(
                    record,
                    issues,
                    issue_type="pairing_failed",
                    stage="shifts",
                    detail=record.pair_error or "Pairing step reported an error",
                )

            output_csv = clean_str(row.get("output_csv"))
            if output_csv:
                output_path = Path(output_csv)
                if output_path.exists():
                    months, row_count = read_pair_months(output_path)
                    record.paired_months.update(months)
                    record.pairs_rows = max(record.pairs_rows, row_count)
                else:
                    append_issue(
                        record,
                        issues,
                        issue_type="pair_output_missing",
                        stage="shifts",
                        detail=f"Pair report points to a missing CSV: {output_csv}",
                    )
    else:
        for pair_csv in sorted(resolved.shifts_dir.glob("*.pairs.csv")):
            employee_name = pair_csv.name[: -len(".pairs.csv")] or "unknown"
            record = ensure_employee(
                employees,
                employees_by_id,
                employees_by_name,
                employee_name=employee_name,
                employee_id=None,
            )
            months, row_count = read_pair_months(pair_csv)
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
                append_issue(
                    record,
                    issues,
                    issue_type="missing_month_after_pairing",
                    stage="shifts",
                    year_month=year_month,
                    detail=build_expected_month_detail(record=record, year_month=year_month),
                )

        complete_pairing_absence = bool(record.expected_months and not record.paired_months)
        if complete_pairing_absence:
            employees_complete_pairing_absence += 1
            append_issue(
                record,
                issues,
                issue_type="complete_pairing_absence",
                stage="shifts",
                detail="No paired months found for employee despite having source documents",
            )

        if record.issues:
            employees_with_issues += 1

        found_event_labels = sorted_month_labels(record.found_event_months)
        missing_required_labels = sorted_month_labels(missing_required_months)
        expected_labels = sorted_month_labels(record.expected_months)
        paired_labels = sorted_month_labels(record.paired_months)
        missing_labels = sorted_month_labels(missing_months)
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
        "layout_mode": "current",
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
