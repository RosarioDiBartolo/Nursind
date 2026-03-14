from __future__ import annotations

import logging
from collections import Counter
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
)
from .inputs import (
    ResolvedAuditInputs,
    YearMonth,
    clean_str,
    derive_manifest_expected_months,
    format_year_month,
    infer_expected_month_from_file,
    iter_csv_rows,
    parse_event_year_month,
    parse_int,
    resolve_audit_inputs,
    read_csv_rows,
    read_pair_months,
)
from .issues import (
    COVERAGE_COLUMNS,
    FINDING_COLUMNS,
    SUMMARY_COLUMNS,
    append_coverage_gap,
    append_finding,
)

logger = logging.getLogger(__name__)

EXPECTED_RANGE_END_YEAR = 2025
EXPECTED_RANGE_END_LABEL = f"{EXPECTED_RANGE_END_YEAR:04d}-12"


def _employee_counter_key(employee: str | None, employee_id: str | None) -> tuple[str, str]:
    return normalize_name(employee or "unknown"), employee_id or ""


def _build_expected_month_range(found_event_months: set[YearMonth]) -> tuple[set[YearMonth], str]:
    eligible_months = [year_month for year_month in found_event_months if year_month[0] <= EXPECTED_RANGE_END_YEAR]
    if not eligible_months:
        return set(), ""

    start_year = min(year for year, _month in eligible_months)
    months = {
        (year, month)
        for year in range(start_year, EXPECTED_RANGE_END_YEAR + 1)
        for month in range(1, 13)
    }
    return months, f"{start_year:04d}-01..{EXPECTED_RANGE_END_YEAR:04d}-12"


def _build_missing_expected_month_detail(year_month: YearMonth) -> str:
    return f"Expected month {format_year_month(year_month)} not present in found events"


def _format_upstream_causes(record: EmployeeAccumulator, year_month: YearMonth) -> str | None:
    upstream_causes = sorted(record.upstream_causes_by_month.get(year_month, set()))
    if not upstream_causes:
        return None
    return ", ".join(upstream_causes)


def _count_rows_by_employee(rows: list[dict[str, Any]]) -> Counter[tuple[str, str]]:
    counts: Counter[tuple[str, str]] = Counter()
    for row in rows:
        counts[
            _employee_counter_key(
                clean_str(row.get("employee")) or "unknown",
                clean_str(row.get("employee_id")),
            )
        ] += 1
    return counts


def audit_missing_timbrature_pipeline(
    pipeline_dir: str | Path,
) -> dict[str, Any]:
    resolved = resolve_audit_inputs(pipeline_dir)

    artifact_errors: list[dict[str, str]] = []
    finding_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
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
                append_finding(
                    record,
                    finding_rows,
                    finding_type="scan_without_included_files",
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

                append_finding(
                    record,
                    finding_rows,
                    finding_type="missing_text_layer",
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
        append_finding(
            record,
            finding_rows,
            finding_type="missing_page_year_month",
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
                if year_month is None or year_month[0] > EXPECTED_RANGE_END_YEAR:
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
                append_finding(
                    record,
                    finding_rows,
                    finding_type="pairing_failed",
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
                    append_finding(
                        record,
                        finding_rows,
                        finding_type="pair_output_missing",
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

    summary_rows: list[dict[str, Any]] = []
    employees_missing_expected_months = 0
    employees_missing_months_after_pairing = 0
    employees_complete_pairing_absence = 0
    missing_expected_months_total = 0
    months_missing_after_pairing_total = 0
    sorted_employees = sorted(
        employees,
        key=lambda item: (normalize_name(item.employee), item.employee_id or ""),
    )
    employee_metrics: dict[int, dict[str, Any]] = {}

    for record in sorted_employees:
        expected_months, expected_month_range = _build_expected_month_range(record.found_event_months)
        missing_expected_months = expected_months - record.found_event_months
        if missing_expected_months:
            employees_missing_expected_months += 1
            missing_expected_months_total += len(missing_expected_months)
            for year_month in sorted(missing_expected_months):
                append_coverage_gap(
                    record,
                    coverage_rows,
                    gap_type="missing_expected_month",
                    stage="events",
                    year_month=year_month,
                    upstream_causes=_format_upstream_causes(record, year_month),
                    detail=_build_missing_expected_month_detail(year_month),
                )

        missing_months_after_pairing = record.expected_months - record.paired_months
        if missing_months_after_pairing:
            employees_missing_months_after_pairing += 1
            months_missing_after_pairing_total += len(missing_months_after_pairing)
            for year_month in sorted(missing_months_after_pairing):
                append_coverage_gap(
                    record,
                    coverage_rows,
                    gap_type="missing_month_after_pairing",
                    stage="shifts",
                    year_month=year_month,
                    upstream_causes=_format_upstream_causes(record, year_month),
                    detail=build_expected_month_detail(record=record, year_month=year_month),
                )

        complete_pairing_absence = bool(record.expected_months and not record.paired_months)
        if complete_pairing_absence:
            employees_complete_pairing_absence += 1
            append_finding(
                record,
                finding_rows,
                finding_type="complete_pairing_absence",
                stage="shifts",
                detail="No paired months found for employee despite having source documents",
            )

        employee_metrics[id(record)] = {
            "expected_month_range": expected_month_range,
            "expected_months_count": len(expected_months),
            "missing_expected_months_count": len(missing_expected_months),
            "document_expected_months_count": len(record.expected_months),
            "paired_months_count": len(record.paired_months),
            "missing_months_after_pairing_count": len(missing_months_after_pairing),
            "complete_pairing_absence": complete_pairing_absence,
        }

    finding_counts_by_type = dict(
        sorted(Counter(str(row.get("finding_type") or "") for row in finding_rows).items())
    )
    coverage_counts_by_type = dict(
        sorted(Counter(str(row.get("gap_type") or "") for row in coverage_rows).items())
    )
    finding_counts_by_employee = _count_rows_by_employee(finding_rows)
    coverage_counts_by_employee = _count_rows_by_employee(coverage_rows)

    for record in sorted_employees:
        key = _employee_counter_key(record.employee, record.employee_id)
        metrics = employee_metrics[id(record)]
        summary_rows.append(
            {
                "employee": record.employee,
                "employee_id": record.employee_id,
                "source_files_total": record.source_files_total,
                "expected_month_range": metrics["expected_month_range"],
                "expected_months_count": metrics["expected_months_count"],
                "found_event_months_count": len(record.found_event_months),
                "missing_expected_months_count": metrics["missing_expected_months_count"],
                "scan_without_included_files": record.scan_without_included_files,
                "missing_text_layer_files": record.missing_text_layer_files,
                "pages_missing_year_month": record.pages_missing_year_month,
                "document_expected_months_count": metrics["document_expected_months_count"],
                "paired_months_count": metrics["paired_months_count"],
                "missing_months_after_pairing_count": metrics["missing_months_after_pairing_count"],
                "complete_pairing_absence": metrics["complete_pairing_absence"],
                "pair_rows": record.pairs_rows,
                "pair_status": record.pair_status,
                "pair_error_code": record.pair_error_code,
                "pair_output_csv": record.pair_output_csv,
                "finding_count": finding_counts_by_employee.get(key, 0),
                "coverage_gap_count": coverage_counts_by_employee.get(key, 0),
            }
        )

    stats = {
        "employees_total": len(summary_rows),
        "employees_with_findings": sum(1 for row in summary_rows if int(row["finding_count"]) > 0),
        "employees_with_coverage_gaps": sum(
            1 for row in summary_rows if int(row["coverage_gap_count"]) > 0
        ),
        "employees_with_any_gaps": sum(
            1
            for row in summary_rows
            if int(row["finding_count"]) > 0 or int(row["coverage_gap_count"]) > 0
        ),
        "findings_total": len(finding_rows),
        "coverage_gaps_total": len(coverage_rows),
        "expected_range_end": EXPECTED_RANGE_END_LABEL,
        "employees_missing_expected_months": employees_missing_expected_months,
        "missing_expected_months_total": missing_expected_months_total,
        "scan_without_included_files": sum(
            1 for row in summary_rows if bool(row["scan_without_included_files"])
        ),
        "source_manifest_files": len(manifest_rows),
        "missing_text_layer_files": sum(
            int(row["missing_text_layer_files"]) for row in summary_rows
        ),
        "pages_missing_year_month": sum(
            int(row["pages_missing_year_month"]) for row in summary_rows
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
        "summary_rows": summary_rows,
        "finding_rows": finding_rows,
        "coverage_rows": coverage_rows,
        "finding_counts_by_type": finding_counts_by_type,
        "coverage_counts_by_type": coverage_counts_by_type,
    }


__all__ = [
    "COVERAGE_COLUMNS",
    "FINDING_COLUMNS",
    "ResolvedAuditInputs",
    "SUMMARY_COLUMNS",
    "audit_missing_timbrature_pipeline",
    "resolve_audit_inputs",
]
