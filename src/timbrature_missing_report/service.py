from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import Any

from cartellino_parser.drive_service.index import MapIndex
from cartellino_parser.drive_service.io_json import load_json
from cartellino_parser.drive_service.names import normalize_name
from cartellino_parser.drive_service.text_extraction_csv import (
    find_text_extraction_csvs,
    read_text_extraction_rows,
)
from cartellino_parser.reporting import build_stage_report, compact_stage_report, resolve_output_path, write_json_report

from .accumulator import EmployeeAccumulator, ensure_employee, register_source_file
from .inputs import (
    ResolvedAuditInputs,
    YearMonth,
    clean_str,
    format_year_month,
    parse_bool,
    parse_int,
    parse_year_month,
    read_csv_rows,
    resolve_audit_inputs,
)
from .issues import (
    COVERAGE_COLUMNS,
    FINDING_COLUMNS,
    SUMMARY_COLUMNS,
    append_coverage_gap,
    append_finding,
)
from .options import (
    TimbratureMissingReportOptions,
    default_coverage_csv_path,
    default_findings_csv_path,
    default_pipeline_dir,
    default_report_json_path,
    default_summary_csv_path,
)

logger = logging.getLogger(__name__)

COVERAGE_START_YEAR = 2014
COVERAGE_END_YEAR = 2025
COVERAGE_MONTH_RANGE_LABEL = f"{COVERAGE_START_YEAR:04d}-01..{COVERAGE_END_YEAR:04d}-12"
REQUIRED_COVERAGE_MONTHS = {
    (year, month)
    for year in range(COVERAGE_START_YEAR, COVERAGE_END_YEAR + 1)
    for month in range(1, 13)
}


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        import csv

        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def _employee_counter_key(employee: str | None, employee_id: str | None) -> tuple[str, str]:
    return normalize_name(employee or "unknown"), employee_id or ""


def _build_missing_coverage_month_detail(year_month: YearMonth) -> str:
    return f"Coverage month {format_year_month(year_month)} not present in relevant pages"


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


def _extract_pair_report_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    for key in ("by_employee", "items"):
        raw_rows = payload.get(key)
        if isinstance(raw_rows, list):
            return [row for row in raw_rows if isinstance(row, dict)]
    return []


def _coverage_month_from_page_row(row: dict[str, str]) -> YearMonth | None:
    if parse_bool(row.get("relevant_for_coverage")) is not True:
        return None
    year_month = parse_year_month(row.get("page_year"), row.get("page_month"))
    if year_month is None:
        return None
    if not (COVERAGE_START_YEAR <= year_month[0] <= COVERAGE_END_YEAR):
        return None
    return year_month


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

                append_finding(
                    record,
                    finding_rows,
                    finding_type="missing_text_layer",
                    stage="documents",
                    file_id=file_id,
                    file_name=file_name,
                    detail="Document was excluded because the PDF had no text layer",
                )
        except Exception as exc:
            artifact_errors.append(
                {"artifact": "excluded_index", "error": f"{type(exc).__name__}: {exc}"}
            )
            logger.exception("Failed reading excluded index from %s", resolved.excluded_index_path)

    for row in pages_rows:
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

        coverage_month = _coverage_month_from_page_row(row)
        if coverage_month is not None:
            record.coverage_months.add(coverage_month)

        decision_reason = clean_str(row.get("decision_reason"))
        dropped = parse_int(row.get("events_dropped_missing_year_month")) or 0
        if decision_reason != "missing_page_year_month" and dropped <= 0:
            continue

        record.pages_missing_year_month += 1
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
            detail="Events were detected on the page but month/year could not be resolved",
            events_dropped=dropped,
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

    pair_report_rows = _extract_pair_report_rows(pair_report_payload)

    for row in pair_report_rows:
        record = ensure_employee(
            employees,
            employees_by_id,
            employees_by_name,
            employee_name=clean_str(row.get("employee")) or "unknown",
            employee_id=clean_str(row.get("employee_id")),
        )
        pair_status = clean_str(row.get("status"))
        pair_error = clean_str(row.get("error"))
        if pair_status != "ok":
            append_finding(
                record,
                finding_rows,
                finding_type="pairing_failed",
                stage="shifts",
                detail=pair_error or "Pairing step reported an error",
            )

        output_csv = clean_str(row.get("output_csv"))
        if output_csv:
            output_path = Path(output_csv)
            if not output_path.exists():
                append_finding(
                    record,
                    finding_rows,
                    finding_type="pair_output_missing",
                    stage="shifts",
                    detail=f"Pair report points to a missing CSV: {output_csv}",
                )

    summary_rows: list[dict[str, Any]] = []
    employees_missing_coverage_months = 0
    missing_coverage_months_total = 0
    sorted_employees = sorted(
        employees,
        key=lambda item: (normalize_name(item.employee), item.employee_id or ""),
    )
    employee_metrics: dict[int, dict[str, int]] = {}

    for record in sorted_employees:
        missing_coverage_months = REQUIRED_COVERAGE_MONTHS - record.coverage_months
        if missing_coverage_months:
            employees_missing_coverage_months += 1
            missing_coverage_months_total += len(missing_coverage_months)
            for year_month in sorted(missing_coverage_months):
                append_coverage_gap(
                    record,
                    coverage_rows,
                    gap_type="missing_coverage_month",
                    stage="events",
                    year_month=year_month,
                    detail=_build_missing_coverage_month_detail(year_month),
                )

        employee_metrics[id(record)] = {
            "coverage_months_count": len(record.coverage_months),
            "missing_coverage_months_count": len(missing_coverage_months),
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
                "coverage_month_range": COVERAGE_MONTH_RANGE_LABEL,
                "coverage_months_count": metrics["coverage_months_count"],
                "missing_coverage_months_count": metrics["missing_coverage_months_count"],
                "scan_without_included_files": record.scan_without_included_files,
                "missing_text_layer_files": record.missing_text_layer_files,
                "pages_missing_year_month": record.pages_missing_year_month,
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
        "coverage_month_range": COVERAGE_MONTH_RANGE_LABEL,
        "coverage_months_total": len(REQUIRED_COVERAGE_MONTHS),
        "employees_missing_coverage_months": employees_missing_coverage_months,
        "missing_coverage_months_total": missing_coverage_months_total,
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


def build_missing_timbrature_report(
    *,
    pipeline_dir: str | None = None,
    report_json: str | None = None,
    summary_csv: str | None = None,
    findings_csv: str | None = None,
    coverage_csv: str | None = None,
) -> dict[str, Any]:
    pipeline_dir = pipeline_dir or default_pipeline_dir()
    report_json = report_json or default_report_json_path()
    summary_csv = summary_csv or default_summary_csv_path()
    findings_csv = findings_csv or default_findings_csv_path()
    coverage_csv = coverage_csv or default_coverage_csv_path()
    audit = audit_missing_timbrature_pipeline(pipeline_dir)

    report_path = resolve_output_path(pipeline_dir, report_json)
    summary_csv_path = resolve_output_path(pipeline_dir, summary_csv)
    findings_csv_path = resolve_output_path(pipeline_dir, findings_csv)
    coverage_csv_path = resolve_output_path(pipeline_dir, coverage_csv)

    _write_csv(summary_csv_path, audit["summary_rows"], SUMMARY_COLUMNS)
    _write_csv(findings_csv_path, audit["finding_rows"], FINDING_COLUMNS)
    _write_csv(coverage_csv_path, audit["coverage_rows"], COVERAGE_COLUMNS)

    report = build_stage_report(
        stage="timbrature_missing_report",
        inputs={"pipeline_dir": str(Path(pipeline_dir).resolve())},
        outputs={
            "report_json": str(report_path.resolve()),
            "summary_csv": str(summary_csv_path.resolve()),
            "findings_csv": str(findings_csv_path.resolve()),
            "coverage_csv": str(coverage_csv_path.resolve()),
        },
        stats={
            **audit["stats"],
            "finding_counts_by_type": audit["finding_counts_by_type"],
            "coverage_counts_by_type": audit["coverage_counts_by_type"],
        },
        row_totals={
            "items": len(audit["summary_rows"]),
            "issues": len(audit["finding_rows"]),
            "coverage_rows": len(audit["coverage_rows"]),
        },
        items=audit["summary_rows"],
        issues=audit["finding_rows"],
    )
    write_json_report(report_path, compact_stage_report(report))
    return report


def run_from_options(options: TimbratureMissingReportOptions) -> dict[str, Any]:
    return build_missing_timbrature_report(
        pipeline_dir=options.pipeline_dir,
        report_json=options.report_json,
        summary_csv=options.summary_csv,
        findings_csv=options.findings_csv,
        coverage_csv=options.coverage_csv,
    )


__all__ = [
    "COVERAGE_COLUMNS",
    "FINDING_COLUMNS",
    "ResolvedAuditInputs",
    "SUMMARY_COLUMNS",
    "audit_missing_timbrature_pipeline",
    "build_missing_timbrature_report",
    "resolve_audit_inputs",
    "run_from_options",
]

