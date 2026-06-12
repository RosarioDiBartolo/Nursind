from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

from core.drive.text_extraction_csv import (
    find_text_extraction_csvs,
    read_text_extraction_rows,
)
from core.reporting import build_stage_report, compact_stage_report, write_json_report

from .options import (
    DEFAULT_MANIFEST_GLOB,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_PAGES_NAME,
    ExtractEventsFromTextOptions,
    default_input_dir,
    default_output_dir,
    default_report_json_path,
)
from .page_analysis import base_process_result, parse_document_pages
from .source_context import (
    load_manifest_document,
    resolve_source_context,
    resolve_source_path,
)
from .writers import EVENT_COLUMNS, PAGE_COLUMNS, write_rows_csv

logger = logging.getLogger(__name__)


def _process_one_document(
    row: dict[str, Any],
    *,
    input_dir: str | None,
    max_unmatched_examples_per_file: int,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    source_context = resolve_source_context(row, {}, input_dir=input_dir)
    payload = load_manifest_document(row, input_dir=input_dir)
    if payload is None:
        result = base_process_result(source_context)
        result["error_code"] = "missing_document_extraction_doc"
        result["error"] = (
            "MISSING_DOCUMENT_EXTRACTION_DOC: canonical document payload is required "
            "and could not be loaded"
        )
        return result, [], [], [], []

    source_context = resolve_source_context(row, payload, input_dir=input_dir)
    source_path = resolve_source_path(
        row,
        payload,
        fallback=source_context["source_file_ref"] or source_context["source_doc_json"],
    )
    try:
        return parse_document_pages(
            document=payload,
            source_context=source_context,
            source_path=source_path,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
        )
    except Exception as exc:
        result = base_process_result(source_context)
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result, [], [], [], []


def process_many_text_rows(
    text_rows: Iterable[dict[str, Any]],
    *,
    output_dir: str | None = None,
    out_name: str = DEFAULT_OUT_NAME,
    pages_name: str = DEFAULT_PAGES_NAME,
    input_dir: str | None = None,
    manifest_glob: str = DEFAULT_MANIFEST_GLOB,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    del max_pattern_examples
    output_dir = output_dir or default_output_dir()
    documents = list(text_rows)

    stats: dict[str, Any] = {
        "files_total": len(documents),
        "files_processed": 0,
        "files_error": 0,
        "files_with_events": 0,
        "files_without_events": 0,
        "pages_total": 0,
        "pages_relevant": 0,
        "pages_skipped_non_event": 0,
        "pages_error": 0,
        "rows_considered": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "events_dropped_missing_year_month": 0,
        "coverage_ratio_relevant_pages": None,
    }
    issues: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    all_event_rows: list[dict[str, Any]] = []
    all_page_rows: list[dict[str, Any]] = []
    pages_missing_year_month = 0
    low_coverage_pages = 0

    for index, row in enumerate(documents, start=1):
        result, event_rows, page_rows, missing_pages, low_cov_pages = _process_one_document(
            row,
            input_dir=input_dir,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
        )

        items.append(
            {
                key: value
                for key, value in result.items()
                if key not in {"output_events_csv", "output_pages_csv"}
            }
        )
        all_event_rows.extend(event_rows)
        all_page_rows.extend(page_rows)
        pages_missing_year_month += len(missing_pages)
        low_coverage_pages += len(low_cov_pages)

        if result["status"] != "ok":
            stats["files_error"] += 1
            issues.append(
                {
                    "code": str(result.get("error_code") or "processing_error"),
                    "source_doc_json": str(result.get("source_doc_json") or ""),
                    "source_file_name": str(result.get("source_file_name") or ""),
                    "message": str(result.get("error") or "processing_error"),
                }
            )
            logger.error("Error processing %s: %s", result["source_doc_json"], result["error"])
            continue

        stats["files_processed"] += 1
        if int(result["events_extracted"]) > 0:
            stats["files_with_events"] += 1
        else:
            stats["files_without_events"] += 1

        for key in (
            "pages_total",
            "pages_relevant",
            "pages_skipped_non_event",
            "pages_error",
            "rows_considered",
            "rows_with_events",
            "rows_without_events",
            "events_extracted",
            "events_dropped_missing_year_month",
        ):
            stats[key] += int(result.get(key) or 0)

        if index % 500 == 0:
            logger.info(
                "Processed %s/%s documents (events=%s)",
                index,
                len(documents),
                stats["events_extracted"],
            )

    if stats["rows_considered"] > 0:
        stats["coverage_ratio_relevant_pages"] = round(
            stats["rows_with_events"] / stats["rows_considered"],
            6,
        )

    out_events_csv = Path(output_dir) / (out_name.strip() or DEFAULT_OUT_NAME)
    out_pages_csv = Path(output_dir) / (pages_name.strip() or DEFAULT_PAGES_NAME)
    write_rows_csv(rows=all_event_rows, out_csv=out_events_csv, columns=EVENT_COLUMNS)
    write_rows_csv(rows=all_page_rows, out_csv=out_pages_csv, columns=PAGE_COLUMNS)

    return build_stage_report(
        stage="extract_events_from_documents",
        inputs={
            "input_mode": "employee_manifest_csv",
            "input_dir": os.path.abspath(input_dir) if input_dir else None,
            "output_dir": os.path.abspath(output_dir),
            "manifest_glob": manifest_glob,
            "out_name": out_name,
            "pages_name": pages_name,
        },
        outputs={
            "events_csv": str(out_events_csv.resolve()),
            "pages_csv": str(out_pages_csv.resolve()),
        },
        stats=stats,
        row_totals={
            "items": len(items),
            "issues": len(issues),
            "event_rows": len(all_event_rows),
            "page_rows": len(all_page_rows),
            "pages_missing_year_month": pages_missing_year_month,
            "low_coverage_pages": low_coverage_pages,
        },
        items=items,
        issues=issues,
    )


def process_one_text_row(
    row: dict[str, Any],
    *,
    output_dir: str | None = None,
    out_name: str = DEFAULT_OUT_NAME,
    pages_name: str = DEFAULT_PAGES_NAME,
    input_dir: str | None = None,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    del max_pattern_examples
    report = process_many_text_rows(
        [row],
        output_dir=output_dir,
        out_name=out_name,
        pages_name=pages_name,
        input_dir=input_dir,
        manifest_glob=DEFAULT_MANIFEST_GLOB,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
    )
    items = report.get("items") or []
    if not items:
        source_context = resolve_source_context(row, {}, input_dir=input_dir)
        result = base_process_result(source_context, error="No rows processed")
        result["error_code"] = "processing_error"
        return result

    item = dict(items[0])
    outputs = report.get("outputs") or {}
    item["output_events_csv"] = outputs.get("events_csv")
    item["output_pages_csv"] = outputs.get("pages_csv")
    return item


def extract_events_from_documents_dir(
    *,
    input_dir: str | None = None,
    output_dir: str | None = None,
    out_name: str = DEFAULT_OUT_NAME,
    pages_name: str = DEFAULT_PAGES_NAME,
    report_json: str | None = None,
    manifest_glob: str = DEFAULT_MANIFEST_GLOB,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    del max_pattern_examples
    input_dir = input_dir or default_input_dir()
    output_dir = output_dir or default_output_dir()
    report_json = report_json or default_report_json_path()
    input_base = Path(input_dir)
    employee_csv_files = find_text_extraction_csvs(input_base)
    if not employee_csv_files:
        raise FileNotFoundError(
            "NO_DOCUMENT_EXTRACTION_MANIFESTS: no employee manifest CSV files were found in "
            f"{input_base}"
        )

    report = process_many_text_rows(
        read_text_extraction_rows(employee_csv_files, hydrate_text=False),
        output_dir=output_dir,
        out_name=out_name,
        pages_name=pages_name,
        input_dir=input_dir,
        manifest_glob=manifest_glob,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
    )
    report["outputs"]["report_json"] = str(Path(report_json).resolve())
    write_json_report(report_json, compact_stage_report(report))
    return report


def run_from_options(options: ExtractEventsFromTextOptions) -> dict[str, Any]:
    return extract_events_from_documents_dir(
        input_dir=options.input_dir,
        output_dir=options.output_dir,
        out_name=options.out_name,
        pages_name=options.pages_name,
        report_json=options.report_json,
        manifest_glob=options.manifest_glob,
        max_pattern_examples=options.max_pattern_examples,
        max_unmatched_examples_per_file=options.max_unmatched_examples_per_file,
    )


__all__ = [
    "extract_events_from_documents_dir",
    "process_many_text_rows",
    "process_one_text_row",
    "run_from_options",
]

