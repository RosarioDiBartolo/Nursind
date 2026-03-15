from __future__ import annotations

import logging
import os
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from src.raw_text_parsing import EVENT_PATTERNS

from .options import (
    DEFAULT_MANIFEST_GLOB,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_PAGES_NAME,
    default_output_dir,
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
    max_pattern_examples: int,
    max_unmatched_examples_per_file: int,
    pattern_examples: dict[str, list[str]],
    pattern_counts: dict[str, int],
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
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        )
    except Exception as exc:
        result = base_process_result(source_context)
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result, [], [], [], []


def _process_many_documents(
    documents: list[Any],
    *,
    input_mode: str,
    output_dir: str,
    out_name: str,
    pages_name: str,
    input_dir: str | None,
    manifest_glob: str,
    max_pattern_examples: int,
    max_unmatched_examples_per_file: int,
) -> dict[str, Any]:
    pattern_examples = {name: [] for name, _ in EVENT_PATTERNS}
    pattern_counts = {name: 0 for name, _ in EVENT_PATTERNS}

    totals: dict[str, Any] = {
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
        "input_dir": os.path.abspath(input_dir) if input_dir else None,
        "input_mode": input_mode,
        "output_dir": os.path.abspath(output_dir),
        "manifest_glob": manifest_glob,
        "out_name": out_name,
        "pages_name": pages_name,
        "output_events_csv": None,
        "output_pages_csv": None,
    }

    parser_counts: Counter[str] = Counter()
    page_kind_counts: Counter[str] = Counter()
    decision_counts: Counter[str] = Counter()
    year_month_source_counts: Counter[str] = Counter()

    files_without_events_list: list[dict[str, Any]] = []
    pages_missing_year_month: list[dict[str, Any]] = []
    low_coverage_pages: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    items: list[dict[str, Any]] = []
    all_event_rows: list[dict[str, Any]] = []
    all_page_rows: list[dict[str, Any]] = []

    for index, row in enumerate(documents, start=1):
        (
            result,
            event_rows,
            page_rows,
            missing_year_month_pages,
            low_cov_pages,
        ) = _process_one_document(
            row,
            input_dir=input_dir,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        )

        all_event_rows.extend(event_rows)
        all_page_rows.extend(page_rows)
        pages_missing_year_month.extend(missing_year_month_pages)
        low_coverage_pages.extend(low_cov_pages)

        for page_row in page_rows:
            page_kind_counts[str(page_row.get("page_kind") or "unknown")] += 1
            decision_counts[str(page_row.get("decision") or "unknown")] += 1
            year_month_source_counts[str(page_row.get("year_month_source") or "none")] += 1

        public_result = dict(result)
        public_result.pop("output_events_csv", None)
        public_result.pop("output_pages_csv", None)
        items.append(public_result)

        parser_id = result.get("parser_id")
        if isinstance(parser_id, str) and parser_id:
            parser_counts[parser_id] += 1

        if result["status"] != "ok":
            totals["files_error"] += 1
            errors.append(
                {
                    "source_doc_json": str(result["source_doc_json"]),
                    "source_file_name": str(result.get("source_file_name") or ""),
                    "error": str(result["error"]),
                }
            )
            logger.error("Error processing %s: %s", result["source_doc_json"], result["error"])
            continue

        totals["files_processed"] += 1
        if int(result["events_extracted"]) > 0:
            totals["files_with_events"] += 1
        else:
            totals["files_without_events"] += 1
            files_without_events_list.append(
                {
                    "source_doc_json": str(result["source_doc_json"]),
                    "source_file_name": result.get("source_file_name"),
                    "source_file_id": result.get("source_file_id"),
                    "source_drive_path": result.get("source_drive_path"),
                    "source_file_link": result.get("source_file_link"),
                    "parser_id": result.get("parser_id"),
                    "doc_format": result.get("doc_format"),
                    "rows_considered": int(result["rows_considered"]),
                    "rows_without_events": int(result["rows_without_events"]),
                    "coverage_ratio": result.get("coverage_ratio"),
                }
            )

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
            totals[key] += int(result.get(key) or 0)

        if index % 500 == 0:
            logger.info(
                "Processed %s/%s documents (events=%s)",
                index,
                len(documents),
                totals["events_extracted"],
            )

    if totals["rows_considered"] > 0:
        totals["coverage_ratio_relevant_pages"] = round(
            totals["rows_with_events"] / totals["rows_considered"],
            6,
        )

    out_events_csv = Path(output_dir) / (out_name.strip() or DEFAULT_OUT_NAME)
    out_pages_csv = Path(output_dir) / (pages_name.strip() or DEFAULT_PAGES_NAME)
    write_rows_csv(rows=all_event_rows, out_csv=out_events_csv, columns=EVENT_COLUMNS)
    write_rows_csv(rows=all_page_rows, out_csv=out_pages_csv, columns=PAGE_COLUMNS)
    totals["output_events_csv"] = str(out_events_csv.resolve())
    totals["output_pages_csv"] = str(out_pages_csv.resolve())

    files_without_events_list.sort(
        key=lambda item: (-int(item["rows_without_events"]), str(item["source_doc_json"]))
    )
    pages_missing_year_month.sort(
        key=lambda item: (
            str(item.get("source_doc_json") or ""),
            int(item.get("page_no") or 0),
        )
    )
    low_coverage_pages.sort(
        key=lambda item: (
            float(item.get("coverage_ratio_page") or 1.0),
            -int(item.get("rows_without_events") or 0),
            str(item.get("source_doc_json") or ""),
            int(item.get("page_no") or 0),
        )
    )

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "counts": {
            "by_parser": dict(sorted(parser_counts.items(), key=lambda item: item[0])),
            "by_page_kind": dict(sorted(page_kind_counts.items(), key=lambda item: item[0])),
            "by_decision": dict(sorted(decision_counts.items(), key=lambda item: item[0])),
            "by_year_month_source": dict(
                sorted(year_month_source_counts.items(), key=lambda item: item[0])
            ),
        },
        "pattern_counts": pattern_counts,
        "pattern_examples": pattern_examples,
        "files_without_events_list": files_without_events_list,
        "pages_missing_year_month": pages_missing_year_month,
        "low_coverage_pages": low_coverage_pages,
        "file_errors": errors,
    }


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
    output_dir = output_dir or default_output_dir()
    normalized_rows = list(text_rows)
    return _process_many_documents(
        normalized_rows,
        input_mode="employee_manifest_csv",
        output_dir=output_dir,
        out_name=out_name,
        pages_name=pages_name,
        input_dir=input_dir,
        manifest_glob=manifest_glob,
        max_pattern_examples=max_pattern_examples,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
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
    pattern_examples: dict[str, list[str]] | None = None,
    pattern_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    del pattern_examples, pattern_counts
    report = process_many_text_rows(
        [row],
        output_dir=output_dir,
        out_name=out_name,
        pages_name=pages_name,
        input_dir=input_dir,
        manifest_glob=DEFAULT_MANIFEST_GLOB,
        max_pattern_examples=max_pattern_examples,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
    )
    if not report["items"]:
        source_context = resolve_source_context(row, {}, input_dir=input_dir)
        result = base_process_result(source_context, error="No rows processed")
        result["error_code"] = "processing_error"
        return result

    item = dict(report["items"][0])
    stats = report.get("stats") or {}
    item["output_events_csv"] = stats.get("output_events_csv")
    item["output_pages_csv"] = stats.get("output_pages_csv")
    return item


__all__ = [
    "process_many_text_rows",
    "process_one_text_row",
]
