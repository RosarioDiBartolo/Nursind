from __future__ import annotations

import csv
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.text_extraction_csv import load_text_extraction_doc
from src.raw_text_parsing import EVENT_PATTERNS, resolve_year_month

from .models import DocumentParseResult, EventRecord, ParsedEvent, ParsedRow
from .options import (
    DEFAULT_MANIFEST_GLOB,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_OUTPUT_DIR,
)
from .parsers import resolve_parser
from .parsers.common import document_text

logger = logging.getLogger(__name__)

EVENT_COLUMNS = [
    "year",
    "month",
    "day",
    "dow",
    "event_index",
    "event_kind",
    "event_time_hhmm",
    "event_ts",
    "event_raw",
    "event_pattern",
    "parser_id",
    "doc_format",
    "source_origin",
    "source_doc_json",
    "source_file_id",
    "source_file_name",
    "source_employee",
    "source_drive_path",
    "source_file_link",
    "source_page_no",
    "source_line_id",
    "source_line_no",
    "source_line_text",
    "source_line_start_char",
    "source_line_end_char",
    "source_slot",
    "source_word_start",
    "source_word_end",
    "source_bbox_x0",
    "source_bbox_y0",
    "source_bbox_x1",
    "source_bbox_y1",
    "source_match_start_char",
    "source_match_end_char",
    "source_match_col_start",
    "source_match_col_end",
    "normalized_from",
    "normalization_kind",
    "source_event_ref",
]


def _header_preview(text: str, *, max_lines: int = 3, max_chars: int = 240) -> str:
    lines: list[str] = []
    for raw in text.splitlines():
        clean = raw.strip()
        if not clean:
            continue
        lines.append(clean)
        if len(lines) >= max_lines:
            break
    preview = " | ".join(lines)
    if len(preview) > max_chars:
        return f"{preview[:max_chars].rstrip()}..."
    return preview


def _parse_hhmm(time_str: str) -> tuple[int, int, int] | None:
    try:
        hour_s, minute_s = time_str.split(":")
        hour = int(hour_s)
        minute = int(minute_s)
    except Exception:
        return None
    if hour == 24 and minute == 0:
        return 1, 0, 0
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return 0, hour, minute
    return None


def _to_dt(base_day: date, time_str: str) -> datetime | None:
    parsed = _parse_hhmm(time_str)
    if parsed is None:
        return None
    day_offset, hour, minute = parsed
    dt = datetime(base_day.year, base_day.month, base_day.day, hour, minute)
    if day_offset:
        dt = dt + timedelta(days=day_offset)
    return dt


def _build_day(*, year: int, month: int, day_value: int) -> date | None:
    try:
        return date(year, month, day_value)
    except Exception:
        return None


def _event_ref(
    *,
    source_doc_json: str,
    page_no: int | None,
    line_id: str | None,
    line_no: int,
    word_start: int | None,
    word_end: int | None,
    slot: str | None,
    col_start: int | None,
) -> str:
    anchor: list[str] = []
    if page_no is not None:
        anchor.append(f"p{page_no}")
    if line_id:
        anchor.append(f"line={line_id}")
    else:
        anchor.append(f"line_no={line_no}")
    if word_start is not None:
        if word_end is None or word_end == word_start:
            anchor.append(f"w{word_start}")
        else:
            anchor.append(f"w{word_start}-{word_end}")
    elif slot:
        anchor.append(f"slot={slot}")
    elif col_start is not None:
        anchor.append(f"col={col_start}")
    return f"{source_doc_json}#{':'.join(anchor)}"


def _dedupe_rows(rows: tuple[ParsedRow, ...]) -> tuple[ParsedRow, ...]:
    deduped: list[ParsedRow] = []
    seen: set[tuple[int, str, str]] = set()
    for row in rows:
        key = (row.day, row.dow, row.line.text)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(key=lambda item: (item.day, item.line.line_no))
    return tuple(deduped)


def _build_event_record(
    *,
    parser_id: str,
    doc_format: str,
    source_context: dict[str, Any],
    year: int,
    month: int,
    event_index: int,
    event: ParsedEvent,
) -> EventRecord:
    day_value = _build_day(year=year, month=month, day_value=event.day)
    event_ts: str | None = None
    if day_value is not None:
        event_dt = _to_dt(day_value, event.event_time_hhmm)
        if event_dt is not None:
            event_ts = event_dt.strftime("%Y-%m-%d %H:%M:%S")

    match_start_char: int | None = None
    match_end_char: int | None = None
    match_col_start: int | None = None
    match_col_end: int | None = None
    if event.match_start is not None and event.match_end is not None:
        match_start_char = event.line.start_char + event.match_start
        match_end_char = event.line.start_char + event.match_end
        match_col_start = event.match_start + 1
        match_col_end = event.match_end + 1

    return EventRecord(
        year=year,
        month=month,
        day=event.day,
        dow=event.dow,
        event_index=event_index,
        event_kind=event.event_kind,
        event_time_hhmm=event.event_time_hhmm,
        event_ts=event_ts,
        event_raw=event.event_raw,
        event_pattern=event.event_pattern,
        parser_id=parser_id,
        doc_format=doc_format,
        source_origin=event.source_origin,
        source_doc_json=source_context["source_doc_json"],
        source_file_id=source_context.get("source_file_id"),
        source_file_name=source_context.get("source_file_name"),
        source_employee=source_context.get("source_employee"),
        source_drive_path=source_context.get("source_drive_path"),
        source_file_link=source_context.get("source_file_link"),
        source_page_no=event.line.page_no,
        source_line_id=event.line.line_id,
        source_line_no=event.line.line_no,
        source_line_text=event.line.text,
        source_line_start_char=event.line.start_char,
        source_line_end_char=event.line.end_char,
        source_slot=event.source_slot,
        source_word_start=event.source_word_start,
        source_word_end=event.source_word_end,
        source_bbox_x0=event.source_bbox_x0,
        source_bbox_y0=event.source_bbox_y0,
        source_bbox_x1=event.source_bbox_x1,
        source_bbox_y1=event.source_bbox_y1,
        source_match_start_char=match_start_char,
        source_match_end_char=match_end_char,
        source_match_col_start=match_col_start,
        source_match_col_end=match_col_end,
        normalized_from=event.normalized_from,
        normalization_kind=event.normalization_kind,
        source_event_ref=_event_ref(
            source_doc_json=source_context["source_doc_json"],
            page_no=event.line.page_no,
            line_id=event.line.line_id,
            line_no=event.line.line_no,
            word_start=event.source_word_start,
            word_end=event.source_word_end,
            slot=event.source_slot,
            col_start=match_col_start,
        ),
    )


def parse_document_payload(
    document: dict[str, Any],
    *,
    source_context: dict[str, Any],
    source_path: Path,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    pattern_examples: dict[str, list[str]] | None = None,
    pattern_counts: dict[str, int] | None = None,
) -> tuple[DocumentParseResult, dict[str, Any]]:
    text = document_text(document)
    parser = resolve_parser(document)
    rows = _dedupe_rows(parser.parse_document(document))
    year, month = resolve_year_month(text, source_path)
    if year is None or month is None:
        raise ValueError("MISSING_YEAR_MONTH")

    local_pattern_examples = (
        pattern_examples
        if pattern_examples is not None
        else {name: [] for name, _ in EVENT_PATTERNS}
    )
    local_pattern_counts = (
        pattern_counts
        if pattern_counts is not None
        else {name: 0 for name, _ in EVENT_PATTERNS}
    )

    events: list[EventRecord] = []
    stats = {
        "rows_considered": len(rows),
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "coverage_ratio": None,
        "rows_without_events_examples": [],
    }

    for row in rows:
        row_events = list(row.events)
        if not row_events:
            stats["rows_without_events"] += 1
            line_example = row.line.text.strip()
            if (
                line_example
                and len(stats["rows_without_events_examples"]) < max_unmatched_examples_per_file
            ):
                stats["rows_without_events_examples"].append(line_example)
            continue

        stats["rows_with_events"] += 1
        stats["events_extracted"] += len(row_events)
        if len(row_events) > 2:
            stats["rows_with_multi_events"] += 1

        for event_index, event in enumerate(row_events):
            events.append(
                _build_event_record(
                    parser_id=parser.parser_id,
                    doc_format=parser.legacy_doc_format,
                    source_context=source_context,
                    year=year,
                    month=month,
                    event_index=event_index,
                    event=event,
                )
            )
            local_pattern_counts[event.event_pattern] = (
                local_pattern_counts.get(event.event_pattern, 0) + 1
            )
            examples = local_pattern_examples.setdefault(event.event_pattern, [])
            line_example = row.line.text.strip()
            if (
                line_example
                and line_example not in examples
                and len(examples) < max_pattern_examples
            ):
                examples.append(line_example)

    if rows:
        stats["coverage_ratio"] = round(stats["rows_with_events"] / len(rows), 6)

    result = DocumentParseResult(
        source_doc_json=source_context["source_doc_json"],
        doc_format=parser.legacy_doc_format,
        parser_id=parser.parser_id,
        year=year,
        month=month,
        rows=rows,
        events=tuple(events),
    )
    return result, stats


def _build_events_output_path_from_source_ref(
    source_ref: str,
    *,
    output_base: Path,
    out_name: str,
) -> Path:
    suffix = out_name.strip() or DEFAULT_OUT_NAME
    rel = Path(source_ref)
    return output_base / rel.with_suffix(f".{suffix}")


def _write_events_csv(events: tuple[EventRecord, ...], out_csv: Path) -> None:
    ensure_parent_dir(str(out_csv))
    with open(out_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_COLUMNS)
        writer.writeheader()
        for event in events:
            writer.writerow(
                {
                    "year": event.year,
                    "month": event.month,
                    "day": event.day,
                    "dow": event.dow,
                    "event_index": event.event_index,
                    "event_kind": event.event_kind,
                    "event_time_hhmm": event.event_time_hhmm,
                    "event_ts": event.event_ts,
                    "event_raw": event.event_raw,
                    "event_pattern": event.event_pattern,
                    "parser_id": event.parser_id,
                    "doc_format": event.doc_format,
                    "source_origin": event.source_origin,
                    "source_doc_json": event.source_doc_json,
                    "source_file_id": event.source_file_id,
                    "source_file_name": event.source_file_name,
                    "source_employee": event.source_employee,
                    "source_drive_path": event.source_drive_path,
                    "source_file_link": event.source_file_link,
                    "source_page_no": event.source_page_no,
                    "source_line_id": event.source_line_id,
                    "source_line_no": event.source_line_no,
                    "source_line_text": event.source_line_text,
                    "source_line_start_char": event.source_line_start_char,
                    "source_line_end_char": event.source_line_end_char,
                    "source_slot": event.source_slot,
                    "source_word_start": event.source_word_start,
                    "source_word_end": event.source_word_end,
                    "source_bbox_x0": event.source_bbox_x0,
                    "source_bbox_y0": event.source_bbox_y0,
                    "source_bbox_x1": event.source_bbox_x1,
                    "source_bbox_y1": event.source_bbox_y1,
                    "source_match_start_char": event.source_match_start_char,
                    "source_match_end_char": event.source_match_end_char,
                    "source_match_col_start": event.source_match_col_start,
                    "source_match_col_end": event.source_match_col_end,
                    "normalized_from": event.normalized_from,
                    "normalization_kind": event.normalization_kind,
                    "source_event_ref": event.source_event_ref,
                }
            )


def process_one_text_row(
    row: dict[str, Any],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_dir: str | None = None,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    pattern_examples: dict[str, list[str]] | None = None,
    pattern_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    source_context = _resolve_source_context(row, {})
    payload = _load_manifest_document(row, input_dir=input_dir)
    if payload is None:
        result = _base_process_result(source_context)
        result["error_code"] = "missing_document_extraction_doc"
        result["error"] = (
            "MISSING_DOCUMENT_EXTRACTION_DOC: canonical document payload is required "
            "and could not be loaded"
        )
        return result

    source_context = _resolve_source_context(row, payload)
    source_path = _resolve_source_path(
        row,
        payload,
        fallback=source_context["source_file_ref"] or source_context["source_doc_json"],
    )
    out_path = _build_events_output_path_from_source_ref(
        source_context["source_file_ref"] or Path(source_context["source_doc_json"]).name,
        output_base=Path(output_dir),
        out_name=out_name,
    )
    return _process_document_payload(
        payload,
        source_context=source_context,
        source_path=source_path,
        out_path=out_path,
        max_pattern_examples=max_pattern_examples,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
        pattern_examples=pattern_examples,
        pattern_counts=pattern_counts,
    )


def _base_process_result(
    source_context: dict[str, Any],
    *,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "status": "error",
        "source_doc_json": source_context["source_doc_json"],
        "source_file_ref": source_context.get("source_file_ref"),
        "source_file_id": source_context.get("source_file_id"),
        "source_file_name": source_context.get("source_file_name"),
        "output_events_csv": None,
        "doc_format": None,
        "parser_id": None,
        "year": None,
        "month": None,
        "rows_considered": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "coverage_ratio": None,
        "rows_without_events_examples": [],
        "error_code": None if error is None else "processing_error",
        "error": error,
        "header_preview": None,
    }


def _process_document_payload(
    document: dict[str, Any],
    *,
    source_context: dict[str, Any],
    source_path: Path,
    out_path: Path,
    max_pattern_examples: int,
    max_unmatched_examples_per_file: int,
    pattern_examples: dict[str, list[str]] | None,
    pattern_counts: dict[str, int] | None,
) -> dict[str, Any]:
    result = _base_process_result(source_context)
    text = document_text(document)

    try:
        parsed, stats = parse_document_payload(
            document,
            source_context=source_context,
            source_path=source_path,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        )
        _write_events_csv(parsed.events, out_path)

        result["status"] = "ok"
        result["output_events_csv"] = str(out_path)
        result["doc_format"] = parsed.doc_format
        result["parser_id"] = parsed.parser_id
        result["year"] = parsed.year
        result["month"] = parsed.month
        for key, value in stats.items():
            result[key] = value
        return result
    except Exception as exc:
        if str(exc) == "MISSING_YEAR_MONTH":
            result["error_code"] = "missing_year_month"
            result["error"] = (
                "MISSING_YEAR_MONTH: unable to resolve month/year from text and filename"
            )
            try:
                result["header_preview"] = _header_preview(text)
                result["doc_format"] = resolve_parser(document).legacy_doc_format
                result["parser_id"] = resolve_parser(document).parser_id
            except Exception:
                result["header_preview"] = None
            return result
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def process_many_text_rows(
    text_rows: Iterable[dict[str, Any]],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_dir: str | None = None,
    manifest_glob: str = DEFAULT_MANIFEST_GLOB,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    normalized_rows = list(text_rows)
    return _process_many_documents(
        normalized_rows,
        input_mode="employee_manifest_csv",
        output_dir=output_dir,
        out_name=out_name,
        input_dir=input_dir,
        manifest_glob=manifest_glob,
        process_document=lambda row, pattern_examples, pattern_counts: process_one_text_row(
            row,
            output_dir=output_dir,
            out_name=out_name,
            input_dir=input_dir,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        ),
    )


def _process_many_documents(
    documents: list[Any],
    *,
    input_mode: str,
    output_dir: str,
    out_name: str,
    input_dir: str | None,
    manifest_glob: str,
    process_document,
) -> dict[str, Any]:
    pattern_examples = {name: [] for name, _ in EVENT_PATTERNS}
    pattern_counts = {name: 0 for name, _ in EVENT_PATTERNS}
    totals: dict[str, Any] = {
        "files_total": len(documents),
        "files_processed": 0,
        "files_error": 0,
        "files_missing_year_month": 0,
        "files_with_events": 0,
        "files_without_events": 0,
        "rows_considered": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "coverage_ratio": None,
        "input_dir": os.path.abspath(input_dir) if input_dir else None,
        "input_mode": input_mode,
        "output_dir": os.path.abspath(output_dir),
        "manifest_glob": manifest_glob,
        "out_name": out_name,
    }
    format_counts: dict[str, int] = {}
    parser_counts: dict[str, int] = {}
    files_missing_year_month: list[dict[str, Any]] = []
    files_with_unmatched_rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    items: list[dict[str, Any]] = []

    for index, document in enumerate(documents, start=1):
        result = process_document(document, pattern_examples, pattern_counts)
        items.append(result)

        doc_format = result.get("doc_format")
        if isinstance(doc_format, str) and doc_format:
            format_counts[doc_format] = format_counts.get(doc_format, 0) + 1
        parser_id = result.get("parser_id")
        if isinstance(parser_id, str) and parser_id:
            parser_counts[parser_id] = parser_counts.get(parser_id, 0) + 1

        if result["status"] != "ok":
            totals["files_error"] += 1
            if result.get("error_code") == "missing_year_month":
                totals["files_missing_year_month"] += 1
                files_missing_year_month.append(
                    {
                        "source_doc_json": result["source_doc_json"],
                        "source_file_name": result.get("source_file_name"),
                        "doc_format": result.get("doc_format"),
                        "parser_id": result.get("parser_id"),
                        "header_preview": result.get("header_preview"),
                    }
                )
            errors.append(
                {
                    "source_doc_json": str(result["source_doc_json"]),
                    "source_file_name": result.get("source_file_name"),
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

        for key in (
            "rows_considered",
            "rows_with_events",
            "rows_without_events",
            "events_extracted",
            "rows_with_multi_events",
        ):
            totals[key] += int(result[key])

        if int(result["rows_without_events"]) > 0:
            files_with_unmatched_rows.append(
                {
                    "source_doc_json": str(result["source_doc_json"]),
                    "source_file_name": result.get("source_file_name"),
                    "rows_considered": int(result["rows_considered"]),
                    "rows_with_events": int(result["rows_with_events"]),
                    "rows_without_events": int(result["rows_without_events"]),
                    "events_extracted": int(result["events_extracted"]),
                    "coverage_ratio": result["coverage_ratio"],
                    "rows_without_events_examples": list(result["rows_without_events_examples"]),
                }
            )

        if index % 500 == 0:
            logger.info(
                "Processed %s/%s source documents (events=%s)",
                index,
                len(documents),
                totals["events_extracted"],
            )

    if totals["rows_considered"] > 0:
        totals["coverage_ratio"] = round(
            totals["rows_with_events"] / totals["rows_considered"],
            6,
        )

    files_with_unmatched_rows.sort(
        key=lambda item: (
            -int(item["rows_without_events"]),
            float(item["coverage_ratio"]) if item["coverage_ratio"] is not None else -1.0,
            item["source_doc_json"],
        )
    )

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "format_counts": format_counts,
        "parser_counts": parser_counts,
        "pattern_counts": pattern_counts,
        "pattern_examples": pattern_examples,
        "files_missing_year_month": files_missing_year_month,
        "files_with_unmatched_rows": files_with_unmatched_rows,
        "file_errors": errors,
    }


def _load_manifest_document(
    row: dict[str, Any],
    *,
    input_dir: str | None,
) -> dict[str, Any] | None:
    doc_json = str(row.get("doc_json") or "").strip()
    if not doc_json:
        return None
    if input_dir:
        payload = load_text_extraction_doc(input_dir, doc_json)
        if isinstance(payload, dict):
            return payload
    doc_path = Path(doc_json)
    if doc_path.is_absolute() and doc_path.exists():
        try:
            import json

            return json.loads(doc_path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _resolve_source_context(
    row: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    source = payload.get("source")
    source_map = source if isinstance(source, dict) else {}
    source_doc_json = str(row.get("doc_json") or "").strip() or "unknown-doc.json"
    source_file_id = _first_non_empty_string(row.get("file_id"), source_map.get("file_id"))
    source_file_name = _first_non_empty_string(
        source_map.get("file_name"),
        row.get("file_name"),
    )
    source_employee = _first_non_empty_string(
        row.get("employee"),
        source_map.get("employee"),
    )
    source_drive_path = _first_non_empty_string(
        source_map.get("drive_path"),
        row.get("drive_path"),
    )
    source_file_link = _first_non_empty_string(
        source_map.get("file_link"),
        row.get("file_link"),
    )
    return {
        "source_doc_json": source_doc_json,
        "source_file_id": source_file_id,
        "source_file_name": source_file_name,
        "source_employee": source_employee,
        "source_drive_path": source_drive_path,
        "source_file_link": source_file_link,
        "source_file_ref": _build_source_file_ref(
            employee=source_employee,
            file_name=source_file_name,
            file_id=source_file_id,
            doc_json=source_doc_json,
        ),
    }


def _resolve_source_path(
    row: dict[str, Any],
    payload: dict[str, Any],
    *,
    fallback: str,
) -> Path:
    source = payload.get("source")
    if isinstance(source, dict):
        file_name = source.get("file_name")
        if isinstance(file_name, str) and file_name.strip():
            return Path(file_name.strip())
    row_file_name = row.get("file_name")
    if isinstance(row_file_name, str) and row_file_name.strip():
        return Path(row_file_name.strip())
    return Path(fallback)


def _build_source_file_ref(
    *,
    employee: str | None,
    file_name: str | None,
    file_id: str | None,
    doc_json: str,
) -> str:
    base_name = file_name or file_id or Path(doc_json).name
    if employee:
        return str(Path(employee) / base_name)
    return base_name


def _first_non_empty_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str):
            clean = value.strip()
            if clean:
                return clean
    return None
