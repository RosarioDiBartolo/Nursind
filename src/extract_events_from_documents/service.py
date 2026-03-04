from __future__ import annotations

import csv
import logging
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.text_extraction_csv import load_text_extraction_doc
from src.raw_text_parsing import (
    EVENT_PATTERNS,
    infer_year_month_from_filename,
    infer_year_month_from_header,
    infer_year_month_from_header_date,
    infer_year_month_from_text,
    normalize_text,
)

from .models import ParsedEvent, ParsedRow
from .options import (
    DEFAULT_MANIFEST_GLOB,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PAGES_NAME,
)
from .parsers import resolve_parser
from .parsers.common import document_text, get_layout_pages

logger = logging.getLogger(__name__)

LOW_COVERAGE_RATIO = 0.6

EVENT_COLUMNS = [
    "event_id",
    "event_ts",
    "event_kind",
    "event_time_hhmm",
    "event_raw",
    "parser_id",
    "source_origin",
    "source_doc_json",
    "source_file_id",
    "source_file_name",
    "source_employee",
    "source_page_no",
    "source_line_id",
    "source_line_no",
    "source_slot",
    "source_event_ref",
]

PAGE_COLUMNS = [
    "page_ref",
    "source_doc_json",
    "source_file_id",
    "source_file_name",
    "source_employee",
    "page_no",
    "page_kind",
    "decision",
    "decision_reason",
    "parser_id",
    "page_year",
    "page_month",
    "year_month_source",
    "relevant_for_coverage",
    "rows_considered",
    "rows_with_events",
    "rows_without_events",
    "events_extracted",
    "events_dropped_missing_year_month",
    "coverage_ratio_page",
    "header_preview",
    "parse_error",
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


def _normalize_page_no(value: object) -> int:
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except Exception:
        return 1
    return parsed if parsed > 0 else 1


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
    page_no: int,
    line_id: str | None,
    line_no: int,
    word_start: int | None,
    word_end: int | None,
    slot: str | None,
    col_start: int | None,
) -> str:
    anchor: list[str] = [f"p{page_no}"]
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
    seen: set[tuple[int, int, str, str]] = set()
    for row in rows:
        page_no = _normalize_page_no(row.line.page_no)
        key = (page_no, row.day, row.dow, row.line.text)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(
        key=lambda item: (_normalize_page_no(item.line.page_no), item.day, item.line.line_no)
    )
    return tuple(deduped)


def _write_rows_csv(
    *,
    rows: list[dict[str, Any]],
    out_csv: Path,
    columns: list[str],
) -> None:
    ensure_parent_dir(str(out_csv))
    with open(out_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def _resolve_page_texts(document: dict[str, Any]) -> dict[int, str]:
    page_texts: dict[int, str] = {}
    for page in get_layout_pages(document):
        page_no = _normalize_page_no(page.get("page_no"))
        raw_lines = page.get("lines")
        if not isinstance(raw_lines, list):
            continue
        lines: list[str] = []
        for raw_line in raw_lines:
            if not isinstance(raw_line, dict):
                continue
            line_text = str(raw_line.get("text") or "").strip("\r\n")
            if line_text:
                lines.append(line_text)
        if lines:
            page_texts[page_no] = "\n".join(lines)

    if not page_texts:
        full_text = document_text(document)
        if full_text:
            page_texts[1] = full_text
    return page_texts


def _resolve_page_numbers(
    *,
    document: dict[str, Any],
    page_texts: dict[int, str],
    rows_by_page: dict[int, list[ParsedRow]],
) -> list[int]:
    page_numbers: set[int] = set(page_texts.keys())

    document_payload = document.get("document")
    if isinstance(document_payload, dict):
        try:
            page_count = int(document_payload.get("page_count"))
        except Exception:
            page_count = 0
        if page_count > 0:
            page_numbers.update(range(1, page_count + 1))

    page_numbers.update(rows_by_page.keys())
    if not page_numbers:
        return [1]
    return sorted(page_numbers)


def _resolve_page_year_month(
    *,
    page_text: str,
    source_path: Path,
) -> tuple[int | None, int | None, str]:
    year, month = infer_year_month_from_text(page_text)
    if year is not None and month is not None:
        return year, month, "text"

    year, month = infer_year_month_from_header(page_text)
    if year is not None and month is not None:
        return year, month, "header"

    year, month = infer_year_month_from_header_date(page_text)
    if year is not None and month is not None:
        return year, month, "header_date"

    year, month = infer_year_month_from_filename(source_path)
    if year is not None and month is not None:
        return year, month, "filename"

    return None, None, "none"


def _classify_page_kind(*, page_text: str, rows_considered: int) -> str:
    if rows_considered > 0:
        return "events_table"

    norm = normalize_text(page_text)
    if not norm:
        return "unknown"
    if any(token in norm for token in ("totali", "riepilogo", "saldo", "indennita")):
        return "summary"
    if any(token in norm for token in ("matricola", "ente", "stabil", "qualifica")):
        return "cover"
    if any(token in norm for token in ("elaborazione", "pag.", "data e ora stampa")):
        return "metadata"
    return "unknown"


def _build_event_row(
    *,
    source_context: dict[str, Any],
    parser_id: str,
    page_no: int,
    page_year: int,
    page_month: int,
    event_index: int,
    event: ParsedEvent,
) -> dict[str, Any] | None:
    day_value = _build_day(year=page_year, month=page_month, day_value=event.day)
    if day_value is None:
        return None
    event_dt = _to_dt(day_value, event.event_time_hhmm)
    if event_dt is None:
        return None

    match_col_start: int | None = None
    if event.match_start is not None:
        match_col_start = event.match_start + 1

    normalized_slot = event.source_slot if event.source_slot else "unknown"
    line_no = int(event.line.line_no)
    event_id = f"{source_context['source_doc_json']}#p{page_no}:l{line_no}:i{event_index}"
    return {
        "event_id": event_id,
        "event_ts": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "event_kind": str(event.event_kind).strip().upper(),
        "event_time_hhmm": event.event_time_hhmm,
        "event_raw": event.event_raw,
        "parser_id": parser_id,
        "source_origin": event.source_origin,
        "source_doc_json": source_context["source_doc_json"],
        "source_file_id": source_context.get("source_file_id"),
        "source_file_name": source_context.get("source_file_name"),
        "source_employee": source_context.get("source_employee"),
        "source_page_no": page_no,
        "source_line_id": event.line.line_id,
        "source_line_no": line_no,
        "source_slot": normalized_slot,
        "source_event_ref": _event_ref(
            source_doc_json=source_context["source_doc_json"],
            page_no=page_no,
            line_id=event.line.line_id,
            line_no=line_no,
            word_start=event.source_word_start,
            word_end=event.source_word_end,
            slot=normalized_slot,
            col_start=match_col_start,
        ),
    }


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
        "output_pages_csv": None,
        "doc_format": None,
        "parser_id": None,
        "rows_considered": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "events_dropped_missing_year_month": 0,
        "pages_total": 0,
        "pages_relevant": 0,
        "pages_skipped_non_event": 0,
        "pages_error": 0,
        "coverage_ratio": None,
        "rows_without_events_examples": [],
        "error_code": None if error is None else "processing_error",
        "error": error,
        "header_preview": None,
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


def _first_non_empty_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str):
            clean = value.strip()
            if clean:
                return clean
    return None


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


def _normalize_source_doc_json(
    *,
    doc_json: str,
    input_dir: str | None,
) -> str:
    normalized = str(doc_json or "").strip()
    if not normalized:
        return "unknown-doc.json"

    cwd = Path.cwd().resolve()
    doc_path = Path(normalized)
    if doc_path.is_absolute():
        try:
            return doc_path.resolve().relative_to(cwd).as_posix()
        except Exception:
            try:
                return Path(os.path.relpath(doc_path.resolve(), cwd)).as_posix()
            except Exception:
                return doc_path.name or "unknown-doc.json"

    if input_dir:
        try:
            resolved = (Path(input_dir) / doc_path).resolve()
            try:
                return resolved.relative_to(cwd).as_posix()
            except Exception:
                return Path(os.path.relpath(resolved, cwd)).as_posix()
        except Exception:
            return doc_path.as_posix()

    return doc_path.as_posix()


def _resolve_source_context(
    row: dict[str, Any],
    payload: dict[str, Any],
    *,
    input_dir: str | None,
) -> dict[str, Any]:
    source = payload.get("source")
    source_map = source if isinstance(source, dict) else {}
    source_doc_json = _normalize_source_doc_json(
        doc_json=str(row.get("doc_json") or ""),
        input_dir=input_dir,
    )
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


def _parse_document_pages(
    *,
    document: dict[str, Any],
    source_context: dict[str, Any],
    source_path: Path,
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
    result = _base_process_result(source_context)
    event_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    pages_missing_year_month: list[dict[str, Any]] = []
    low_coverage_pages: list[dict[str, Any]] = []

    parser = resolve_parser(document)
    deduped_rows = _dedupe_rows(parser.parse_document(document))
    rows_by_page: dict[int, list[ParsedRow]] = defaultdict(list)
    for row in deduped_rows:
        rows_by_page[_normalize_page_no(row.line.page_no)].append(row)

    page_texts = _resolve_page_texts(document)
    page_numbers = _resolve_page_numbers(
        document=document,
        page_texts=page_texts,
        rows_by_page=rows_by_page,
    )

    rows_without_events_examples: list[str] = []
    rows_considered_total = 0
    rows_with_events_total = 0
    rows_without_events_total = 0
    events_extracted_total = 0
    events_dropped_missing_year_month_total = 0
    pages_relevant_total = 0
    pages_skipped_non_event_total = 0
    pages_error_total = 0

    for page_no in page_numbers:
        page_rows_input = rows_by_page.get(page_no, [])
        rows_considered = len(page_rows_input)
        page_text = page_texts.get(page_no, "")
        if not page_text and page_rows_input:
            page_text = "\n".join(row.line.text for row in page_rows_input if row.line.text)
        if not page_text and page_no == 1:
            page_text = document_text(document)

        page_year, page_month, year_month_source = _resolve_page_year_month(
            page_text=page_text,
            source_path=source_path,
        )
        page_kind = _classify_page_kind(page_text=page_text, rows_considered=rows_considered)

        rows_with_events = 0
        rows_without_events = 0
        events_extracted = 0
        candidate_events = 0
        events_dropped_missing_year_month = 0

        if rows_considered <= 0:
            decision = "skipped_non_event"
            decision_reason = "no_parsed_rows"
            relevant_for_coverage = False
        else:
            has_page_year_month = page_year is not None and page_month is not None
            for parsed_row in page_rows_input:
                row_events = list(parsed_row.events)
                candidate_events += len(row_events)

                if not row_events:
                    rows_without_events += 1
                    line_example = parsed_row.line.text.strip()
                    if (
                        line_example
                        and len(rows_without_events_examples) < max_unmatched_examples_per_file
                    ):
                        rows_without_events_examples.append(line_example)
                    continue

                emitted_for_row = 0
                if has_page_year_month:
                    for event_index, event in enumerate(row_events):
                        event_row = _build_event_row(
                            source_context=source_context,
                            parser_id=parser.parser_id,
                            page_no=page_no,
                            page_year=int(page_year),
                            page_month=int(page_month),
                            event_index=event_index,
                            event=event,
                        )
                        if event_row is None:
                            continue
                        emitted_for_row += 1
                        event_rows.append(event_row)
                        events_extracted += 1
                        pattern_name = str(event.event_pattern or "unknown")
                        pattern_counts[pattern_name] = pattern_counts.get(pattern_name, 0) + 1
                        examples = pattern_examples.setdefault(pattern_name, [])
                        line_example = parsed_row.line.text.strip()
                        if (
                            line_example
                            and line_example not in examples
                            and len(examples) < max_pattern_examples
                        ):
                            examples.append(line_example)

                if emitted_for_row > 0:
                    rows_with_events += 1
                else:
                    rows_without_events += 1

            if candidate_events > 0 and (page_year is None or page_month is None):
                decision = "error_missing_year_month"
                decision_reason = "missing_page_year_month"
                relevant_for_coverage = False
                pages_error_total += 1
                events_dropped_missing_year_month = candidate_events
                events_dropped_missing_year_month_total += events_dropped_missing_year_month
                pages_missing_year_month.append(
                    {
                        "source_doc_json": source_context["source_doc_json"],
                        "source_file_name": source_context.get("source_file_name"),
                        "source_file_id": source_context.get("source_file_id"),
                        "page_no": page_no,
                        "rows_considered": rows_considered,
                        "candidate_events": candidate_events,
                        "header_preview": _header_preview(page_text) if page_text else None,
                    }
                )
                if events_extracted > 0:
                    event_rows = [
                        row
                        for row in event_rows
                        if int(row.get("source_page_no") or -1) != page_no
                    ]
                    events_extracted = 0
                rows_with_events = 0
                rows_without_events = rows_considered
            else:
                decision = "parsed"
                decision_reason = "parsed"
                relevant_for_coverage = bool(page_kind == "events_table")
                if relevant_for_coverage:
                    pages_relevant_total += 1

        if decision == "skipped_non_event":
            pages_skipped_non_event_total += 1

        if relevant_for_coverage:
            rows_considered_total += rows_considered
            rows_with_events_total += rows_with_events
            rows_without_events_total += rows_without_events

        events_extracted_total += events_extracted
        coverage_ratio_page = None
        if relevant_for_coverage and rows_considered > 0:
            coverage_ratio_page = round(rows_with_events / rows_considered, 6)
            if coverage_ratio_page < LOW_COVERAGE_RATIO:
                low_coverage_pages.append(
                    {
                        "source_doc_json": source_context["source_doc_json"],
                        "source_file_name": source_context.get("source_file_name"),
                        "source_file_id": source_context.get("source_file_id"),
                        "page_no": page_no,
                        "coverage_ratio_page": coverage_ratio_page,
                        "rows_considered": rows_considered,
                        "rows_without_events": rows_without_events,
                        "events_extracted": events_extracted,
                    }
                )

        page_rows.append(
            {
                "page_ref": f"{source_context['source_doc_json']}#p{page_no}",
                "source_doc_json": source_context["source_doc_json"],
                "source_file_id": source_context.get("source_file_id"),
                "source_file_name": source_context.get("source_file_name"),
                "source_employee": source_context.get("source_employee"),
                "page_no": page_no,
                "page_kind": page_kind,
                "decision": decision,
                "decision_reason": decision_reason,
                "parser_id": parser.parser_id,
                "page_year": page_year,
                "page_month": page_month,
                "year_month_source": year_month_source,
                "relevant_for_coverage": relevant_for_coverage,
                "rows_considered": rows_considered,
                "rows_with_events": rows_with_events,
                "rows_without_events": rows_without_events,
                "events_extracted": events_extracted,
                "events_dropped_missing_year_month": events_dropped_missing_year_month,
                "coverage_ratio_page": coverage_ratio_page,
                "header_preview": _header_preview(page_text) if page_text else None,
                "parse_error": None,
            }
        )

    result["status"] = "ok"
    result["error"] = None
    result["error_code"] = None
    result["doc_format"] = parser.legacy_doc_format
    result["parser_id"] = parser.parser_id
    result["rows_considered"] = rows_considered_total
    result["rows_with_events"] = rows_with_events_total
    result["rows_without_events"] = rows_without_events_total
    result["events_extracted"] = events_extracted_total
    result["events_dropped_missing_year_month"] = events_dropped_missing_year_month_total
    result["pages_total"] = len(page_numbers)
    result["pages_relevant"] = pages_relevant_total
    result["pages_skipped_non_event"] = pages_skipped_non_event_total
    result["pages_error"] = pages_error_total
    result["rows_without_events_examples"] = rows_without_events_examples
    if rows_considered_total > 0:
        result["coverage_ratio"] = round(rows_with_events_total / rows_considered_total, 6)
    else:
        result["coverage_ratio"] = None
    result["header_preview"] = _header_preview(document_text(document))
    return result, event_rows, page_rows, pages_missing_year_month, low_coverage_pages


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
    source_context = _resolve_source_context(row, {}, input_dir=input_dir)
    payload = _load_manifest_document(row, input_dir=input_dir)
    if payload is None:
        result = _base_process_result(source_context)
        result["error_code"] = "missing_document_extraction_doc"
        result["error"] = (
            "MISSING_DOCUMENT_EXTRACTION_DOC: canonical document payload is required "
            "and could not be loaded"
        )
        return result, [], [], [], []

    source_context = _resolve_source_context(row, payload, input_dir=input_dir)
    source_path = _resolve_source_path(
        row,
        payload,
        fallback=source_context["source_file_ref"] or source_context["source_doc_json"],
    )
    try:
        return _parse_document_pages(
            document=payload,
            source_context=source_context,
            source_path=source_path,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        )
    except Exception as exc:
        result = _base_process_result(source_context)
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
    _write_rows_csv(rows=all_event_rows, out_csv=out_events_csv, columns=EVENT_COLUMNS)
    _write_rows_csv(rows=all_page_rows, out_csv=out_pages_csv, columns=PAGE_COLUMNS)
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
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    pages_name: str = DEFAULT_PAGES_NAME,
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
        pages_name=pages_name,
        input_dir=input_dir,
        manifest_glob=manifest_glob,
        max_pattern_examples=max_pattern_examples,
        max_unmatched_examples_per_file=max_unmatched_examples_per_file,
    )


def process_one_text_row(
    row: dict[str, Any],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
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
        source_context = _resolve_source_context(row, {}, input_dir=input_dir)
        result = _base_process_result(source_context, error="No rows processed")
        result["error_code"] = "processing_error"
        return result

    item = dict(report["items"][0])
    stats = report.get("stats") or {}
    item["output_events_csv"] = stats.get("output_events_csv")
    item["output_pages_csv"] = stats.get("output_pages_csv")
    return item
