from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.raw_text_parsing import (
    infer_year_month_from_filename,
    infer_year_month_from_header,
    infer_year_month_from_header_date,
    infer_year_month_from_text,
    normalize_text,
)

from .models import ParsedEvent, ParsedRow
from .parsers import resolve_parser
from .parsers.common import document_text, get_layout_pages

LOW_COVERAGE_RATIO = 0.6


def header_preview(text: str, *, max_lines: int = 3, max_chars: int = 240) -> str:
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


def normalize_page_no(value: object) -> int:
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


def dedupe_rows(rows: tuple[ParsedRow, ...]) -> tuple[ParsedRow, ...]:
    deduped: list[ParsedRow] = []
    seen: set[tuple[int, int, str, str]] = set()
    for row in rows:
        page_no = normalize_page_no(row.line.page_no)
        key = (page_no, row.day, row.dow, row.line.text)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(
        key=lambda item: (normalize_page_no(item.line.page_no), item.day, item.line.line_no)
    )
    return tuple(deduped)


def resolve_page_texts(document: dict[str, Any]) -> dict[int, str]:
    page_texts: dict[int, str] = {}
    for page in get_layout_pages(document):
        page_no = normalize_page_no(page.get("page_no"))
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


def resolve_page_numbers(
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


def resolve_page_year_month(
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


def classify_page_kind(*, page_text: str, rows_considered: int) -> str:
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


def build_event_row(
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


def base_process_result(
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


def parse_document_pages(
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
    result = base_process_result(source_context)
    event_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    pages_missing_year_month: list[dict[str, Any]] = []
    low_coverage_pages: list[dict[str, Any]] = []

    parser = resolve_parser(document)
    deduped_rows = dedupe_rows(parser.parse_document(document))
    rows_by_page: dict[int, list[ParsedRow]] = defaultdict(list)
    for row in deduped_rows:
        rows_by_page[normalize_page_no(row.line.page_no)].append(row)

    page_texts = resolve_page_texts(document)
    page_numbers = resolve_page_numbers(
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

        page_year, page_month, year_month_source = resolve_page_year_month(
            page_text=page_text,
            source_path=source_path,
        )
        page_kind = classify_page_kind(page_text=page_text, rows_considered=rows_considered)

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
                        event_row = build_event_row(
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
                        "header_preview": header_preview(page_text) if page_text else None,
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
                "header_preview": header_preview(page_text) if page_text else None,
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
    result["header_preview"] = header_preview(document_text(document))
    return result, event_rows, page_rows, pages_missing_year_month, low_coverage_pages


__all__ = [
    "base_process_result",
    "parse_document_pages",
]
