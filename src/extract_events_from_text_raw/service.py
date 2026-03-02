from __future__ import annotations

import csv
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from src.drive_service.fs_utils import ensure_parent_dir
from src.raw_text_parsing import EVENT_PATTERNS, extract_events, line_has_event, parse_day_header, resolve_year_month

from .models import DayRowRecord, DocumentLine, DocumentParseResult, EventRecord
from .options import (
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_TEXT_GLOB,
)
from .parsers import ParseContext, resolve_parser
from .parsers.common import normalized_raw

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
    "source_txt",
    "source_line_no",
    "source_line_text",
    "source_line_start_char",
    "source_line_end_char",
    "source_match_start_char",
    "source_match_end_char",
    "source_match_col_start",
    "source_match_col_end",
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


def _iter_indexed_lines(text: str) -> list[DocumentLine]:
    lines: list[DocumentLine] = []
    offset = 0
    for line_no, segment in enumerate(text.splitlines(keepends=True), start=1):
        content = segment.rstrip("\r\n")
        lines.append(
            DocumentLine(
                line_no=line_no,
                text=content,
                start_char=offset,
                end_char=offset + len(content),
            )
        )
        offset += len(segment)
    if not text:
        return []
    if text and not text.endswith(("\n", "\r")):
        return lines
    if text.endswith(("\n", "\r")) and (not lines or lines[-1].end_char != len(text)):
        lines.append(
            DocumentLine(
                line_no=len(lines) + 1,
                text="",
                start_char=offset,
                end_char=offset,
            )
        )
    return lines


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


def _event_ref(path: str, line_no: int, col_start: int | None) -> str:
    if col_start is None:
        return f"{path}:{line_no}"
    return f"{path}:{line_no}:{col_start}"


def _parse_rows(
    text: str,
    *,
    parser=None,
) -> tuple[list[DayRowRecord], str, int, int, Any]:
    parser = parser or resolve_parser(text)
    indexed_lines = _iter_indexed_lines(text)
    candidates: list[tuple[DocumentLine, int, str, bool]] = []
    for line in indexed_lines:
        if not line.text.strip():
            continue
        header = parse_day_header(line.text)
        if header is None:
            continue
        day, dow = header
        has_event = line_has_event(line.text)
        candidates.append((line, day, dow, has_event))

    any_event = any(has_event for _, _, _, has_event in candidates)
    rows: list[DayRowRecord] = []
    for line, day, dow, has_event in candidates:
        values = parser.parse_row(
            line.text,
            has_event=has_event,
            any_event=any_event,
            ctx=ParseContext(normalized_raw=normalized_raw(line.text)),
        )
        rows.append(
            DayRowRecord(
                day=day,
                dow=dow,
                line=line,
                has_event=has_event,
                mo_f=values.values.mo_f,
                mo_t=values.values.mo_t,
                mo_lav=values.values.mo_lav,
                parser_id=parser.parser_id,
                event_hints=values.event_hints,
            )
        )

    deduped: list[DayRowRecord] = []
    seen: set[tuple[int, str, str]] = set()
    for row in rows:
        key = (row.day, row.dow, row.line.text)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(key=lambda item: (item.day, item.line.line_no))
    return (
        deduped,
        parser.legacy_doc_format,
        sum(1 for _, _, _, has_event in candidates if has_event),
        len(candidates),
        parser,
    )


def _parse_rows_for_file(text: str, *, parser=None) -> tuple[list[DayRowRecord], dict[str, Any]]:
    rows, _, rows_with_events, rows_considered, _ = _parse_rows(text, parser=parser)
    stats = {
        "candidate_day_lines": rows_considered,
        "rows_parsed": len(rows),
        "rows_with_event": rows_with_events,
        "rows_without_event": max(0, len(rows) - rows_with_events),
    }
    return rows, stats


def _build_event_record(
    *,
    source_txt: str,
    year: int,
    month: int,
    row: DayRowRecord,
    event_index: int,
    event_kind: str,
    event_time_hhmm: str,
    event_raw: str,
    event_pattern: str,
    match_start: int | None,
    match_end: int | None,
) -> EventRecord:
    day_value = _build_day(year=year, month=month, day_value=row.day)
    event_ts: str | None = None
    if day_value is not None:
        event_dt = _to_dt(day_value, event_time_hhmm)
        if event_dt is not None:
            event_ts = event_dt.strftime("%Y-%m-%d %H:%M:%S")

    match_start_char: int | None = None
    match_end_char: int | None = None
    match_col_start: int | None = None
    match_col_end: int | None = None
    if match_start is not None and match_end is not None:
        match_start_char = row.line.start_char + match_start
        match_end_char = row.line.start_char + match_end
        match_col_start = match_start + 1
        match_col_end = match_end + 1

    return EventRecord(
        year=year,
        month=month,
        day=row.day,
        dow=row.dow,
        event_index=event_index,
        event_kind=event_kind,
        event_time_hhmm=event_time_hhmm,
        event_ts=event_ts,
        event_raw=event_raw,
        event_pattern=event_pattern,
        source_txt=source_txt,
        source_line_no=row.line.line_no,
        source_line_text=row.line.text,
        source_line_start_char=row.line.start_char,
        source_line_end_char=row.line.end_char,
        source_match_start_char=match_start_char,
        source_match_end_char=match_end_char,
        source_match_col_start=match_col_start,
        source_match_col_end=match_col_end,
        source_event_ref=_event_ref(source_txt, row.line.line_no, match_col_start),
    )


def parse_document_text(
    text: str,
    *,
    source_path: Path,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    pattern_examples: dict[str, list[str]] | None = None,
    pattern_counts: dict[str, int] | None = None,
) -> tuple[DocumentParseResult, dict[str, Any]]:
    rows, doc_format, _, rows_considered, parser = _parse_rows(text)
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
        "rows_considered": rows_considered,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "rows_with_hint_events": 0,
        "rows_fallback_regex": 0,
        "events_from_hints": 0,
        "events_from_regex": 0,
        "rows_with_invalid_hints": 0,
        "coverage_ratio": None,
        "rows_without_events_examples": [],
    }

    for row in rows:
        row_events: list[EventRecord] = []
        if row.event_hints:
            valid_hints = 0
            row_has_invalid_hint = False
            for hint_index, hint in enumerate(row.event_hints):
                kind = str(hint.kind or "").strip().upper()
                time_hhmm = str(hint.time_hhmm or "").strip()
                source = str(hint.source or "").strip() or "hint"
                parsed_hint_time = _parse_hhmm(time_hhmm)
                if kind not in {"E", "U"} or parsed_hint_time is None:
                    row_has_invalid_hint = True
                    continue
                if parsed_hint_time == (1, 0, 0):
                    normalized_time = "24:00"
                else:
                    _, hint_hour, hint_minute = parsed_hint_time
                    normalized_time = f"{hint_hour:02d}:{hint_minute:02d}"
                event_pattern = f"hint:{row.parser_id}:{source}"
                row_events.append(
                    _build_event_record(
                        source_txt=str(source_path),
                        year=year,
                        month=month,
                        row=row,
                        event_index=hint_index,
                        event_kind=kind,
                        event_time_hhmm=normalized_time,
                        event_raw=f"{kind} {normalized_time}",
                        event_pattern=event_pattern,
                        match_start=None,
                        match_end=None,
                    )
                )
                local_pattern_counts[event_pattern] = local_pattern_counts.get(event_pattern, 0) + 1
                examples = local_pattern_examples.setdefault(event_pattern, [])
                line_example = row.line.text.strip()
                if (
                    line_example
                    and line_example not in examples
                    and len(examples) < max_pattern_examples
                ):
                    examples.append(line_example)
                valid_hints += 1
            if row_has_invalid_hint:
                stats["rows_with_invalid_hints"] += 1
            if valid_hints:
                stats["rows_with_hint_events"] += 1
                stats["events_from_hints"] += valid_hints
        if not row_events:
            stats["rows_fallback_regex"] += 1
            regex_events = extract_events(row.line.text)
            if not regex_events:
                stats["rows_without_events"] += 1
                line_example = row.line.text.strip()
                if (
                    line_example
                    and len(stats["rows_without_events_examples"]) < max_unmatched_examples_per_file
                ):
                    stats["rows_without_events_examples"].append(line_example)
                continue
            for event_index, event in enumerate(regex_events):
                row_events.append(
                    _build_event_record(
                        source_txt=str(source_path),
                        year=year,
                        month=month,
                        row=row,
                        event_index=event_index,
                        event_kind=event.kind,
                        event_time_hhmm=event.time_str,
                        event_raw=row.line.text[event.start : event.end],
                        event_pattern=event.pattern,
                        match_start=event.start,
                        match_end=event.end,
                    )
                )
                local_pattern_counts[event.pattern] = local_pattern_counts.get(event.pattern, 0) + 1
                examples = local_pattern_examples.setdefault(event.pattern, [])
                line_example = row.line.text.strip()
                if (
                    line_example
                    and line_example not in examples
                    and len(examples) < max_pattern_examples
                ):
                    examples.append(line_example)
            stats["events_from_regex"] += len(row_events)

        stats["rows_with_events"] += 1
        stats["events_extracted"] += len(row_events)
        if len(row_events) > 2:
            stats["rows_with_multi_events"] += 1
        events.extend(row_events)

    if rows_considered > 0:
        stats["coverage_ratio"] = round(stats["rows_with_events"] / rows_considered, 6)

    result = DocumentParseResult(
        source_txt=str(source_path),
        doc_format=doc_format,
        year=year,
        month=month,
        parser=parser,
        rows=tuple(rows),
        events=tuple(events),
    )
    return result, stats


def _build_events_output_path(
    txt_path: Path,
    *,
    input_base: Path | None,
    output_base: Path,
    out_name: str,
) -> Path:
    suffix = out_name.strip() or DEFAULT_OUT_NAME
    if input_base is None:
        rel = Path(txt_path.name)
    else:
        try:
            rel = txt_path.relative_to(input_base)
        except ValueError:
            rel = Path(txt_path.name)
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
                    "source_txt": event.source_txt,
                    "source_line_no": event.source_line_no,
                    "source_line_text": event.source_line_text,
                    "source_line_start_char": event.source_line_start_char,
                    "source_line_end_char": event.source_line_end_char,
                    "source_match_start_char": event.source_match_start_char,
                    "source_match_end_char": event.source_match_end_char,
                    "source_match_col_start": event.source_match_col_start,
                    "source_match_col_end": event.source_match_col_end,
                    "source_event_ref": event.source_event_ref,
                }
            )


def process_one_text_file(
    txt_path: str | Path,
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    pattern_examples: dict[str, list[str]] | None = None,
    pattern_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    source_path = Path(txt_path)
    output_base = Path(output_dir)
    input_base_path = Path(input_base) if input_base is not None else None
    result: dict[str, Any] = {
        "status": "error",
        "source_txt": str(source_path),
        "output_events_csv": None,
        "doc_format": None,
        "year": None,
        "month": None,
        "rows_considered": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "rows_with_hint_events": 0,
        "rows_fallback_regex": 0,
        "events_from_hints": 0,
        "events_from_regex": 0,
        "rows_with_invalid_hints": 0,
        "coverage_ratio": None,
        "rows_without_events_examples": [],
        "error_code": None,
        "error": None,
        "header_preview": None,
    }

    try:
        text = source_path.read_text(encoding="utf-8", errors="replace")
        parsed, stats = parse_document_text(
            text,
            source_path=source_path,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        )
        out_path = _build_events_output_path(
            source_path,
            input_base=input_base_path,
            output_base=output_base,
            out_name=out_name,
        )
        _write_events_csv(parsed.events, out_path)

        result["status"] = "ok"
        result["output_events_csv"] = str(out_path)
        result["doc_format"] = parsed.doc_format
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
                text = source_path.read_text(encoding="utf-8", errors="replace")
                result["header_preview"] = _header_preview(text)
                result["doc_format"] = resolve_parser(text).legacy_doc_format
            except Exception:
                result["header_preview"] = None
            return result
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def process_many_text_files(
    text_files: Iterable[str | Path],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    text_glob: str = DEFAULT_TEXT_GLOB,
    input_dir: str | None = None,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    normalized_input_base = Path(input_base) if input_base is not None else None
    normalized_files = sorted(Path(path) for path in text_files)

    resolved_input_dir = input_dir
    if resolved_input_dir is None and normalized_input_base is not None:
        resolved_input_dir = str(normalized_input_base)

    pattern_examples = {name: [] for name, _ in EVENT_PATTERNS}
    pattern_counts = {name: 0 for name, _ in EVENT_PATTERNS}
    totals: dict[str, Any] = {
        "files_total": len(normalized_files),
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
        "rows_with_hint_events": 0,
        "rows_fallback_regex": 0,
        "events_from_hints": 0,
        "events_from_regex": 0,
        "rows_with_invalid_hints": 0,
        "coverage_ratio": None,
        "input_dir": os.path.abspath(resolved_input_dir) if resolved_input_dir else None,
        "output_dir": os.path.abspath(output_dir),
        "text_glob": text_glob,
        "out_name": out_name,
    }
    format_counts: dict[str, int] = {}
    files_missing_year_month: list[dict[str, Any]] = []
    files_with_unmatched_rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    items: list[dict[str, Any]] = []

    for index, txt_path in enumerate(normalized_files, start=1):
        result = process_one_text_file(
            txt_path,
            output_dir=output_dir,
            out_name=out_name,
            input_base=normalized_input_base,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        )
        items.append(result)

        doc_format = result.get("doc_format")
        if isinstance(doc_format, str) and doc_format:
            format_counts[doc_format] = format_counts.get(doc_format, 0) + 1

        if result["status"] != "ok":
            totals["files_error"] += 1
            if result.get("error_code") == "missing_year_month":
                totals["files_missing_year_month"] += 1
                files_missing_year_month.append(
                    {
                        "source_txt": result["source_txt"],
                        "doc_format": result.get("doc_format"),
                        "header_preview": result.get("header_preview"),
                    }
                )
            errors.append(
                {
                    "source_txt": str(result["source_txt"]),
                    "error": str(result["error"]),
                }
            )
            logger.error("Error processing %s: %s", result["source_txt"], result["error"])
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
            "rows_with_hint_events",
            "rows_fallback_regex",
            "events_from_hints",
            "events_from_regex",
            "rows_with_invalid_hints",
        ):
            totals[key] += int(result[key])

        if int(result["rows_without_events"]) > 0:
            files_with_unmatched_rows.append(
                {
                    "source_txt": str(result["source_txt"]),
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
                "Processed %s/%s text files (events=%s)",
                index,
                len(normalized_files),
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
            item["source_txt"],
        )
    )

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "format_counts": format_counts,
        "pattern_counts": pattern_counts,
        "pattern_examples": pattern_examples,
        "files_missing_year_month": files_missing_year_month,
        "files_with_unmatched_rows": files_with_unmatched_rows,
        "file_errors": errors,
    }
