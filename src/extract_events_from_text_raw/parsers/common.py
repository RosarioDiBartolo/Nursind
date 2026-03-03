from __future__ import annotations

import re
from typing import Any

from src.raw_text_parsing import DAY_PREFIX_RE, QTA_RE, extract_events, normalize_text

from ..models import DocumentLine, ParsedEvent, ParsedRow

NUMBER_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")
HHMM_RE = re.compile(r"^[+-]?\d{1,3}:\d{2}$")
DAY_PREFIX_OCR_RE = re.compile(r"^\s*\d{4}\s*[a-z\.]+")


def document_text(document: dict[str, Any]) -> str:
    payload = document.get("document")
    if not isinstance(payload, dict):
        return ""
    full_text = payload.get("full_text")
    if isinstance(full_text, str):
        return full_text
    return ""


def get_layout_pages(document: dict[str, Any]) -> list[dict[str, Any]]:
    payload = document.get("layout")
    if not isinstance(payload, dict):
        return []
    pages = payload.get("pages")
    if not isinstance(pages, list):
        return []
    return [page for page in pages if isinstance(page, dict)]


def iter_text_lines(document: dict[str, Any]) -> list[DocumentLine]:
    text = document_text(document)
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
    if text and not text.endswith(("\n", "\r")):
        return lines
    if text and (not lines or lines[-1].end_char != len(text)):
        lines.append(
            DocumentLine(
                line_no=len(lines) + 1,
                text="",
                start_char=offset,
                end_char=offset,
            )
        )
    return lines


def iter_layout_lines(document: dict[str, Any]) -> list[DocumentLine]:
    pages = get_layout_pages(document)
    if not pages:
        return []

    lines: list[DocumentLine] = []
    line_no = 1
    offset = 0

    for page in pages:
        raw_words = page.get("words")
        raw_lines = page.get("lines")
        if not isinstance(raw_words, list) or not isinstance(raw_lines, list):
            continue

        words_by_index: dict[int, dict[str, Any]] = {}
        for raw_word in raw_words:
            if not isinstance(raw_word, dict):
                continue
            try:
                word_index = int(raw_word.get("word_index"))
            except Exception:
                continue
            words_by_index[word_index] = raw_word

        page_no = None
        try:
            page_no = int(page.get("page_no"))
        except Exception:
            page_no = None

        for raw_line in raw_lines:
            if not isinstance(raw_line, dict):
                continue
            text = str(raw_line.get("text") or "")
            word_indices = raw_line.get("word_indices")
            resolved_words: list[dict[str, Any]] = []
            if isinstance(word_indices, list):
                for value in word_indices:
                    try:
                        index = int(value)
                    except Exception:
                        continue
                    word = words_by_index.get(index)
                    if word is not None:
                        resolved_words.append(word)
            lines.append(
                DocumentLine(
                    line_no=line_no,
                    text=text,
                    start_char=offset,
                    end_char=offset + len(text),
                    page_no=page_no,
                    line_id=str(raw_line.get("line_id") or "") or None,
                    words=tuple(resolved_words),
                )
            )
            line_no += 1
            offset += len(text) + 1

    return lines


def iter_preferred_lines(document: dict[str, Any]) -> list[DocumentLine]:
    lines = iter_layout_lines(document)
    if lines:
        return lines
    return iter_text_lines(document)


def _format_token_sign(token: str) -> str:
    clean = token.strip().strip("|,;!")
    if not clean:
        return ""
    if clean.endswith("-") and not clean.startswith("-"):
        clean = f"-{clean[:-1]}"
    if clean.endswith("+") and not clean.startswith("+"):
        clean = f"+{clean[:-1]}"
    return clean


def parse_hhmm(token: str) -> float | None:
    clean = _format_token_sign(token)
    sign = -1.0 if clean.startswith("-") else 1.0
    clean = clean.lstrip("+-")
    if not HHMM_RE.fullmatch(clean):
        return None
    hour_s, minute_s = clean.split(":")
    hour = int(hour_s)
    minute = int(minute_s)
    if not (0 <= minute <= 59):
        return None
    return sign * (hour + minute / 60.0)


def parse_decimal(token: str) -> float | None:
    clean = _format_token_sign(token)
    if not NUMBER_RE.fullmatch(clean):
        return None
    normalized = clean.replace(",", ".")
    value = float(normalized)
    if "." not in normalized:
        return value

    sign = -1.0 if value < 0 else 1.0
    abs_value = abs(value)
    hours = int(abs_value)
    minutes = int(round((abs_value - hours) * 100))
    if 0 <= minutes <= 59:
        return sign * (hours + minutes / 60.0)
    return value


def parse_numeric_token(
    token: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> float | None:
    parsed = parse_hhmm(token) if allow_hhmm else None
    if parsed is None:
        parsed = parse_decimal(token)
    if parsed is None:
        return None
    if max_abs is not None and abs(parsed) > max_abs:
        return None
    return parsed


def extract_leading_values(
    value_text: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> list[float]:
    values: list[float] = []
    for token in value_text.split():
        parsed = parse_numeric_token(token, allow_hhmm=allow_hhmm, max_abs=max_abs)
        if parsed is not None:
            values.append(parsed)
    return values


def extract_trailing_values(
    value_text: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> list[float]:
    tokens = value_text.split()
    out_rev: list[float] = []
    collecting = False
    for raw_token in reversed(tokens):
        parsed = parse_numeric_token(raw_token, allow_hhmm=allow_hhmm, max_abs=max_abs)
        if parsed is None:
            if collecting:
                break
            continue
        collecting = True
        out_rev.append(parsed)
    out_rev.reverse()
    return out_rev


def extract_all_values(
    value_text: str,
    *,
    allow_hhmm: bool,
    max_abs: float | None = None,
) -> list[float]:
    values: list[float] = []
    for token in value_text.split():
        parsed = parse_numeric_token(token, allow_hhmm=allow_hhmm, max_abs=max_abs)
        if parsed is not None:
            values.append(parsed)
    return values


def strip_day_prefix(value_text: str) -> str:
    trimmed = DAY_PREFIX_RE.sub("", value_text, count=1)
    if trimmed != value_text:
        return trimmed
    return DAY_PREFIX_OCR_RE.sub("", value_text, count=1)


def strip_day_prefix_and_qta(value_text: str) -> str:
    rest = strip_day_prefix(value_text)
    return QTA_RE.sub("", rest)


def split_bang_segments(value_text: str) -> list[str]:
    return [segment.strip() for segment in value_text.split("!") if segment.strip()]


def normalized_raw(raw: str) -> str:
    return normalize_text(raw)


def explicit_events_for_line(
    line: DocumentLine,
    *,
    day: int,
    dow: str,
) -> tuple[ParsedEvent, ...]:
    events: list[ParsedEvent] = []
    for event in extract_events(line.text):
        word_position = _event_word_position(
            line,
            match_start=event.start,
            match_end=event.end,
        )
        events.append(
            ParsedEvent(
                line=line,
                day=day,
                dow=dow,
                event_kind=event.kind,
                event_time_hhmm=event.time_str,
                event_raw=line.text[event.start : event.end],
                event_pattern=event.pattern,
                match_start=event.start,
                match_end=event.end,
                source_origin="text_regex",
                source_word_start=word_position.get("source_word_start"),
                source_word_end=word_position.get("source_word_end"),
                source_bbox_x0=word_position.get("source_bbox_x0"),
                source_bbox_y0=word_position.get("source_bbox_y0"),
                source_bbox_x1=word_position.get("source_bbox_x1"),
                source_bbox_y1=word_position.get("source_bbox_y1"),
            )
        )
    return tuple(events)


def to_row_result(
    day: int,
    dow: str,
    line: DocumentLine,
    events: list[ParsedEvent] | tuple[ParsedEvent, ...],
) -> ParsedRow:
    return ParsedRow(day=day, dow=dow, line=line, events=tuple(events))


def _event_word_position(
    line: DocumentLine,
    *,
    match_start: int | None,
    match_end: int | None,
) -> dict[str, int | float]:
    if match_start is None or match_end is None or not line.words:
        return {}

    overlaps: list[tuple[int, dict[str, Any]]] = []
    for word_index, word_start, word_end, word in _line_word_spans(line):
        if word_end <= match_start or word_start >= match_end:
            continue
        overlaps.append((word_index, word))

    if not overlaps:
        return {}

    result: dict[str, int | float] = {
        "source_word_start": overlaps[0][0],
        "source_word_end": overlaps[-1][0],
    }
    bbox = _union_bbox(word for _, word in overlaps)
    result.update(bbox)
    return result


def _line_word_spans(
    line: DocumentLine,
) -> list[tuple[int, int, int, dict[str, Any]]]:
    spans: list[tuple[int, int, int, dict[str, Any]]] = []
    cursor = 0
    text = line.text
    for index, word in enumerate(line.words):
        token = str(word.get("text") or "")
        if not token:
            continue
        start = text.find(token, cursor)
        if start < 0:
            start = text.find(token)
        if start < 0:
            continue
        end = start + len(token)
        spans.append((index, start, end, word))
        cursor = end
    return spans


def _union_bbox(words: Any) -> dict[str, float]:
    coords: dict[str, list[float]] = {
        "x0": [],
        "y0": [],
        "x1": [],
        "y1": [],
    }
    for word in words:
        for key in ("x0", "y0", "x1", "y1"):
            try:
                coords[key].append(float(word[key]))
            except Exception:
                pass
    if not all(coords.values()):
        return {}
    return {
        "source_bbox_x0": min(coords["x0"]),
        "source_bbox_y0": min(coords["y0"]),
        "source_bbox_x1": max(coords["x1"]),
        "source_bbox_y1": max(coords["y1"]),
    }
