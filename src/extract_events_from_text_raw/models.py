from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class DocumentLine:
    line_no: int
    text: str
    start_char: int
    end_char: int
    page_no: int | None = None
    line_id: str | None = None
    words: tuple[dict[str, Any], ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ParsedEvent:
    line: DocumentLine
    day: int
    dow: str
    event_kind: str
    event_time_hhmm: str
    event_raw: str
    event_pattern: str
    match_start: int | None = None
    match_end: int | None = None
    source_origin: str = "text_regex"
    source_slot: str | None = None
    source_word_start: int | None = None
    source_word_end: int | None = None
    source_bbox_x0: float | None = None
    source_bbox_y0: float | None = None
    source_bbox_x1: float | None = None
    source_bbox_y1: float | None = None
    normalized_from: str | None = None
    normalization_kind: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedRow:
    day: int
    dow: str
    line: DocumentLine
    events: tuple[ParsedEvent, ...]


@dataclass(frozen=True, slots=True)
class EventRecord:
    year: int | None
    month: int | None
    day: int
    dow: str
    event_index: int
    event_kind: str
    event_time_hhmm: str
    event_ts: str | None
    event_raw: str
    event_pattern: str
    parser_id: str
    doc_format: str
    source_origin: str
    source_doc_json: str
    source_file_id: str | None
    source_file_name: str | None
    source_employee: str | None
    source_drive_path: str | None
    source_file_link: str | None
    source_page_no: int | None
    source_line_id: str | None
    source_line_no: int
    source_line_text: str
    source_line_start_char: int
    source_line_end_char: int
    source_slot: str | None
    source_word_start: int | None
    source_word_end: int | None
    source_bbox_x0: float | None
    source_bbox_y0: float | None
    source_bbox_x1: float | None
    source_bbox_y1: float | None
    source_match_start_char: int | None
    source_match_end_char: int | None
    source_match_col_start: int | None
    source_match_col_end: int | None
    normalized_from: str | None
    normalization_kind: str | None
    source_event_ref: str


@dataclass(frozen=True, slots=True)
class DocumentParseResult:
    source_doc_json: str
    doc_format: str
    parser_id: str
    year: int
    month: int
    rows: tuple[ParsedRow, ...]
    events: tuple[EventRecord, ...]
