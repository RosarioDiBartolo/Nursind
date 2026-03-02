from __future__ import annotations

from dataclasses import dataclass

from .parsers.base import BaseFormatParser, EventHint


@dataclass(frozen=True, slots=True)
class DocumentLine:
    line_no: int
    text: str
    start_char: int
    end_char: int


@dataclass(frozen=True, slots=True)
class DayRowRecord:
    day: int
    dow: str
    line: DocumentLine
    has_event: bool
    mo_f: float | None
    mo_t: float | None
    mo_lav: float | None
    parser_id: str
    event_hints: tuple[EventHint, ...]


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
    source_txt: str
    source_line_no: int
    source_line_text: str
    source_line_start_char: int
    source_line_end_char: int
    source_match_start_char: int | None
    source_match_end_char: int | None
    source_match_col_start: int | None
    source_match_col_end: int | None
    source_event_ref: str


@dataclass(frozen=True, slots=True)
class DocumentParseResult:
    source_txt: str
    doc_format: str
    year: int
    month: int
    parser: BaseFormatParser
    rows: tuple[DayRowRecord, ...]
    events: tuple[EventRecord, ...]
