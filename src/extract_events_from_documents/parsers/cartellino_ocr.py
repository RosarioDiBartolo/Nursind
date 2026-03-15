from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from src.raw_text_parsing import normalize_text, parse_day_header

from ..models import DocumentLine, ParsedEvent
from .base import BaseFormatParser
from .common import (
    document_text,
    explicit_events_for_line,
    iter_layout_lines,
    iter_text_lines,
    normalized_raw,
    split_bang_segments,
    strip_day_prefix,
    to_row_result,
)


class CartellinoOcrParser(BaseFormatParser):
    parser_id = "cartellino_ocr"
    legacy_doc_format = "cartellino_classic"
    priority = 20
    _SLOT_ORDER = (
        ("ent_1", "E"),
        ("usc_1", "U"),
        ("ent_2", "E"),
        ("usc_2", "U"),
    )

    def score_document(self, document: dict[str, Any]) -> int:
        text = document_text(document)
        norm = normalize_text(text)
        score = -100
        if "data t i m b r a t u r e o r e" in norm:
            score += 160
        if "r r i i l l e e v v a a z z i i o o n n e e" in norm:
            score += 160
        if "rr ii ll ee vv aa zz ii oo nn ee pp rr ee ss ee nn zz ee" in norm:
            score += 120
        elif "rr ii ll ee vv aa zz ii oo nn ee" in norm:
            score += 80
        if norm.count(" ! ") >= 8:
            score += 20
        return score

    def parse_document(self, document: dict[str, Any]):
        layout_rows = self._parse_layout_document(document)
        if layout_rows:
            return layout_rows
        return self._parse_text_document(document)

    def _parse_layout_document(self, document: dict[str, Any]):
        layout_lines = [line for line in iter_layout_lines(document) if line.text.strip()]
        if not layout_lines:
            return ()

        by_page: dict[int, list[DocumentLine]] = defaultdict(list)
        for line in layout_lines:
            by_page[int(line.page_no or 0)].append(line)

        rows = []
        resolved_any_columns = False
        for page_no in sorted(by_page):
            page_lines = by_page[page_no]
            bands = self._resolve_column_bands(page_lines)
            if bands:
                resolved_any_columns = True
            for line in page_lines:
                header = parse_day_header(line.text)
                if header is None:
                    continue
                day, dow = header
                events: list[ParsedEvent] = []
                if bands:
                    events.extend(self._events_from_layout_line(line=line, day=day, dow=dow, bands=bands))
                if not events:
                    events.extend(self._events_from_text_line(line=line, day=day, dow=dow))
                rows.append(to_row_result(day, dow, line, events))
        if not resolved_any_columns:
            return ()
        return tuple(rows)

    def _parse_text_document(self, document: dict[str, Any]):
        rows = []
        for line in iter_text_lines(document):
            if not line.text.strip():
                continue
            header = parse_day_header(line.text)
            if header is None:
                continue
            day, dow = header
            rows.append(
                to_row_result(
                    day,
                    dow,
                    line,
                    self._events_from_text_line(line=line, day=day, dow=dow),
                )
            )
        return tuple(rows)

    def _resolve_column_bands(self, page_lines: list[DocumentLine]) -> dict[str, tuple[float, float]] | None:
        header_centers: dict[str, float] = {}
        for line in page_lines:
            for word in line.words:
                token = self._normalized_header_token(str(word.get("text") or ""))
                slot = self._header_slot(token)
                if slot is None or slot in header_centers:
                    continue
                try:
                    x0 = float(word["x0"])
                    x1 = float(word["x1"])
                except Exception:
                    continue
                header_centers[slot] = (x0 + x1) / 2.0
            if len(header_centers) == len(self._SLOT_ORDER):
                break

        if len(header_centers) != len(self._SLOT_ORDER):
            return None

        centers = [header_centers[slot] for slot, _ in self._SLOT_ORDER]
        left = centers[0] - (centers[1] - centers[0]) / 2.0
        mid_1 = (centers[0] + centers[1]) / 2.0
        mid_2 = (centers[1] + centers[2]) / 2.0
        mid_3 = (centers[2] + centers[3]) / 2.0
        right = centers[3] + (centers[3] - centers[2]) / 2.0
        return {
            "ent_1": (left, mid_1),
            "usc_1": (mid_1, mid_2),
            "ent_2": (mid_2, mid_3),
            "usc_2": (mid_3, right),
        }

    def _header_slot(self, token: str) -> str | None:
        if token in {"ent1", "entr1"}:
            return "ent_1"
        if token in {"usc1", "usci"} or token.startswith("usc1"):
            return "usc_1"
        if token in {"ent2", "entr2"}:
            return "ent_2"
        if token in {"usc2"} or token.startswith("usc2"):
            return "usc_2"
        return None

    def _normalized_header_token(self, token: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", normalize_text(token))

    def _events_from_layout_line(
        self,
        *,
        line: DocumentLine,
        day: int,
        dow: str,
        bands: dict[str, tuple[float, float]],
    ) -> tuple[ParsedEvent, ...]:
        values_by_slot: dict[str, tuple[str, str, dict[str, Any]]] = {}
        for word in line.words:
            token = str(word.get("text") or "").strip()
            if not token:
                continue
            try:
                x_center = (float(word["x0"]) + float(word["x1"])) / 2.0
            except Exception:
                continue
            slot = self._slot_from_x(x_center, bands)
            if slot is None or slot in values_by_slot:
                continue
            time_hhmm = self._normalize_layout_time(token)
            if time_hhmm is None:
                continue
            values_by_slot[slot] = (time_hhmm, token, word)

        events: list[ParsedEvent] = []
        for slot, kind in self._SLOT_ORDER:
            parsed = values_by_slot.get(slot)
            if parsed is None:
                continue
            time_hhmm, raw_token, source_word = parsed
            events.append(
                ParsedEvent(
                    line=line,
                    day=day,
                    dow=dow,
                    event_kind=kind,
                    event_time_hhmm=time_hhmm,
                    event_raw=raw_token,
                    event_pattern=f"{self.parser_id}:pdf_column:{slot}",
                    source_origin="layout_slot",
                    source_slot=slot,
                    source_word_start=self._word_index_for(line, raw_token),
                    source_word_end=self._word_index_for(line, raw_token),
                    source_bbox_x0=self._float_or_none(source_word.get("x0")),
                    source_bbox_y0=self._float_or_none(source_word.get("y0")),
                    source_bbox_x1=self._float_or_none(source_word.get("x1")),
                    source_bbox_y1=self._float_or_none(source_word.get("y1")),
                    normalized_from=raw_token,
                    normalization_kind="layout_time_cleanup",
                )
            )
        return tuple(events)

    def _slot_from_x(
        self,
        x_center: float,
        bands: dict[str, tuple[float, float]],
    ) -> str | None:
        for slot, (left, right) in bands.items():
            if left <= x_center < right:
                return slot
        return None

    def _normalize_layout_time(self, token: str) -> str | None:
        clean = token.strip().upper().replace("_", "").replace(" ", "")
        if not clean:
            return None

        match = re.search(r"(?P<h>\d{1,2})[:.,](?P<m>\d{2})", clean)
        if match:
            hour = int(match.group("h"))
            minute = int(match.group("m"))
            if self._valid_time(hour, minute):
                return f"{hour:02d}:{minute:02d}"

        digits = re.sub(r"\D", "", clean)
        if len(digits) == 3:
            hour = int(digits[0])
            minute = int(digits[1:])
        elif len(digits) == 4:
            hour = int(digits[:2])
            minute = int(digits[2:])
        else:
            return None
        if not self._valid_time(hour, minute):
            return None
        return f"{hour:02d}:{minute:02d}"

    def _valid_time(self, hour: int, minute: int) -> bool:
        if minute < 0 or minute > 59:
            return False
        if 0 <= hour <= 23:
            return True
        return hour == 24 and minute == 0

    def _events_from_text_line(self, *, line: DocumentLine, day: int, dow: str) -> tuple[ParsedEvent, ...]:
        body = strip_day_prefix(normalized_raw(line.text))
        segments = split_bang_segments(body)
        primary = segments[0] if segments else body
        events = list(self._aligned_events_from_text(primary=primary, line=line, day=day, dow=dow))
        if not events:
            events.extend(explicit_events_for_line(line, day=day, dow=dow))
        return tuple(events)

    def _aligned_events_from_text(
        self,
        *,
        primary: str,
        line: DocumentLine,
        day: int,
        dow: str,
    ) -> tuple[ParsedEvent, ...]:
        times: list[str] = []
        for raw_token in primary.split():
            time_hhmm = self._normalize_layout_time(raw_token)
            if time_hhmm is None:
                continue
            times.append(time_hhmm)

        if not times:
            return ()

        events: list[ParsedEvent] = []
        for idx, hhmm in enumerate(times[:12]):
            kind = "E" if idx % 2 == 0 else "U"
            if len(times) == 1 and "rec" in primary:
                kind = "U"
            events.append(
                ParsedEvent(
                    line=line,
                    day=day,
                    dow=dow,
                    event_kind=kind,
                    event_time_hhmm=hhmm,
                    event_raw=f"{kind} {hhmm}",
                    event_pattern=f"{self.parser_id}:text_order:{idx+1}",
                    source_origin="text_order",
                )
            )
        return tuple(events)

    def _word_index_for(self, line: DocumentLine, raw_token: str) -> int | None:
        for index, word in enumerate(line.words):
            if str(word.get("text") or "").strip() == raw_token:
                return index
        return None

    def _float_or_none(self, value: object) -> float | None:
        try:
            return float(str(value))
        except Exception:
            return None
