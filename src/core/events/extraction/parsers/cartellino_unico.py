from __future__ import annotations

import re
from collections import defaultdict

from core.parsing import normalize_text, parse_day_header

from ..models import DocumentLine, ParsedEvent
from .punch_columns import column_events, ocr_columns, parse_time
from .base import BaseFormatParser
from .common import (
    document_text,
    explicit_events_for_line,
    iter_preferred_lines,
    normalized_raw,
    strip_day_prefix,
    to_row_result,
)


class CartellinoUnicoParser(BaseFormatParser):
    parser_id = "cartellino_unico"
    legacy_doc_format = "cartellino_classic"
    priority = 10
    _EXPLICIT_PUNCH_HEADER = re.compile(r"\bgiorno\s+timbrature\s+mensa\b")
    _SIX_PAIR_HEADER = re.compile(
        r"\bgiorno\s+orario\s+anomalie\s+"
        + r"\s+".join(f"ent {index} usc {index}" for index in range(1, 7))
        + r"\b"
    )

    def score_document(self, document: dict[str, object]) -> int:
        text = document_text(document)
        norm = normalize_text(text)
        score = -100
        if "stampa cartellino esteso" in norm and "giorno timbrature tot." in norm:
            score += 200
        if "cartellino unico" in norm:
            score += 160
        if "giorno orario anomalie ent 1 usc 1" in norm:
            score += 80
        if "data e ora stampa" in norm:
            score += 20
        if re.search(r"\bent\W*1\(a\).*?usc\W*1\(b\).*?ent\W*2\(c\).*?usc\W*2\(d\)", norm):
            score += 240
        if re.search(r"giorno\s+orario\s+anomalie\W+ent\W*[i1]?\W+_?usc", norm):
            score += 160
        return score

    def _aligned_events(
        self, *, day: int, dow: str, line, norm_body: str
    ) -> tuple[ParsedEvent, ...]:
        tokens = norm_body.split()
        marker_positions: set[int] = set()
        all_times: list[tuple[int, str, int]] = []

        for idx, token in enumerate(tokens):
            parsed = parse_time(token)
            if parsed is None:
                continue
            hhmm, minutes = parsed
            all_times.append((idx, hhmm, minutes))
            if idx + 1 < len(tokens):
                nxt = tokens[idx + 1].strip().lower()
                if nxt.startswith(("o", "a", "t")):
                    marker_positions.add(idx)

        if not all_times:
            return ()

        selected: list[tuple[str, int]]

        if marker_positions:
            selected_positions = sorted(marker_positions)
            first_marked = selected_positions[0]
            previous_candidates = [item for item in all_times if item[0] < first_marked]
            if previous_candidates and (first_marked - previous_candidates[-1][0] <= 2):
                selected_positions.insert(0, previous_candidates[-1][0])
            selected = [
                (hhmm, minutes)
                for pos, hhmm, minutes in all_times
                if pos in set(selected_positions)
            ]
        else:
            if len(all_times) < 2:
                return ()
            first = all_times[0]
            second = all_times[1]
            delta = second[2] - first[2]
            if delta < 0:
                delta += 24 * 60
            if delta < 90:
                return ()
            selected = [(first[1], first[2]), (second[1], second[2])]

        events: list[ParsedEvent] = []
        for idx, (hhmm, _) in enumerate(selected[:12]):
            kind = "E" if idx % 2 == 0 else "U"
            events.append(
                ParsedEvent(
                    line=line,
                    day=day,
                    dow=dow,
                    event_kind=kind,
                    event_time_hhmm=hhmm,
                    event_raw=f"{kind} {hhmm}",
                    event_pattern=f"{self.parser_id}:aligned_{idx + 1}",
                    source_origin="text_alignment",
                )
            )
        return tuple(events)

    def _extended_events(self, line, day, dow, total_x):
        # The punch block lies between the weekday and the separate total column.
        punches = []
        for index, word in enumerate(line.words[2:], start=2):
            if float(word["x1"]) >= total_x:
                break
            raw = word["text"]
            parsed = parse_time(raw.rstrip("*"))
            if parsed is None:
                return ()
            punches.append((index, raw, parsed))
        if len(punches) % 2:
            return ()
        events = []
        for slot, (index, raw, (hhmm, minutes)) in enumerate(punches):
            if slot % 2 and minutes < punches[slot - 1][2][1]:
                return ()
            events.append(
                ParsedEvent(
                    line=line,
                    day=day,
                    dow=dow,
                    event_kind="U" if slot % 2 else "E",
                    event_time_hhmm=hhmm,
                    event_raw=raw,
                    event_pattern="extended_punch",
                    source_origin="layout_column",
                    source_word_start=index,
                    source_word_end=index,
                )
            )
        return tuple(events)

    def parse_document(self, document: dict[str, object]):
        lines = iter_preferred_lines(document)
        explicit_pages = {
            line.page_no
            for line in lines
            if self._EXPLICIT_PUNCH_HEADER.search(normalize_text(line.text))
        }
        marked_days = defaultdict(set)
        for line in lines:
            header = parse_day_header(line.text)
            if header and re.search(r"\b[EU]\d{1,2}:\d{2}", line.text, re.I):
                marked_days[line.page_no].add(header[0])
        explicit_pages.update(page for page, days in marked_days.items() if len(days) >= 2)
        extended_totals = {}
        extended_pages = {
            line.page_no for line in lines if "stampa cartellino esteso" in line.text.lower()
        }
        for line in lines:
            if line.page_no in extended_pages and line.text.lower().startswith(
                "giorno timbrature tot."
            ):
                for word in line.words:
                    if word["text"].lower() == "tot.":
                        extended_totals[line.page_no] = float(word["x0"])
                        break
        page_columns = {}
        centered_pages = set()
        numbered_pages = set()
        for line in lines:
            if re.search(r"\bent\W*2\b.*?usc\W*2\b", line.text, re.I):
                numbered_pages.add(line.page_no)
            if not self._SIX_PAIR_HEADER.search(normalize_text(line.text)):
                if re.search(r"(?:ent\W*[i1]|usc\W*[i1])", line.text, re.I):
                    positions = ocr_columns(line)
                    if positions:
                        page_columns[line.page_no] = positions
                        centered_pages.add(line.page_no)
                continue
            columns = tuple(
                float(word["x0"])
                for word in line.words
                if str(word["text"]).lower() in {"ent", "usc"}
            )
            if len(columns) == 12 and all(
                left < right for left, right in zip(columns, columns[1:])
            ):
                page_columns[line.page_no] = columns
                centered_pages.discard(line.page_no)
        rows = []
        for line in lines:
            if not line.text.strip():
                continue
            header = parse_day_header(line.text)
            if header is None:
                continue
            day, dow = header
            if line.page_no in extended_pages:
                events = (
                    self._extended_events(line, day, dow, extended_totals[line.page_no])
                    if line.page_no in extended_totals
                    else ()
                )
            elif line.page_no in page_columns:
                events = column_events(
                    parser_id=self.parser_id,
                    day=day, dow=dow, line=line, columns=page_columns[line.page_no],
                    centered=line.page_no in centered_pages,
                )
            elif line.page_no in explicit_pages or line.page_no in numbered_pages:
                events = explicit_events_for_line(line, day=day, dow=dow)
            else:
                events = explicit_events_for_line(line, day=day, dow=dow)
                if not events:
                    rest = strip_day_prefix(normalized_raw(line.text))
                    events = self._aligned_events(day=day, dow=dow, line=line, norm_body=rest)
            rows.append(to_row_result(day, dow, line, events))
        return tuple(rows)
