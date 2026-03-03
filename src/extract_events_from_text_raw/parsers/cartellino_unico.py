from __future__ import annotations

import re

from src.raw_text_parsing import normalize_text, parse_day_header

from ..models import ParsedEvent
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
    _TIME_RE = re.compile(r"^(?P<h>[0-2]?\d)[:.,](?P<m>[0-5]\d)$")

    def score_document(self, document: dict[str, object]) -> int:
        text = document_text(document)
        norm = normalize_text(text)
        score = -100
        if "cartellino unico" in norm:
            score += 160
        if "giorno orario anomalie ent 1 usc 1" in norm:
            score += 80
        if "data e ora stampa" in norm:
            score += 20
        return score

    def _parse_time(self, token: str) -> tuple[str, int] | None:
        match = self._TIME_RE.match(token)
        if not match:
            return None
        hour = int(match.group("h"))
        minute = int(match.group("m"))
        if not ((0 <= hour <= 23) or (hour == 24 and minute == 0)):
            return None
        hhmm = f"{hour:02d}:{minute:02d}"
        minutes = hour * 60 + minute
        return hhmm, minutes

    def _aligned_events(self, *, day: int, dow: str, line, norm_body: str) -> tuple[ParsedEvent, ...]:
        tokens = norm_body.split()
        marker_positions: set[int] = set()
        all_times: list[tuple[int, str, int]] = []

        for idx, token in enumerate(tokens):
            parsed = self._parse_time(token)
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
                    event_pattern=f"{self.parser_id}:aligned_{idx+1}",
                    source_origin="text_alignment",
                )
            )
        return tuple(events)

    def parse_document(self, document: dict[str, object]):
        rows = []
        for line in iter_preferred_lines(document):
            if not line.text.strip():
                continue
            header = parse_day_header(line.text)
            if header is None:
                continue
            day, dow = header
            rest = strip_day_prefix(normalized_raw(line.text))
            events = list(self._aligned_events(day=day, dow=dow, line=line, norm_body=rest))
            if not events:
                events.extend(explicit_events_for_line(line, day=day, dow=dow))
            rows.append(to_row_result(day, dow, line, events))
        return tuple(rows)
