from __future__ import annotations

from datetime import date
import re

from core.parsing import ALT_DAY_HEADER_RE, DAY_HEADER_RE
from core.parsing import calendar_dow, infer_year_month_from_text, normalize_text, parse_day_header

from .base import BaseFormatParser
from .punch_columns import column_events, ocr_columns
from .common import (
    document_text,
    explicit_events_for_line,
    iter_preferred_lines,
    to_row_result,
)


class CartellinoClassicParser(BaseFormatParser):
    parser_id = "cartellino_classic"
    legacy_doc_format = "cartellino_classic"
    priority = 40

    def score_document(self, document: dict[str, object]) -> int:
        text = document_text(document)
        norm = normalize_text(text)
        score = -40
        if "riepilogo presenze/assenze" in norm:
            score += 120
        if "rilevazione del mese di" in norm:
            score += 80
        if "rilevazione presenze del mese" in norm and "ricon" in norm:
            score += 40
        if "totali fine mese" in norm:
            score += 40
        if "data t i m b r a t u r e o r e" in norm:
            score += 20
        if re.search(r"\b[eu]\d{3,4}(?:-[a-z])?\b", norm):
            score += 30
        short_dow = 0
        long_dow = 0
        for line in text.splitlines():
            norm_line = normalize_text(line)
            match = DAY_HEADER_RE.match(norm_line) or ALT_DAY_HEADER_RE.match(norm_line)
            if not match:
                continue
            token = match.group("dow")
            if token in {"lu", "ma", "me", "gi", "ve", "sa", "do"}:
                short_dow += 1
            else:
                long_dow += 1
        if short_dow > 0 or long_dow > 0:
            score += 20 if short_dow >= long_dow else 0
        return score

    def parse_document(self, document: dict[str, object]):
        rows = []
        lines = iter_preferred_lines(document)
        periods = {}
        page_columns = {}
        for line in lines:
            if re.search(r"\bricon\b", line.text, re.I):
                columns = ocr_columns(line, presenze=True)
                if len(columns) == 4:
                    page_columns[line.page_no] = columns
            if "rilevazione del mese di" in line.text.lower():
                period = infer_year_month_from_text(line.text)
                periods[line.page_no] = (
                    period if periods.get(line.page_no, period) == period else (None, None)
                )
        for line in lines:
            if not line.text.strip():
                continue
            header = parse_day_header(line.text)
            if header is None and line.page_no in periods:
                # Some scans clip the first letter of the two-letter weekday.
                # Accept a surviving suffix only when the printed period confirms it.
                match = re.match(r"^[\s.\u2018']*([uaeio])\*?\s*(\d{2})\b", line.text, re.I)
                year, month = periods[line.page_no]
                if match and year and month:
                    day = int(match[2])
                    try:
                        dow = calendar_dow(date(year, month, day))
                    except ValueError:
                        continue
                    if dow and dow.endswith(match[1].upper()):
                        header = day, dow
            if header is None:
                continue
            day, dow = header
            events = explicit_events_for_line(line, day=day, dow=dow)
            if not events and line.page_no in page_columns:
                events = column_events(
                    parser_id=self.parser_id, day=day, dow=dow, line=line,
                    columns=page_columns[line.page_no], centered=True, roll_overnight=False,
                )
            rows.append(
                to_row_result(
                    day,
                    dow,
                    line,
                    events,
                )
            )
        return tuple(rows)
