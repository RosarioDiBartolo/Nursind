from __future__ import annotations

from cartellino_parser.raw_text_parsing import DAY_HEADER_RE
from cartellino_parser.raw_text_parsing import normalize_text, parse_day_header

from .base import BaseFormatParser
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
        if "totali fine mese" in norm:
            score += 40
        if "data t i m b r a t u r e o r e" in norm:
            score += 20
        short_dow = 0
        long_dow = 0
        for line in text.splitlines():
            norm_line = normalize_text(line)
            match = DAY_HEADER_RE.match(norm_line)
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
        for line in iter_preferred_lines(document):
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
                    explicit_events_for_line(line, day=day, dow=dow),
                )
            )
        return tuple(rows)

