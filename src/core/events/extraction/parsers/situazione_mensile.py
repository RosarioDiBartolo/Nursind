from __future__ import annotations

from core.parsing import normalize_text, parse_day_header

from .base import BaseFormatParser
from .common import (
    document_text,
    explicit_events_for_line,
    iter_preferred_lines,
    to_row_result,
)


class SituazioneMensileParser(BaseFormatParser):
    parser_id = "situazione_mensile"
    legacy_doc_format = "situazione_mensile"
    priority = 50

    def score_document(self, document: dict[str, object]) -> int:
        text = document_text(document)
        norm = normalize_text(text)
        score = -100
        if "situazione mensile presenze" in norm:
            score += 140
        if "totali mensili nel mese di" in norm:
            score += 20
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

