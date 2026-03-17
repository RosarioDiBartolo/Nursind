from __future__ import annotations

import re

from cartellino_parser.raw_text_parsing import normalize_text, parse_day_header

from .base import BaseFormatParser
from .common import (
    document_text,
    explicit_events_for_line,
    iter_preferred_lines,
    to_row_result,
)


class TimbratureWebParser(BaseFormatParser):
    parser_id = "timbrature_web"
    legacy_doc_format = "timbrature_web"
    priority = 30

    def score_document(self, document: dict[str, object]) -> int:
        text = document_text(document)
        norm = normalize_text(text)
        score = 0
        if "elenco timbrature" in norm:
            score += 120
        if "data ent usc" in norm:
            score += 40
        if re.search(r"\b[eu]\s*[0-2]?\d:[0-5]\d", norm):
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

