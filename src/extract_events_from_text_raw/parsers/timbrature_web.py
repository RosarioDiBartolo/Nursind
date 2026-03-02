from __future__ import annotations

import re

from src.raw_text_parsing import normalize_text

from .base import BaseFormatParser, ParseContext, RowParseResult
from .common import (
    assign_timbrature,
    extract_leading_values,
    hints_from_explicit_events,
    strip_day_prefix_and_qta,
    to_row_result,
)


class TimbratureWebParser(BaseFormatParser):
    parser_id = "timbrature_web"
    legacy_doc_format = "timbrature_web"
    priority = 30

    def score_document(self, text: str) -> int:
        norm = normalize_text(text)
        score = 0
        if "elenco timbrature" in norm:
            score += 120
        if "data ent usc" in norm:
            score += 40
        if re.search(r"\b[eu]\s*[0-2]?\d:[0-5]\d", norm):
            score += 20
        return score

    def parse_row(
        self,
        raw: str,
        *,
        has_event: bool,
        any_event: bool,
        ctx: ParseContext,
    ) -> RowParseResult:
        rest = strip_day_prefix_and_qta(ctx.normalized_raw)
        values = extract_leading_values(rest, allow_hhmm=False)
        return to_row_result(
            assign_timbrature(values, has_event=has_event, any_event=any_event),
            hints=hints_from_explicit_events(raw, source="explicit", confidence=0.99),
        )
