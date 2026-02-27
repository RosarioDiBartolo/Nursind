from __future__ import annotations

from src.raw_text_parsing import normalize_text

from .base import BaseFormatParser, ParseContext, ParseValues
from .common import assign_situazione, extract_trailing_values


class SituazioneMensileParser(BaseFormatParser):
    parser_id = "situazione_mensile"
    legacy_doc_format = "situazione_mensile"
    priority = 50

    def score_document(self, text: str) -> int:
        norm = normalize_text(text)
        score = -100
        if "situazione mensile presenze" in norm:
            score += 140
        if "totali mensili nel mese di" in norm:
            score += 20
        return score

    def parse_row(
        self,
        raw: str,
        *,
        has_event: bool,
        any_event: bool,
        ctx: ParseContext,
    ) -> ParseValues:
        values = extract_trailing_values(ctx.normalized_raw, allow_hhmm=True)
        return assign_situazione(values, has_event=has_event, any_event=any_event)
