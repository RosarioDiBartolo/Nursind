from __future__ import annotations

from src.raw_text_parsing import DAY_HEADER_RE
from src.raw_text_parsing import normalize_text

from .base import BaseFormatParser, ParseContext, ParseValues
from .common import assign_cartellino, extract_trailing_values


class CartellinoClassicParser(BaseFormatParser):
    parser_id = "cartellino_classic"
    legacy_doc_format = "cartellino_classic"
    priority = 40

    def score_document(self, text: str) -> int:
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

    def parse_row(
        self,
        raw: str,
        *,
        has_event: bool,
        any_event: bool,
        ctx: ParseContext,
    ) -> ParseValues:
        values = extract_trailing_values(ctx.normalized_raw, allow_hhmm=False)
        return assign_cartellino(values)
