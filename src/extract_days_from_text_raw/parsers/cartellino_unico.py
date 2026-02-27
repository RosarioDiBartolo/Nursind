from __future__ import annotations

from src.raw_text_parsing import normalize_text

from .base import BaseFormatParser, ParseContext, ParseValues
from .common import (
    extract_trailing_values,
    finalize_presence_values,
    strip_day_prefix,
)


class CartellinoUnicoParser(BaseFormatParser):
    parser_id = "cartellino_unico"
    legacy_doc_format = "cartellino_classic"
    priority = 10

    def score_document(self, text: str) -> int:
        norm = normalize_text(text)
        score = -100
        if "cartellino unico" in norm:
            score += 160
        if "giorno orario anomalie ent 1 usc 1" in norm:
            score += 80
        if "data e ora stampa" in norm:
            score += 20
        return score

    def _infer_presence_values(self, values: list[float]) -> tuple[float | None, float | None]:
        if len(values) >= 3:
            recognized = values[-3]
            contracted = values[-2]
            last = values[-1]
            if last < 0:
                worked = contracted + last
            elif abs((recognized - contracted) - last) <= 0.2:
                worked = recognized
            else:
                worked = last
            return contracted, worked

        if len(values) == 2:
            contracted = values[0]
            last = values[1]
            worked = contracted + last if last < 0 else last
            return contracted, worked

        if len(values) == 1:
            return values[0], values[0]
        return None, None

    def parse_row(
        self,
        raw: str,
        *,
        has_event: bool,
        any_event: bool,
        ctx: ParseContext,
    ) -> ParseValues:
        rest = strip_day_prefix(ctx.normalized_raw)
        values = extract_trailing_values(rest, allow_hhmm=True, max_abs=24.0)
        contracted, worked = self._infer_presence_values(values)
        return finalize_presence_values(
            contracted,
            worked,
            has_event=has_event,
            any_event=any_event,
        )
