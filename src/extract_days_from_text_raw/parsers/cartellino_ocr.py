from __future__ import annotations

from src.raw_text_parsing import normalize_text

from .base import BaseFormatParser, ParseContext, ParseValues
from .common import (
    extract_all_values,
    extract_trailing_values,
    finalize_presence_values,
    split_bang_segments,
    strip_day_prefix,
)


class CartellinoOcrParser(BaseFormatParser):
    parser_id = "cartellino_ocr"
    legacy_doc_format = "cartellino_classic"
    priority = 20

    def score_document(self, text: str) -> int:
        norm = normalize_text(text)
        score = -100
        if "data t i m b r a t u r e o r e" in norm:
            score += 160
        if "rr ii ll ee vv aa zz ii oo nn ee" in norm:
            score += 80
        if norm.count(" ! ") >= 8:
            score += 20
        return score

    def _from_segments(self, segments: list[str]) -> tuple[float | None, float | None]:
        if len(segments) < 2:
            return None, None

        contracted: float | None = None
        worked: float | None = None
        candidate_idx: int | None = None

        for idx, segment in enumerate(segments[1:], start=1):
            vals = extract_all_values(segment, allow_hhmm=True, max_abs=24.0)
            if not vals:
                continue
            candidate_idx = idx
            if len(vals) >= 2:
                worked = vals[0]
                contracted = vals[-1]
            else:
                contracted = vals[0]
                worked = None
            break

        if candidate_idx is None:
            return None, None

        diff: float | None = None
        if candidate_idx + 1 < len(segments):
            diff_vals = extract_all_values(
                segments[candidate_idx + 1],
                allow_hhmm=True,
                max_abs=24.0,
            )
            if diff_vals:
                diff = diff_vals[0]

        if worked is None and contracted is not None and diff is not None:
            if diff < 0:
                worked = contracted + diff
            elif abs(diff - contracted) <= 0.2:
                worked = diff

        return contracted, worked

    def parse_row(
        self,
        raw: str,
        *,
        has_event: bool,
        any_event: bool,
        ctx: ParseContext,
    ) -> ParseValues:
        segments = split_bang_segments(ctx.normalized_raw)
        contracted, worked = self._from_segments(segments)
        if contracted is None and worked is None:
            body = strip_day_prefix(ctx.normalized_raw)
            fallback = extract_trailing_values(
                body,
                allow_hhmm=True,
                max_abs=24.0,
            )
            if len(fallback) >= 2:
                contracted = fallback[-2]
                worked = fallback[-1]
            elif len(fallback) == 1:
                contracted = fallback[0]
                worked = fallback[0]

        return finalize_presence_values(
            contracted,
            worked,
            has_event=has_event,
            any_event=any_event,
        )
