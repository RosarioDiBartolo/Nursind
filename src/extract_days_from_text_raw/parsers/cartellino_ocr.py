from __future__ import annotations

import re

from src.raw_text_parsing import normalize_text

from .base import BaseFormatParser, EventHint, ParseContext, RowParseResult
from .common import (
    extract_all_values,
    extract_trailing_values,
    finalize_presence_values,
    hints_from_explicit_events,
    split_bang_segments,
    strip_day_prefix,
    to_row_result,
)


class CartellinoOcrParser(BaseFormatParser):
    parser_id = "cartellino_ocr"
    legacy_doc_format = "cartellino_classic"
    priority = 20
    _TIME_RE = re.compile(r"(?<!\d)(?P<h>[0-2]?\d)[:.,](?P<m>[0-5]\d)(?!\d)")

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

    def _aligned_hints(self, body: str) -> tuple[EventHint, ...]:
        segments = split_bang_segments(body)
        if not segments:
            return ()
        primary = segments[0]
        times: list[str] = []
        for match in self._TIME_RE.finditer(primary):
            hour = int(match.group("h"))
            minute = int(match.group("m"))
            if not ((0 <= hour <= 23) or (hour == 24 and minute == 0)):
                continue
            times.append(f"{hour:02d}:{minute:02d}")

        if not times:
            return ()

        hints: list[EventHint] = []
        confidence = 0.9 if len(times) >= 2 else 0.7
        for idx, hhmm in enumerate(times[:12]):
            kind = "E" if idx % 2 == 0 else "U"
            if len(times) == 1 and "rec" in primary:
                kind = "U"
            hints.append(
                EventHint(
                    kind=kind,
                    time_hhmm=hhmm,
                    source=f"segment_1_pos_{idx+1}",
                    confidence=confidence,
                )
            )
        return tuple(hints)

    def parse_row(
        self,
        raw: str,
        *,
        has_event: bool,
        any_event: bool,
        ctx: ParseContext,
    ) -> RowParseResult:
        body = strip_day_prefix(ctx.normalized_raw)
        segments = split_bang_segments(body)
        contracted, worked = self._from_segments(segments)
        if contracted is None and worked is None:
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

        hints = self._aligned_hints(body)
        if not hints:
            hints = hints_from_explicit_events(raw, source="explicit", confidence=0.85)
        return to_row_result(
            finalize_presence_values(
                contracted,
                worked,
                has_event=has_event,
                any_event=any_event,
            ),
            hints=hints,
        )
