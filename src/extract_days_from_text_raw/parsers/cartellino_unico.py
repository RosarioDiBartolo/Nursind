from __future__ import annotations

import re

from src.raw_text_parsing import normalize_text

from .base import BaseFormatParser, EventHint, ParseContext, RowParseResult
from .common import (
    extract_trailing_values,
    finalize_presence_values,
    strip_day_prefix,
    to_row_result,
)


class CartellinoUnicoParser(BaseFormatParser):
    parser_id = "cartellino_unico"
    legacy_doc_format = "cartellino_classic"
    priority = 10
    _TIME_RE = re.compile(r"^(?P<h>[0-2]?\d)[:.,](?P<m>[0-5]\d)$")

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

    def _parse_time(self, token: str) -> tuple[str, int] | None:
        match = self._TIME_RE.match(token)
        if not match:
            return None
        hour = int(match.group("h"))
        minute = int(match.group("m"))
        if not ((0 <= hour <= 23) or (hour == 24 and minute == 0)):
            return None
        hhmm = f"{hour:02d}:{minute:02d}"
        minutes = hour * 60 + minute
        return hhmm, minutes

    def _aligned_hints(self, norm_body: str) -> tuple[EventHint, ...]:
        tokens = norm_body.split()
        marker_positions: set[int] = set()
        all_times: list[tuple[int, str, int]] = []

        for idx, token in enumerate(tokens):
            parsed = self._parse_time(token)
            if parsed is None:
                continue
            hhmm, minutes = parsed
            all_times.append((idx, hhmm, minutes))
            if idx + 1 < len(tokens):
                nxt = tokens[idx + 1].strip().lower()
                if nxt.startswith(("o", "a", "t")):
                    marker_positions.add(idx)

        if not all_times:
            return ()

        selected: list[tuple[str, int]]
        confidence = 0.75

        if marker_positions:
            selected_positions = sorted(marker_positions)
            first_marked = selected_positions[0]
            previous_candidates = [item for item in all_times if item[0] < first_marked]
            if previous_candidates and (first_marked - previous_candidates[-1][0] <= 2):
                selected_positions.insert(0, previous_candidates[-1][0])
            selected = [
                (hhmm, minutes)
                for pos, hhmm, minutes in all_times
                if pos in set(selected_positions)
            ]
            confidence = 0.9
        else:
            if len(all_times) < 2:
                return ()
            first = all_times[0]
            second = all_times[1]
            delta = second[2] - first[2]
            if delta < 0:
                delta += 24 * 60
            if delta < 90:
                return ()
            selected = [(first[1], first[2]), (second[1], second[2])]

        hints: list[EventHint] = []
        for idx, (hhmm, _) in enumerate(selected[:12]):
            kind = "E" if idx % 2 == 0 else "U"
            hints.append(
                EventHint(
                    kind=kind,
                    time_hhmm=hhmm,
                    source=f"aligned_{idx+1}",
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
        rest = strip_day_prefix(ctx.normalized_raw)
        values = extract_trailing_values(rest, allow_hhmm=True, max_abs=24.0)
        contracted, worked = self._infer_presence_values(values)
        hints = self._aligned_hints(rest)
        return to_row_result(
            finalize_presence_values(
                contracted,
                worked,
                has_event=has_event,
                any_event=any_event,
            ),
            hints=hints,
        )
