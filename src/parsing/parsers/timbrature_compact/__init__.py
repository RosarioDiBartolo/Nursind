from __future__ import annotations

from ...core.base import ParserBase
from ...core.detect import analyze_detection
from .parser import parse_pdf, parse_text


class TimbratureCompactParser(ParserBase):
    name = "timbrature_compact"
    family = "timbrature"

    def parse_text(self, text: str, source: object | None = None):
        return parse_text(text, source)

    def parse_pdf(self, source):
        return parse_pdf(source)

    def score(self, text: str) -> int:
        info = analyze_detection(text)
        base = int(info.get("score_timb", 0))
        signals = info.get("timbrature_signals", {}) or {}
        compact_hits = int(signals.get("compact_hits", 0) or 0)
        event_lines = int(signals.get("event_lines", 0) or 0)
        score = base + compact_hits * 2
        if event_lines == 0:
            score += 1
        return score


__all__ = ["TimbratureCompactParser", "parse_pdf", "parse_text"]
