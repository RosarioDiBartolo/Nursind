from __future__ import annotations

from ...core.base import ParserBase
from ...core.detect import analyze_detection
from .parser import parse_pdf, parse_text


class CartellinoParser(ParserBase):
    name = "cartellino"
    family = "cartellino"

    def parse_text(self, text: str, source: object | None = None):
        return parse_text(text, source)

    def parse_pdf(self, source):
        return parse_pdf(source)

    def score(self, text: str) -> int:
        info = analyze_detection(text)
        return int(info.get("score_cart", 0))


__all__ = ["CartellinoParser", "parse_pdf", "parse_text"]
