from __future__ import annotations

from typing import Optional

from .core.registry import ParserRegistry
from .parsers import CartellinoParser, TimbratureCompactParser, TimbratureElencoParser

_REGISTRY: ParserRegistry | None = None


def get_registry() -> ParserRegistry:
    global _REGISTRY
    if _REGISTRY is None:
        registry = ParserRegistry()
        registry.register_all(
            [
                CartellinoParser(),
                TimbratureCompactParser(),
                TimbratureElencoParser(),
            ]
        )
        _REGISTRY = registry
    return _REGISTRY


class ParsingService:
    def __init__(self, registry: ParserRegistry | None = None) -> None:
        self._registry = registry or get_registry()

    @property
    def registry(self) -> ParserRegistry:
        return self._registry

    def parse_text(self, text: str, source: object | None = None, *, strict: bool | None = None):
        return self._registry.parse_text(text, source, strict=strict)

    def parse_pdf(self, source, *, strict: bool | None = None):
        return self._registry.parse_pdf(source, strict=strict)

    def analyze_detection(self, text: str) -> dict:
        return self._registry.analyze(text)


def parse_text(text: str, source: object | None = None, *, strict: bool | None = None):
    return get_registry().parse_text(text, source, strict=strict)


def parse_pdf(source, *, strict: bool | None = None):
    return get_registry().parse_pdf(source, strict=strict)
