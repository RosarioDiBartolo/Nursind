from __future__ import annotations

import logging
from typing import Callable, Dict, Iterable, List, Optional

from .base import ParserBase
from .detect import analyze_detection, detect_document_family, detect_timbrature_variant
from .errors import DetectionError, ParseError
from .extractor import extract_text, extract_text_vertical

LOGGER = logging.getLogger(__name__)

SelectorFn = Callable[[str, Dict[str, ParserBase], Optional[bool]], ParserBase]


def _default_selector(
    text: str,
    parsers: Dict[str, ParserBase],
    strict: bool | None,
) -> ParserBase:
    family = detect_document_family(text)
    if family == "cartellino":
        parser = parsers.get("cartellino")
        if not parser:
            raise DetectionError("Cartellino parser not registered.")
        return parser

    variant = detect_timbrature_variant(text, strict=strict)
    parser = parsers.get(variant)
    if not parser:
        raise DetectionError(f"Timbrature parser '{variant}' not registered.")
    return parser


class ParserRegistry:
    def __init__(self, selector: SelectorFn | None = None) -> None:
        self._parsers: Dict[str, ParserBase] = {}
        self._selector = selector or _default_selector

    def register(self, parser: ParserBase) -> None:
        if not parser.name:
            raise ValueError("Parser must define a non-empty name.")
        if parser.name in self._parsers:
            raise ValueError(f"Parser '{parser.name}' already registered.")
        self._parsers[parser.name] = parser

    def register_all(self, parsers: Iterable[ParserBase]) -> None:
        for parser in parsers:
            self.register(parser)

    def get(self, name: str) -> ParserBase:
        return self._parsers[name]

    def list_parsers(self) -> List[ParserBase]:
        return list(self._parsers.values())

    def analyze(self, text: str) -> dict:
        info = analyze_detection(text)
        info["registered_parsers"] = sorted(self._parsers.keys())
        return info

    def select_parser(self, text: str, strict: bool | None = None) -> ParserBase:
        if not self._parsers:
            raise DetectionError("No parsers registered.")
        return self._selector(text, self._parsers, strict)

    def parse_text(
        self,
        text: str,
        source: object | None = None,
        *,
        strict: bool | None = None,
    ):
        parser = self.select_parser(text, strict=strict)
        return parser.parse_text(text, source)

    def parse_pdf(
        self,
        source,
        *,
        strict: bool | None = None,
    ):
        tried_vertical = False
        text = extract_text(source)
        detect_info = analyze_detection(text)
        if detect_info["score_cart"] == 0 and detect_info["score_timb"] == 0:
            text = extract_text_vertical(source)
            tried_vertical = True

        try:
            return self.parse_text(text, source, strict=strict)
        except (DetectionError, ParseError) as exc:
            if tried_vertical:
                raise
            LOGGER.info("Retrying with vertical extraction after %s", exc.__class__.__name__)
            text_vertical = extract_text_vertical(source)
            return self.parse_text(text_vertical, source, strict=strict)

