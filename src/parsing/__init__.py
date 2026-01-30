"""Parsing service facade and public API."""

from .service import ParsingService, get_registry, parse_pdf, parse_text
from .core.errors import DetectionError, ParseError
from .parsers import (
    CartellinoParser,
    TimbratureCompactParser,
    TimbratureElencoParser,
)

__all__ = [
    "ParsingService",
    "get_registry",
    "parse_pdf",
    "parse_text",
    "DetectionError",
    "ParseError",
    "CartellinoParser",
    "TimbratureCompactParser",
    "TimbratureElencoParser",
]
