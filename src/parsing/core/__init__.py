"""Core parser framework (base classes, detection, registry, and shared helpers)."""

from .base import ParserBase
from .detect import analyze_detection, detect_document_family, detect_timbrature_variant
from .errors import DetectionError, ParseError
from .extractor import extract_text, extract_text_vertical
from .registry import ParserRegistry
from .types import ParsedDocument

__all__ = [
    "ParserBase",
    "ParserRegistry",
    "ParsedDocument",
    "ParseError",
    "DetectionError",
    "extract_text",
    "extract_text_vertical",
    "detect_document_family",
    "detect_timbrature_variant",
    "analyze_detection",
]
