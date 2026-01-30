"""Compatibility wrapper for the legacy parser_service router."""

from __future__ import annotations

from ..core.detect import analyze_detection, detect_document_family, detect_timbrature_variant
from ..service import parse_pdf, parse_text

__all__ = [
    "analyze_detection",
    "detect_document_family",
    "detect_timbrature_variant",
    "parse_pdf",
    "parse_text",
]
