"""Shared error types for parsing and detection."""

from __future__ import annotations

from ..parser_shared.models import CartellinoParseError as ParseError
from ..parser_shared.models import ParserDetectionError as DetectionError

__all__ = ["ParseError", "DetectionError"]
