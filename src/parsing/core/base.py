"""Base classes for parsers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from .extractor import extract_text
from .types import ParsedDocument


class ParserBase(ABC):
    """Base parser interface used by the parsing service."""

    name: str
    family: str

    @abstractmethod
    def parse_text(self, text: str, source: object | None = None) -> ParsedDocument:
        raise NotImplementedError

    def parse_pdf(self, source) -> ParsedDocument:
        text = extract_text(source)
        return self.parse_text(text, source)

    def score(self, text: str) -> int:
        """Optional score for custom selector implementations."""
        return 0

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, family={self.family!r})"
