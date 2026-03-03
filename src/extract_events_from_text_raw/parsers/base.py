from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..models import ParsedRow


class BaseFormatParser(ABC):
    parser_id: str = ""
    legacy_doc_format: str = "unknown"
    priority: int = 100

    @abstractmethod
    def score_document(self, document: dict[str, Any]) -> int:
        raise NotImplementedError

    @abstractmethod
    def parse_document(self, document: dict[str, Any]) -> tuple[ParsedRow, ...]:
        raise NotImplementedError
