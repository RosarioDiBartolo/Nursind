from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParseContext:
    normalized_raw: str


@dataclass(frozen=True, slots=True)
class ParseValues:
    mo_f: float | None
    mo_t: float | None
    mo_lav: float | None


class BaseFormatParser(ABC):
    parser_id: str = ""
    legacy_doc_format: str = "unknown"
    priority: int = 100

    @abstractmethod
    def score_document(self, text: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def parse_row(
        self,
        raw: str,
        *,
        has_event: bool,
        any_event: bool,
        ctx: ParseContext,
    ) -> ParseValues:
        raise NotImplementedError
