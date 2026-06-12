from __future__ import annotations

from typing import Any

from .base import BaseFormatParser
from .loader import load_parsers


def resolve_parser(document: dict[str, Any]) -> BaseFormatParser:
    selected: BaseFormatParser | None = None
    best_key: tuple[int, int, str] | None = None

    for parser in load_parsers():
        score = int(parser.score_document(document))
        key = (score, -int(parser.priority), parser.parser_id)
        if best_key is None or key > best_key:
            selected = parser
            best_key = key

    if selected is None:
        raise RuntimeError("Parser resolution failed: no parser candidates available")
    return selected
