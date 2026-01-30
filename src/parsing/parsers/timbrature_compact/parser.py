from __future__ import annotations

from ...timbrature_shared.parser_base import parse_pdf_with, parse_text_with

from .parse_days import parse_days


def parse_pdf(source):
    return parse_pdf_with(source, parse_days)


def parse_text(text: str, source: object | None = None):
    return parse_text_with(text, parse_days, source)
