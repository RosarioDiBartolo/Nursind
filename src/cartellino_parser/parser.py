from __future__ import annotations

import logging
from typing import Any, Dict

from parser_shared.extract import extract_text, extract_text_vertical
from parser_shared.models import CartellinoParseError, ParsedCartellino
from cartellino_parser.parse_days import has_day_lines, parse_days
from cartellino_parser.parse_pairs import parse_pairs
from cartellino_parser.parse_totals import parse_totals
from cartellino_parser.utils import parse_employee, parse_month_year
from parser_shared.validate import validate_cartellino
from parser_shared.records import records_to_df

LOGGER = logging.getLogger(__name__)


def _build_meta(text: str) -> Dict[str, Any]:
    month, year, month_name = parse_month_year(text)
    employee_name, employee_id = parse_employee(text)
    return {
        "employee_name": employee_name,
        "employee_id": employee_id,
        "month_name": month_name,
        "month": month,
        "year": year,
        "unit": None,
        "turno": None,
        "qualifica": None,
    }


def parse_pdf(source) -> ParsedCartellino:
    text = extract_text(source)
    lines = text.splitlines()
    if not has_day_lines(lines):
        LOGGER.info("No day lines found in extracted text for %s; trying vertical reconstruction", source)
        text = extract_text_vertical(source)
    return parse_text(text, source)


def parse_text(text: str, source: object | None = None) -> ParsedCartellino:
    lines = text.splitlines()

    meta = _build_meta(text)
    records = parse_days(lines, meta.get("year"), meta.get("month"))
    if not records:
        label = source if source is not None else "<text>"
        LOGGER.error("No day lines found in %s", label)
        raise CartellinoParseError(f"No day lines found in {label}")

    days_df = records_to_df(records)
    pairs_df = parse_pairs(lines, meta.get("year"), meta.get("month"))
    totals = parse_totals(text)
    validation = validate_cartellino(days_df, totals)

    #Each document has different sections
    return ParsedCartellino(
        meta=meta,
        days_df=days_df,
        pairs_df=pairs_df,
        totals=totals,
        validation=validation,
    )
