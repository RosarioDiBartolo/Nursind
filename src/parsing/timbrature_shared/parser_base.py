from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List

from ..parser_shared.extract import extract_text
from ..parser_shared.models import CartellinoParseError, DayRecord, ParsedCartellino
from ..parser_shared.records import records_to_df
from ..parser_shared.validate import validate_timbrature

from .parse_pairs import parse_pairs
from .parse_totals import parse_totals
from .utils import parse_employee, parse_month_year

LOGGER = logging.getLogger(__name__)

ParseDaysFn = Callable[[Iterable[str], int | None, int | None], List[DayRecord]]


def build_meta(text: str) -> Dict[str, Any]:
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


def parse_text_with(
    text: str,
    parse_days: ParseDaysFn,
    source: object | None = None,
) -> ParsedCartellino:
    lines = text.splitlines()

    meta = build_meta(text)
    records = parse_days(lines, meta.get("year"), meta.get("month"))
    if not records:
        label = source if source is not None else "<text>"
        LOGGER.error("No day lines found in %s", label)
        raise CartellinoParseError(f"No day lines found in {label}")

    days_df = records_to_df(records)
    pairs_df = parse_pairs(lines, meta.get("year"), meta.get("month"))
    totals = parse_totals(text)
    validation = validate_timbrature(pairs_df, totals)

    return ParsedCartellino(
        meta=meta,
        days_df=days_df,
        pairs_df=pairs_df,
        totals=totals,
        validation=validation,
    )


def parse_pdf_with(source, parse_days: ParseDaysFn) -> ParsedCartellino:
    text = extract_text(source)
    return parse_text_with(text, parse_days, source)
