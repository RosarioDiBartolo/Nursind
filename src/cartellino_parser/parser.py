from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from cartellino_parser.extract import extract_text
from cartellino_parser.models import CartellinoParseError, DayRecord, ParsedCartellino
from cartellino_parser.parse_days import parse_days
from cartellino_parser.parse_pairs import parse_pairs
from cartellino_parser.parse_totals import parse_totals
from cartellino_parser.utils import parse_employee, parse_month_year
from cartellino_parser.validate import validate_cartellino

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


def _records_to_df(records: Iterable[DayRecord]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in records:
        rows.append(asdict(record))
    return pd.DataFrame(
        rows,
        columns=["year", "month", "day", "dow", "mo_f", "mo_t", "mo_lav", "raw"],
    )


def parse_pdf(pdf_path: str | Path) -> ParsedCartellino:
    pdf_path = Path(pdf_path)
    text = extract_text(pdf_path)
    lines = text.splitlines()

    meta = _build_meta(text)
    records = parse_days(lines, meta.get("year"), meta.get("month"))
    if not records:
        LOGGER.error("No day lines found in %s", pdf_path)
        raise CartellinoParseError(f"No day lines found in {pdf_path}")

    days_df = _records_to_df(records)
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
