from __future__ import annotations

import re
from typing import Optional

from parser_shared.numbers import MONTHS_IT, extract_numeric_tokens, hhmm_to_decimal, parse_number


def parse_month_year(text: str) -> tuple[Optional[int], Optional[int], Optional[str]]:
    match = re.search(
        r"RIEPILOGO PRESENZE/ASSENZE\s*-\s*(?P<month>[A-Z]+)\s+(?P<year>\d{4})",
        text,
    )
    if not match:
        return None, None, None
    month_name = match.group("month").upper()
    month = MONTHS_IT.get(month_name)
    year = int(match.group("year"))
    return month, year, month_name


def parse_employee(text: str) -> tuple[Optional[str], Optional[str]]:
    match = re.search(r"^(?P<name>[A-Z' ]+?)\s*-\s*(?P<id>\d{4,})", text, re.MULTILINE)
    if not match:
        return None, None
    name = match.group("name").strip()
    employee_id = match.group("id")
    return name, employee_id
