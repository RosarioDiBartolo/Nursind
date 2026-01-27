from __future__ import annotations

import re
from typing import Optional

from parser_shared.numbers import extract_numeric_tokens, hhmm_to_decimal, parse_number

from .utils import parse_day_header

DAY_PREFIX_RE = re.compile(r"^\s*\d{1,2}\s*\S+\s*")
QTA_RE = re.compile(r"qta\s*:?[\s]*\d{1,3}[,.]\d{2}", re.IGNORECASE)
EVENT_RE = re.compile(r"[EU]\s*\d{2}:\d{2}", re.IGNORECASE)


def extract_day_values(
    line: str,
) -> Optional[tuple[int, str, list[float], bool]]:
    header = parse_day_header(line)
    if not header:
        return None
    day, dow = header
    has_event = bool(EVENT_RE.search(line))
    rest = DAY_PREFIX_RE.sub("", line)
    cleaned = QTA_RE.sub("", rest)
    numbers = extract_numeric_tokens(cleaned)
    values = [hhmm_to_decimal(parse_number(value)) for value in numbers]
    return day, dow, values, has_event
