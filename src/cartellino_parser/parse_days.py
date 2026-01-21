from __future__ import annotations

import logging
import re
from typing import Iterable, List

from cartellino_parser.models import DayRecord
from cartellino_parser.utils import extract_numeric_tokens, hhmm_to_decimal, parse_number

LOGGER = logging.getLogger(__name__)

DAY_LINE_RE = re.compile(r"^(?P<day>0[1-9]|[12][0-9]|3[01])\s+(?P<dow>LU|MA|ME|GI|VE|SA|DO)\b")


def _parse_day_line(line: str, year: int | None, month: int | None) -> DayRecord | None:
    match = DAY_LINE_RE.match(line.strip())
    if not match:
        return None

    rest = line[match.end() :].strip()
    numbers = extract_numeric_tokens(rest)
    if len(numbers) < 3:
        LOGGER.debug("Day line has fewer than 3 numeric tokens: %s", line)
        return None

    mo_f_raw, mo_t_raw, mo_lav_raw = (parse_number(value) for value in numbers[-3:])
    mo_f = hhmm_to_decimal(mo_f_raw)
    mo_t = hhmm_to_decimal(mo_t_raw)
    mo_lav = hhmm_to_decimal(mo_lav_raw)
    return DayRecord(
        year=year,
        month=month,
        day=int(match.group("day")),
        dow=match.group("dow"),
        mo_f=mo_f,
        mo_t=mo_t,
        mo_lav=mo_lav,
        raw=line,
    )


def parse_days(lines: Iterable[str], year: int | None, month: int | None) -> List[DayRecord]:
    records: List[DayRecord] = []
    for line in lines:
        record = _parse_day_line(line, year, month)
        if record:
            records.append(record)
    return records
