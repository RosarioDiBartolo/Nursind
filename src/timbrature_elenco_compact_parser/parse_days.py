from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Tuple

from parser_shared.models import DayRecord
from timbrature_shared.day_values import extract_day_values

LOGGER = logging.getLogger(__name__)


def _pick_contratt_lavorato(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not values:
        return None, None
    large = [value for value in values if value >= 1.0]
    if len(large) >= 2:
        return large[0], large[1]
    if len(large) == 1:
        return large[0], large[0]
    return values[0], values[0]


def _parse_day_line(line: str, year: int | None, month: int | None) -> DayRecord | None:
    raw = line.strip()
    if not raw:
        return None

    extracted = extract_day_values(raw)
    if not extracted:
        return None

    day, dow, values, _has_event = extracted
    contratt, lavorato = _pick_contratt_lavorato(values)
    mo_f = contratt if contratt is not None else 0.0
    mo_t = lavorato if lavorato is not None else 0.0
    mo_lav = lavorato if lavorato is not None else 0.0

    return DayRecord(
        year=year,
        month=month,
        day=day,
        dow=dow,
        mo_f=mo_f,
        mo_t=mo_t,
        mo_lav=mo_lav,
        raw=raw,
    )


def parse_days(lines: Iterable[str], year: int | None, month: int | None) -> List[DayRecord]:
    records: List[DayRecord] = []
    for line in lines:
        record = _parse_day_line(line, year, month)
        if record:
            records.append(record)
    return records
