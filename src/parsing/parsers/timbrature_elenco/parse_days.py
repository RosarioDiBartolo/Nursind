from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Tuple

from ...parser_shared.models import DayRecord
from ...timbrature_shared.day_values import extract_day_values

LOGGER = logging.getLogger(__name__)


def _pick_contratt_lavorato(
    values: List[float],
    has_event: bool,
) -> Tuple[Optional[float], Optional[float]]:
    if not values:
        return None, None
    if not has_event:
        if len(values) >= 2:
            return values[0], values[1]
        return values[0], values[0]
    if len(values) >= 2:
        return values[0], values[1]
    return None, values[0]


def _parse_day_line(
    line: str,
    year: int | None,
    month: int | None,
    any_event: bool,
) -> DayRecord | None:
    raw = line.strip()
    if not raw:
        return None

    extracted = extract_day_values(raw)
    if not extracted:
        return None

    day, dow, values, has_event = extracted
    contratt, lavorato = _pick_contratt_lavorato(values, has_event)
    mo_f = contratt if contratt is not None else 0.0
    mo_t = lavorato if lavorato is not None else 0.0
    mo_lav = lavorato if lavorato is not None else 0.0
    if any_event and not has_event:
        mo_lav = 0.0

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
    lines_list = list(lines)
    any_event = any(
        (extracted := extract_day_values(line)) and extracted[3] for line in lines_list
    )
    records: List[DayRecord] = []
    for line in lines_list:
        record = _parse_day_line(line, year, month, any_event)
        if record:
            records.append(record)
    return records
