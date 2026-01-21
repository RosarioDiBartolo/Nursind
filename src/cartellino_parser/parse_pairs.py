from __future__ import annotations

import logging
import re
from dataclasses import asdict
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from cartellino_parser.models import PairRecord

LOGGER = logging.getLogger(__name__)

DAY_LINE_RE = re.compile(r"^(?P<day>0[1-9]|[12][0-9]|3[01])\s+(?P<dow>LU|MA|ME|GI|VE|SA|DO)\b")
EVENT_RE = re.compile(r"\b(?P<kind>[EU])\s*\(?(?P<time>\d{2}:\d{2})\)?")


def _append_pair(
    pairs: List[PairRecord],
    year: int | None,
    month: int | None,
    day: int,
    dow: str,
    pair_index: int,
    entry: Optional[Tuple[str, str]],
    exit_time: Optional[str],
    exit_raw: Optional[str],
) -> None:
    entry_time = entry[0] if entry else None
    entry_raw = entry[1] if entry else None
    duration_hhmm = _compute_duration(entry_time, exit_time)
    turno = _compute_turno(entry_time)
    pairs.append(
        PairRecord(
            year=year,
            month=month,
            day=day,
            dow=dow,
            pair_index=pair_index,
            entry_time=entry_time,
            exit_time=exit_time,
            duration_hhmm=duration_hhmm,
            turno=turno,
            entry_raw=entry_raw,
            exit_raw=exit_raw,
        )
    )


def _time_to_minutes(value: str) -> int:
    hours, minutes = value.split(":")
    return int(hours) * 60 + int(minutes)


def _compute_duration(entry_time: Optional[str], exit_time: Optional[str]) -> Optional[str]:
    if not entry_time or not exit_time:
        return None
    start = _time_to_minutes(entry_time)
    end = _time_to_minutes(exit_time)
    if end < start:
        end += 24 * 60
    minutes = end - start
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def _compute_turno(entry_time: Optional[str]) -> Optional[str]:
    if not entry_time:
        return None
    entry_minutes = _time_to_minutes(entry_time)
    targets = {
        "Mattina": 8 * 60,
        "Pomeriggio": 14 * 60,
        "Notte": 20 * 60,
    }
    closest = min(targets.items(), key=lambda item: abs(entry_minutes - item[1]))
    return closest[0]


def parse_pairs(lines: Iterable[str], year: int | None, month: int | None) -> pd.DataFrame:
    pairs: List[PairRecord] = []
    current_day: Optional[int] = None
    current_dow: Optional[str] = None
    current_entry: Optional[Tuple[str, str]] = None
    pair_index = 0

    for line in lines:
        match = DAY_LINE_RE.match(line.strip())
        if not match:
            continue

        day = int(match.group("day"))
        dow = match.group("dow")
        if current_day is None:
            current_day, current_dow, pair_index = day, dow, 0
        elif day != current_day or dow != current_dow:
            current_day, current_dow, pair_index = day, dow, 0

        events = list(EVENT_RE.finditer(line))
        if not events:
            LOGGER.debug("No E/U events found for day line: %s", line)
            continue

        for event in events:
            kind = event.group("kind")
            time_value = event.group("time")
            if kind == "E":
                if current_entry is not None:
                    _append_pair(
                        pairs,
                        year,
                        month,
                        current_day or day,
                        current_dow or dow,
                        pair_index,
                        current_entry,
                        None,
                        None,
                    )
                    pair_index += 1
                current_entry = (time_value, line)
            else:
                if current_entry is None:
                    _append_pair(
                        pairs,
                        year,
                        month,
                        current_day or day,
                        current_dow or dow,
                        pair_index,
                        None,
                        time_value,
                        line,
                    )
                    pair_index += 1
                else:
                    _append_pair(
                        pairs,
                        year,
                        month,
                        current_day or day,
                        current_dow or dow,
                        pair_index,
                        current_entry,
                        time_value,
                        line,
                    )
                    pair_index += 1
                    current_entry = None

    if current_entry and current_day is not None and current_dow is not None:
        _append_pair(
            pairs,
            year,
            month,
            current_day,
            current_dow,
            pair_index,
            current_entry,
            None,
            None,
        )

    rows = [asdict(record) for record in pairs]
    return pd.DataFrame(
        rows,
        columns=[
            "year",
            "month",
            "day",
            "dow",
            "pair_index",
            "entry_time",
            "exit_time",
            "duration_hhmm",
            "turno",
            "entry_raw",
            "exit_raw",
        ],
    )
