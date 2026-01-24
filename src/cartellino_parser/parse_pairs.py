from __future__ import annotations

import logging
import re
from dataclasses import asdict
from datetime import datetime, timedelta
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
    entry_ts = _build_datetime(year, month, day, entry_time)
    exit_ts = _build_datetime(year, month, day, exit_time)
    if entry_ts and exit_ts and exit_ts < entry_ts:
        exit_ts = exit_ts + timedelta(days=1)
    duration_hhmm = _compute_duration(entry_ts, exit_ts)
    turno = _compute_turno(entry_ts)
    pairs.append(
        PairRecord(
            year=year,
            month=month,
            day=day,
            dow=dow,
            pair_index=pair_index,
            entry_ts=entry_ts,
            exit_ts=exit_ts,
            duration_hhmm=duration_hhmm,
            turno=turno,
            entry_raw=entry_raw,
            exit_raw=exit_raw,
        )
    )


def _build_datetime(
    year: int | None, month: int | None, day: int, time_value: Optional[str]
) -> Optional[datetime]:
    if not time_value or year is None or month is None:
        return None
    hours, minutes = time_value.split(":")
    hours_i = int(hours)
    minutes_i = int(minutes)
    if hours_i == 24 and minutes_i == 0:
        return datetime(year, month, day, 0, 0) + timedelta(days=1)
    return datetime(year, month, day, hours_i, minutes_i)


def _compute_duration(entry_ts: Optional[datetime], exit_ts: Optional[datetime]) -> Optional[str]:
    if not entry_ts or not exit_ts:
        return None
    delta = exit_ts - entry_ts
    minutes = int(delta.total_seconds() // 60)
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def _compute_turno(entry_ts: Optional[datetime]) -> Optional[str]:
    if not entry_ts:
        return None
    entry_minutes = entry_ts.hour * 60 + entry_ts.minute
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
    # pair_index orders emitted pairs within the current day; it resets on day change.
    pair_index = 0

    for line in lines:
        stripped = line.strip()
        match = DAY_LINE_RE.match(stripped)
        if match:
            day = int(match.group("day"))
            dow = match.group("dow")
            if current_day is None:
                current_day, current_dow, pair_index = day, dow, 0
            elif day != current_day or dow != current_dow:
                if current_entry is not None:
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
                    pair_index += 1
                    current_entry = None
                current_day, current_dow, pair_index = day, dow, 0

        if current_day is None:
            continue

        events = list(EVENT_RE.finditer(line))
        if not events:
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
            "entry_ts",
            "exit_ts",
            "duration_hhmm",
            "turno",
            "entry_raw",
            "exit_raw",
        ],
    )
