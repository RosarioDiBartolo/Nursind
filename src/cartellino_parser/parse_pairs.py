from __future__ import annotations

import logging
import re
from dataclasses import asdict
from datetime import timedelta
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from parser_shared.models import PairRecord
from parser_shared.pairs import (
    build_datetime as _build_datetime,
    compute_duration as _compute_duration,
    compute_turno as _compute_turno,
)

LOGGER = logging.getLogger(__name__)

DAY_LINE_RE = re.compile(r"^(?P<day>0[1-9]|[12][0-9]|3[01])\s+(?P<dow>LU|MA|ME|GI|VE|SA|DO)\b")
EVENT_RE = re.compile(r"\b(?P<kind>[EU])\s*\(?(?P<time>\d{2}:\d{2})\)?")


def _day_allows_match(entry_day: int, exit_day: int) -> bool:
    return exit_day == entry_day or exit_day == entry_day + 1


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


def parse_pairs(lines: Iterable[str], year: int | None, month: int | None) -> pd.DataFrame:
    pairs: List[PairRecord] = []
    current_day: Optional[int] = None
    current_dow: Optional[str] = None
    pending_entry: Optional[dict] = None
    pair_index_by_day: dict[int, int] = {}

    for line in lines:
        stripped = line.strip()
        match = DAY_LINE_RE.match(stripped)
        if match:
            day = int(match.group("day"))
            dow = match.group("dow")
            if pending_entry is not None and not _day_allows_match(pending_entry["day"], day):
                pending_day = pending_entry["day"]
                _append_pair(
                    pairs,
                    year,
                    month,
                    pending_day,
                    pending_entry["dow"],
                    pending_entry["pair_index"],
                    (pending_entry["time"], pending_entry["raw"]),
                    None,
                    None,
                )
                pair_index_by_day[pending_day] = pair_index_by_day.get(pending_day, 0) + 1
                pending_entry = None
            current_day, current_dow = day, dow
            pair_index_by_day.setdefault(current_day, 0)

        if current_day is None:
            continue

        events = list(EVENT_RE.finditer(line))
        if not events:
            continue

        for event in events:
            kind = event.group("kind")
            time_value = event.group("time")
            if kind == "E":
                if pending_entry is not None:
                    pending_day = pending_entry["day"]
                    _append_pair(
                        pairs,
                        year,
                        month,
                        pending_day,
                        pending_entry["dow"],
                        pending_entry["pair_index"],
                        (pending_entry["time"], pending_entry["raw"]),
                        None,
                        None,
                    )
                    pair_index_by_day[pending_day] = pair_index_by_day.get(pending_day, 0) + 1
                    pending_entry = None
                if current_day is None or current_dow is None:
                    continue
                pending_entry = {
                    "day": current_day,
                    "dow": current_dow,
                    "time": time_value,
                    "raw": line,
                    "pair_index": pair_index_by_day[current_day],
                }
            else:
                if pending_entry is None:
                    _append_pair(
                        pairs,
                        year,
                        month,
                        current_day,
                        current_dow,
                        pair_index_by_day[current_day],
                        None,
                        time_value,
                        line,
                    )
                    pair_index_by_day[current_day] = pair_index_by_day.get(current_day, 0) + 1
                else:
                    if _day_allows_match(pending_entry["day"], current_day):
                        pending_day = pending_entry["day"]
                        _append_pair(
                            pairs,
                            year,
                            month,
                            pending_day,
                            pending_entry["dow"],
                            pending_entry["pair_index"],
                            (pending_entry["time"], pending_entry["raw"]),
                            time_value,
                            line,
                        )
                        pair_index_by_day[pending_day] = pair_index_by_day.get(pending_day, 0) + 1
                        pending_entry = None
                    else:
                        pending_day = pending_entry["day"]
                        _append_pair(
                            pairs,
                            year,
                            month,
                            pending_day,
                            pending_entry["dow"],
                            pending_entry["pair_index"],
                            (pending_entry["time"], pending_entry["raw"]),
                            None,
                            None,
                        )
                        pair_index_by_day[pending_day] = pair_index_by_day.get(pending_day, 0) + 1
                        pending_entry = None
                        _append_pair(
                            pairs,
                            year,
                            month,
                            current_day,
                            current_dow,
                            pair_index_by_day[current_day],
                            None,
                            time_value,
                            line,
                        )
                        pair_index_by_day[current_day] = pair_index_by_day.get(current_day, 0) + 1

    if pending_entry is not None:
        pending_day = pending_entry["day"]
        _append_pair(
            pairs,
            year,
            month,
            pending_day,
            pending_entry["dow"],
            pending_entry["pair_index"],
            (pending_entry["time"], pending_entry["raw"]),
            None,
            None,
        )
        pair_index_by_day[pending_day] = pair_index_by_day.get(pending_day, 0) + 1

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
