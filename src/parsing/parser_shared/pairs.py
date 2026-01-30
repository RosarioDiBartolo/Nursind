from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional


def build_datetime(
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


def compute_duration(entry_ts: Optional[datetime], exit_ts: Optional[datetime]) -> Optional[str]:
    if not entry_ts or not exit_ts:
        return None
    delta = exit_ts - entry_ts
    minutes = int(delta.total_seconds() // 60)
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def compute_turno(entry_ts: Optional[datetime]) -> Optional[str]:
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
