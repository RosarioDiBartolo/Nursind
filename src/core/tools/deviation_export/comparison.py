"""Compare complete shifts against dated theoretical schedule boundaries."""
from __future__ import annotations

from datetime import datetime, timedelta
import math
import re


def schedule_minutes(value: str) -> int:
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d|24:00", value):
        raise ValueError(f"Invalid theoretical time: {value}")
    hour, minute = map(int, value.split(":"))
    return hour * 60 + minute


def validate_schedules(schedules: dict) -> None:
    if not schedules:
        raise ValueError("No theoretical schedules configured")
    for code, spec in schedules.items():
        start, end = schedule_minutes(spec["entry"]), schedule_minutes(spec["exit"])
        if not 0 <= start < end <= 1440:
            raise ValueError(f"Invalid boundaries for {code}")
        continuation = spec.get("continuation")
        if continuation:
            other = schedules.get(continuation)
            if end != 1440 or not other or schedule_minutes(other["entry"]) != 0:
                raise ValueError(f"Invalid night continuation for {code}")


def compare_shift(row: dict, entry_code: str, exit_code: str, schedules: dict, threshold: float) -> dict:
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("Threshold must be finite and non-negative")
    entry, exit_ = datetime.fromisoformat(row["entry_ts"]), datetime.fromisoformat(row["exit_ts"])
    if exit_ <= entry:
        raise ValueError("Non-positive shift duration")
    if entry_code not in schedules or exit_code not in schedules:
        raise ValueError(f"Unconfigured schedule: {entry_code} / {exit_code}")
    spec = schedules[entry_code]
    if spec.get("continuation_only"):
        raise ValueError("Isolated morning portion of a night shift")
    midnight = entry.replace(hour=0, minute=0, second=0, microsecond=0)
    theoretical_entry = midnight + timedelta(minutes=schedule_minutes(spec["entry"]))
    if spec.get("continuation"):
        if exit_code != spec["continuation"] or (exit_.date() - entry.date()).days != 1:
            raise ValueError("Night shift lacks its matching next-day continuation")
        theoretical_exit = midnight + timedelta(days=1, minutes=schedule_minutes(schedules[exit_code]["exit"]))
        code = f"{entry_code} + {exit_code}"
    else:
        if entry_code != exit_code or exit_.date() != entry.date():
            raise ValueError("Incompatible source codes or dates")
        theoretical_exit = midnight + timedelta(minutes=schedule_minutes(spec["exit"]))
        code = entry_code
    diff_entry = (entry - theoretical_entry).total_seconds() / 60
    diff_exit = (exit_ - theoretical_exit).total_seconds() / 60
    return {
        **row, "code": code, "month": entry.strftime("%Y-%m"),
        "theoretical_entry": theoretical_entry.isoformat(sep=" "),
        "theoretical_exit": theoretical_exit.isoformat(sep=" "),
        "diff_entry_minutes": diff_entry, "diff_exit_minutes": diff_exit,
        "over_threshold": abs(diff_entry) > threshold or abs(diff_exit) > threshold,
    }
