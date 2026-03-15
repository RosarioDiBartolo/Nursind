from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import holidays
import pandas as pd


def to_datetime_series(values: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(values, errors="coerce", format="mixed")
    except TypeError:
        return pd.to_datetime(values, errors="coerce")


def _to_bool_series(values: pd.Series) -> pd.Series:
    if values.dtype == bool:
        return values.fillna(False)
    normalized = values.astype(str).str.strip().str.lower()
    return normalized.isin({"true", "1", "yes", "y", "t"})


def assign_turno_code(df: pd.DataFrame) -> pd.Series:
    required_cols = {"is_holiday", "is_afternoon", "is_night"}
    if not required_cols.issubset(set(df.columns)):
        return pd.Series(index=df.index, dtype="object")

    is_holiday = _to_bool_series(df["is_holiday"])
    is_afternoon = _to_bool_series(df["is_afternoon"])
    is_night = _to_bool_series(df["is_night"])

    turno_code = pd.Series("D", index=df.index, dtype="object")
    turno_code = turno_code.mask(is_afternoon, "P")
    turno_code = turno_code.mask(is_night, "N")
    turno_code = turno_code.mask(is_holiday, "F")
    return turno_code


def assign_turno_bucket(df: pd.DataFrame, *, min_hours: float = 6.0) -> pd.Series:
    required_cols = {"duration_hours", "is_holiday", "is_afternoon", "is_night"}
    if not required_cols.issubset(set(df.columns)):
        return pd.Series(index=df.index, dtype="object")

    duration_hours = pd.to_numeric(df["duration_hours"], errors="coerce")
    is_holiday = _to_bool_series(df["is_holiday"])
    is_afternoon = _to_bool_series(df["is_afternoon"])
    is_night = _to_bool_series(df["is_night"])
    is_long = duration_hours > float(min_hours)

    bucket = pd.Series("S", index=df.index, dtype="object")
    bucket = bucket.mask(is_long & is_holiday, "F")
    bucket = bucket.mask(is_long & ~is_holiday & is_night, "N")
    bucket = bucket.mask(is_long & ~is_holiday & ~is_night & is_afternoon, "P")
    bucket = bucket.mask(is_long & ~is_holiday & ~is_night & ~is_afternoon, "M")
    return bucket


def compute_turno(entry_ts: pd.Timestamp | None) -> str | None:
    if entry_ts is None or pd.isna(entry_ts):
        return None
    entry_minutes = int(entry_ts.hour) * 60 + int(entry_ts.minute)
    targets = {"Mattina": 8 * 60, "Pomeriggio": 14 * 60, "Notte": 20 * 60}
    return min(targets.items(), key=lambda item: abs(entry_minutes - item[1]))[0]


class ItalianHolidayCalendar:
    def __init__(self) -> None:
        self._cache: dict[int, set[date]] = {}

    def dates_for_years(self, years: Iterable[int]) -> set[date]:
        dates: set[date] = set()
        for year in years:
            year_i = int(year)
            if year_i not in self._cache:
                self._cache[year_i] = set(holidays.country_holidays("IT", years=year_i).keys())
            dates.update(self._cache[year_i])
        return dates


@dataclass(slots=True)
class PairsCloser:
    max_gap_hours: float = 16.0
    mark_inferred: bool = False
    preserve_exit_raw: bool = False
    clear_duration_hhmm: bool = False

    def close(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "entry_ts" not in df.columns or "exit_ts" not in df.columns:
            return df

        working = df.copy()
        working["entry_ts"] = to_datetime_series(working["entry_ts"])
        working["exit_ts"] = to_datetime_series(working["exit_ts"])

        complete_mask = working["entry_ts"].notna() & working["exit_ts"].notna()
        closed_rows = working.loc[complete_mask].copy()
        if self.mark_inferred:
            closed_rows["closed_inferred"] = False

        incomplete = working.loc[~complete_mask].copy()
        events: list[tuple[pd.Timestamp, str, pd.Series]] = []
        for _, row in incomplete.iterrows():
            if pd.notna(row.get("entry_ts")):
                events.append((row["entry_ts"], "entry", row))
            elif pd.notna(row.get("exit_ts")):
                events.append((row["exit_ts"], "exit", row))

        events.sort(key=lambda item: item[0])
        pending_entry: pd.Series | None = None
        max_gap = pd.Timedelta(hours=self.max_gap_hours)

        for ts, kind, row in events:
            if kind == "entry":
                pending_entry = row
                continue
            if pending_entry is None:
                continue

            entry_ts = pending_entry["entry_ts"]
            exit_ts = ts
            if exit_ts < entry_ts:
                exit_ts = exit_ts + pd.Timedelta(days=1)
            if self.max_gap_hours > 0 and (exit_ts - entry_ts) > max_gap:
                pending_entry = None
                continue

            merged = pending_entry.copy()
            merged["exit_ts"] = exit_ts
            if self.preserve_exit_raw and "exit_raw" in merged.index:
                merged["exit_raw"] = row.get("exit_raw")
            if self.clear_duration_hhmm and "duration_hhmm" in merged.index:
                merged["duration_hhmm"] = None
            if self.mark_inferred:
                merged["closed_inferred"] = True
            closed_rows = pd.concat([closed_rows, pd.DataFrame([merged])], ignore_index=True)
            pending_entry = None

        return closed_rows.reset_index(drop=True)


@dataclass(slots=True)
class ShiftClassifier:
    calendar: ItalianHolidayCalendar | None = None
    include_holidays: bool = False
    match_mode: str = "contains"

    def _match(self, values: pd.Series, token: str) -> pd.Series:
        if self.match_mode == "equals":
            return values == token
        return values.str.contains(token, na=False)

    def classify(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "entry_ts" not in df.columns or "exit_ts" not in df.columns:
            return df

        working = df.copy()
        working["entry_ts"] = to_datetime_series(working["entry_ts"])
        working["exit_ts"] = to_datetime_series(working["exit_ts"])
        working["duration"] = working["exit_ts"] - working["entry_ts"]
        working["anno"] = working["entry_ts"].dt.year

        turno_norm = (
            working.get("turno", pd.Series(index=working.index, dtype="object"))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )
        working["turno_norm"] = turno_norm
        working["is_night"] = self._match(turno_norm, "notte")
        working["is_afternoon"] = self._match(turno_norm, "pomeriggio")

        is_sunday = working["entry_ts"].dt.dayofweek == 6
        if self.include_holidays and self.calendar is not None:
            years = sorted({int(y) for y in working["anno"].dropna().unique()})
            holiday_dates = self.calendar.dates_for_years(years)
            is_holiday = is_sunday | working["entry_ts"].dt.date.isin(holiday_dates)
        else:
            is_holiday = is_sunday
        working["is_holiday"] = is_holiday
        return working


__all__ = [
    "ItalianHolidayCalendar",
    "PairsCloser",
    "ShiftClassifier",
    "assign_turno_bucket",
    "assign_turno_code",
    "compute_turno",
    "to_datetime_series",
]
