from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
import os
from typing import Any, Callable, Iterable

import holidays
import pandas as pd

from drive_service.names import safe_name

logger = logging.getLogger(__name__)


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
    targets = {
        "Mattina": 8 * 60,
        "Pomeriggio": 14 * 60,
        "Notte": 20 * 60,
    }
    closest = min(targets.items(), key=lambda item: abs(entry_minutes - item[1]))
    return closest[0]


class HolidayCalendar:
    def is_holiday(self, day: date) -> bool:
        raise NotImplementedError

    def dates_for_years(self, years: Iterable[int]) -> set[date]:
        raise NotImplementedError


class ItalianHolidayCalendar(HolidayCalendar):
    def __init__(self) -> None:
        self._cache: dict[int, set[date]] = {}

    def dates_for_years(self, years: Iterable[int]) -> set[date]:
        dates: set[date] = set()
        for year in years:
            year_i = int(year)
            if year_i not in self._cache:
                self._cache[year_i] = set(
                    holidays.country_holidays("IT", years=year_i).keys()
                )
            dates.update(self._cache[year_i])
        return dates

    def is_holiday(self, day: date) -> bool:
        return day in self.dates_for_years([day.year])


@dataclass(frozen=True)
class TurnoMatchPolicy:
    mode: str = "contains"  # "contains" or "equals"

    def match(self, values: pd.Series, token: str) -> pd.Series:
        if self.mode == "equals":
            return values == token
        return values.str.contains(token, na=False)


class PairsPathResolver:
    def __init__(self, index_path: str) -> None:
        self.index_path = index_path

    def expected_pairs_path(
        self, emp_name: str, file_name: str | None, file_id: str | None
    ) -> str:
        base_dir = os.path.dirname(os.path.abspath(self.index_path))
        safe_emp = safe_name(emp_name or "unknown")
        base_name = safe_name(file_name or "unknown.pdf")
        if not base_name.lower().endswith(".pdf"):
            base_name = f"{base_name}.pdf"
        if file_id:
            file_tag = f"{os.path.splitext(base_name)[0]}__{file_id[:8]}"
        else:
            file_tag = os.path.splitext(base_name)[0]
        return os.path.abspath(os.path.join(base_dir, safe_emp, file_tag, "pairs.csv"))

    def resolve_pairs_path(
        self,
        emp_name: str,
        file_name: str | None,
        file_id: str | None,
        pairs_rel: str | None,
    ) -> str:
        if pairs_rel:
            return os.path.abspath(
                os.path.join(os.path.dirname(os.path.abspath(self.index_path)), pairs_rel)
            )
        return self.expected_pairs_path(emp_name, file_name, file_id)

    def path_for_log(self, path: str) -> str:
        base_dir = os.path.dirname(os.path.abspath(self.index_path))
        try:
            return os.path.relpath(path, start=base_dir)
        except ValueError:
            return os.path.abspath(path)


class PairsCloser:
    def __init__(
        self,
        *,
        max_gap_hours: float = 16.0,
        mark_inferred: bool = False,
        preserve_exit_raw: bool = False,
        clear_duration_hhmm: bool = False,
    ) -> None:
        self.max_gap_hours = max_gap_hours
        self.mark_inferred = mark_inferred
        self.preserve_exit_raw = preserve_exit_raw
        self.clear_duration_hhmm = clear_duration_hhmm

    def close(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
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


class ShiftClassifier:
    def __init__(
        self,
        *,
        calendar: HolidayCalendar | None = None,
        include_holidays: bool = False,
        match_policy: TurnoMatchPolicy | None = None,
    ) -> None:
        self.calendar = calendar
        self.include_holidays = include_holidays
        self.match_policy = match_policy or TurnoMatchPolicy()

    def classify(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
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
        working["is_night"] = self.match_policy.match(turno_norm, "notte")
        working["is_afternoon"] = self.match_policy.match(turno_norm, "pomeriggio")

        is_sunday = working["entry_ts"].dt.dayofweek == 6
        if self.include_holidays and self.calendar is not None:
            years = sorted({int(y) for y in working["anno"].dropna().unique()})
            holiday_dates = self.calendar.dates_for_years(years)
            is_holiday = is_sunday | working["entry_ts"].dt.date.isin(holiday_dates)
        else:
            is_holiday = is_sunday
        working["is_holiday"] = is_holiday

        return working


class EmployeeGrouper:
    def __init__(self, normalize: Callable[[str | None], str]) -> None:
        self.normalize = normalize

    def key(self, name: str | None, employee_id: str | None) -> str:
        if employee_id:
            return f"id:{employee_id}"
        norm = self.normalize(name)
        return f"name:{norm or 'unknown'}"

    def group(self, files: list[Any]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        missing_name = 0
        missing_id = 0
        for item in files:
            name = getattr(item, "employee", None) or "unknown"
            if name == "unknown":
                missing_name += 1
            employee_id = getattr(item, "employee_id", None)
            if not employee_id:
                missing_id += 1
            key = self.key(name, employee_id)
            if key not in grouped:
                grouped[key] = {
                    "employee": name,
                    "employee_id": employee_id,
                    "files": [],
                    "key": key,
                }
            grouped[key]["files"].append(item)
        logger.debug(
            "Grouped %s files into %s employees (missing_name=%s missing_id=%s)",
            len(files),
            len(grouped),
            missing_name,
            missing_id,
        )
        return list(grouped.values())
