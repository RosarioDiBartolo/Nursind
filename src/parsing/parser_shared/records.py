from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, Iterable, List

import pandas as pd

from .models import DayRecord


def records_to_df(records: Iterable[DayRecord]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in records:
        rows.append(asdict(record))
    return pd.DataFrame(
        rows,
        columns=["year", "month", "day", "dow", "mo_f", "mo_t", "mo_lav", "raw"],
    )


def apply_pairs_to_days(
    days_df: pd.DataFrame,
    pairs_df: pd.DataFrame,
    columns: Iterable[str] = ("mo_lav",),
) -> pd.DataFrame:
    if days_df.empty or pairs_df is None or pairs_df.empty:
        return days_df
    if "entry_ts" not in pairs_df.columns or "exit_ts" not in pairs_df.columns:
        return days_df

    durations = pairs_df.dropna(subset=["entry_ts", "exit_ts"]).copy()
    if durations.empty:
        return days_df

    durations["duration_hours"] = (
        durations["exit_ts"] - durations["entry_ts"]
    ).dt.total_seconds() / 3600.0
    grouped = (
        durations.groupby(["year", "month", "day"], dropna=False)["duration_hours"]
        .sum()
        .reset_index()
    )

    updated = days_df.merge(grouped, on=["year", "month", "day"], how="left")
    mask = updated["duration_hours"].notna()
    for column in columns:
        if column in updated.columns:
            updated.loc[mask, column] = updated.loc[mask, "duration_hours"]
    updated = updated.drop(columns=["duration_hours"])
    return updated
