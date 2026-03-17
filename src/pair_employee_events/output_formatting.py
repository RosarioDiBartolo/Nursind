from __future__ import annotations

import pandas as pd

from cartellino_parser.shift_services import compute_turno, to_datetime_series


def empty_pair_rows() -> pd.DataFrame:
    return pd.DataFrame(
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
            "file_id",
            "file_name",
            "source_csv",
        ]
    )


def dedupe_closed_pairs(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df, 0
    if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
        return df, 0
    working = df.copy()
    before = len(working)
    working = working.sort_values(
        by=["entry_ts", "exit_ts", "source_csv", "file_name"],
        kind="stable",
    )
    working = working.drop_duplicates(subset=["entry_ts", "exit_ts"], keep="first")
    deduped = before - len(working)
    return working.reset_index(drop=True), int(deduped)


def _duration_hhmm(entry: pd.Series, exit_: pd.Series) -> pd.Series:
    complete_mask = entry.notna() & exit_.notna()
    out = pd.Series([None] * len(entry), index=entry.index, dtype="object")
    if not complete_mask.any():
        return out
    duration = exit_.loc[complete_mask] - entry.loc[complete_mask]
    minutes = (duration.dt.total_seconds() / 60.0).round().astype("Int64")
    hours = (minutes // 60).astype("Int64")
    mins = (minutes % 60).astype("Int64")
    out.loc[complete_mask] = (
        hours.astype(int).map("{:02d}".format)
        + ":"
        + mins.astype(int).map("{:02d}".format)
    )
    return out


def _turno_from_entry(entry: pd.Series) -> pd.Series:
    out = pd.Series([None] * len(entry), index=entry.index, dtype="object")
    valid = entry.notna()
    if not valid.any():
        return out
    out.loc[valid] = entry.loc[valid].map(compute_turno)
    return out


def format_output_pairs(df: pd.DataFrame, *, keep_inferred_column: bool) -> pd.DataFrame:
    if df.empty:
        return empty_pair_rows()

    working = df.copy()
    working["entry_ts"] = to_datetime_series(working["entry_ts"])
    working["exit_ts"] = to_datetime_series(working["exit_ts"])
    working = working.loc[working["entry_ts"].notna() & working["exit_ts"].notna()].copy()
    if working.empty:
        return empty_pair_rows()

    overnight = working["exit_ts"] < working["entry_ts"]
    if overnight.any():
        working.loc[overnight, "exit_ts"] = working.loc[overnight, "exit_ts"] + pd.Timedelta(
            days=1
        )

    working = working.sort_values(by=["entry_ts", "exit_ts"], kind="stable")
    working["year"] = working["entry_ts"].dt.year
    working["month"] = working["entry_ts"].dt.month
    working["day"] = working["entry_ts"].dt.day
    working["dow"] = working["dow"].where(working["dow"].notna(), None)
    working["pair_index"] = (
        working.groupby(["year", "month", "day"], dropna=False).cumcount().astype(int)
    )
    working["duration_hhmm"] = _duration_hhmm(working["entry_ts"], working["exit_ts"])
    working["turno"] = _turno_from_entry(working["entry_ts"])

    working["entry_ts"] = working["entry_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    working["exit_ts"] = working["exit_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")

    preferred = [
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
        "file_id",
        "file_name",
        "source_csv",
    ]
    if keep_inferred_column and "closed_inferred" in working.columns:
        preferred.append("closed_inferred")
    missing = [col for col in preferred if col not in working.columns]
    for col in missing:
        working[col] = None
    return working[preferred].reset_index(drop=True)


__all__ = [
    "dedupe_closed_pairs",
    "empty_pair_rows",
    "format_output_pairs",
]

