from __future__ import annotations

import pandas as pd

from cartellino_parser.shift_services import to_datetime_series


def normalize_employee(name: str | None) -> str:
    return " ".join((name or "").strip().lower().split()) or "unknown"


def _event_sort_key(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    if "source_row_index" not in working.columns:
        if "source_line_no" in working.columns:
            working["source_row_index"] = working["source_line_no"]
        else:
            working["source_row_index"] = pd.NA
    if "event_index" not in working.columns:
        working["event_index"] = pd.NA
    working["source_row_index"] = pd.to_numeric(working["source_row_index"], errors="coerce")
    working["event_index"] = pd.to_numeric(working["event_index"], errors="coerce")
    return working


def normalize_events_file(
    df: pd.DataFrame,
    *,
    source_events_csv: str,
    file_id: str | None,
    file_name: str | None,
    source_employee: str | None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    scoped_df = df
    if source_employee and "source_employee" in df.columns:
        employee_key = normalize_employee(source_employee)
        employee_series = df["source_employee"].fillna("").astype(str).map(normalize_employee)
        scoped_df = df.loc[employee_series == employee_key].copy()

    stats = {
        "events_rows_in": int(len(scoped_df)),
        "events_valid": 0,
        "events_invalid_kind": 0,
        "events_invalid_ts": 0,
    }
    if scoped_df.empty:
        return pd.DataFrame(), stats
    if "event_kind" not in scoped_df.columns or "event_ts" not in scoped_df.columns:
        stats["events_invalid_kind"] = int(len(scoped_df))
        return pd.DataFrame(), stats

    kind = scoped_df["event_kind"].fillna("").astype(str).str.strip().str.upper()
    ts = to_datetime_series(scoped_df["event_ts"])

    valid_kind = kind.isin(["E", "U"])
    valid_ts = ts.notna()
    valid_mask = valid_kind & valid_ts

    stats["events_valid"] = int(valid_mask.sum())
    stats["events_invalid_kind"] = int((~valid_kind).sum())
    stats["events_invalid_ts"] = int((valid_kind & ~valid_ts).sum())

    if not valid_mask.any():
        return pd.DataFrame(), stats

    working = scoped_df.loc[valid_mask].copy()
    working["event_kind"] = kind.loc[valid_mask].astype("object")
    working["event_ts"] = ts.loc[valid_mask]

    if "event_raw" not in working.columns:
        working["event_raw"] = None
    if "source_row_index" not in working.columns:
        if "source_line_no" in working.columns:
            working["source_row_index"] = scoped_df.loc[valid_mask, "source_line_no"]
        else:
            working["source_row_index"] = pd.NA
    if "event_index" not in working.columns:
        working["event_index"] = pd.NA
    if file_id is not None:
        working["file_id"] = file_id
    elif "file_id" not in working.columns:
        working["file_id"] = None

    if file_name is not None:
        working["file_name"] = file_name
    elif "file_name" not in working.columns:
        working["file_name"] = None

    working["source_events_csv"] = source_events_csv
    return _event_sort_key(working), stats


def events_to_partial_pairs(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "month",
                "day",
                "dow",
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

    events = events_df.sort_values(
        by=["event_ts", "source_events_csv", "source_row_index", "event_index"],
        kind="stable",
    ).copy()

    entry = events.loc[events["event_kind"] == "E"].copy()
    entry["entry_ts"] = entry["event_ts"]
    entry["exit_ts"] = pd.NaT
    entry["entry_raw"] = entry.get("event_raw")
    entry["exit_raw"] = None
    entry["source_csv"] = entry["source_events_csv"]

    exit_ = events.loc[events["event_kind"] == "U"].copy()
    exit_["entry_ts"] = pd.NaT
    exit_["exit_ts"] = exit_["event_ts"]
    exit_["entry_raw"] = None
    exit_["exit_raw"] = exit_.get("event_raw")
    exit_["source_csv"] = exit_["source_events_csv"]

    out = pd.concat([entry, exit_], ignore_index=True)
    out["_sort_ts"] = out["entry_ts"].combine_first(out["exit_ts"])
    out = out.sort_values(
        by=["_sort_ts", "source_csv", "source_row_index", "event_index"],
        kind="stable",
    )

    out["year"] = out["_sort_ts"].dt.year
    out["month"] = out["_sort_ts"].dt.month
    out["day"] = out["_sort_ts"].dt.day
    out["dow"] = None
    out["duration_hhmm"] = None
    out["turno"] = None
    return out[
        [
            "year",
            "month",
            "day",
            "dow",
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
    ].reset_index(drop=True)


def dedupe_events(events_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if events_df.empty:
        return events_df, 0
    working = events_df.copy()
    if "event_raw" not in working.columns:
        working["event_raw"] = None
    working["_event_raw_key"] = working["event_raw"].fillna("").astype(str).str.strip()
    before = len(working)
    working = working.sort_values(
        by=[
            "event_ts",
            "event_kind",
            "_event_raw_key",
            "source_events_csv",
            "source_row_index",
            "event_index",
        ],
        kind="stable",
    )
    working = working.drop_duplicates(
        subset=["event_kind", "event_ts", "_event_raw_key"],
        keep="first",
    )
    deduped = before - len(working)
    return working.drop(columns=["_event_raw_key"]).reset_index(drop=True), int(deduped)


__all__ = [
    "dedupe_events",
    "events_to_partial_pairs",
    "normalize_employee",
    "normalize_events_file",
]

