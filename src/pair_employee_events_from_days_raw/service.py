from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.names import safe_name
from src.shift_services import PairsCloser, compute_turno, to_datetime_series

from .options import DEFAULT_MAX_GAP_HOURS, DEFAULT_OUTPUT_DIR

logger = logging.getLogger(__name__)


def normalize_employee(name: str | None) -> str:
    return " ".join((name or "").strip().lower().split()) or "unknown"


def _event_sort_key(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    if "source_row_index" not in working.columns:
        working["source_row_index"] = pd.NA
    if "event_index" not in working.columns:
        working["event_index"] = pd.NA
    working["source_row_index"] = pd.to_numeric(working["source_row_index"], errors="coerce")
    working["event_index"] = pd.to_numeric(working["event_index"], errors="coerce")
    return working


def _empty_pair_rows() -> pd.DataFrame:
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


def _normalize_events_file(
    df: pd.DataFrame,
    *,
    source_events_csv: str,
    file_id: str | None,
    file_name: str | None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {
        "events_rows_in": int(len(df)),
        "events_valid": 0,
        "events_invalid_kind": 0,
        "events_invalid_ts": 0,
    }
    if df.empty:
        return pd.DataFrame(), stats
    if "event_kind" not in df.columns or "event_ts" not in df.columns:
        stats["events_invalid_kind"] = int(len(df))
        return pd.DataFrame(), stats

    kind = df["event_kind"].fillna("").astype(str).str.strip().str.upper()
    ts = to_datetime_series(df["event_ts"])

    valid_kind = kind.isin(["E", "U"])
    valid_ts = ts.notna()
    valid_mask = valid_kind & valid_ts

    stats["events_valid"] = int(valid_mask.sum())
    stats["events_invalid_kind"] = int((~valid_kind).sum())
    stats["events_invalid_ts"] = int((valid_kind & ~valid_ts).sum())

    if not valid_mask.any():
        return pd.DataFrame(), stats

    working = df.loc[valid_mask].copy()
    working["event_kind"] = kind.loc[valid_mask].astype("object")
    working["event_ts"] = ts.loc[valid_mask]
    if "event_raw" not in working.columns:
        working["event_raw"] = None
    if "source_row_index" not in working.columns:
        working["source_row_index"] = pd.NA
    if "event_index" not in working.columns:
        working["event_index"] = pd.NA
    working["file_id"] = file_id
    working["file_name"] = file_name
    working["source_events_csv"] = source_events_csv
    return _event_sort_key(working), stats


def _events_to_partial_pairs(events_df: pd.DataFrame) -> pd.DataFrame:
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


def _dedupe_events(events_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
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


def _dedupe_closed_pairs(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
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


def _format_output_pairs(df: pd.DataFrame, *, keep_inferred_column: bool) -> pd.DataFrame:
    if df.empty:
        return _empty_pair_rows()

    working = df.copy()
    working["entry_ts"] = to_datetime_series(working["entry_ts"])
    working["exit_ts"] = to_datetime_series(working["exit_ts"])
    working = working.loc[working["entry_ts"].notna() & working["exit_ts"].notna()].copy()
    if working.empty:
        return _empty_pair_rows()

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


def process_one_employee_events(
    employee: dict[str, Any],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    keep_inferred_column: bool = False,
) -> dict[str, Any]:
    employee_name = str(employee.get("employee") or "unknown")
    employee_id = employee.get("employee_id")
    employee_files = list(employee.get("files") or [])
    result: dict[str, Any] = {
        "status": "error",
        "source_employee": employee_name,
        "employee_id": employee_id,
        "files_total": len(employee_files),
        "files_loaded": 0,
        "files_missing": 0,
        "files_error": 0,
        "events_rows_in": 0,
        "events_valid": 0,
        "events_invalid_kind": 0,
        "events_invalid_ts": 0,
        "events_deduped": 0,
        "partial_rows": 0,
        "pairs_out": 0,
        "pairs_deduped": 0,
        "inferred_pairs": 0,
        "rows_unmatched_after_close": 0,
        "output_csv": None,
        "missing_event_files": [],
        "error_event_files": [],
        "error_code": None,
        "error": None,
    }

    try:
        closer = PairsCloser(
            max_gap_hours=max_gap_hours,
            mark_inferred=True,
            preserve_exit_raw=True,
            clear_duration_hhmm=True,
        )
        events_frames: list[pd.DataFrame] = []

        for file_desc in employee_files:
            source_events_csv = str(file_desc.get("events_csv") or "")
            file_id = file_desc.get("file_id")
            file_name = file_desc.get("file_name")

            if not source_events_csv or not os.path.exists(source_events_csv):
                result["files_missing"] += 1
                result["missing_event_files"].append(
                    {
                        "employee": employee_name,
                        "file_id": str(file_id or ""),
                        "events_csv": source_events_csv,
                    }
                )
                continue

            try:
                raw_df = pd.read_csv(source_events_csv)
                normalized, load_stats = _normalize_events_file(
                    raw_df,
                    source_events_csv=source_events_csv,
                    file_id=file_id,
                    file_name=file_name,
                )
            except Exception as exc:
                result["files_error"] += 1
                result["error_event_files"].append(
                    {
                        "employee": employee_name,
                        "file_id": str(file_id or ""),
                        "events_csv": source_events_csv,
                        "error": str(exc),
                    }
                )
                logger.exception("Errore leggendo %s", source_events_csv)
                continue

            result["files_loaded"] += 1
            for key in ("events_rows_in", "events_valid", "events_invalid_kind", "events_invalid_ts"):
                result[key] += int(load_stats[key])

            if not normalized.empty:
                events_frames.append(normalized)

        if int(result["files_loaded"]) <= 0:
            result["error_code"] = "no_events_loaded"
            result["error"] = (
                "No readable events files for employee "
                f"{employee_name}: missing={result['files_missing']} read_errors={result['files_error']}"
            )
            return result

        if events_frames:
            events_merged = pd.concat(events_frames, ignore_index=True)
            events_merged = events_merged.sort_values(
                by=["event_ts", "source_events_csv", "source_row_index", "event_index"],
                kind="stable",
            )
            events_merged, events_deduped = _dedupe_events(events_merged)
            result["events_deduped"] = int(events_deduped)
            partial_pairs = _events_to_partial_pairs(events_merged)
        else:
            partial_pairs = _events_to_partial_pairs(pd.DataFrame())

        partial_rows = int(len(partial_pairs))
        result["partial_rows"] = partial_rows

        if partial_pairs.empty:
            closed = partial_pairs.copy()
        else:
            closed = closer.close(partial_pairs)

        inferred_pairs = 0
        if "closed_inferred" in closed.columns:
            inferred_pairs = int(closed["closed_inferred"].fillna(False).sum())
        result["inferred_pairs"] = inferred_pairs
        result["rows_unmatched_after_close"] = max(
            0, partial_rows - int(len(closed)) - inferred_pairs
        )

        closed, pairs_deduped = _dedupe_closed_pairs(closed)
        result["pairs_deduped"] = int(pairs_deduped)

        out_df = _format_output_pairs(closed, keep_inferred_column=keep_inferred_column)
        result["pairs_out"] = int(len(out_df))

        employee_safe = safe_name(employee_name)
        out_path = os.path.abspath(os.path.join(output_dir, f"{employee_safe}.pairs.csv"))
        ensure_parent_dir(out_path)
        out_df.to_csv(out_path, index=False)
        result["output_csv"] = out_path

        result["status"] = "ok"
        return result
    except Exception as exc:
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def process_many_employee_events(
    employees: Iterable[dict[str, Any]],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    keep_inferred_column: bool = False,
    input_mode: str | None = None,
    input_dir: str | None = None,
    index_path: str | None = None,
    events_name: str | None = None,
    discovered_event_files_total: int = 0,
) -> dict[str, Any]:
    normalized_employees = list(employees)

    totals: dict[str, Any] = {
        "files_total": len(normalized_employees),
        "files_processed": 0,
        "files_error": 0,
        "employees_total": len(normalized_employees),
        "employees_processed": 0,
        "employees_with_pairs": 0,
        "event_files_total": 0,
        "event_files_loaded": 0,
        "event_files_missing": 0,
        "event_files_error": 0,
        "events_rows_in": 0,
        "events_valid": 0,
        "events_invalid_kind": 0,
        "events_invalid_ts": 0,
        "events_deduped": 0,
        "partial_rows": 0,
        "pairs_out": 0,
        "pairs_deduped": 0,
        "inferred_pairs": 0,
        "rows_unmatched_after_close": 0,
        "input_mode": input_mode,
        "input_dir": os.path.abspath(input_dir) if input_dir else None,
        "index_path": os.path.abspath(index_path) if index_path else None,
        "output_dir": os.path.abspath(output_dir),
        "events_name": events_name,
        "max_gap_hours": float(max_gap_hours),
        "discovered_event_files_total": int(discovered_event_files_total),
    }

    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    by_employee: list[dict[str, Any]] = []
    missing_event_files: list[dict[str, str]] = []
    error_event_files: list[dict[str, str]] = []

    for employee_index, employee in enumerate(normalized_employees, start=1):
        employee_name = str(employee.get("employee") or "unknown")
        logger.info(
            "Dipendente %s/%s: %s (file=%s)",
            employee_index,
            len(normalized_employees),
            employee_name,
            len(list(employee.get("files") or [])),
        )

        result = process_one_employee_events(
            employee,
            output_dir=output_dir,
            max_gap_hours=max_gap_hours,
            keep_inferred_column=keep_inferred_column,
        )
        items.append(result)
        by_employee.append(
            {
                "employee": result["source_employee"],
                "employee_id": result["employee_id"],
                "files_total": int(result["files_total"]),
                "files_loaded": int(result["files_loaded"]),
                "files_missing": int(result["files_missing"]),
                "files_error": int(result["files_error"]),
                "events_rows_in": int(result["events_rows_in"]),
                "events_valid": int(result["events_valid"]),
                "events_invalid_kind": int(result["events_invalid_kind"]),
                "events_invalid_ts": int(result["events_invalid_ts"]),
                "events_deduped": int(result["events_deduped"]),
                "partial_rows": int(result["partial_rows"]),
                "pairs_out": int(result["pairs_out"]),
                "pairs_deduped": int(result["pairs_deduped"]),
                "inferred_pairs": int(result["inferred_pairs"]),
                "rows_unmatched_after_close": int(result["rows_unmatched_after_close"]),
                "output_csv": result["output_csv"],
                "status": result["status"],
                "error_code": result["error_code"],
                "error": result["error"],
            }
        )

        totals["employees_processed"] += 1
        if result["status"] == "ok":
            totals["files_processed"] += 1
        else:
            totals["files_error"] += 1
            errors.append(
                {
                    "employee": str(result["source_employee"]),
                    "error": str(result["error"]),
                }
            )

        if int(result["pairs_out"]) > 0:
            totals["employees_with_pairs"] += 1

        totals["event_files_total"] += int(result["files_total"])
        totals["event_files_loaded"] += int(result["files_loaded"])
        totals["event_files_missing"] += int(result["files_missing"])
        totals["event_files_error"] += int(result["files_error"])
        for key in (
            "events_rows_in",
            "events_valid",
            "events_invalid_kind",
            "events_invalid_ts",
            "events_deduped",
            "partial_rows",
            "pairs_out",
            "pairs_deduped",
            "inferred_pairs",
            "rows_unmatched_after_close",
        ):
            totals[key] += int(result[key])

        missing_event_files.extend(list(result["missing_event_files"]))
        error_event_files.extend(list(result["error_event_files"]))

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "by_employee": by_employee,
        "missing_event_files": missing_event_files,
        "error_event_files": error_event_files,
    }
