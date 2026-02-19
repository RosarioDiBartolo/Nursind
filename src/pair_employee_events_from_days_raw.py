from __future__ import annotations

"""Pair cleaned raw E/U events at employee scope across multiple files."""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from src.drive_service.fs_utils import ensure_dir, ensure_parent_dir
from src.drive_service.logging_utils import setup_logging
from src.drive_service.index import MapIndex
from src.drive_service.names import safe_name
from src.shift_services import (
    EmployeeGrouper,
    PairsCloser,
    PairsPathResolver,
    compute_turno,
    to_datetime_series,
)

logger = logging.getLogger(__name__)

DEFAULT_INPUT_DIR = "output/parsed_from_text"
DEFAULT_INDEX = None
DEFAULT_OUTPUT_DIR = "output/employee_shifts_from_raw"
DEFAULT_EVENTS_NAME = "events_from_days_raw.cleaned.csv"
DEFAULT_REPORT_JSON = "output/employee_shifts_from_raw/pair_employee_events_from_days_raw.report.json"
DEFAULT_MAX_GAP_HOURS = 16.0


def normalize_employee(name: str | None) -> str:
    return " ".join((name or "").strip().lower().split()) or "unknown"


def _item_attr(item: Any, name: str) -> Any:
    if isinstance(item, dict):
        return item.get(name)
    return getattr(item, name, None)


def _discover_employees_from_events_dir(
    *,
    input_dir: str,
    events_name: str,
) -> tuple[list[dict[str, Any]], int]:
    base = Path(input_dir)
    event_files = sorted(base.rglob(events_name))
    grouped: dict[str, dict[str, Any]] = {}

    for event_path in event_files:
        rel = event_path.relative_to(base)
        if len(rel.parts) >= 2:
            employee_name = rel.parts[0]
        else:
            employee_name = "unknown"
        key = normalize_employee(employee_name)
        if key not in grouped:
            grouped[key] = {
                "employee": employee_name,
                "employee_id": None,
                "files": [],
                "key": f"name:{key}",
            }
        grouped[key]["files"].append(
            {
                "events_csv": str(event_path.resolve()),
                "file_id": None,
                "file_name": event_path.parent.name,
            }
        )

    return list(grouped.values()), len(event_files)


def _event_sort_key(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    if "source_row_index" not in working.columns:
        working["source_row_index"] = pd.NA
    if "event_index" not in working.columns:
        working["event_index"] = pd.NA
    working["source_row_index"] = pd.to_numeric(working["source_row_index"], errors="coerce")
    working["event_index"] = pd.to_numeric(working["event_index"], errors="coerce")
    return working


def _resolve_cleaned_events_path(
    *,
    resolver: PairsPathResolver,
    index_dir: str,
    inc: Any,
    emp_name: str,
    events_name: str,
) -> str:
    outputs = getattr(inc, "outputs", None)
    days_rel = getattr(outputs, "days_csv", None) if outputs else None
    if days_rel:
        days_abs = os.path.abspath(os.path.join(index_dir, days_rel))
    else:
        expected_pairs = resolver.expected_pairs_path(
            emp_name,
            getattr(inc, "file_name", None),
            getattr(inc, "file_id", None),
        )
        days_abs = os.path.join(os.path.dirname(expected_pairs), "days.csv")
    return os.path.abspath(os.path.join(os.path.dirname(days_abs), events_name))


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
        working.loc[overnight, "exit_ts"] = working.loc[overnight, "exit_ts"] + pd.Timedelta(days=1)

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


def pair_employee_events(
    *,
    input_dir: str | None = DEFAULT_INPUT_DIR,
    index_path: str | None = DEFAULT_INDEX,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    events_name: str = DEFAULT_EVENTS_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    employee_filter: str | None = None,
    keep_inferred_column: bool = False,
) -> dict[str, Any]:
    ensure_dir(output_dir)
    use_folder_mode = bool(input_dir)

    employees: list[dict[str, Any]]
    resolver: PairsPathResolver | None = None
    index_dir = ""
    index_abs: str | None = None
    discovered_event_files_total = 0

    if use_folder_mode:
        employees, discovered_event_files_total = _discover_employees_from_events_dir(
            input_dir=str(input_dir),
            events_name=events_name,
        )
    else:
        if not index_path:
            raise ValueError("Either --input-dir or --index must be provided")
        index_abs = os.path.abspath(index_path)
        index_dir = os.path.dirname(index_abs)
        report = MapIndex.load_index(index_abs, strict=True)
        resolver = PairsPathResolver(index_abs)
        grouper = EmployeeGrouper(normalize_employee)
        employees = grouper.group(list(report.files.values()))

    if employee_filter:
        token = normalize_employee(employee_filter)
        employees = [
            emp for emp in employees if normalize_employee(emp.get("employee")) == token
        ]

    closer = PairsCloser(
        max_gap_hours=max_gap_hours,
        mark_inferred=True,
        preserve_exit_raw=True,
        clear_duration_hhmm=True,
    )

    totals: dict[str, Any] = {
        "employees_total": len(employees),
        "employees_processed": 0,
        "employees_with_pairs": 0,
        "files_total": 0,
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
        "input_mode": "folder" if use_folder_mode else "index",
        "input_dir": os.path.abspath(str(input_dir)) if use_folder_mode else None,
        "index_path": index_abs,
        "output_dir": os.path.abspath(output_dir),
        "events_name": events_name,
        "max_gap_hours": float(max_gap_hours),
        "discovered_event_files_total": int(discovered_event_files_total),
    }
    by_employee: list[dict[str, Any]] = []
    missing_files: list[dict[str, str]] = []
    error_files: list[dict[str, str]] = []

    for emp_idx, emp in enumerate(employees, start=1):
        emp_name = emp.get("employee", "unknown")
        emp_id = emp.get("employee_id")
        emp_files = emp.get("files", [])
        logger.info(
            "Dipendente %s/%s: %s (file=%s)",
            emp_idx,
            len(employees),
            emp_name,
            len(emp_files),
        )

        employee_stats: dict[str, Any] = {
            "employee": emp_name,
            "employee_id": emp_id,
            "files_total": len(emp_files),
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
        }

        events_frames: list[pd.DataFrame] = []
        for inc in emp_files:
            totals["files_total"] += 1
            if use_folder_mode:
                source_events_csv = str(_item_attr(inc, "events_csv") or "")
            else:
                assert resolver is not None
                source_events_csv = _resolve_cleaned_events_path(
                    resolver=resolver,
                    index_dir=index_dir,
                    inc=inc,
                    emp_name=emp_name,
                    events_name=events_name,
                )
            file_id = _item_attr(inc, "file_id")
            file_name = _item_attr(inc, "file_name")
            if not os.path.exists(source_events_csv):
                totals["files_missing"] += 1
                employee_stats["files_missing"] += 1
                missing_files.append(
                    {
                        "employee": emp_name,
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
                totals["files_error"] += 1
                employee_stats["files_error"] += 1
                error_files.append(
                    {
                        "employee": emp_name,
                        "file_id": str(file_id or ""),
                        "events_csv": source_events_csv,
                        "error": str(exc),
                    }
                )
                logger.exception("Errore leggendo %s", source_events_csv)
                continue

            totals["files_loaded"] += 1
            employee_stats["files_loaded"] += 1

            for key in ("events_rows_in", "events_valid", "events_invalid_kind", "events_invalid_ts"):
                totals[key] += int(load_stats[key])
                employee_stats[key] += int(load_stats[key])

            if not normalized.empty:
                events_frames.append(normalized)

        if not events_frames:
            by_employee.append(employee_stats)
            totals["employees_processed"] += 1
            continue

        events_merged = pd.concat(events_frames, ignore_index=True)
        events_merged = events_merged.sort_values(
            by=["event_ts", "source_events_csv", "source_row_index", "event_index"],
            kind="stable",
        )
        events_merged, events_deduped = _dedupe_events(events_merged)
        employee_stats["events_deduped"] = int(events_deduped)
        totals["events_deduped"] += int(events_deduped)

        partial_pairs = _events_to_partial_pairs(events_merged)
        partial_rows = int(len(partial_pairs))
        employee_stats["partial_rows"] = partial_rows
        totals["partial_rows"] += partial_rows

        closed = closer.close(partial_pairs)
        inferred_pairs = 0
        if "closed_inferred" in closed.columns:
            inferred_pairs = int(closed["closed_inferred"].fillna(False).sum())
        employee_stats["inferred_pairs"] = inferred_pairs
        totals["inferred_pairs"] += inferred_pairs

        rows_unmatched_after_close = max(0, partial_rows - int(len(closed)) - inferred_pairs)
        employee_stats["rows_unmatched_after_close"] = rows_unmatched_after_close
        totals["rows_unmatched_after_close"] += rows_unmatched_after_close

        closed, pairs_deduped = _dedupe_closed_pairs(closed)
        employee_stats["pairs_deduped"] = int(pairs_deduped)
        totals["pairs_deduped"] += int(pairs_deduped)

        out_df = _format_output_pairs(closed, keep_inferred_column=keep_inferred_column)
        employee_stats["pairs_out"] = int(len(out_df))
        totals["pairs_out"] += int(len(out_df))
        if not out_df.empty:
            totals["employees_with_pairs"] += 1

        emp_safe = safe_name(emp_name)
        out_path = os.path.abspath(os.path.join(output_dir, f"{emp_safe}.pairs.csv"))
        ensure_parent_dir(out_path)
        out_df.to_csv(out_path, index=False)
        employee_stats["output_csv"] = out_path
        by_employee.append(employee_stats)
        totals["employees_processed"] += 1

    report_out = {
        "stats": totals,
        "by_employee": by_employee,
        "missing_event_files": missing_files,
        "error_event_files": error_files,
    }
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report_out, handle, ensure_ascii=False, indent=2)
    return report_out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Accoppia eventi E/U puliti a livello dipendente su tutti i file, "
            "consentendo accoppiamenti cross-file."
        )
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=(
            "Directory radice con eventi per-file in struttura "
            "<employee>/<document>/events_from_days_raw.cleaned.csv "
            "(default: output/parsed_from_text)"
        ),
    )
    parser.add_argument(
        "--index",
        default=DEFAULT_INDEX,
        help=(
            "Path to included.index.json (legacy mode, used only when --input-dir is empty; "
            "default: disabled)"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory output separata per i pairs per dipendente.",
    )
    parser.add_argument(
        "--events-name",
        default=DEFAULT_EVENTS_NAME,
        help=(
            "Nome file eventi puliti da leggere accanto a days.csv "
            "(default: events_from_days_raw.cleaned.csv)"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help="Path report JSON finale.",
    )
    parser.add_argument(
        "--max-gap-hours",
        type=float,
        default=DEFAULT_MAX_GAP_HOURS,
        help="Massimo gap ore per chiudere entry/exit (default: 16).",
    )
    parser.add_argument(
        "--employee",
        help="Filtra per dipendente (case-insensitive).",
    )
    parser.add_argument(
        "--keep-inferred-column",
        action="store_true",
        help="Mantieni la colonna closed_inferred nell'output per dipendente.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    report = pair_employee_events(
        input_dir=args.input_dir,
        index_path=args.index,
        output_dir=args.output_dir,
        events_name=args.events_name,
        report_json=args.report_json,
        max_gap_hours=args.max_gap_hours,
        employee_filter=args.employee,
        keep_inferred_column=args.keep_inferred_column,
    )
    stats = report["stats"]
    logger.info(
        "Completato: employees=%s processed=%s with_pairs=%s files(total=%s loaded=%s missing=%s error=%s) events_deduped=%s pairs=%s pairs_deduped=%s inferred=%s unmatched=%s",
        stats["employees_total"],
        stats["employees_processed"],
        stats["employees_with_pairs"],
        stats["files_total"],
        stats["files_loaded"],
        stats["files_missing"],
        stats["files_error"],
        stats["events_deduped"],
        stats["pairs_out"],
        stats["pairs_deduped"],
        stats["inferred_pairs"],
        stats["rows_unmatched_after_close"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
