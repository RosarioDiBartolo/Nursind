from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir

from .options import (
    DEFAULT_EVENTS_NAME,
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
)

logger = logging.getLogger(__name__)


def _midnight_mask(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    index = df.index
    ts_midnight = pd.Series(False, index=index, dtype="bool")
    hhmm_midnight = pd.Series(False, index=index, dtype="bool")

    if "event_ts" in df.columns:
        dt = pd.to_datetime(df["event_ts"], errors="coerce")
        ts_midnight = (
            dt.notna()
            & (dt.dt.hour == 0)
            & (dt.dt.minute == 0)
            & (dt.dt.second == 0)
        )

    if "event_time_hhmm" in df.columns:
        hhmm = df["event_time_hhmm"].fillna("").astype(str).str.strip()
        hhmm_midnight = hhmm.isin({"00:00", "0:00", "24:00"})

    mask = ts_midnight | hhmm_midnight
    reason = pd.Series("", index=index, dtype="object")
    reason.loc[ts_midnight] = "event_ts_midnight"
    reason.loc[~ts_midnight & hhmm_midnight] = "event_time_hhmm_midnight"
    return mask, reason


def _clean_events_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    stats = {
        "rows_in": int(len(df)),
        "rows_removed_midnight": 0,
        "rows_out": 0,
    }
    if df.empty:
        return df.copy(), pd.DataFrame(columns=list(df.columns) + ["filter_reason"]), stats

    mask, reason = _midnight_mask(df)
    removed = df.loc[mask].copy()
    if not removed.empty:
        removed["filter_reason"] = reason.loc[removed.index].astype(str)
    cleaned = df.loc[~mask].copy()

    stats["rows_removed_midnight"] = int(mask.sum())
    stats["rows_out"] = int(len(cleaned))
    return cleaned, removed, stats


def _removed_examples(removed: pd.DataFrame, limit: int) -> list[str]:
    if removed.empty or limit <= 0:
        return []

    if "raw" in removed.columns:
        values = removed["raw"].fillna("").astype(str).str.strip()
    elif "event_raw" in removed.columns:
        values = removed["event_raw"].fillna("").astype(str).str.strip()
    else:
        values = pd.Series([""] * len(removed), index=removed.index, dtype="object")

    out: list[str] = []
    for value in values:
        if not value or value in out:
            continue
        out.append(value)
        if len(out) >= limit:
            break
    return out


def _build_cleaned_output_path(
    event_path: Path,
    *,
    input_base: Path | None,
    output_base: Path,
    out_name: str,
    in_place: bool,
) -> Path:
    if in_place:
        return event_path

    if event_path.name == "events_from_days_raw.csv":
        if input_base is None:
            rel_dir = Path(event_path.parent.name)
        else:
            try:
                rel_dir = event_path.parent.relative_to(input_base)
            except ValueError:
                rel_dir = Path(event_path.parent.name)
        return output_base / rel_dir / out_name

    marker = ".events_from_days_raw.csv"
    if event_path.name.endswith(marker):
        prefix = event_path.name[: -len(marker)]
    else:
        prefix = event_path.stem
    if input_base is None:
        rel_parent = Path(event_path.parent.name)
    else:
        try:
            rel_parent = event_path.parent.relative_to(input_base)
        except ValueError:
            rel_parent = Path(event_path.parent.name)
    return output_base / rel_parent / f"{prefix}.{out_name}"


def _removed_export_records(removed: pd.DataFrame, *, source_path: Path) -> list[dict[str, Any]]:
    if removed.empty:
        return []
    removed_export = removed.copy()
    removed_export.insert(0, "source_events_csv", str(source_path))
    removed_export.insert(1, "source_events_row_index", removed_export.index.astype(int))
    return removed_export.to_dict(orient="records")


def process_one_events_file(
    event_path: str | Path,
    *,
    output_dir: str = DEFAULT_INPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
    in_place: bool = False,
    include_removed_rows_records: bool = False,
) -> dict[str, Any]:
    source_path = Path(event_path)
    output_base = Path(output_dir)
    input_base_path = Path(input_base) if input_base is not None else None
    result: dict[str, Any] = {
        "status": "error",
        "source_events_csv": str(source_path),
        "output_events_csv": None,
        "rows_in": 0,
        "rows_removed_midnight": 0,
        "rows_out": 0,
        "removed_examples": [],
        "error_code": None,
        "error": None,
    }
    if include_removed_rows_records:
        result["_removed_rows_records"] = []

    try:
        df = pd.read_csv(source_path)
        cleaned, removed, stats = _clean_events_df(df)
        out_path = _build_cleaned_output_path(
            source_path,
            input_base=input_base_path,
            output_base=output_base,
            out_name=out_name,
            in_place=in_place,
        )
        ensure_parent_dir(str(out_path))
        cleaned.to_csv(out_path, index=False)

        result["status"] = "ok"
        result["output_events_csv"] = str(out_path)
        result["rows_in"] = int(stats["rows_in"])
        result["rows_removed_midnight"] = int(stats["rows_removed_midnight"])
        result["rows_out"] = int(stats["rows_out"])
        result["removed_examples"] = _removed_examples(
            removed, max_removed_examples_per_file
        )
        if include_removed_rows_records:
            result["_removed_rows_records"] = _removed_export_records(
                removed, source_path=source_path
            )
        return result
    except Exception as exc:
        result["error_code"] = "processing_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def process_many_events_files(
    event_files: Iterable[str | Path],
    *,
    output_dir: str = DEFAULT_INPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    events_name: str = DEFAULT_EVENTS_NAME,
    input_dir: str | None = None,
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
    in_place: bool = False,
    include_removed_rows_records: bool = False,
) -> dict[str, Any]:
    normalized_event_files = sorted(Path(path) for path in event_files)
    base_path = Path(input_base) if input_base is not None else None

    resolved_input_dir = input_dir
    if resolved_input_dir is None and base_path is not None:
        resolved_input_dir = str(base_path)

    totals: dict[str, Any] = {
        "files_total": len(normalized_event_files),
        "files_processed": 0,
        "files_error": 0,
        "files_with_removed": 0,
        "rows_in": 0,
        "rows_removed_midnight": 0,
        "rows_out": 0,
        "input_dir": os.path.abspath(resolved_input_dir) if resolved_input_dir else None,
        "output_dir": os.path.abspath(output_dir),
        "events_name": events_name,
        "out_name": out_name if not in_place else events_name,
        "in_place": bool(in_place),
    }

    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    files_with_removed: list[dict[str, Any]] = []
    removed_rows_records: list[dict[str, Any]] = []

    for index, source_path in enumerate(normalized_event_files, start=1):
        result = process_one_events_file(
            source_path,
            output_dir=output_dir,
            out_name=out_name,
            input_base=base_path,
            max_removed_examples_per_file=max_removed_examples_per_file,
            in_place=in_place,
            include_removed_rows_records=include_removed_rows_records,
        )
        if include_removed_rows_records:
            removed_rows_records.extend(
                result.pop("_removed_rows_records", [])
            )

        items.append(result)
        if result["status"] != "ok":
            totals["files_error"] += 1
            errors.append(
                {
                    "events_csv": str(result["source_events_csv"]),
                    "error": str(result["error"]),
                }
            )
            logger.error(
                "Errore elaborando %s: %s", result["source_events_csv"], result["error"]
            )
            continue

        totals["files_processed"] += 1
        totals["rows_in"] += int(result["rows_in"])
        totals["rows_removed_midnight"] += int(result["rows_removed_midnight"])
        totals["rows_out"] += int(result["rows_out"])

        if int(result["rows_removed_midnight"]) > 0:
            totals["files_with_removed"] += 1
            files_with_removed.append(
                {
                    "events_csv": str(result["source_events_csv"]),
                    "rows_in": int(result["rows_in"]),
                    "rows_removed_midnight": int(result["rows_removed_midnight"]),
                    "rows_out": int(result["rows_out"]),
                    "removed_examples": list(result["removed_examples"]),
                }
            )

        if index % 500 == 0:
            logger.info(
                "Processati %s/%s file eventi (removed_midnight=%s)",
                index,
                len(normalized_event_files),
                totals["rows_removed_midnight"],
            )

    files_with_removed.sort(
        key=lambda item: (
            -int(item["rows_removed_midnight"]),
            item["events_csv"],
        )
    )

    report: dict[str, Any] = {
        "stats": totals,
        "items": items,
        "errors": errors,
        "files_with_removed": files_with_removed,
        "file_errors": errors,
    }
    if include_removed_rows_records:
        report["_removed_rows_records"] = removed_rows_records
    return report
