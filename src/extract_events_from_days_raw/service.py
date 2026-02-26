from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir
from src.raw_text_parsing import (
    EVENT_PATTERNS,
    extract_events,
    infer_year_month_from_filename,
)

from .options import (
    DEFAULT_DAYS_NAME,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_OUTPUT_DIR,
)

logger = logging.getLogger(__name__)


def _parse_hhmm(time_str: str) -> tuple[int, int, int] | None:
    try:
        hour_s, minute_s = time_str.split(":")
        hour = int(hour_s)
        minute = int(minute_s)
    except Exception:
        return None

    if hour == 24 and minute == 0:
        return 1, 0, 0
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return 0, hour, minute
    return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(float(value))
    except Exception:
        return None


def _to_dt(base_day: date, time_str: str) -> datetime | None:
    parsed = _parse_hhmm(time_str)
    if parsed is None:
        return None
    day_offset, hour, minute = parsed
    dt = datetime(base_day.year, base_day.month, base_day.day, hour, minute)
    if day_offset:
        dt = dt + timedelta(days=day_offset)
    return dt


def _infer_year_month_from_days_path(days_path: Path) -> tuple[int | None, int | None]:
    return infer_year_month_from_filename(days_path.parent)


def _build_events_output_path(
    days_path: Path,
    *,
    input_base: Path | None,
    output_base: Path,
    out_name: str,
) -> Path:
    if days_path.name == "days.csv":
        if input_base is None:
            rel_dir = Path(days_path.parent.name)
        else:
            try:
                rel_dir = days_path.parent.relative_to(input_base)
            except ValueError:
                rel_dir = Path(days_path.parent.name)
        return output_base / rel_dir / out_name

    marker = ".days.csv"
    if days_path.name.endswith(marker):
        prefix = days_path.name[: -len(marker)]
    else:
        prefix = days_path.stem
    if input_base is None:
        rel_parent = Path(days_path.parent.name)
    else:
        try:
            rel_parent = days_path.parent.relative_to(input_base)
        except ValueError:
            rel_parent = Path(days_path.parent.name)
    return output_base / rel_parent / f"{prefix}.{out_name}"


def _build_day(
    row: pd.Series,
    *,
    default_year: int | None,
    default_month: int | None,
) -> date | None:
    day = _coerce_int(row.get("day"))
    if day is None:
        return None

    year = _coerce_int(row.get("year"))
    month = _coerce_int(row.get("month"))
    if year is None:
        year = default_year
    if month is None:
        month = default_month
    if year is None or month is None:
        return None
    try:
        return date(year, month, day)
    except Exception:
        return None


def _empty_events_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "year",
            "month",
            "day",
            "dow",
            "event_index",
            "event_kind",
            "event_time_hhmm",
            "event_ts",
            "event_raw",
            "event_pattern",
            "raw",
            "source_row_index",
            "source_days_csv",
        ]
    )


def _extract_events_from_days_df(
    df: pd.DataFrame,
    *,
    days_path: Path,
    default_year: int | None,
    default_month: int | None,
    pattern_examples: dict[str, list[str]],
    pattern_counts: dict[str, int],
    max_pattern_examples: int,
    max_unmatched_examples_per_file: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    stats = {
        "rows_total": int(len(df)),
        "rows_with_raw": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "rows_without_events_examples": [],
    }
    if df.empty or "raw" not in df.columns:
        return _empty_events_df(), stats

    out_rows: list[dict[str, Any]] = []
    for row_index, row in df.iterrows():
        raw = str(row.get("raw") or "").strip()
        if not raw:
            continue
        stats["rows_with_raw"] += 1

        events = extract_events(raw)
        if not events:
            stats["rows_without_events"] += 1
            if len(stats["rows_without_events_examples"]) < max_unmatched_examples_per_file:
                stats["rows_without_events_examples"].append(raw)
            continue
        stats["rows_with_events"] += 1
        stats["events_extracted"] += len(events)
        if len(events) > 2:
            stats["rows_with_multi_events"] += 1

        year = _coerce_int(row.get("year"))
        month = _coerce_int(row.get("month"))
        day = _coerce_int(row.get("day"))
        if year is None:
            year = default_year
        if month is None:
            month = default_month

        day_value = _build_day(
            row,
            default_year=default_year,
            default_month=default_month,
        )

        for event_index, ev in enumerate(events):
            pattern_counts[ev.pattern] = pattern_counts.get(ev.pattern, 0) + 1
            if (
                raw not in pattern_examples[ev.pattern]
                and len(pattern_examples[ev.pattern]) < max_pattern_examples
            ):
                pattern_examples[ev.pattern].append(raw)

            event_ts: str | None = None
            if day_value is not None:
                event_dt = _to_dt(day_value, ev.time_str)
                if event_dt is not None:
                    event_ts = event_dt.strftime("%Y-%m-%d %H:%M:%S")

            out_rows.append(
                {
                    "year": year,
                    "month": month,
                    "day": day,
                    "dow": row.get("dow"),
                    "event_index": event_index,
                    "event_kind": ev.kind,
                    "event_time_hhmm": ev.time_str,
                    "event_ts": event_ts,
                    "event_raw": raw[ev.start : ev.end],
                    "event_pattern": ev.pattern,
                    "raw": raw,
                    "source_row_index": row_index,
                    "source_days_csv": str(days_path),
                }
            )

    if not out_rows:
        return _empty_events_df(), stats
    return pd.DataFrame(out_rows), stats


def process_one_days_file(
    days_path: str | Path,
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    pattern_examples: dict[str, list[str]] | None = None,
    pattern_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    source_path = Path(days_path)
    output_base = Path(output_dir)
    input_base_path = Path(input_base) if input_base is not None else None
    local_pattern_examples = (
        pattern_examples
        if pattern_examples is not None
        else {name: [] for name, _ in EVENT_PATTERNS}
    )
    local_pattern_counts = (
        pattern_counts
        if pattern_counts is not None
        else {name: 0 for name, _ in EVENT_PATTERNS}
    )

    base_result: dict[str, Any] = {
        "status": "error",
        "source_days_csv": str(source_path),
        "output_events_csv": None,
        "rows_total": 0,
        "rows_with_raw": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "coverage_ratio": None,
        "rows_without_events_examples": [],
        "error_code": None,
        "error": None,
    }

    try:
        df = pd.read_csv(source_path)
        default_year, default_month = _infer_year_month_from_days_path(source_path)
        events_df, stats = _extract_events_from_days_df(
            df,
            days_path=source_path,
            default_year=default_year,
            default_month=default_month,
            pattern_examples=local_pattern_examples,
            pattern_counts=local_pattern_counts,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
        )

        out_path = _build_events_output_path(
            source_path,
            input_base=input_base_path,
            output_base=output_base,
            out_name=out_name,
        )
        ensure_parent_dir(str(out_path))
        events_df.to_csv(out_path, index=False)

        rows_with_raw = int(stats["rows_with_raw"])
        rows_with_events = int(stats["rows_with_events"])
        coverage_ratio = (
            round(rows_with_events / rows_with_raw, 6) if rows_with_raw > 0 else None
        )

        base_result["status"] = "ok"
        base_result["output_events_csv"] = str(out_path)
        base_result["rows_total"] = int(stats["rows_total"])
        base_result["rows_with_raw"] = rows_with_raw
        base_result["rows_with_events"] = rows_with_events
        base_result["rows_without_events"] = int(stats["rows_without_events"])
        base_result["events_extracted"] = int(stats["events_extracted"])
        base_result["rows_with_multi_events"] = int(stats["rows_with_multi_events"])
        base_result["coverage_ratio"] = coverage_ratio
        base_result["rows_without_events_examples"] = list(
            stats["rows_without_events_examples"]
        )
        return base_result
    except Exception as exc:
        base_result["error_code"] = "processing_error"
        base_result["error"] = f"{type(exc).__name__}: {exc}"
        return base_result


def process_many_days_files(
    day_files: Iterable[str | Path],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    out_name: str = DEFAULT_OUT_NAME,
    input_base: str | Path | None = None,
    days_name: str = DEFAULT_DAYS_NAME,
    input_dir: str | None = None,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    base_path = Path(input_base) if input_base is not None else None
    normalized_day_files = sorted(Path(path) for path in day_files)

    resolved_input_dir = input_dir
    if resolved_input_dir is None and base_path is not None:
        resolved_input_dir = str(base_path)

    pattern_examples = {name: [] for name, _ in EVENT_PATTERNS}
    pattern_counts = {name: 0 for name, _ in EVENT_PATTERNS}
    totals: dict[str, Any] = {
        "files_total": len(normalized_day_files),
        "files_processed": 0,
        "files_error": 0,
        "files_with_events": 0,
        "rows_total": 0,
        "rows_with_raw": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "input_dir": os.path.abspath(resolved_input_dir) if resolved_input_dir else None,
        "output_dir": os.path.abspath(output_dir),
        "days_name": days_name,
        "out_name": out_name,
    }
    file_coverage: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    items: list[dict[str, Any]] = []

    for i, day_path in enumerate(normalized_day_files, start=1):
        result = process_one_days_file(
            day_path,
            output_dir=output_dir,
            out_name=out_name,
            input_base=base_path,
            max_pattern_examples=max_pattern_examples,
            max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            pattern_examples=pattern_examples,
            pattern_counts=pattern_counts,
        )
        items.append(result)

        if result["status"] != "ok":
            totals["files_error"] += 1
            errors.append(
                {
                    "days_csv": str(result["source_days_csv"]),
                    "error": str(result["error"]),
                }
            )
            logger.error(
                "Errore elaborando %s: %s", result["source_days_csv"], result["error"]
            )
            continue

        totals["files_processed"] += 1
        if int(result["events_extracted"]) > 0:
            totals["files_with_events"] += 1

        for key in (
            "rows_total",
            "rows_with_raw",
            "rows_with_events",
            "rows_without_events",
            "events_extracted",
            "rows_with_multi_events",
        ):
            totals[key] += int(result[key])

        file_coverage.append(
            {
                "days_csv": str(result["source_days_csv"]),
                "rows_total": int(result["rows_total"]),
                "rows_with_raw": int(result["rows_with_raw"]),
                "rows_with_events": int(result["rows_with_events"]),
                "rows_without_events": int(result["rows_without_events"]),
                "events_extracted": int(result["events_extracted"]),
                "rows_with_multi_events": int(result["rows_with_multi_events"]),
                "coverage_ratio": result["coverage_ratio"],
                "rows_without_events_examples": list(
                    result["rows_without_events_examples"]
                ),
            }
        )

        if i % 500 == 0:
            logger.info(
                "Processati %s/%s file days.csv (events=%s)",
                i,
                len(normalized_day_files),
                totals["events_extracted"],
            )

    files_with_unmatched_rows = [item for item in file_coverage if item["rows_without_events"] > 0]
    files_with_unmatched_rows.sort(
        key=lambda item: (
            -int(item["rows_without_events"]),
            float(item["coverage_ratio"]) if item["coverage_ratio"] is not None else -1.0,
            item["days_csv"],
        )
    )
    if totals["rows_with_raw"] > 0:
        totals["coverage_ratio"] = round(totals["rows_with_events"] / totals["rows_with_raw"], 6)
    else:
        totals["coverage_ratio"] = None

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "pattern_counts": pattern_counts,
        "pattern_examples": pattern_examples,
        "files_with_unmatched_rows": files_with_unmatched_rows,
        "file_errors": errors,
    }
