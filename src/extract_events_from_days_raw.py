from __future__ import annotations

"""Extract raw E/U events from days.csv raw strings."""

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from drive_service.fs_utils import ensure_parent_dir
    from drive_service.logging_utils import setup_logging
    from raw_text_parsing import EVENT_PATTERNS, extract_events, infer_year_month_from_filename
except ModuleNotFoundError:
    from src.drive_service.fs_utils import ensure_parent_dir
    from src.drive_service.logging_utils import setup_logging
    from src.raw_text_parsing import EVENT_PATTERNS, extract_events, infer_year_month_from_filename

logger = logging.getLogger(__name__)

DEFAULT_INPUT_DIR = "output/parsed_from_text"
DEFAULT_DAYS_NAME = "days.csv"
DEFAULT_OUT_NAME = "events_from_days_raw.csv"
DEFAULT_REPORT_JSON = "output/parsed_from_text/extract_events_from_days_raw.report.json"
DEFAULT_MAX_PATTERN_EXAMPLES = 12
DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE = 5


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


def extract_events_from_days_dir(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    days_name: str = DEFAULT_DAYS_NAME,
    out_name: str = DEFAULT_OUT_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES,
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
) -> dict[str, Any]:
    base = Path(input_dir)
    day_files = sorted(base.rglob(days_name))

    pattern_examples = {name: [] for name, _ in EVENT_PATTERNS}
    pattern_counts = {name: 0 for name, _ in EVENT_PATTERNS}
    totals: dict[str, Any] = {
        "files_total": len(day_files),
        "files_processed": 0,
        "files_error": 0,
        "files_with_events": 0,
        "rows_total": 0,
        "rows_with_raw": 0,
        "rows_with_events": 0,
        "rows_without_events": 0,
        "events_extracted": 0,
        "rows_with_multi_events": 0,
        "input_dir": os.path.abspath(input_dir),
        "days_name": days_name,
        "out_name": out_name,
    }
    file_coverage: list[dict[str, Any]] = []
    file_errors: list[dict[str, str]] = []

    for i, day_path in enumerate(day_files, start=1):
        try:
            df = pd.read_csv(day_path)
            default_year, default_month = _infer_year_month_from_days_path(day_path)
            events_df, stats = _extract_events_from_days_df(
                df,
                days_path=day_path,
                default_year=default_year,
                default_month=default_month,
                pattern_examples=pattern_examples,
                pattern_counts=pattern_counts,
                max_pattern_examples=max_pattern_examples,
                max_unmatched_examples_per_file=max_unmatched_examples_per_file,
            )
            out_path = day_path.with_name(out_name)
            ensure_parent_dir(str(out_path))
            events_df.to_csv(out_path, index=False)
            totals["files_processed"] += 1
            if not events_df.empty:
                totals["files_with_events"] += 1
            rows_with_raw = int(stats["rows_with_raw"])
            rows_with_events = int(stats["rows_with_events"])
            rows_without_events = int(stats["rows_without_events"])
            coverage_ratio = (
                round(rows_with_events / rows_with_raw, 6) if rows_with_raw > 0 else None
            )
            file_coverage.append(
                {
                    "days_csv": str(day_path),
                    "rows_total": int(stats["rows_total"]),
                    "rows_with_raw": rows_with_raw,
                    "rows_with_events": rows_with_events,
                    "rows_without_events": rows_without_events,
                    "events_extracted": int(stats["events_extracted"]),
                    "rows_with_multi_events": int(stats["rows_with_multi_events"]),
                    "coverage_ratio": coverage_ratio,
                    "rows_without_events_examples": list(stats["rows_without_events_examples"]),
                }
            )

            for key in (
                "rows_total",
                "rows_with_raw",
                "rows_with_events",
                "rows_without_events",
                "events_extracted",
                "rows_with_multi_events",
            ):
                totals[key] += int(stats[key])

            if i % 500 == 0:
                logger.info(
                    "Processati %s/%s file days.csv (events=%s)",
                    i,
                    len(day_files),
                    totals["events_extracted"],
                )
        except Exception as exc:
            totals["files_error"] += 1
            file_errors.append({"days_csv": str(day_path), "error": str(exc)})
            logger.exception("Errore elaborando %s", day_path)

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

    report = {
        "stats": totals,
        "pattern_counts": pattern_counts,
        "pattern_examples": pattern_examples,
        "files_with_unmatched_rows": files_with_unmatched_rows,
        "file_errors": file_errors,
    }
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Estrae eventi E/U dal campo raw di days.csv usando parser condiviso."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help="Directory radice da cui cercare days.csv (default: output/parsed_from_text)",
    )
    parser.add_argument(
        "--days-name",
        default=DEFAULT_DAYS_NAME,
        help="Nome file days da cercare ricorsivamente (default: days.csv)",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help="Nome file output eventi accanto a ogni days.csv (default: events_from_days_raw.csv)",
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=(
            "Path report JSON finale "
            "(default: output/parsed_from_text/extract_events_from_days_raw.report.json)"
        ),
    )
    parser.add_argument(
        "--max-pattern-examples",
        type=int,
        default=DEFAULT_MAX_PATTERN_EXAMPLES,
        help="Massimo numero di esempi raw per pattern nel report (default: 12)",
    )
    parser.add_argument(
        "--max-unmatched-examples-per-file",
        type=int,
        default=DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
        help="Massimo numero di esempi raw non matchati per file nel report (default: 5)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    report = extract_events_from_days_dir(
        input_dir=args.input_dir,
        days_name=args.days_name,
        out_name=args.out_name,
        report_json=args.report_json,
        max_pattern_examples=args.max_pattern_examples,
        max_unmatched_examples_per_file=args.max_unmatched_examples_per_file,
    )
    stats = report["stats"]
    logger.info(
        (
            "Completato: files=%s processati=%s errori=%s files_with_events=%s "
            "events=%s rows_with_events=%s"
        ),
        stats["files_total"],
        stats["files_processed"],
        stats["files_error"],
        stats["files_with_events"],
        stats["events_extracted"],
        stats["rows_with_events"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
