from __future__ import annotations

"""Filter fake midnight events from per-file events CSVs."""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.logging_utils import setup_logging
from src.drive_service.output_paths import build_output_paths

logger = logging.getLogger(__name__)

DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.events_output)
DEFAULT_EVENTS_NAME = "*.events_from_days_raw.csv"
DEFAULT_OUT_NAME = "events_from_days_raw.cleaned.csv"
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.events_output / "events_from_days_raw.clean_midnight.report.json"
)
DEFAULT_REMOVED_CSV = str(
    DEFAULT_OUTPUTS.events_output / "events_from_days_raw.midnight_removed.csv"
)
DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE = 10


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
        stats["rows_out"] = 0
        return df.copy(), pd.DataFrame(), stats

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
        if not value:
            continue
        if value in out:
            continue
        out.append(value)
        if len(out) >= limit:
            break
    return out


def _build_cleaned_output_path(event_path: Path, out_name: str, *, in_place: bool) -> Path:
    if in_place:
        return event_path
    if event_path.name == "events_from_days_raw.csv":
        # Legacy layout.
        return event_path.with_name(out_name)
    marker = ".events_from_days_raw.csv"
    if event_path.name.endswith(marker):
        prefix = event_path.name[: -len(marker)]
    else:
        prefix = event_path.stem
    return event_path.with_name(f"{prefix}.{out_name}")


def filter_midnight_events_dir(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    events_name: str = DEFAULT_EVENTS_NAME,
    out_name: str = DEFAULT_OUT_NAME,
    report_json: str = DEFAULT_REPORT_JSON,
    removed_csv: str = DEFAULT_REMOVED_CSV,
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
    in_place: bool = False,
) -> dict[str, Any]:
    base = Path(input_dir)
    event_files = sorted(base.rglob(events_name))
    totals: dict[str, Any] = {
        "files_total": len(event_files),
        "files_processed": 0,
        "files_error": 0,
        "files_with_removed": 0,
        "rows_in": 0,
        "rows_removed_midnight": 0,
        "rows_out": 0,
        "input_dir": os.path.abspath(input_dir),
        "events_name": events_name,
        "out_name": out_name if not in_place else events_name,
        "in_place": bool(in_place),
    }
    files_with_removed: list[dict[str, Any]] = []
    file_errors: list[dict[str, str]] = []
    removed_rows: list[pd.DataFrame] = []

    for i, event_path in enumerate(event_files, start=1):
        try:
            df = pd.read_csv(event_path)
            cleaned, removed, stats = _clean_events_df(df)
            out_path = _build_cleaned_output_path(
                event_path,
                out_name,
                in_place=in_place,
            )
            ensure_parent_dir(str(out_path))
            cleaned.to_csv(out_path, index=False)

            totals["files_processed"] += 1
            totals["rows_in"] += int(stats["rows_in"])
            totals["rows_removed_midnight"] += int(stats["rows_removed_midnight"])
            totals["rows_out"] += int(stats["rows_out"])

            if not removed.empty:
                totals["files_with_removed"] += 1
                files_with_removed.append(
                    {
                        "events_csv": str(event_path),
                        "rows_in": int(stats["rows_in"]),
                        "rows_removed_midnight": int(stats["rows_removed_midnight"]),
                        "rows_out": int(stats["rows_out"]),
                        "removed_examples": _removed_examples(
                            removed, max_removed_examples_per_file
                        ),
                    }
                )
                removed_export = removed.copy()
                removed_export.insert(0, "source_events_csv", str(event_path))
                removed_export.insert(1, "source_events_row_index", removed_export.index.astype(int))
                removed_rows.append(removed_export)

            if i % 500 == 0:
                logger.info(
                    "Processati %s/%s file eventi (removed_midnight=%s)",
                    i,
                    len(event_files),
                    totals["rows_removed_midnight"],
                )
        except Exception as exc:
            totals["files_error"] += 1
            file_errors.append({"events_csv": str(event_path), "error": str(exc)})
            logger.exception("Errore elaborando %s", event_path)

    files_with_removed.sort(
        key=lambda item: (
            -int(item["rows_removed_midnight"]),
            item["events_csv"],
        )
    )

    if removed_rows:
        removed_all = pd.concat(removed_rows, ignore_index=True)
    else:
        removed_all = pd.DataFrame(
            columns=[
                "source_events_csv",
                "source_events_row_index",
                "filter_reason",
            ]
        )
    ensure_parent_dir(removed_csv)
    removed_all.to_csv(removed_csv, index=False)

    report = {
        "stats": totals,
        "removed_rows_csv": os.path.abspath(removed_csv),
        "files_with_removed": files_with_removed,
        "file_errors": file_errors,
    }
    ensure_parent_dir(report_json)
    with open(report_json, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Filtra eventi con timestamp a mezzanotte dai CSV eventi per-file."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Directory radice in cui cercare i CSV eventi (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--events-name",
        default=DEFAULT_EVENTS_NAME,
        help="Pattern file eventi da cercare ricorsivamente (default: *.events_from_days_raw.csv)",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help=(
            "Nome file output pulito accanto a ogni file eventi "
            "(default: events_from_days_raw.cleaned.csv)"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=(
            "Path report JSON finale "
            f"(default: {DEFAULT_REPORT_JSON})"
        ),
    )
    parser.add_argument(
        "--removed-csv",
        default=DEFAULT_REMOVED_CSV,
        help=(
            "Path CSV aggregato con tutte le righe rimosse "
            f"(default: {DEFAULT_REMOVED_CSV})"
        ),
    )
    parser.add_argument(
        "--max-removed-examples-per-file",
        type=int,
        default=DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
        help="Massimo numero di esempi rimossi per file nel report JSON (default: 10)",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Sovrascrive i file eventi originali invece di scrivere *.cleaned.csv",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    report = filter_midnight_events_dir(
        input_dir=args.input_dir,
        events_name=args.events_name,
        out_name=args.out_name,
        report_json=args.report_json,
        removed_csv=args.removed_csv,
        max_removed_examples_per_file=args.max_removed_examples_per_file,
        in_place=args.in_place,
    )
    stats = report["stats"]
    logger.info(
        "Completato: files=%s processati=%s errori=%s files_with_removed=%s rows_removed_midnight=%s",
        stats["files_total"],
        stats["files_processed"],
        stats["files_error"],
        stats["files_with_removed"],
        stats["rows_removed_midnight"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
