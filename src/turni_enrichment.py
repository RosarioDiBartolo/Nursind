from __future__ import annotations

"""Enrich merged per-employee pairs with shift classification fields.

Input: output/employee_shifts_from_raw/<EMP>.pairs.csv (prepared step).
Output: per-employee enriched CSV under output/enriched/employee_pairs.
"""

import argparse
import json
import logging
import os
from pathlib import Path

import pandas as pd

from src.drive_service.fs_utils import ensure_dir, ensure_parent_dir
from src.drive_service.logging_utils import setup_logging
from src.drive_service.names import safe_name
from src.shift_services import (
    ItalianHolidayCalendar,
    ShiftClassifier,
    assign_turno_bucket,
    assign_turno_code,
    to_datetime_series,
)

logger = logging.getLogger(__name__)

DEFAULT_MIN_HOURS = 6.0
DEFAULT_INPUT_DIR = "output/employee_shifts_from_raw"
DEFAULT_OUTPUT_DIR = "output/enriched/employee_pairs"
ENRICHED_COLUMNS = [
    "employee",
    "entry_ts",
    "exit_ts",
    "duration_hours",
    "is_long",
    "is_holiday",
    "is_afternoon",
    "is_night",
    "turno_code",
    "turno_bucket",
    "year",
    "turno",
    "file_id",
    "file_name",
    "source_csv",
]


def _employee_from_path(path: Path) -> str:
    name = path.stem
    if name.lower().endswith(".pairs"):
        name = name[: -len(".pairs")]
    return name or "unknown"


def _apply_overnight_fix(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    mask = df["exit_ts"] < df["entry_ts"]
    if not mask.any():
        return 0
    df.loc[mask, "exit_ts"] = df.loc[mask, "exit_ts"] + pd.Timedelta(days=1)
    return int(mask.sum())


def _empty_enriched_df() -> pd.DataFrame:
    return pd.DataFrame(columns=ENRICHED_COLUMNS)


def _enrich_pairs(
    df: pd.DataFrame,
    *,
    classifier: ShiftClassifier,
    min_hours: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {"righe_totali": len(df), "righe_completate": 0, "righe_enriched": 0, "overnight_fix": 0}
    if df.empty:
        return df, stats

    working = df.copy()
    if "entry_ts" not in working.columns or "exit_ts" not in working.columns:
        return pd.DataFrame(), stats

    working["entry_ts"] = to_datetime_series(working["entry_ts"])
    working["exit_ts"] = to_datetime_series(working["exit_ts"])
    working = working.loc[
        working["entry_ts"].notna() & working["exit_ts"].notna()
    ].copy()
    stats["righe_completate"] = len(working)
    if working.empty:
        return pd.DataFrame(), stats

    stats["overnight_fix"] = _apply_overnight_fix(working)
    working["duration_hours"] = (
        (working["exit_ts"] - working["entry_ts"]).dt.total_seconds() / 3600.0
    )
    working["is_long"] = working["duration_hours"] > float(min_hours)
    working["year"] = working["entry_ts"].dt.year

    working = classifier.classify(working)
    required_cols = {"is_holiday", "is_afternoon", "is_night"}
    if not required_cols.issubset(set(working.columns)):
        return pd.DataFrame(), stats

    working["turno_code"] = assign_turno_code(working)
    working["turno_bucket"] = assign_turno_bucket(working, min_hours=min_hours)
    stats["righe_enriched"] = len(working)
    return working, stats


def enrich_pairs_by_employee(
    *,
    input_dir: str = DEFAULT_INPUT_DIR,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
) -> dict[str, int]:
    ensure_dir(output_dir)
    classifier = ShiftClassifier(
        calendar=ItalianHolidayCalendar(),
        include_holidays=include_holidays,
    )

    input_path = Path(input_dir)
    pairs_files = sorted(input_path.glob("*.pairs.csv"))

    stats = {
        "dipendenti": len(pairs_files),
        "file_totali": 0,
        "file_mancanti": 0,
        "file_errori": 0,
        "righe_totali": 0,
        "righe_completate": 0,
        "righe_enriched": 0,
        "overnight_fix": 0,
    }

    for emp_index, pair_path in enumerate(pairs_files, start=1):
        emp_name = _employee_from_path(pair_path)
        logger.info(
            "Dipendente %s/%s: %s",
            emp_index,
            len(pairs_files),
            emp_name,
        )

        stats["file_totali"] += 1
        try:
            df = pd.read_csv(pair_path)
        except Exception:
            stats["file_errori"] += 1
            out_path = os.path.join(output_dir, f"{safe_name(emp_name)}.enriched.csv")
            ensure_parent_dir(out_path)
            _empty_enriched_df().to_csv(out_path, index=False)
            continue

        df = df.copy()
        df["source_csv"] = str(pair_path)

        enriched, sub_stats = _enrich_pairs(df, classifier=classifier, min_hours=min_hours)
        stats["righe_totali"] += sub_stats["righe_totali"]
        stats["righe_completate"] += sub_stats["righe_completate"]
        stats["righe_enriched"] += sub_stats["righe_enriched"]
        stats["overnight_fix"] += sub_stats["overnight_fix"]

        if enriched.empty:
            out_path = os.path.join(output_dir, f"{safe_name(emp_name)}.enriched.csv")
            ensure_parent_dir(out_path)
            _empty_enriched_df().to_csv(out_path, index=False)
            continue

        enriched = enriched.copy()
        enriched["employee"] = emp_name
        ordered_cols = [col for col in ENRICHED_COLUMNS if col in enriched.columns]
        ordered_cols += [col for col in enriched.columns if col not in ordered_cols]
        enriched = enriched[ordered_cols]

        out_path = os.path.join(output_dir, f"{safe_name(emp_name)}.enriched.csv")
        ensure_parent_dir(out_path)
        enriched.to_csv(out_path, index=False)
        logger.info("Salvato %s", out_path)

    return stats


def _write_json(out_path: str, stats: dict[str, int]) -> None:
    ensure_parent_dir(out_path)
    payload = {"stats": stats}
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Arricchisce i pairs.csv per dipendente con classificazioni turni."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help="Directory dei pairs.csv per dipendente (default: output/employee_shifts_from_raw)",
    )
    parser.add_argument(
        "--out-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory di output (default: output/enriched/employee_pairs)",
    )
    parser.add_argument(
        "--min-hours",
        type=float,
        default=DEFAULT_MIN_HOURS,
        help="Soglia ore per classificazione lunga (durata > soglia, default: 6.0)",
    )
    parser.add_argument(
        "--no-holidays",
        action="store_true",
        help="Non considerare le festivita italiane (solo domeniche).",
    )
    parser.add_argument(
        "--stats-json",
        help="Se specificato, salva le statistiche in JSON.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    stats = enrich_pairs_by_employee(
        input_dir=args.input_dir,
        output_dir=args.out_dir,
        min_hours=args.min_hours,
        include_holidays=not args.no_holidays,
    )

    if args.stats_json:
        _write_json(args.stats_json, stats)

    logger.info(
        "Completato: dipendenti=%s file_totali=%s mancanti=%s errori=%s righe=%s completate=%s enriched=%s overnight_fix=%s",
        stats["dipendenti"],
        stats["file_totali"],
        stats["file_mancanti"],
        stats["file_errori"],
        stats["righe_totali"],
        stats["righe_completate"],
        stats["righe_enriched"],
        stats["overnight_fix"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
