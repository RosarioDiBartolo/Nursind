from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.drive_service.fs_utils import ensure_parent_dir
from src.drive_service.names import safe_name
from src.shift_services import (
    ItalianHolidayCalendar,
    ShiftClassifier,
    assign_turno_bucket,
    assign_turno_code,
    to_datetime_series,
)

from .options import DEFAULT_INPUT_DIR, DEFAULT_MIN_HOURS, DEFAULT_OUTPUT_DIR

logger = logging.getLogger(__name__)

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
    stats = {
        "righe_totali": int(len(df)),
        "righe_completate": 0,
        "righe_enriched": 0,
        "overnight_fix": 0,
    }
    if df.empty:
        return df, stats

    working = df.copy()
    if "entry_ts" not in working.columns or "exit_ts" not in working.columns:
        return pd.DataFrame(), stats

    working["entry_ts"] = to_datetime_series(working["entry_ts"])
    working["exit_ts"] = to_datetime_series(working["exit_ts"])
    working = working.loc[working["entry_ts"].notna() & working["exit_ts"].notna()].copy()
    stats["righe_completate"] = int(len(working))
    if working.empty:
        return pd.DataFrame(), stats

    stats["overnight_fix"] = _apply_overnight_fix(working)
    working["duration_hours"] = (working["exit_ts"] - working["entry_ts"]).dt.total_seconds() / 3600.0
    working["is_long"] = working["duration_hours"] > float(min_hours)
    working["year"] = working["entry_ts"].dt.year

    working = classifier.classify(working)
    required_cols = {"is_holiday", "is_afternoon", "is_night"}
    if not required_cols.issubset(set(working.columns)):
        return pd.DataFrame(), stats

    working["turno_code"] = assign_turno_code(working)
    working["turno_bucket"] = assign_turno_bucket(working, min_hours=min_hours)
    stats["righe_enriched"] = int(len(working))
    return working, stats


def process_one_pairs_file(
    pair_path: str | Path,
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
    classifier: ShiftClassifier | None = None,
) -> dict[str, Any]:
    source_path = Path(pair_path)
    employee_name = _employee_from_path(source_path)
    out_path = os.path.abspath(os.path.join(output_dir, f"{safe_name(employee_name)}.enriched.csv"))
    result: dict[str, Any] = {
        "status": "error",
        "source_pairs_csv": str(source_path),
        "output_enriched_csv": out_path,
        "employee": employee_name,
        "rows_total": 0,
        "rows_complete": 0,
        "rows_enriched": 0,
        "overnight_fix": 0,
        "error_code": None,
        "error": None,
    }

    local_classifier = classifier
    if local_classifier is None:
        local_classifier = ShiftClassifier(
            calendar=ItalianHolidayCalendar(),
            include_holidays=include_holidays,
        )

    if not source_path.exists():
        ensure_parent_dir(out_path)
        _empty_enriched_df().to_csv(out_path, index=False)
        result["error_code"] = "missing_input"
        result["error"] = f"Missing pairs file: {source_path}"
        return result

    try:
        df = pd.read_csv(source_path)
    except Exception as exc:
        ensure_parent_dir(out_path)
        _empty_enriched_df().to_csv(out_path, index=False)
        result["error_code"] = "read_error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    df = df.copy()
    df["source_csv"] = str(source_path)
    enriched, sub_stats = _enrich_pairs(df, classifier=local_classifier, min_hours=min_hours)

    result["rows_total"] = int(sub_stats["righe_totali"])
    result["rows_complete"] = int(sub_stats["righe_completate"])
    result["rows_enriched"] = int(sub_stats["righe_enriched"])
    result["overnight_fix"] = int(sub_stats["overnight_fix"])

    if enriched.empty:
        ensure_parent_dir(out_path)
        _empty_enriched_df().to_csv(out_path, index=False)
        result["status"] = "ok"
        return result

    enriched = enriched.copy()
    enriched["employee"] = employee_name
    ordered_cols = [col for col in ENRICHED_COLUMNS if col in enriched.columns]
    ordered_cols += [col for col in enriched.columns if col not in ordered_cols]
    enriched = enriched[ordered_cols]

    ensure_parent_dir(out_path)
    enriched.to_csv(out_path, index=False)
    logger.info("Salvato %s", out_path)
    result["status"] = "ok"
    return result


def process_many_pairs_files(
    pairs_files: Iterable[str | Path],
    *,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
    input_dir: str | None = DEFAULT_INPUT_DIR,
) -> dict[str, Any]:
    normalized_pairs = sorted(Path(path) for path in pairs_files)
    classifier = ShiftClassifier(
        calendar=ItalianHolidayCalendar(),
        include_holidays=include_holidays,
    )

    totals: dict[str, Any] = {
        "files_total": len(normalized_pairs),
        "files_processed": 0,
        "files_error": 0,
        "dipendenti": len(normalized_pairs),
        "file_totali": 0,
        "file_mancanti": 0,
        "file_errori": 0,
        "righe_totali": 0,
        "righe_completate": 0,
        "righe_enriched": 0,
        "overnight_fix": 0,
        "input_dir": os.path.abspath(input_dir) if input_dir else None,
        "output_dir": os.path.abspath(output_dir),
        "min_hours": float(min_hours),
        "include_holidays": bool(include_holidays),
    }

    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for pair_index, pair_path in enumerate(normalized_pairs, start=1):
        employee_name = _employee_from_path(pair_path)
        logger.info(
            "Dipendente %s/%s: %s",
            pair_index,
            len(normalized_pairs),
            employee_name,
        )
        totals["file_totali"] += 1

        item = process_one_pairs_file(
            pair_path,
            output_dir=output_dir,
            min_hours=min_hours,
            include_holidays=include_holidays,
            classifier=classifier,
        )
        items.append(item)

        if item["status"] != "ok":
            totals["files_error"] += 1
            totals["file_errori"] += 1
            if item["error_code"] == "missing_input":
                totals["file_mancanti"] += 1
            errors.append(
                {
                    "pairs_csv": str(item["source_pairs_csv"]),
                    "error": str(item["error"]),
                }
            )
            continue

        totals["files_processed"] += 1
        totals["righe_totali"] += int(item["rows_total"])
        totals["righe_completate"] += int(item["rows_complete"])
        totals["righe_enriched"] += int(item["rows_enriched"])
        totals["overnight_fix"] += int(item["overnight_fix"])

    return {
        "stats": totals,
        "items": items,
        "errors": errors,
        "file_errors": errors,
    }
