from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.drive_service.fs_utils import ensure_dir, ensure_parent_dir
from src.drive_service.names import safe_name
from src.reporting import build_stage_report, compact_stage_report, write_json_report
from src.shift_services import (
    ItalianHolidayCalendar,
    ShiftClassifier,
    assign_turno_bucket,
    assign_turno_code,
    to_datetime_series,
)

from .options import (
    DEFAULT_MIN_HOURS,
    TurniEnrichmentOptions,
    default_input_dir,
    default_output_dir,
    default_report_json_path,
)

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
    stats = {"rows_total": int(len(df)), "rows_complete": 0, "rows_enriched": 0, "overnight_fix": 0}
    if df.empty:
        return df, stats
    if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
        return pd.DataFrame(), stats

    working = df.copy()
    working["entry_ts"] = to_datetime_series(working["entry_ts"])
    working["exit_ts"] = to_datetime_series(working["exit_ts"])
    working = working.loc[working["entry_ts"].notna() & working["exit_ts"].notna()].copy()
    stats["rows_complete"] = int(len(working))
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
    stats["rows_enriched"] = int(len(working))
    return working, stats


def process_one_pairs_file(
    pair_path: str | Path,
    *,
    output_dir: str | None = None,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
    classifier: ShiftClassifier | None = None,
) -> dict[str, Any]:
    output_dir = output_dir or default_output_dir()
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

    local_classifier = classifier or ShiftClassifier(
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

    result["rows_total"] = int(sub_stats["rows_total"])
    result["rows_complete"] = int(sub_stats["rows_complete"])
    result["rows_enriched"] = int(sub_stats["rows_enriched"])
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
    logger.info("Saved %s", out_path)
    result["status"] = "ok"
    return result


def process_many_pairs_files(
    pairs_files: Iterable[str | Path],
    *,
    output_dir: str | None = None,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
    input_dir: str | None = None,
) -> dict[str, Any]:
    output_dir = output_dir or default_output_dir()
    input_dir = input_dir or default_input_dir()
    normalized_pairs = sorted(Path(path) for path in pairs_files)
    classifier = ShiftClassifier(
        calendar=ItalianHolidayCalendar(),
        include_holidays=include_holidays,
    )

    stats: dict[str, Any] = {
        "files_total": len(normalized_pairs),
        "files_processed": 0,
        "files_error": 0,
        "employees_total": len(normalized_pairs),
        "rows_total": 0,
        "rows_complete": 0,
        "rows_enriched": 0,
        "overnight_fix": 0,
        "include_holidays": bool(include_holidays),
        "min_hours": float(min_hours),
    }
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []

    for pair_index, pair_path in enumerate(normalized_pairs, start=1):
        employee_name = _employee_from_path(pair_path)
        logger.info("Employee %s/%s: %s", pair_index, len(normalized_pairs), employee_name)

        item = process_one_pairs_file(
            pair_path,
            output_dir=output_dir,
            min_hours=min_hours,
            include_holidays=include_holidays,
            classifier=classifier,
        )
        items.append(item)

        if item["status"] != "ok":
            stats["files_error"] += 1
            issues.append(
                {
                    "code": str(item.get("error_code") or "processing_error"),
                    "pairs_csv": str(item.get("source_pairs_csv") or ""),
                    "message": str(item.get("error") or "processing_error"),
                }
            )
            continue

        stats["files_processed"] += 1
        stats["rows_total"] += int(item["rows_total"])
        stats["rows_complete"] += int(item["rows_complete"])
        stats["rows_enriched"] += int(item["rows_enriched"])
        stats["overnight_fix"] += int(item["overnight_fix"])

    return build_stage_report(
        stage="turni_enrichment",
        inputs={
            "input_dir": os.path.abspath(input_dir) if input_dir else None,
            "output_dir": os.path.abspath(output_dir),
            "include_holidays": bool(include_holidays),
            "min_hours": float(min_hours),
        },
        outputs={"output_dir": os.path.abspath(output_dir)},
        stats=stats,
        row_totals={
            "items": len(items),
            "issues": len(issues),
            "enriched_files": stats["files_processed"],
        },
        items=items,
        issues=issues,
    )


def build_turni_enrichment_from_dir(
    *,
    input_dir: str | None = None,
    output_dir: str | None = None,
    min_hours: float = DEFAULT_MIN_HOURS,
    include_holidays: bool = True,
    report_json: str | None = None,
) -> dict[str, Any]:
    input_dir = input_dir or default_input_dir()
    output_dir = output_dir or default_output_dir()
    report_json = report_json or default_report_json_path()
    ensure_dir(output_dir)
    input_path = Path(input_dir)
    pairs_files = sorted(input_path.glob("*.pairs.csv"))
    report = process_many_pairs_files(
        pairs_files,
        output_dir=output_dir,
        min_hours=min_hours,
        include_holidays=include_holidays,
        input_dir=input_dir,
    )
    report["outputs"]["report_json"] = str(Path(report_json).resolve())
    write_json_report(report_json, compact_stage_report(report))
    return report


def run_from_options(options: TurniEnrichmentOptions) -> dict[str, Any]:
    return build_turni_enrichment_from_dir(
        input_dir=options.input_dir,
        output_dir=options.output_dir,
        min_hours=options.min_hours,
        include_holidays=options.include_holidays,
        report_json=options.report_json,
    )


__all__ = [
    "build_turni_enrichment_from_dir",
    "process_many_pairs_files",
    "process_one_pairs_file",
    "run_from_options",
]
