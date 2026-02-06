import argparse
import json
import os
from typing import Any

import pandas as pd

from drive_scripts.fs_utils import ensure_dir, ensure_parent_dir
from drive_scripts.map_index_service import MapIndex
from drive_scripts.logging_utils import get_logger
from drive_scripts.names import safe_name
from shift_services import (
    EmployeeGrouper,
    PairsCloser,
    PairsLoader,
    PairsPathResolver,
    ShiftClassifier,
    TurnoMatchPolicy,
    format_datetime_series,
)

logger = get_logger()
DATETIME_OUTPUT_FORMAT = "%Y-%m-%d %H:%M:%S"


def _normalize_employee(name: str | None) -> str:
    return (name or "").strip().lower()


def _employee_key(name: str | None, employee_id: str | None) -> str:
    if employee_id:
        return f"id:{employee_id}"
    norm = _normalize_employee(name)
    return f"name:{norm or 'unknown'}"


def _summarize_excluded(
    excluded: MapIndex,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    counts: dict[str, dict[str, Any]] = {}
    meta: dict[str, dict[str, Any]] = {}
    for item in excluded.files.values():
        name = getattr(item, "employee", None) or "unknown"
        employee_id = getattr(item, "employee_id", None)
        key = _employee_key(name, employee_id)
        if key not in counts:
            counts[key] = {"broken": 0, "excluded": 0, "broken_ids": []}
            meta[key] = {"employee": name, "employee_id": employee_id}
        reason = (getattr(item, "reason", None) or "").lower()
        if "pdfminer" in reason:
            counts[key]["broken"] += 1
            file_id = getattr(item, "file_id", None)
            if file_id:
                counts[key]["broken_ids"].append(file_id)
        else:
            counts[key]["excluded"] += 1
    return counts, meta


def calculate_overtime(
    index_path: str,
    output_path: str,
    employee_filter: str | None = None,
    hours_threshold: float = 6.0,
    close_gap_hours: float = 16.0,
    excluded_index_path: str | None = None,
) -> None:
    ensure_parent_dir(output_path)
    output_dir = os.path.dirname(output_path) or "output"
    output_dir = os.path.abspath(output_dir)
    ensure_dir(output_dir)

    report = MapIndex.load_index(index_path, strict=True, allow_legacy=True)
    grouper = EmployeeGrouper(_normalize_employee)
    employees = grouper.group(list(report.files.values()))
    resolver = PairsPathResolver(index_path)
    loader = PairsLoader(resolver)
    closer = PairsCloser(
        max_gap_hours=close_gap_hours,
        mark_inferred=True,
        preserve_exit_raw=True,
        clear_duration_hhmm=True,
    )
    classifier = ShiftClassifier(
        include_holidays=False,
        match_policy=TurnoMatchPolicy(mode="equals"),
    )
    excluded_counts: dict[str, dict[str, int]] = {}
    excluded_meta: dict[str, dict[str, Any]] = {}
    if excluded_index_path:
        excluded = MapIndex.load_index(excluded_index_path, strict=True, allow_legacy=True)
        excluded_counts, excluded_meta = _summarize_excluded(excluded)

    if employee_filter:
        # Normalize for case-insensitive match
        filter_norm = employee_filter.strip().lower()
        employees = [
            emp for emp in employees if _normalize_employee(emp.get("employee")) == filter_norm
        ]
        if not employees:
            logger.warning(f"No employee found matching '{employee_filter}'")
            return
    
    summary = []
    seen_keys = set()

    for emp in employees:
        emp_name = emp.get("employee", "unknown")
        emp_id = emp.get("employee_id")
        emp_key = emp.get("key") or _employee_key(emp_name, emp_id)
        seen_keys.add(emp_key)
        counts = excluded_counts.get(emp_key, {"broken": 0, "excluded": 0, "broken_ids": []})
        broken = counts.get("broken", 0)
        excluded_other = counts.get("excluded", 0)
        broken_ids = sorted({fid for fid in counts.get("broken_ids", []) if fid})
        excluded_total = broken + excluded_other
        pairs_dfs = []

        for inc in emp.get("files", []):
            pairs_rel = None
            outputs = getattr(inc, "outputs", None)
            if outputs:
                pairs_rel = getattr(outputs, "pairs_csv", None)
            expected_path = resolver.resolve_pairs_path(
                emp_name,
                getattr(inc, "file_name", None),
                getattr(inc, "file_id", None),
                pairs_rel,
            )
            if not os.path.exists(expected_path):
                logger.warning(
                    "Missing pairs_csv for %s (expected: %s)",
                    getattr(inc, "file_name", "unknown"),
                    resolver.path_for_log(expected_path),
                )
                continue
            try:
                df = loader.load_pairs(expected_path)
            except Exception as e:
                logger.error("Error loading %s: %s", expected_path, e)
                continue
            missing_cols = {"entry_ts", "exit_ts"} - set(df.columns)
            if missing_cols:
                logger.warning(
                    "Skipping %s, missing columns: %s",
                    expected_path,
                    ", ".join(sorted(missing_cols)),
                )
                continue
            df["file_id"] = getattr(inc, "file_id", None)
            df["file_name"] = getattr(inc, "file_name", None)
            df["pairs_csv"] = expected_path
            closed_df = closer.close(df)
            if closed_df.empty:
                continue
            pairs_dfs.append(closed_df)

        merged_df = None
        total_shifts = 0
        overtime_count = 0
        turni_notte = 0
        turni_pomeriggio = 0
        turni_festivi = 0
        turni_notte_pomeriggio_festivi = 0
        straordinari_notte = 0
        straordinari_pomeriggio = 0
        straordinari_festivi = 0
        if pairs_dfs:
            merged_df = pd.concat(pairs_dfs, ignore_index=True)

        if merged_df is not None:
            classified = classifier.classify(merged_df)
            valid_shifts = classified.dropna(subset=["entry_ts", "exit_ts", "duration"])
            valid_shifts = valid_shifts[valid_shifts["duration"] >= pd.Timedelta(0)]
            total_shifts = len(valid_shifts)
            overtime_mask = valid_shifts["duration"] > pd.Timedelta(hours=hours_threshold)
            overtime_count = int(overtime_mask.sum())
            notte_mask = valid_shifts["is_night"]
            pomeriggio_mask = valid_shifts["is_afternoon"]
            domenica_mask = valid_shifts["is_holiday"]

            turni_notte = int(notte_mask.sum())
            turni_pomeriggio = int(pomeriggio_mask.sum())
            turni_festivi = int(domenica_mask.sum())
            turni_notte_pomeriggio_festivi = int(
                (notte_mask | pomeriggio_mask | domenica_mask).sum()
            )

            straordinari_notte = int((notte_mask & overtime_mask).sum())
            straordinari_pomeriggio = int((pomeriggio_mask & overtime_mask).sum())
            straordinari_festivi = int((domenica_mask & overtime_mask).sum())

        # Save employee-specific CSV (when available) and report
        emp_safe_name = safe_name(emp_name)
        emp_output_dir = os.path.join(output_dir, emp_safe_name)
        ensure_dir(emp_output_dir)

        if merged_df is not None:
            csv_path = os.path.join(emp_output_dir, "result.csv")
            output_df = merged_df.copy()
            if "entry_ts" in output_df.columns:
                output_df["entry_ts"] = format_datetime_series(
                    output_df["entry_ts"], DATETIME_OUTPUT_FORMAT
                )
            if "exit_ts" in output_df.columns:
                output_df["exit_ts"] = format_datetime_series(
                    output_df["exit_ts"], DATETIME_OUTPUT_FORMAT
                )
            output_df.to_csv(csv_path, index=False)
        
        emp_report = {
            "dipendente": emp_name,
            "matricola": emp_id,
            "turni_totali": int(total_shifts),
            "turni_straordinari": int(overtime_count),
            "soglia_straordinario_ore": hours_threshold,
            "turni_notte": int(turni_notte),
            "turni_pomeriggio": int(turni_pomeriggio),
            "turni_festivi": int(turni_festivi),
            "turni_notte_pomeriggio_festivi": int(turni_notte_pomeriggio_festivi),
            "turni_straordinari_notte": int(straordinari_notte),
            "turni_straordinari_pomeriggio": int(straordinari_pomeriggio),
            "turni_straordinari_festivi": int(straordinari_festivi),
            "danneggiati": int(broken),
            "esclusi": int(excluded_other),
            "esclusi_totali": int(excluded_total),
            "file_danneggiati": broken_ids,
            "generato_il": pd.Timestamp.now().isoformat(),
        }
        
        index_path_emp = os.path.join(emp_output_dir, "report.json")
        with open(index_path_emp, "w", encoding="utf-8") as f:
            json.dump(emp_report, f, indent=2, ensure_ascii=False)
        
        logger.info(
            "Saved %s report: %s",
            emp_name,
            resolver.path_for_log(index_path_emp),
        )

        summary.append(
            {
                "dipendente": emp_name,
                "matricola": emp_id,
                "turni_totali": int(total_shifts),
                "turni_straordinari": int(overtime_count),
                "turni_notte": int(turni_notte),
                "turni_pomeriggio": int(turni_pomeriggio),
                "turni_festivi": int(turni_festivi),
                "turni_notte_pomeriggio_festivi": int(turni_notte_pomeriggio_festivi),
                "turni_straordinari_notte": int(straordinari_notte),
                "turni_straordinari_pomeriggio": int(straordinari_pomeriggio),
                "turni_straordinari_festivi": int(straordinari_festivi),
                "danneggiati": int(broken),
                "esclusi": int(excluded_other),
                "esclusi_totali": int(excluded_total),
                "file_danneggiati": broken_ids,
            }
        )

    for key, meta in excluded_meta.items():
        if key in seen_keys:
            continue
        counts = excluded_counts.get(key, {"broken": 0, "excluded": 0, "broken_ids": []})
        emp_name = meta.get("employee", "unknown")
        emp_id = meta.get("employee_id")
        broken = int(counts.get("broken", 0))
        excluded_other = int(counts.get("excluded", 0))
        broken_ids = sorted({fid for fid in counts.get("broken_ids", []) if fid})
        excluded_total = broken + excluded_other
        summary.append(
            {
                "dipendente": emp_name,
                "matricola": emp_id,
                "turni_totali": 0,
                "turni_straordinari": 0,
                "turni_notte": 0,
                "turni_pomeriggio": 0,
                "turni_festivi": 0,
                "turni_notte_pomeriggio_festivi": 0,
                "turni_straordinari_notte": 0,
                "turni_straordinari_pomeriggio": 0,
                "turni_straordinari_festivi": 0,
                "danneggiati": broken,
                "esclusi": excluded_other,
                "esclusi_totali": excluded_total,
                "file_danneggiati": broken_ids,
            }
        )
        emp_safe_name = safe_name(emp_name)
        emp_output_dir = os.path.join(output_dir, emp_safe_name)
        ensure_dir(emp_output_dir)
        emp_report = {
            "dipendente": emp_name,
            "matricola": emp_id,
            "turni_totali": 0,
            "turni_straordinari": 0,
            "soglia_straordinario_ore": hours_threshold,
            "turni_notte": 0,
            "turni_pomeriggio": 0,
            "turni_festivi": 0,
            "turni_notte_pomeriggio_festivi": 0,
            "turni_straordinari_notte": 0,
            "turni_straordinari_pomeriggio": 0,
            "turni_straordinari_festivi": 0,
            "danneggiati": broken,
            "esclusi": excluded_other,
            "esclusi_totali": excluded_total,
            "file_danneggiati": broken_ids,
            "generato_il": pd.Timestamp.now().isoformat(),
        }
        index_path_emp = os.path.join(emp_output_dir, "report.json")
        with open(index_path_emp, "w", encoding="utf-8") as f:
            json.dump(emp_report, f, indent=2, ensure_ascii=False)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    summary_csv_path = os.path.splitext(output_path)[0] + ".csv"
    try:
        pd.DataFrame(summary).to_csv(summary_csv_path, index=False)
    except Exception as exc:
        logger.error("Failed to write summary CSV %s: %s", summary_csv_path, exc)

    total_shifts = sum(e.get("turni_totali", 0) for e in summary)
    total_overtime = sum(e.get("turni_straordinari", 0) for e in summary)
    logger.info("Summary saved to %s", resolver.path_for_log(output_path))
    logger.info("Summary CSV saved to %s", resolver.path_for_log(summary_csv_path))
    logger.info(
        "Processati %s dipendenti: %s turni totali, %s turni straordinari",
        len(summary),
        total_shifts,
        total_overtime,
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--index",
        required=True,
        help="Path to included.index.json from fetch_index",
    )
    parser.add_argument(
        "--output",
        default="output/overtime_summary.json",
        help="Output path for summary (default: output/overtime_summary.json)",
    )
    parser.add_argument("--employee", help="Process only this employee (case-insensitive)")
    parser.add_argument("--hours", type=float, default=6.0, help="Overtime threshold in hours (default: 6.0)")
    parser.add_argument(
        "--close-gap-hours",
        type=float,
        default=16.0,
        help="Max hours between entry/exit when closing incomplete pairs (default: 16.0)",
    )
    parser.add_argument(
        "--excluded",
        help="Path to excluded.index.json (adds broken/excluded counts to summary/report)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    from drive_scripts.logging_utils import setup_logging
    setup_logging(args.verbose)

    calculate_overtime(
        args.index,
        args.output,
        args.employee,
        args.hours,
        args.close_gap_hours,
        args.excluded,
    )
