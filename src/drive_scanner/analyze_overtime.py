import argparse
import json
import os
from typing import Any

import pandas as pd

from drive_scanner.fs_utils import ensure_dir, ensure_parent_dir
from .index_service import Index
from .logging_utils import get_logger
from .names import safe_name

logger = get_logger()


def _normalize_employee(name: str | None) -> str:
    return (name or "").strip().lower()


def _group_files_by_employee(files: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in files:
        name = getattr(item, "employee", None) or "unknown"
        key = _normalize_employee(name) or "unknown"
        if key not in grouped:
            grouped[key] = {"employee": name, "files": []}
        grouped[key]["files"].append(item)
    return list(grouped.values())


def _expected_pairs_path(
    index_path: str, emp_name: str, file_name: str | None, file_id: str | None
) -> str:
    base_dir = os.path.dirname(os.path.abspath(index_path))
    safe_emp = safe_name(emp_name or "unknown")
    base_name = safe_name(file_name or "unknown.pdf")
    if not base_name.lower().endswith(".pdf"):
        base_name = f"{base_name}.pdf"
    if file_id:
        file_tag = f"{os.path.splitext(base_name)[0]}__{file_id[:8]}"
    else:
        file_tag = os.path.splitext(base_name)[0]
    return os.path.abspath(os.path.join(base_dir, safe_emp, file_tag, "pairs.csv"))


def _path_for_log(path: str, index_path: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(index_path))
    try:
        return os.path.relpath(path, start=base_dir)
    except ValueError:
        return os.path.abspath(path)


def _close_incomplete_pairs(df: pd.DataFrame, max_gap_hours: float = 16.0) -> pd.DataFrame:
    if df.empty:
        return df
    if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
        return df

    working = df.copy()
    working["entry_ts"] = pd.to_datetime(working["entry_ts"], errors="coerce")
    working["exit_ts"] = pd.to_datetime(working["exit_ts"], errors="coerce")

    complete_mask = working["entry_ts"].notna() & working["exit_ts"].notna()
    closed_rows = working.loc[complete_mask].copy()
    closed_rows["closed_inferred"] = False

    incomplete = working.loc[~complete_mask].copy()
    events: list[tuple[pd.Timestamp, str, pd.Series]] = []
    for _, row in incomplete.iterrows():
        if pd.notna(row.get("entry_ts")):
            events.append((row["entry_ts"], "entry", row))
        elif pd.notna(row.get("exit_ts")):
            events.append((row["exit_ts"], "exit", row))

    events.sort(key=lambda item: item[0])
    pending_entry: pd.Series | None = None
    max_gap = pd.Timedelta(hours=max_gap_hours)

    for ts, kind, row in events:
        if kind == "entry":
            pending_entry = row
            continue
        if pending_entry is None:
            continue

        entry_ts = pending_entry["entry_ts"]
        exit_ts = ts
        if exit_ts < entry_ts:
            exit_ts = exit_ts + pd.Timedelta(days=1)
        if max_gap_hours > 0 and (exit_ts - entry_ts) > max_gap:
            pending_entry = None
            continue

        merged = pending_entry.copy()
        merged["exit_ts"] = exit_ts
        if "exit_raw" in merged.index:
            merged["exit_raw"] = row.get("exit_raw")
        merged["duration_hhmm"] = None
        merged["closed_inferred"] = True
        closed_rows = pd.concat([closed_rows, pd.DataFrame([merged])], ignore_index=True)
        pending_entry = None

    return closed_rows.reset_index(drop=True)


def calculate_overtime(
    index_path: str,
    output_path: str,
    employee_filter: str | None = None,
    hours_threshold: float = 6.0,
    close_gap_hours: float = 16.0,
) -> None:
    ensure_parent_dir(output_path)
    output_dir = os.path.dirname(output_path) or "output"
    output_dir = os.path.abspath(output_dir)
    ensure_dir(output_dir)

    report = Index.load_index(index_path, strict=True)
    employees = _group_files_by_employee(report.files)

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

    for emp in employees:
        emp_name = emp.get("employee", "unknown")
        pairs_dfs = []

        for inc in emp.get("files", []):
            expected_path = _expected_pairs_path(
                index_path,
                emp_name,
                getattr(inc, "file_name", None),
                getattr(inc, "file_id", None),
            )
            if not os.path.exists(expected_path):
                logger.warning(
                    "Missing pairs_csv for %s (expected: %s)",
                    getattr(inc, "file_name", "unknown"),
                    _path_for_log(expected_path, index_path),
                )
                continue
            try:
                df = pd.read_csv(expected_path)
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
            closed_df = _close_incomplete_pairs(df, max_gap_hours=close_gap_hours)
            if closed_df.empty:
                continue
            pairs_dfs.append(closed_df)

        if not pairs_dfs:
            summary.append({
                "employee": emp_name,
                "total_shifts": 0,
                "overtime_shifts": 0
            })
            continue

        merged_df = pd.concat(pairs_dfs, ignore_index=True)
        
        # Convert timestamps to datetime
        merged_df['entry_ts'] = pd.to_datetime(merged_df['entry_ts'], errors='coerce')
        merged_df['exit_ts'] = pd.to_datetime(merged_df['exit_ts'], errors='coerce')
        
        # Calculate durations
        merged_df['duration'] = merged_df['exit_ts'] - merged_df['entry_ts']
        
        # Filter valid shifts and count overtime
        valid_shifts = merged_df.dropna(subset=["entry_ts", "exit_ts", "duration"])
        valid_shifts = valid_shifts[valid_shifts["duration"] >= pd.Timedelta(0)]
        total_shifts = len(valid_shifts)
        overtime_count = (valid_shifts['duration'] > pd.Timedelta(hours=hours_threshold)).sum()

        # Save employee-specific CSV and report
        emp_safe_name = safe_name(emp_name)
        emp_output_dir = os.path.join(output_dir, emp_safe_name)
        ensure_dir(emp_output_dir)
        
        csv_path = os.path.join(emp_output_dir, "result.csv")
        merged_df.to_csv(csv_path, index=False)
        
        emp_report = {
            "employee": emp_name,
            "total_shifts": int(total_shifts),
            "overtime_shifts": int(overtime_count),
            "overtime_threshold_hours": hours_threshold,
            "generated_at": pd.Timestamp.now().isoformat()
        }
        
        index_path_emp = os.path.join(emp_output_dir, "report.json")
        with open(index_path_emp, "w", encoding="utf-8") as f:
            json.dump(emp_report, f, indent=2, ensure_ascii=False)
        
        logger.info(
            "Saved %s: %s and %s",
            emp_name,
            _path_for_log(csv_path, index_path),
            _path_for_log(index_path_emp, index_path),
        )

        summary.append({
            "employee": emp_name,
            "total_shifts": int(total_shifts),
            "overtime_shifts": int(overtime_count)
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    summary_csv_path = os.path.splitext(output_path)[0] + ".csv"
    try:
        pd.DataFrame(summary).to_csv(summary_csv_path, index=False)
    except Exception as exc:
        logger.error("Failed to write summary CSV %s: %s", summary_csv_path, exc)

    total_shifts = sum(e["total_shifts"] for e in summary)
    total_overtime = sum(e["overtime_shifts"] for e in summary)
    logger.info("Summary saved to %s", _path_for_log(output_path, index_path))
    logger.info("Summary CSV saved to %s", _path_for_log(summary_csv_path, index_path))
    logger.info(f"Processed {len(summary)} employees: {total_shifts} total shifts, {total_overtime} overtime shifts")

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
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    from .logging_utils import setup_logging
    setup_logging(args.verbose)

    calculate_overtime(args.index, args.output, args.employee, args.hours, args.close_gap_hours)
