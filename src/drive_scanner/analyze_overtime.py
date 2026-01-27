import json
import os
import pandas as pd
import argparse

from drive_scanner.fs_utils import ensure_dir, ensure_parent_dir
from .logging_utils import get_logger
from .names import safe_name
from .io_json import load_json

logger = get_logger()

def calculate_overtime(
    index_path: str, output_path: str, employee_filter: str | None = None, hours_threshold: float = 6.0
) -> None:
    ensure_parent_dir(output_path)
    output_dir = os.path.dirname(output_path) or "output"
    ensure_dir(output_dir)
    
    report = Index.load (index_path)
    employees = report.get("included", [])
    
    if employee_filter:
        # Normalize for case-insensitive match
        filter_norm = employee_filter.strip().lower()
        employees = [emp for emp in employees if emp.get("employee", "").strip().lower() == filter_norm]
        if not employees:
            logger.warning(f"No employee found matching '{employee_filter}'")
            return
    
    summary = []

    for emp in employees:
        emp_name = emp.get("employee", "unknown")
        pairs_dfs = []

        for inc in emp.get("files", []):
            outputs = inc.get("outputs", {})
            pairs_path = outputs.get("pairs_csv")
            if pairs_path and os.path.exists(pairs_path):
                try:
                    df = pd.read_csv(pairs_path)
                    pairs_dfs.append(df)
                except Exception as e:
                    logger.error(f"Error loading {pairs_path}: {e}")

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
        valid_shifts = merged_df.dropna(subset=['duration'])
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
        
        logger.info(f"Saved {emp_name}: {csv_path} and {index_path_emp}")

        summary.append({
            "employee": emp_name,
            "total_shifts": int(total_shifts),
            "overtime_shifts": int(overtime_count)
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    total_shifts = sum(e["total_shifts"] for e in summary)
    total_overtime = sum(e["overtime_shifts"] for e in summary)
    logger.info(f"Summary saved to {output_path}")
    logger.info(f"Processed {len(summary)} employees: {total_shifts} total shifts, {total_overtime} overtime shifts")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--index"  , help="Path to index")
    parser.add_argument("--output", default="output/overtime_summary.json", help="Output path for summary (default: output/overtime_summary.json)")
    parser.add_argument("--employee", help="Process only this employee (case-insensitive)")
    parser.add_argument("--hours", type=float, default=6.0, help="Overtime threshold in hours (default: 6.0)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    from .logging_utils import setup_logging
    setup_logging(args.verbose)

    calculate_overtime(args.index, args.output, args.employee, args.hours)
