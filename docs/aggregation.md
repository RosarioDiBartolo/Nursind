# Aggregation pipeline

This document describes the final summary stage after enrichment.

## Goal (input -> output)

- **Input:** enriched CSVs from `src.turni_enrichment` (default `output/enriched/employee_pairs`).
- **Output:** `output/aggregates/turni_employee_summary.csv` (or JSON).
- **Invariant:** this step counts shifts per employee/turno/year and does not alter pairing.

## Behavior

- Default year window: `2014` to `2025` (`--year-start`, `--year-end`).
- Uses `turno_bucket` when present; fallback computes it from enriched columns.
- Counts only `turno_bucket` in `N`, `P`, `F`, `M`, `S`.
- CSV output schema: one row per `(employee, turno)` with year columns as strings (for example `2014`, `2015`, ...).

## Run

```powershell
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.csv" --year-start 2014 --year-end 2025 --format "csv" --verbose
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.json" --format "json" --verbose
```

## Function-level testing (`turni_employee_summary`)

```python
from src.turni_employee_summary.service import (
    process_many_enriched_files,
    process_one_enriched_file,
)

single = process_one_enriched_file(
    "output/enriched/employee_pairs/ROSSI.enriched.csv",
    year_start=2014,
    year_end=2025,
)

batch = process_many_enriched_files(
    [
        "output/enriched/employee_pairs/ROSSI.enriched.csv",
        "output/enriched/employee_pairs/BIANCHI.enriched.csv",
    ],
    enriched_dir="output/enriched/employee_pairs",
    year_start=2014,
    year_end=2025,
)
```
