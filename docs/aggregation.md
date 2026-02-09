# Aggregation pipeline

This document describes the final summary stage after enrichment.

## Goal (input -> output)

- **Input:** enriched CSVs from `src.turni_enrichment` (default `output/enriched/employee_pairs`).
- **Output:** `output/aggregates/turni_employee_summary.csv` (or JSON).
- **Invariant:** this step counts shifts per employee/turno/year and does not alter pairing.

## Behavior

- Default year window: `2016` to `2025` (`--year-start`, `--year-end`).
- Uses `is_long` when present; fallback is `--min-hours` with `duration_hours`.
- Counts only `turno_code` in `N`, `P`, `F`.
- CSV output schema: one row per `(employee, turno)` with year columns as strings (for example `2016`, `2017`, ...).

## Run

```powershell
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.csv" --year-start 2016 --year-end 2025 --format "csv" --verbose
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.json" --format "json" --verbose
```
