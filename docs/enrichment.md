# Enrichment pipeline

This stage enriches employee pair CSVs with classification fields used by aggregation.

## Entry point

- `python -m "src.turni_enrichment"`

## Input

- Canonical pipeline input: `output/employee_shifts_from_raw/*.pairs.csv`
- CLI default if `--input-dir` is omitted: `output/employee_shifts/*.pairs.csv`

## Output

- Per-employee enriched files under `output/enriched/employee_pairs/*.enriched.csv`

## Output columns (ordered)

- `employee`
- `entry_ts`, `exit_ts`, `duration_hours`, `is_long`
- `is_holiday`, `is_afternoon`, `is_night`, `turno_code`, `year`
- `turno`, `file_id`, `file_name`, `source_csv`

## Run

```powershell
python -m "src.turni_enrichment" --input-dir "output/employee_shifts_from_raw" --out-dir "output/enriched/employee_pairs" --min-hours 6 --verbose
```

Optional flags:
- `--no-holidays`: classify `F` using Sundays only (skip Italian holidays)
- `--stats-json "<PATH>"`: write enrichment run stats as JSON
