# Enrichment pipeline

This stage enriches employee pair CSVs with classification fields used by aggregation.

## Entry point

- `python -m "src.turni_enrichment"`

## Input

- Canonical pipeline input: `output/employee_shifts_from_raw/*.pairs.csv`
- CLI default if `--input-dir` is omitted: `output/employee_shifts_from_raw/*.pairs.csv`

## Output

- Per-employee enriched files under `output/enriched/employee_pairs/*.enriched.csv`

## Output columns (ordered)

- `employee`
- `entry_ts`, `exit_ts`, `duration_hours`, `is_long`
- `is_holiday`, `is_afternoon`, `is_night`, `turno_code`, `turno_bucket`, `year`
- `turno`, `file_id`, `file_name`, `source_csv`

`turno_bucket` values:
- `N`: notte con durata `> --min-hours`
- `P`: pomeriggio con durata `> --min-hours`
- `F`: domenica/festivo con durata `> --min-hours` (override su N/P/M)
- `M`: mattina/altro con durata `> --min-hours`
- `S`: turno corto (durata `<= --min-hours`)

## Run

```powershell
python -m "src.turni_enrichment" --input-dir "output/employee_shifts_from_raw" --out-dir "output/enriched/employee_pairs" --min-hours 6 --verbose
```

Optional flags:
- `--no-holidays`: classify `F` using Sundays only (skip Italian holidays)
- `--stats-json "<PATH>"`: write enrichment run stats as JSON

## Function-level testing (`turni_enrichment`)

```python
from src.turni_enrichment import process_many_pairs_files, process_one_pairs_file

single = process_one_pairs_file(
    "output/employee_shifts_from_raw/ROSSI.pairs.csv",
    output_dir="output/enriched/employee_pairs",
    min_hours=6.0,
)

batch = process_many_pairs_files(
    [
        "output/employee_shifts_from_raw/ROSSI.pairs.csv",
        "output/employee_shifts_from_raw/BIANCHI.pairs.csv",
    ],
    output_dir="output/enriched/employee_pairs",
    input_dir="output/employee_shifts_from_raw",
    min_hours=6.0,
)
```
