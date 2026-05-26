# Enrichment Pipeline

This stage enriches employee pair CSVs with classification fields used by aggregation.

## Entry point

- `python -m "src.turni_enrichment"`

Canonical pipeline root example:
`output/default`

## Input

- Canonical pipeline input: `output/default/shifts/*.pairs.csv`
- CLI default if `--input-dir` is omitted: `output/default/shifts`

## Output

- Per-employee enriched files under `output/default/enrichment/*.enriched.csv`

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
python -m "src.turni_enrichment" --input-dir "output/default/shifts" --out-dir "output/default/enrichment" --min-hours 6 --verbose
python -m "src.turni_afternoon_long_export" --enriched-dir "output/default/enrichment" --pairs-dir "output/default/shifts" --out-dir "output/afternoon_long_export" --verbose
python -m pytest -q
```

Optional flags:
- `--no-holidays`: classify `F` using Sundays only
- `--stats-json "<PATH>"`: write enrichment run stats as JSON

## Post-enrichment export

Use `python -m "src.turni_afternoon_long_export"` when you want one folder per employee containing:

- `<employee>.pomeriggi.csv`: filtered enriched rows where `is_afternoon`, `is_long`, and rounded entry target `14:00` are true
- `<employee>.csv`: the matching result file from the pairs step

## Function-level testing (`turni_enrichment`)

```python
from src.turni_enrichment.service import process_many_pairs_files, process_one_pairs_file

single = process_one_pairs_file(
    "output/default/shifts/ROSSI.pairs.csv",
    output_dir="output/default/enrichment",
    min_hours=6.0,
)

batch = process_many_pairs_files(
    [
        "output/default/shifts/ROSSI.pairs.csv",
        "output/default/shifts/BIANCHI.pairs.csv",
    ],
    output_dir="output/default/enrichment",
    input_dir="output/default/shifts",
    min_hours=6.0,
)
```
