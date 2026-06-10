# Afternoon Long Export

This step exports per-employee files for long afternoon shifts.

## Entry point

- `python -m "src.turni_afternoon_long_export"`
- Console script: `cartellino-turni-afternoon-long`

## Input

- Enriched CSVs: `<pipeline>/enrichment/*.enriched.csv`
- Pairs CSVs: `<pipeline>/shifts/*.pairs.csv`

## Output

One folder per employee under the selected output directory:

- `<employee>/<employee>.pomeriggi.csv`: filtered enriched rows where `is_afternoon`, `is_long`, and rounded entry target `14:00` are true
- `<employee>/<employee>.csv`: copied from the matching pairs-step result
- `<employee>/<employee>.pdf`: employee report titled `Report Pomeriggi oltre le 6 ore`, with summary metrics and the filtered table

## Run

```powershell
cartellino-turni-afternoon-long --enriched-dir "gruppi/GRUPPO 2/enrichment" --pairs-dir "gruppi/GRUPPO 2/shifts" --out-dir "gruppi/GRUPPO 2/afternoon_long_export" --verbose
```

Equivalent module command:

```powershell
.\.venv\Scripts\python.exe -m cartellino_parser.turni_afternoon_long_export --enriched-dir "gruppi/GRUPPO 2/enrichment" --pairs-dir "gruppi/GRUPPO 2/shifts" --out-dir "gruppi/GRUPPO 2/afternoon_long_export" --verbose
```

## PDF contents

Each employee PDF includes:

- employee name and surname
- title: `Report Pomeriggi oltre le 6 ore`
- summary metrics: number of rows, total duration, date period, holiday count
- table with the filtered employee rows
