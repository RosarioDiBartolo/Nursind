# Nursind Text Pipeline

Pipeline for extracting text from payroll PDFs, parsing raw text into day/event data, and producing per-employee shift outputs.

## Active Flow

```
Drive -> scan_directory -> scan/included.index.json
-> extract_text_from_index -> output/text_extracted/<employee>/*.txt
-> extract_days_from_text_raw -> output/parsed_from_text/**/days.csv
-> extract_events_from_days_raw -> output/parsed_from_text/**/events_from_days_raw.csv
-> filter_midnight_events_from_days_raw -> output/parsed_from_text/**/events_from_days_raw.cleaned.csv
-> pair_employee_events_from_days_raw -> output/employee_shifts_from_raw/*.pairs.csv
-> turni_enrichment -> output/enriched/employee_pairs/*.enriched.csv
-> turni_employee_summary -> output/aggregates/turni_employee_summary.csv
```

## Quickstart

```powershell
python -m pip install -e .[dev]
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "scan" --included "included.index.json" --filtered "filtered.index.json" --verbose
python -m "src.extract_text_from_index" --index "scan/included.index.json" --out "output/text_extracted" --verbose
python -m "src.extract_days_from_text_raw" --input-dir "output/text_extracted" --out-dir "output/parsed_from_text" --verbose
python -m "src.extract_events_from_days_raw" --input-dir "output/parsed_from_text" --verbose
python -m "src.filter_midnight_events_from_days_raw" --input-dir "output/parsed_from_text" --verbose
python -m "src.pair_employee_events_from_days_raw" --input-dir "output/parsed_from_text" --output-dir "output/employee_shifts_from_raw" --verbose
python -m "src.turni_enrichment" --input-dir "output/employee_shifts_from_raw" --out-dir "output/enriched/employee_pairs" --verbose
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.csv" --format "csv" --verbose
```

## Docs

- `PIPELINE_COMMANDS.md`
- `docs/ingestion.md`
- `docs/preparation.md`
- `docs/enrichment.md`
- `docs/aggregation.md`
- `docs/schemas.md`
- `docs/shared_logic_registry.md`
