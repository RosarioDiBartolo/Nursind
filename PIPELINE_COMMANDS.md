# Pipeline Commands

All commands use:
`python -m "module" ...`

## 0) Scan Drive folders into index files
```powershell
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "scan" --included "included.index.json" --filtered "filtered.index.json" --verbose
```

## 0b) Optional: Download PDFs from a map index
```powershell
python src/download_from_index.py --index "scan/samples.index.scan.map.json" --out "samples/from_index" --limit 20 --verbose
```
Useful for building small local datasets from an existing index.

## 1) Extract plain text from indexed PDFs
```powershell
python -m "src.extract_text_from_index" --index "scan/included.index.json" --out "output/text_extracted" --included "included_text.index.json" --excluded "excluded_text.index.json" --verbose
```

## 2) Build `days.csv` from extracted text
```powershell
python -m "src.extract_days_from_text_raw" --input-dir "output/text_extracted" --out-dir "output/parsed_from_text" --out-name "days.csv" --report-json "output/parsed_from_text/extract_days_from_text_raw.report.json" --verbose
```

## 3) Extract raw events from `days.csv`
```powershell
python -m "src.extract_events_from_days_raw" --input-dir "output/parsed_from_text" --days-name "days.csv" --out-name "events_from_days_raw.csv" --report-json "output/parsed_from_text/extract_events_from_days_raw.report.json" --verbose
```

## 4) Remove fake midnight events
```powershell
python -m "src.filter_midnight_events_from_days_raw" --input-dir "output/parsed_from_text" --events-name "events_from_days_raw.csv" --out-name "events_from_days_raw.cleaned.csv" --report-json "output/parsed_from_text/events_from_days_raw.clean_midnight.report.json" --removed-csv "output/parsed_from_text/events_from_days_raw.midnight_removed.csv" --verbose
```

## 5) Pair cleaned events at employee scope
```powershell
python -m "src.pair_employee_events_from_days_raw" --input-dir "output/parsed_from_text" --output-dir "output/employee_shifts_from_raw" --events-name "events_from_days_raw.cleaned.csv" --report-json "output/employee_shifts_from_raw/pair_employee_events_from_days_raw.report.json" --max-gap-hours 16 --verbose
```

## 6) Enrich employee pairs
```powershell
python -m "src.turni_enrichment" --input-dir "output/employee_shifts_from_raw" --out-dir "output/enriched/employee_pairs" --min-hours 6 --verbose
```
Note: `src.turni_enrichment` defaults `--input-dir` to `output/employee_shifts`, so pass `output/employee_shifts_from_raw` for this pipeline.

## 7) Build per-employee turno summary
```powershell
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.csv" --year-start 2016 --year-end 2025 --format "csv" --verbose
```
