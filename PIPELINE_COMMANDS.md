# Pipeline Commands

All commands use:
`python -m "module" ...`

## 0) Scan Drive folders into index files
```powershell
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "scan" --included "included.index.json" --filtered "filtered.index.json" --verbose
```

## 0b) Optional: Download PDFs from a map index
```powershell
python -m "src.download_from_index" --index "scan/samples.index.scan.map.json" --out "samples/from_index" --random-sample 20 --seed 42 --verbose
```
Useful for building small local datasets from an existing index (including ZIP-member entries).
Default output is flat under `samples/from_index/`; pass `--no-flat-output` to restore per-employee folders.

## 1) Extract canonical documents from indexed PDFs
```powershell
python -m "src.extract_documents_from_index" --index "scan/included.index.json" --out "output/text_extracted" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
```

## 2) Extract events from canonical documents
```powershell
python -m "src.extract_events_from_documents" --input-dir "output/text_extracted" --output-dir "output/events" --out-name "events.csv" --pages-name "pages.csv" --report-json "output/events/extract_events.report.json" --verbose
```

## 3) Remove fake midnight events
```powershell
python -m "src.filter_midnight_events" --input-dir "output/events" --events-name "events.csv" --out-name "events.cleaned.csv" --report-json "output/events/events.clean_midnight.report.json" --removed-csv "output/events/events.midnight_removed.csv" --verbose
```

## 4) Pair cleaned events at employee scope
```powershell
python -m "src.pair_employee_events" --input-dir "output/events" --output-dir "output/employee_shifts_from_raw" --events-name "events.cleaned.csv" --report-json "output/employee_shifts_from_raw/pair_employee_events.report.json" --max-gap-hours 16 --verbose
```

## 5) Enrich employee pairs
```powershell
python -m "src.turni_enrichment" --input-dir "output/employee_shifts_from_raw" --out-dir "output/enriched/employee_pairs" --min-hours 6 --verbose
```

## 6) Build per-employee turno summary
```powershell
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.csv" --year-start 2016 --year-end 2025 --format "csv" --verbose
```

