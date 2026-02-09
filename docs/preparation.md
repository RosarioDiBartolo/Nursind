# Preparation (Days, Events, Cleaning, Pairing)

This stage transforms extracted text into cleaned event streams and employee-level pairs.

## Entry points

- `python -m "src.extract_days_from_text_raw"`
- `python -m "src.extract_events_from_days_raw"`
- `python -m "src.filter_midnight_events_from_days_raw"`
- `python -m "src.pair_employee_events_from_days_raw"`

## Flow

1. Parse text files into per-document `days.csv`.
2. Extract raw `E/U` events into `events_from_days_raw.csv`.
3. Remove fake midnight events into `events_from_days_raw.cleaned.csv`.
4. Pair cleaned events at employee scope into `output/employee_shifts_from_raw/*.pairs.csv`.

## Inputs

- Root text folder: `output/text_extracted`
- Parsed days root: `output/parsed_from_text`

## Outputs

- `output/parsed_from_text/**/days.csv`
- `output/parsed_from_text/**/events_from_days_raw.csv`
- `output/parsed_from_text/**/events_from_days_raw.cleaned.csv`
- `output/employee_shifts_from_raw/*.pairs.csv`
- Stage reports in `output/parsed_from_text/*.report.json` and `output/employee_shifts_from_raw/*.report.json`

## Typical commands

```powershell
python -m "src.extract_days_from_text_raw" --input-dir "output/text_extracted" --out-dir "output/parsed_from_text" --out-name "days.csv" --report-json "output/parsed_from_text/extract_days_from_text_raw.report.json" --verbose
python -m "src.extract_events_from_days_raw" --input-dir "output/parsed_from_text" --days-name "days.csv" --out-name "events_from_days_raw.csv" --report-json "output/parsed_from_text/extract_events_from_days_raw.report.json" --verbose
python -m "src.filter_midnight_events_from_days_raw" --input-dir "output/parsed_from_text" --events-name "events_from_days_raw.csv" --out-name "events_from_days_raw.cleaned.csv" --report-json "output/parsed_from_text/events_from_days_raw.clean_midnight.report.json" --removed-csv "output/parsed_from_text/events_from_days_raw.midnight_removed.csv" --verbose
python -m "src.pair_employee_events_from_days_raw" --input-dir "output/parsed_from_text" --output-dir "output/employee_shifts_from_raw" --events-name "events_from_days_raw.cleaned.csv" --report-json "output/employee_shifts_from_raw/pair_employee_events_from_days_raw.report.json" --max-gap-hours 16 --verbose
```
