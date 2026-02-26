# Preparation (Days, Events, Cleaning, Pairing)

This stage transforms extracted text into cleaned event streams and employee-level pairs.

Step structure reference: `docs/step_contract_v1.md`

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

## Function-level testing (`extract_days_from_text_raw`)

Use these helpers when you want to test one file or a small custom batch without CLI orchestration:

```python
from src.extract_days_from_text_raw import process_one_text_file, process_many_text_files

single = process_one_text_file(
    "output/text_extracted/ROSSI/documento.txt",
    out_dir="output/parsed_from_text",
    input_base="output/text_extracted",
)

batch = process_many_text_files(
    [
        "output/text_extracted/ROSSI/doc1.txt",
        "output/text_extracted/ROSSI/doc2.txt",
    ],
    out_dir="output/parsed_from_text",
    input_base="output/text_extracted",
)
```

## Function-level testing (`extract_events_from_days_raw`)

```python
from src.extract_events_from_days_raw import process_one_days_file, process_many_days_files

single = process_one_days_file(
    "output/parsed_from_text/ROSSI/document.days.csv",
    output_dir="output/parsed_from_text",
    input_base="output/parsed_from_text",
)

batch = process_many_days_files(
    [
        "output/parsed_from_text/ROSSI/doc1.days.csv",
        "output/parsed_from_text/ROSSI/doc2.days.csv",
    ],
    output_dir="output/parsed_from_text",
    input_base="output/parsed_from_text",
)
```

## Function-level testing (`filter_midnight_events_from_days_raw`)

```python
from src.filter_midnight_events_from_days_raw import (
    process_many_events_files,
    process_one_events_file,
)

single = process_one_events_file(
    "output/parsed_from_text/ROSSI/document.events_from_days_raw.csv",
    output_dir="output/parsed_from_text",
    input_base="output/parsed_from_text",
)

batch = process_many_events_files(
    [
        "output/parsed_from_text/ROSSI/doc1.events_from_days_raw.csv",
        "output/parsed_from_text/ROSSI/doc2.events_from_days_raw.csv",
    ],
    output_dir="output/parsed_from_text",
    input_base="output/parsed_from_text",
)
```

## Function-level testing (`pair_employee_events_from_days_raw`)

```python
from src.pair_employee_events_from_days_raw import (
    process_many_employee_events,
    process_one_employee_events,
)

one_employee = {
    "employee": "ROSSI",
    "employee_id": "emp-rossi",
    "files": [
        {
            "events_csv": "output/parsed_from_text/ROSSI/doc1.events_from_days_raw.cleaned.csv",
            "file_id": "doc1",
            "file_name": "doc1",
        }
    ],
}

single = process_one_employee_events(
    one_employee,
    output_dir="output/employee_shifts_from_raw",
    max_gap_hours=16.0,
)

batch = process_many_employee_events(
    [one_employee],
    output_dir="output/employee_shifts_from_raw",
    max_gap_hours=16.0,
)
```
