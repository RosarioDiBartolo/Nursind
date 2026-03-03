# Preparation (Direct Events, Cleaning, Pairing)

This stage transforms canonical extracted document payloads directly into cleaned event streams and employee-level pairs.

Step structure reference: `docs/step_contract_v1.md`

## Entry points

- `python -m "src.extract_events_from_text_raw"`
- `python -m "src.filter_midnight_events_from_days_raw"`
- `python -m "src.pair_employee_events_from_days_raw"`

## Flow

1. Parse per-employee document manifest CSV rows, load each canonical `docs/*.json` payload, and emit per-document `events_from_text_raw.csv`.
2. Remove fake midnight events into `events_from_text_raw.cleaned.csv`.
3. Pair cleaned events at employee scope into `output/employee_shifts_from_raw/*.pairs.csv`.

The direct event step no longer scans legacy raw `.txt` files; canonical manifest rows with `doc_json` are required.

## Inputs

- Root document manifests folder: `output/text_extracted`
- Raw events root: `output/events`

## Outputs

- `output/text_extracted/<employee>.csv`
- `output/text_extracted/docs/*.json`
- `output/events/**/events_from_text_raw.csv`
- `output/events/**/events_from_text_raw.cleaned.csv`
- `output/employee_shifts_from_raw/*.pairs.csv`
- Stage reports in `output/events/*.report.json` and `output/employee_shifts_from_raw/*.report.json`

## Typical commands

```powershell
python -m "src.extract_events_from_text_raw" --input-dir "output/text_extracted" --output-dir "output/events" --out-name "events_from_text_raw.csv" --report-json "output/events/extract_events_from_text_raw.report.json" --verbose
python -m "src.filter_midnight_events_from_days_raw" --input-dir "output/events" --events-name "events_from_text_raw.csv" --out-name "events_from_text_raw.cleaned.csv" --report-json "output/events/events_from_text_raw.clean_midnight.report.json" --removed-csv "output/events/events_from_text_raw.midnight_removed.csv" --verbose
python -m "src.pair_employee_events_from_days_raw" --input-dir "output/events" --output-dir "output/employee_shifts_from_raw" --events-name "events_from_text_raw.cleaned.csv" --report-json "output/employee_shifts_from_raw/pair_employee_events_from_days_raw.report.json" --max-gap-hours 16 --verbose
```

## Function-level testing (`extract_events_from_text_raw`)

```python
from src.extract_events_from_text_raw import process_one_text_row, process_many_text_rows

single = process_one_text_row(
    {
        "source_text_ref": "ROSSI/documento.txt",
        "doc_json": "docs/documento.json",
        "file_name": "documento.pdf",
    },
    output_dir="output/events",
    input_dir="output/text_extracted",
)

batch = process_many_text_rows(
    [
        {
            "source_text_ref": "ROSSI/doc1.txt",
            "doc_json": "docs/doc1.json",
            "file_name": "doc1.pdf",
        },
        {
            "source_text_ref": "ROSSI/doc2.txt",
            "doc_json": "docs/doc2.json",
            "file_name": "doc2.pdf",
        },
    ],
    output_dir="output/events",
    input_dir="output/text_extracted",
)
```

## Function-level testing (`filter_midnight_events_from_days_raw`)

```python
from src.filter_midnight_events_from_days_raw import (
    process_many_events_files,
    process_one_events_file,
)

single = process_one_events_file(
    "output/events/ROSSI/document.events_from_text_raw.csv",
    output_dir="output/events",
    input_base="output/events",
)

batch = process_many_events_files(
    [
        "output/events/ROSSI/doc1.events_from_text_raw.csv",
        "output/events/ROSSI/doc2.events_from_text_raw.csv",
    ],
    output_dir="output/events",
    input_base="output/events",
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
            "events_csv": "output/events/ROSSI/doc1.events_from_text_raw.cleaned.csv",
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
