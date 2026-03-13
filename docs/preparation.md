# Preparation (Direct Events, Cleaning, Pairing)

This stage transforms canonical extracted document payloads directly into cleaned event streams and employee-level pairs.

Step structure reference: `docs/step_contract_v1.md`

## Entry points

- `python -m "src.extract_events_from_documents"`
- `python -m "src.filter_midnight_events"`
- `python -m "src.pair_employee_events"`

## Flow

1. Parse employee document manifest CSV rows, load each canonical `docs/*.json` payload, and emit run-level `events.csv` plus page diagnostics `pages.csv`.
2. Remove fake midnight events into `events.cleaned.csv`.
3. Pair cleaned events at employee scope into `output/employee_shifts_from_raw/*.pairs.csv`.

The direct event step no longer scans legacy raw `.txt` files; canonical manifest rows with `doc_json` are required.

## Inputs

- Root document manifests folder: `output/text_extracted`
- Raw events root: `output/events`

## Outputs

- `output/text_extracted/<employee>.csv`
- `output/text_extracted/docs/*.json`
- `output/events/events.csv`
- `output/events/pages.csv`
- `output/events/events.cleaned.csv`
- `output/employee_shifts_from_raw/*.pairs.csv`
- Stage reports in `output/events/*.report.json` and `output/employee_shifts_from_raw/*.report.json`
- Optional audit outputs from `src.timbrature_missing_report`:
  - `missing_timbrature.report.json`
  - `missing_timbrature.employees.csv`
  - `missing_timbrature.issues.csv`

## Typical commands

```powershell
python -m "src.extract_events_from_documents" --input-dir "output/text_extracted" --output-dir "output/events" --out-name "events.csv" --pages-name "pages.csv" --report-json "output/events/extract_events.report.json" --verbose
python -m "src.filter_midnight_events" --input-dir "output/events" --events-name "events.csv" --out-name "events.cleaned.csv" --report-json "output/events/events.clean_midnight.report.json" --removed-csv "output/events/events.midnight_removed.csv" --verbose
python -m "src.pair_employee_events" --input-dir "output/events" --output-dir "output/employee_shifts_from_raw" --events-name "events.cleaned.csv" --report-json "output/employee_shifts_from_raw/pair_employee_events.report.json" --max-gap-hours 16 --verbose
python -m "src.timbrature_missing_report" --pipeline-dir "output/default" --verbose
```

## Missing timbrature audit

Use `src.timbrature_missing_report` after pairing when you need an employee-centered exception report instead of another transformation step.

The audit merges:

- scan-report employees whose direct root folder produced `0` included files
- document exclusions with reason `missing_text_layer`
- event pages with `decision_reason=missing_page_year_month`
- per-employee pair outputs to spot missing month/year coverage after pairing

The report is read-only against pipeline artifacts and supports both current and legacy folder layouts.

## Function-level testing (`extract_events_from_documents`)

```python
from src.extract_events_from_documents import process_one_text_row, process_many_text_rows

single = process_one_text_row(
    {
        "source_text_ref": "ROSSI/documento.txt",
        "doc_json": "docs/documento.json",
        "file_name": "documento.pdf",
    },
    output_dir="output/events",
    out_name="events.csv",
    pages_name="pages.csv",
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
    out_name="events.csv",
    pages_name="pages.csv",
    input_dir="output/text_extracted",
)
```

## Function-level testing (`filter_midnight_events`)

```python
from src.filter_midnight_events import (
    process_many_events_files,
    process_one_events_file,
)

single = process_one_events_file(
    "output/events/events.csv",
    output_dir="output/events",
    input_base="output/events",
)

batch = process_many_events_files(
    [
        "output/events/events.csv",
        "output/events/missing.csv",
    ],
    output_dir="output/events",
    out_name="events.cleaned.csv",
    input_base="output/events",
)
```

## Function-level testing (`pair_employee_events`)

```python
from src.pair_employee_events import (
    process_many_employee_events,
    process_one_employee_events,
)

one_employee = {
    "employee": "ROSSI",
    "employee_id": "emp-rossi",
    "files": [
        {
            "events_csv": "output/events/events.cleaned.csv",
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

