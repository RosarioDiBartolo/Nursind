# Preparation (Direct Events, Cleaning, Pairing)

This stage transforms canonical extracted document payloads directly into cleaned event streams and employee-level pairs.

Step structure reference: `docs/step_contract_v1.md`

## Entry points

- `python -m "src.extract_events_from_documents"`
- `python -m "src.filter_midnight_events"`
- `python -m "src.pair_employee_events"`
- `python -m "src.timbrature_missing_report"`

Canonical pipeline root example:
`output/default`

## Flow

1. Parse employee document manifest CSV rows, load each canonical `docs/*.json` payload, and emit run-level `events.csv` plus page diagnostics `pages.csv`.
2. Remove fake midnight events into `events.cleaned.csv`.
3. Pair cleaned events at employee scope into `output/default/shifts/*.pairs.csv`.
4. Run the missing-timbrature audit against the canonical pipeline root when you need an employee-centered exception report.

The direct event step no longer scans raw `.txt` files. Pairing no longer supports legacy index mode. The audit expects the canonical `documents/events/shifts` layout.

## Inputs

- Root document manifests folder: `output/default/documents`
- Raw events root: `output/default/events`

## Outputs

- `output/default/documents/<employee>.csv`
- `output/default/documents/docs/*.json`
- `output/default/events/events.csv`
- `output/default/events/pages.csv`
- `output/default/events/events.cleaned.csv`
- `output/default/shifts/*.pairs.csv`
- Stage reports in `output/default/events/*.report.json` and `output/default/shifts/*.report.json`
- Optional audit outputs:
  - `output/default/missing_timbrature.report.json`
  - `output/default/missing_timbrature.summary.csv`
  - `output/default/missing_timbrature.findings.csv`
  - `output/default/missing_timbrature.coverage.csv`

## Typical commands

```powershell
python -m "src.extract_events_from_documents" --input-dir "output/default/documents" --output-dir "output/default/events" --out-name "events.csv" --pages-name "pages.csv" --report-json "output/default/events/extract_events.report.json" --verbose
python -m "src.filter_midnight_events" --input-dir "output/default/events" --events-name "events.csv" --out-name "events.cleaned.csv" --report-json "output/default/events/events.clean_midnight.report.json" --removed-csv "output/default/events/events.midnight_removed.csv" --verbose
python -m "src.pair_employee_events" --input-dir "output/default/events" --output-dir "output/default/shifts" --events-name "events.cleaned.csv" --report-json "output/default/shifts/pair_employee_events.report.json" --max-gap-hours 16 --verbose
python -m "src.timbrature_missing_report" --pipeline-dir "output/default" --verbose
python -m pytest -q
```

## Missing timbrature audit

The audit merges:

- the full scan-report `employees_found` inventory so every scanned employee appears in the summary CSV
- fixed employee coverage for every month from `2014-01` through `2025-12`
- coverage months derived only from `pages.csv` rows where `relevant_for_coverage=true` and `page_year/page_month` are valid
- valid coverage pages still count even when `events_extracted=0`
- findings rows in `missing_timbrature.findings.csv` for scan, document, page, and pairing exceptions
- coverage rows in `missing_timbrature.coverage.csv` for missing coverage months only
- scan-report employees whose direct root folder produced `0` included files
- document exclusions with reason `missing_text_layer`
- event pages with `decision_reason=missing_page_year_month`
- per-employee pair outputs only to surface pairing failures and output-file issues

## Function-level testing (`extract_events_from_documents`)

```python
from src.extract_events_from_documents import process_one_text_row, process_many_text_rows

single = process_one_text_row(
    {
        "source_text_ref": "ROSSI/documento.txt",
        "doc_json": "docs/documento.json",
        "file_name": "documento.pdf",
    },
    output_dir="output/default/events",
    out_name="events.csv",
    pages_name="pages.csv",
    input_dir="output/default/documents",
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
    output_dir="output/default/events",
    out_name="events.csv",
    pages_name="pages.csv",
    input_dir="output/default/documents",
)
```

## Function-level testing (`filter_midnight_events`)

```python
from src.filter_midnight_events import (
    process_many_events_files,
    process_one_events_file,
)

single = process_one_events_file(
    "output/default/events/events.csv",
    output_dir="output/default/events",
    input_base="output/default/events",
)

batch = process_many_events_files(
    [
        "output/default/events/events.csv",
        "output/default/events/missing.csv",
    ],
    output_dir="output/default/events",
    out_name="events.cleaned.csv",
    input_base="output/default/events",
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
            "events_csv": "output/default/events/events.cleaned.csv",
            "file_id": "doc1",
            "file_name": "doc1",
        }
    ],
}

single = process_one_employee_events(
    one_employee,
    output_dir="output/default/shifts",
    max_gap_hours=16.0,
)

batch = process_many_employee_events(
    [one_employee],
    output_dir="output/default/shifts",
    max_gap_hours=16.0,
)
```
