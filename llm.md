# LLM Codebase Briefing (Detailed)
Last updated: 2026-02-14

Instruction: Update this file whenever a codebase edit changes behavior, structure, public APIs, outputs, or schemas.

## Repository purpose
- Process payroll PDFs through a text-first pipeline.
- Extract plain text, parse days/events, clean fake midnight events, pair shifts per employee, enrich shifts, and build per-employee summaries.

## Active pipeline (canonical)
1. `src/scan_directory`
2. `src/extract_text_from_index/`
3. `src/extract_days_from_text_raw.py`
4. `src/extract_events_from_days_raw.py`
5. `src/filter_midnight_events_from_days_raw.py`
6. `src/pair_employee_events_from_days_raw.py`
7. `src/turni_enrichment.py`
8. `src/turni_employee_summary.py`

## Top-level layout
- `src/scan_directory/`: Drive scanning CLI/service that produces `scan/included.index.json` and `scan/filtered.index.json`.
- `src/drive_service/`: Drive auth/client/config/schema and shared runtime helpers.
- `src/drive_service/index/`: Index schemas (`MapIndex`, `ListIndex`) plus shared map<->list conversion utilities and CLI.
- `src/extract_text_from_index/`: Download+extract text pipeline package from index with resumable included/excluded text indexes (`runtime.py` orchestration, `workers.py` I/O/extraction workers, `options.py` CLI options, `cli.py` wrapper).
- `src/pdf_text_extraction.py`: Shared `extract_text` / `extract_text_vertical` helpers.
- `src/raw_text_parsing.py`: Shared regex/document-format parsing utilities for raw text.
- `src/download_from_index.py`: Simple utility CLI that reads a MapIndex and downloads listed PDFs to a local folder.
- `src/extract_days_from_text_raw.py`: Build per-document `days.csv` from extracted text.
- `src/extract_events_from_days_raw.py`: Build per-document `events_from_days_raw.csv` from `days.csv`.
- `src/filter_midnight_events_from_days_raw.py`: Remove fake midnight events and write cleaned CSV + removed rows export.
- `src/pair_employee_events_from_days_raw.py`: Pair cleaned `E/U` events at employee scope across files.
- `src/shift_services.py`: Shared pairing/classification helpers (`PairsCloser`, `compute_turno`, etc.).
- `src/turni_enrichment.py`: Enrich employee pairs with turno classification fields.
- `src/turni_employee_summary.py`: Aggregate enriched files into per-employee yearly turno counts.
- `docs/`: ingestion/preparation/enrichment/aggregation/schema/shared-registry docs.

## Core index model
- `MapIndex` in `src/drive_service/index/map_index.py`
  - Fields: `root_id`, `generated_at`, `employee_count`, `total_files`, `files` (dict by `file_id`).
- `ListIndex` in `src/drive_service/index/list_index.py`
  - Fields: `root_id`, `generated_at`, `employee_count`, `total_files`, `files` (flat list; each item requires `file_id`).
- `IndexFile` in `src/drive_service/schema.py`
  - Fields: `employee`, `employee_id`, `file_id`, `file_name`, `drive_path?`, `outputs?`, `reason?`, `type?`.

## Pipeline details
### 1) Scan
- Entry: `python -m "src.scan_directory"`
- Outputs:
  - `scan/included.index.json`
  - `scan/filtered.index.json`
- Behavior:
  - Recursively scans employee folders for PDFs and ZIPs.
  - ZIP archives are expanded as virtual folders during scan; each PDF member is indexed as an individual file entry with a synthetic `file_id`.
  - ZIP scan issues are recorded in filtered index with reasons (`invalid_zip_archive`, `zip_scan_error:*`, `zip_no_pdf_members`).

### 2) Extract text from index
- Entry: `python -m "src.extract_text_from_index"`
- Input: `scan/included.index.json`
- Outputs:
  - `output/text_extracted/<employee>/*.txt`
  - `included_text.index.json`, `excluded_text.index.json`, run report JSON
- Behavior:
  - `--skip-included` by default.
  - `--reprocess-included` and `--reprocess-excluded` supported.
  - Uses `drive_service/index_runtime.py` for path/doc/index checkpoint shared logic.
  - Supports ZIP-member synthetic IDs from scan (`zip::...`): downloads the archive, extracts the referenced PDF member, then runs the normal/vertical text quality selection.
  - ZIP downloads are cached per download worker thread for member reuse.
  - ZIP-member download failures are tracked in excluded text index with explicit reasons (`zip_archive_download_error:*`, `zip_member_not_found`, `zip_member_invalid_pdf`).

### 3) Days from text
- Entry: `python -m "src.extract_days_from_text_raw"`
- Input: `output/text_extracted/**/*.txt`
- Output: `output/parsed_from_text/**/days.csv`

### 4) Events from days
- Entry: `python -m "src.extract_events_from_days_raw"`
- Input: `output/parsed_from_text/**/days.csv`
- Output: `output/parsed_from_text/**/events_from_days_raw.csv`

### 5) Clean midnight events
- Entry: `python -m "src.filter_midnight_events_from_days_raw"`
- Input: `events_from_days_raw.csv`
- Output:
  - `events_from_days_raw.cleaned.csv`
  - removed rows aggregate CSV

### 6) Pair employee events
- Entry: `python -m "src.pair_employee_events_from_days_raw"`
- Input: cleaned events in folder mode (`--input-dir`)
- Output: `output/employee_shifts_from_raw/*.pairs.csv`

### 7) Enrichment
- Entry: `python -m "src.turni_enrichment"`
- Input: `output/employee_shifts_from_raw/*.pairs.csv` (canonical pipeline). Note: CLI default input dir is `output/employee_shifts`.
- Output: `output/enriched/employee_pairs/*.enriched.csv`
- Adds `turno_bucket` classification with values:
  - `N` night, `P` afternoon, `F` holiday/Sunday, `M` morning/other when duration is `> --min-hours`
  - `S` short shifts when duration is `<= --min-hours`

### 8) Employee summary
- Entry: `python -m "src.turni_employee_summary"`
- Input: enriched files
- Output: `output/aggregates/turni_employee_summary.csv` (or JSON)
- Counts yearly totals for `turno_bucket` values `N/P/F/M/S` (fallback computes bucket if missing).

## Canonical commands
See `PIPELINE_COMMANDS.md` for full command list.

## Shared logic guidance
- Shared utility inventory is maintained in `docs/shared_logic_registry.md`.
- Before adding helper logic, check and reuse modules from:
  - `src/drive_service/`
  - `src/pdf_text_extraction.py`
  - `src/raw_text_parsing.py`
  - `src/shift_services.py`
- Prefer small, focused files and split modules along clear logic boundaries.
- Keep modules as independent as practical, with explicit interfaces between orchestration, I/O, and business logic.
- Keep each file/module to a maximum of 3 responsibilities; split once a 4th responsibility emerges.

## Tests
- Current tests focus on Drive scan utilities:
  - `tests/test_scan_service.py`

## Dependencies
- Python >= 3.11
- `pdfplumber`, `pandas`, `holidays`, `pytest` (dev)
- Google Drive: `google-api-python-client`, `google-auth`, `google-auth-oauthlib`
- `python-dotenv`

## Where to start reading
- `src/extract_text_from_index/runtime.py`
- `src/raw_text_parsing.py`
- `src/extract_days_from_text_raw.py`
- `src/extract_events_from_days_raw.py`
- `src/filter_midnight_events_from_days_raw.py`
- `src/pair_employee_events_from_days_raw.py`
- `src/turni_enrichment.py`
- `src/turni_employee_summary.py`
