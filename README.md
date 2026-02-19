# Healthcare Shift Processing Pipeline

A resilient data pipeline for extracting, pairing, enriching, and aggregating employee shift data from heterogeneous healthcare PDF time logs stored in Google Drive.

The pipeline is designed for noisy real-world inputs: mixed layouts, ZIP archives, malformed records, missing events, and overnight shifts.

## Overview

Input shape:

- Drive root folder
- One subfolder per employee
- PDF files (and ZIP archives containing PDF members)

Core outputs:

- Per-document parsed days/events
- Per-employee paired shifts
- Per-employee enriched shifts
- Per-employee yearly shift summaries

## Architecture Flow

1. Drive indexing
2. Text extraction
3. Dynamic day parsing
4. Raw event extraction
5. Midnight-event cleaning
6. Employee-level pairing
7. Shift enrichment
8. Yearly aggregation

## Technical Implementation Details

### Persistent Indexing Strategy

A key architectural element is the JSON index layer built around `MapIndex` models.

Instead of relying on in-memory state, pipeline stages persist execution state to disk through index files. This enables:

- Step-level resumability
- Crash recovery
- Incremental processing
- Deterministic skip/reprocess decisions

### Dual-Index Pattern (Included / Excluded)

The project uses an included/excluded split to make processing outcomes explicit.

In practice:

- Drive scan emits `scan/included.index.json` and `scan/filtered.index.json`.
- Text extraction maintains `included_text.index.json` and `excluded_text.index.json`.

Semantics:

- Included index: successfully processed files.
- Excluded/filtered index: files that failed validation/processing, with explicit reasons.

This gives traceability without mixing successful and failed records.

### Resumability and Reprocessing Controls

Text extraction is explicitly resumable across runs by loading prior included/excluded indexes.

Default behavior:

- Skip files already in included index (`--skip-included` default: true)
- Skip files already in excluded index (unless `--reprocess-excluded`)

Explicit overrides:

- `--reprocess-included`
- `--reprocess-excluded`

This conservative default reduces repeated failures and accelerates incremental runs.

### File-Based State (No Database Dependency)

Execution state is persisted as JSON files in the filesystem.

Benefits:

- Transparent and inspectable state
- Easy local debugging
- Portable runs (no external state service)
- CI-friendly behavior

### Architectural Rationale

This indexing approach separates business logic from execution state and makes long-running batch workflows safer:

- Partial failures do not invalidate the whole run
- Progress can be resumed without reprocessing everything
- Success/failure paths remain auditable at file level

## Pipeline Steps

### 1) Drive Indexing

Entry point: `python -m "src.scan_directory"`

Outputs:

- `scan/included.index.json`
- `scan/filtered.index.json`

Index records include fields such as:

- `employee`, `employee_id`
- `file_id`, `file_name`, `drive_path`
- `type`, `reason` (when filtered)

Notes:

- ZIP files are expanded as virtual folders and PDF members are indexed with synthetic IDs.
- ZIP failures are explicitly tracked (for example: invalid archive, no PDF members, scan errors).
- Employee identity is anchored to the Drive folder scope (`employee_id`).

### 2) Text Extraction

Entry point: `python -m "src.extract_text_from_index"`

Input: `scan/included.index.json`  
Output folder: `output/text_extracted`

Produced artifacts:

- `output/text_extracted/<employee>/*.txt`
- `output/text_extracted/included_text.index.json`
- `output/text_extracted/excluded_text.index.json`
- `output/text_extracted/extract_text_from_index.report.json`

Behavior:

- Resumable via included/excluded text indexes.
- `--skip-included` is enabled by default.
- ZIP-member files are downloaded via archive and extracted per member.
- Main pipeline stores extracted text, not PDF binaries.

### 3) Dynamic Parsing

Entry point: `python -m "src.extract_days_from_text_raw"`

Input: extracted text files  
Output: `output/parsed_from_text/**/days.csv`

Behavior:

- Format detection is dynamic (`cartellino_classic`, `timbrature_web`, `situazione_mensile`, fallback).
- Parsing is layout-aware and does not depend on a single fixed template.

### 4) Raw Event Extraction

Entry point: `python -m "src.extract_events_from_days_raw"`

Input: `days.csv`  
Output: `output/parsed_from_text/**/events_from_days_raw.csv`

Event model includes:

- `event_kind` (`E`/`U`)
- `event_time_hhmm`, `event_ts`
- `event_raw`, `event_pattern`
- source row references

Special handling:

- `24:00` is normalized to the next calendar day.

### 5) Midnight Event Cleaning

Entry point: `python -m "src.filter_midnight_events_from_days_raw"`

Input: `events_from_days_raw.csv`  
Outputs:

- `events_from_days_raw.cleaned.csv`
- aggregate removed rows CSV

Behavior:

- Removes midnight events (`00:00`, `0:00`, `24:00`) according to project cleaning rules.
- Preserves traceability through dedicated reports and removed-row exports.

### 6) Employee-Level Pairing

Entry point: `python -m "src.pair_employee_events_from_days_raw"`

Input: cleaned events across all files for each employee  
Output: `output/employee_shifts_from_raw/*.pairs.csv`

Pairing logic:

- Employee-scope chronological pairing (not limited to single document boundaries).
- Temporal compatibility with `--max-gap-hours` guard.
- Overnight crossings are supported.

Unmatched handling:

- Open events are not force-closed at day boundaries.
- Rows missing complete `entry_ts` + `exit_ts` are excluded from final pairs output.
- Unmatched totals are tracked in reports (`rows_unmatched_after_close`).

### 7) Shift Enrichment

Entry point: `python -m "src.turni_enrichment"`

Input (canonical pipeline): `output/employee_shifts_from_raw/*.pairs.csv`  
Output: `output/enriched/employee_pairs/*.enriched.csv`

Added fields include:

- `turno_code`, `turno_bucket`
- `is_holiday`, `is_night`, `is_afternoon`
- `duration_hours`, `year`

Classification:

- `turno_code` is deterministic from derived flags.
- `turno_bucket` uses configurable `--min-hours`.
- Holidays use Italian calendar rules, with optional `--no-holidays`.

### 8) Yearly Aggregation

Entry point: `python -m "src.turni_employee_summary"`

Input: enriched employee CSVs  
Output: `output/aggregates/turni_employee_summary.csv` (or JSON)

Counts per turno bucket:

- `N`, `P`, `F`, `M`, `S`

Behavior:

- Default year window: `2016..2025` (configurable).
- Writes to target path specified by `--out` (overwrite semantics, no built-in versioning).

## Idempotency and Determinism

- Core transformations are deterministic on identical inputs and parameters.
- Pipeline outputs are designed to be repeatable at data level.
- Some metadata fields are run-time dependent (for example index `generated_at`, run durations), so file bytes are not guaranteed to be identical across runs.

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

## Optional: Download PDFs From an Index

Use this helper when you already have a map index and want local PDF samples.

```powershell
python -m "src.download_from_index" --index "scan/samples.index.scan.map.json" --out "samples/from_index" --limit 20 --verbose
```

Main flags:

- `--index`: input map index JSON
- `--out`: destination folder
- `--limit`: first N files only (`0` means all)
- `--skip-existing` / `--no-skip-existing`: keep or overwrite existing files

## Documentation Map

- `PIPELINE_COMMANDS.md`
- `docs/ingestion.md`
- `docs/preparation.md`
- `docs/enrichment.md`
- `docs/aggregation.md`
- `docs/schemas.md`
- `docs/shared_logic_registry.md`

## TODO

- [ ] Expand automated tests to cover all pipeline stages (not only scan utilities).
- [ ] Add a reproducible environment setup with all runtime dependencies in `pyproject.toml`.
- [ ] Refactor large CLI modules into thinner orchestration + service layers.
- [ ] Remove deprecated compatibility paths after a migration window.
- [ ] Add CI checks for smoke runs of canonical `python -m "src.*"` entry points.
