# Healthcare Shift Processing Pipeline

A resilient data pipeline for extracting, pairing, enriching, and aggregating employee shift data from heterogeneous healthcare PDF time logs stored in Google Drive.

The pipeline is designed for noisy real-world inputs: mixed layouts, ZIP archives, malformed records, missing events, and overnight shifts.

## Overview

Input shape:

- Drive root folder
- One subfolder per employee
- PDF files (and ZIP archives containing PDF members)

Core outputs:

- Per-document raw event streams
- Per-employee paired shifts
- Per-employee enriched shifts
- Per-employee yearly shift summaries

## Architecture Flow

1. Drive indexing
2. Document extraction
3. Direct text-to-events extraction
4. Midnight-event cleaning
5. Employee-level pairing
6. Shift enrichment
7. Yearly aggregation

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
- Document extraction maintains `included_documents.index.json` and `excluded_documents.index.json`.

Semantics:

- Included index: successfully processed files.
- Excluded/filtered index: files that failed validation/processing, with explicit reasons.

This gives traceability without mixing successful and failed records.

### Resumability and Reprocessing Controls

Document extraction is explicitly resumable across runs by loading prior included/excluded indexes.

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
- `scan/scan_directory.report.json`

Index records include fields such as:

- `employee`, `employee_id`
- `file_id`, `file_name`, `drive_path`
- `type`, `reason` (when filtered)

Notes:

- ZIP files are expanded as virtual folders and PDF members are indexed with synthetic IDs.
- ZIP failures are explicitly tracked (for example: invalid archive, no PDF members, scan errors).
- Employee identity is anchored to the Drive folder scope (`employee_id`).
- Scan continues on per-employee failures and records them in `scan/scan_directory.report.json`.

### 2) Document Extraction

Entry point: `python -m "src.extract_documents_from_index"`

Input: `scan/included.index.json`  
Output folder: `output/text_extracted`

Produced artifacts:

- `output/text_extracted/docs/*.json`
- `output/text_extracted/<employee>.csv`
- `output/text_extracted/included_documents.index.json`
- `output/text_extracted/excluded_documents.index.json`
- `output/text_extracted/extract_documents_from_index.report.json`

Behavior:

- Resumable via included/excluded text indexes.
- `--skip-included` is enabled by default.
- ZIP-member files are downloaded via archive and extracted per member.
- PDFs without a text layer are excluded at this step and recorded in the excluded text index with reason `missing_text_layer`.
- Main pipeline stores one structured JSON per document (`docs/*.json`) as the canonical artifact, plus one thin manifest CSV per employee pointing to those JSON files.

### 3) Direct Text-To-Events Extraction

Entry point: `python -m "src.extract_events_from_text_raw"`

Input: per-employee extracted text manifest CSVs + canonical `docs/*.json` payloads  
Output: `output/events/**/events_from_text_raw.csv`

Behavior:

- Format detection is dynamic (`cartellino_classic`, `timbrature_web`, `situazione_mensile`, fallback).
- Documents are parsed from the canonical extracted document JSON payloads and emitted once from full-document context.
- The step requires employee manifest CSVs plus canonical `docs/*.json` payloads; it no longer falls back to legacy raw `.txt` inputs.
- Each event row carries source traceability fields (`source_doc_json`, source file metadata, page/line/slot positions, char offsets, geometry, and `source_event_ref`).
- `24:00` is normalized to the next calendar day in `event_ts`.

### 4) Midnight Event Cleaning

Entry point: `python -m "src.filter_midnight_events_from_days_raw"`

Input: `events_from_text_raw.csv`  
Outputs:

- `events_from_text_raw.cleaned.csv`
- aggregate removed rows CSV

Behavior:

- Removes midnight events (`00:00`, `0:00`, `24:00`) according to project cleaning rules.
- Preserves traceability through dedicated reports and removed-row exports.

### 5) Employee-Level Pairing

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

### 6) Shift Enrichment

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

### 7) Yearly Aggregation

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
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "scan" --included "included.index.json" --filtered "filtered.index.json" --report "scan_directory.report.json" --verbose
python -m "src.extract_documents_from_index" --index "scan/included.index.json" --out "output/text_extracted" --verbose
python -m "src.extract_events_from_text_raw" --input-dir "output/text_extracted" --output-dir "output/events" --verbose
python -m "src.filter_midnight_events_from_days_raw" --input-dir "output/events" --verbose
python -m "src.pair_employee_events_from_days_raw" --input-dir "output/events" --output-dir "output/employee_shifts_from_raw" --verbose
python -m "src.turni_enrichment" --input-dir "output/employee_shifts_from_raw" --out-dir "output/enriched/employee_pairs" --verbose
python -m "src.turni_employee_summary" --enriched-dir "output/enriched/employee_pairs" --out "output/aggregates/turni_employee_summary.csv" --format "csv" --verbose
```

## Optional: Download PDFs From an Index

Use this helper when you already have a map index and want local PDF samples.

```powershell
python -m "src.download_from_index" --index "scan/samples.index.scan.map.json" --out "samples/from_index" --random-sample 20 --seed 42 --verbose
```

Main flags:

- `--index`: input map index JSON
- `--out`: destination folder
- `--limit`: first N files only (`0` means all)
- `--random-sample`: random sample size from the index (`0` disables sampling)
- `--seed`: optional deterministic seed for `--random-sample`
- `--skip-existing` / `--no-skip-existing`: keep or overwrite existing files
- `--flat-output` / `--no-flat-output`: flat folder output (default) or per-employee subfolders

Notes:

- ZIP-member entries (synthetic `zip::...` file IDs emitted by scan) are supported.
- If both `--random-sample` and `--limit` are set, sampling runs first and then limit is applied.
- Flat output uses collision-safe names (`<employee>_<file-stem>_<id-hash>.pdf`).

## Notebook Pipeline Showcase

Use step notebooks in `src/notebooks` to demonstrate each pipeline stage with reproducible checks.

- `src/notebooks/_step_template.ipynb`: solid scaffold to start any new step notebook.
- `src/notebooks/shared_config.json`: single place to set the shared `root_id`, output root, and per-step filenames for all notebooks.
- `src/notebooks/run_pipeline.ipynb`: runs the full pipeline end-to-end using the same shared notebook config.
- `src/notebooks/scan.ipynb`: runs the current Drive scan runtime using the shared notebook config.
- `src/notebooks/extract_documents.ipynb`: runs document extraction against the shared scan output.
- `src/notebooks/extract_events_from_text_raw.ipynb`: runs direct text-to-events parsing against the shared extraction output.

Expected scan artifacts from the scan notebook:

- `<shared_output_root>/scan/included.index.json`
- `<shared_output_root>/scan/filtered.index.json`
- `<shared_output_root>/scan/scan_directory.report.json`

Next command after scan notebook:

```powershell
python -m "src.extract_documents_from_index" --index "<shared_output_root>/scan/included.index.json" --out "<shared_output_root>/text_extracted" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
```

## Documentation Map

- `PIPELINE_COMMANDS.md`
- `docs/ingestion.md`
- `src/notebooks/_step_template.ipynb`
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
