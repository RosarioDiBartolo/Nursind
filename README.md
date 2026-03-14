# Healthcare Shift Processing Pipeline

A resilient filesystem-first pipeline for extracting, pairing, enriching, and aggregating employee shift data from healthcare PDF time logs stored in Google Drive.

The active workflow is intentionally narrow and explicit:

- scan Drive folders into canonical index files
- extract canonical document payloads
- parse events from those documents
- audit parser recall from page diagnostics
- clean midnight artifacts
- pair events into shifts
- enrich shifts
- aggregate yearly summaries
- audit missing timbrature against the same pipeline root

## Canonical Pipeline Layout

Use one shared pipeline root, typically `output/default`.

Stage folders under that root:

- `scan`
- `documents`
- `events`
- `shifts`
- `enrichment`
- `aggregation`

The current pipeline supports only this layout. Legacy folders such as `text_extracted` and `employee_shifts_from_raw` are no longer part of the supported workflow.

## Architecture Flow

1. Drive indexing via `python -m "src.scan_directory"`
2. Document extraction via `python -m "src.extract_documents_from_index"`
3. Direct document-to-events extraction via `python -m "src.extract_events_from_documents"`
4. Parser recall audit via `python -m "src.parser_recall_audit"`
5. Midnight cleanup via `python -m "src.filter_midnight_events"`
6. Employee-level pairing via `python -m "src.pair_employee_events"`
7. Shift enrichment via `python -m "src.turni_enrichment"`
8. Yearly aggregation via `python -m "src.turni_employee_summary"`
9. Missing timbrature audit via `python -m "src.timbrature_missing_report"`

## Core Outputs

- `output/default/scan/included.index.json`
- `output/default/scan/filtered.index.json`
- `output/default/documents/docs/*.json`
- `output/default/documents/<employee>.csv`
- `output/default/events/events.csv`
- `output/default/events/pages.csv`
- `output/default/events/events.cleaned.csv`
- `output/default/shifts/*.pairs.csv`
- `output/default/enrichment/*.enriched.csv`
- `output/default/aggregation/turni_employee_summary.csv`
- `output/suspicious_pages.csv`
- `output/default/missing_timbrature.*`

## Technical Notes

### Persistent indexing

The scan and extraction stages persist state to disk through JSON indexes instead of relying on in-memory state. That gives:

- resumability
- crash recovery
- deterministic skip/reprocess decisions
- auditable included vs excluded outcomes

### Dual-index pattern

The pipeline uses explicit included/excluded outputs:

- scan emits `scan/included.index.json` and `scan/filtered.index.json`
- document extraction emits `documents/included_documents.index.json` and `documents/excluded_documents.index.json`

This keeps successful and failed records separate while preserving file-level traceability.

### Canonical document artifacts

Document extraction writes one structured JSON per document under `documents/docs/*.json` and one thin manifest CSV per employee under `documents/<employee>.csv`. Later stages operate on those canonical artifacts and no longer fall back to legacy raw text folders.

## Quickstart

```powershell
python -m pip install -e .[dev]
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "output/default/scan" --included "included.index.json" --filtered "filtered.index.json" --report "scan_directory.report.json" --verbose
python -m "src.extract_documents_from_index" --index "output/default/scan/included.index.json" --out "output/default/documents" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
python -m "src.extract_events_from_documents" --input-dir "output/default/documents" --output-dir "output/default/events" --verbose
python -m "src.parser_recall_audit" --root-dir "output" --verbose
python -m "src.filter_midnight_events" --input-dir "output/default/events" --verbose
python -m "src.pair_employee_events" --input-dir "output/default/events" --output-dir "output/default/shifts" --verbose
python -m "src.turni_enrichment" --input-dir "output/default/shifts" --out-dir "output/default/enrichment" --verbose
python -m "src.turni_employee_summary" --enriched-dir "output/default/enrichment" --out "output/default/aggregation/turni_employee_summary.csv" --format "csv" --verbose
python -m "src.timbrature_missing_report" --pipeline-dir "output/default" --verbose
python -m pytest -q
```

## Step Details

### 1) Drive indexing

Entry point: `python -m "src.scan_directory"`

Outputs:

- `output/default/scan/included.index.json`
- `output/default/scan/filtered.index.json`
- `output/default/scan/scan_directory.report.json`

Notes:

- ZIP files are expanded as virtual folders and PDF members are indexed with synthetic IDs.
- Direct subfolders of the scan root are treated as the employee inventory.
- The scan report includes `employees_found` and `employees_without_included_files`.

### 2) Document extraction

Entry point: `python -m "src.extract_documents_from_index"`

Input:

- `output/default/scan/included.index.json`

Produced artifacts:

- `output/default/documents/docs/*.json`
- `output/default/documents/<employee>.csv`
- `output/default/documents/included_documents.index.json`
- `output/default/documents/excluded_documents.index.json`
- `output/default/documents/extract_documents_from_index.report.json`

Behavior:

- resumable through included/excluded indexes
- ZIP-member-aware downloads
- text-layer validation with `missing_text_layer` exclusions
- one canonical JSON payload per document

### 3) Document-to-events extraction

Entry point: `python -m "src.extract_events_from_documents"`

Input:

- `output/default/documents`

Outputs:

- `output/default/events/events.csv`
- `output/default/events/pages.csv`

Behavior:

- dynamic format routing (`cartellino_classic`, `timbrature_web`, `situazione_mensile`, fallback)
- full-document parsing from canonical JSON payloads
- traceability fields carried onto event rows
- no raw `.txt` fallback

### 4) Parser recall audit

Entry point: `python -m "src.parser_recall_audit"`

Input:

- output root containing one or more pipeline folders, typically `output`

Outputs:

- `output/suspicious_pages.csv`
- `output/parser_recall_audit.report.json`

Behavior:

- reads each `output/*/events/pages.csv`
- ranks tiny pages, zero-event pages, low-coverage pages, and missing-year-month pages
- carries direct `source_file_link` references for opening the original Drive PDF
- adds heuristics such as neighboring page coverage and absence-keyword hits for manual triage

### 5) Midnight cleanup

Entry point: `python -m "src.filter_midnight_events"`

Input:

- `output/default/events/events.csv`

Outputs:

- `output/default/events/events.cleaned.csv`
- `output/default/events/events.midnight_removed.csv`

### 6) Employee-level pairing

Entry point: `python -m "src.pair_employee_events"`

Inputs:

- cleaned events under `output/default/events`

Outputs:

- `output/default/shifts/*.pairs.csv`
- `output/default/shifts/pair_employee_events.report.json`

Behavior:

- employee-scope chronological pairing
- `--max-gap-hours` guard
- overnight support
- no deprecated `--index` mode

### 7) Shift enrichment

Entry point: `python -m "src.turni_enrichment"`

Inputs:

- `output/default/shifts/*.pairs.csv`

Outputs:

- `output/default/enrichment/*.enriched.csv`
- optional `output/default/enrichment/turni_enrichment.stats.json`

### 8) Yearly aggregation

Entry point: `python -m "src.turni_employee_summary"`

Inputs:

- `output/default/enrichment/*.enriched.csv`

Outputs:

- `output/default/aggregation/turni_employee_summary.csv`

### 9) Missing timbrature audit

Entry point: `python -m "src.timbrature_missing_report"`

Input:

- canonical pipeline root `output/default`

Typical outputs:

- `output/default/missing_timbrature.report.json`
- `output/default/missing_timbrature.summary.csv`
- `output/default/missing_timbrature.findings.csv`
- `output/default/missing_timbrature.coverage.csv`

Behavior:

- reads the canonical `documents`, `events`, and `shifts` folders
- uses the scan report `employees_found` inventory
- flags non-OCR source files, unresolved page month/year, pairing failures, and missing `2014-01..2025-12` coverage months from `pages.csv` rows marked `relevant_for_coverage=true`
- rejects legacy-only layouts instead of auto-detecting them

## Optional: Download PDFs From an Index

Use this helper when you already have a map index and want local PDF samples.

```powershell
python -m "src.download_from_index" --index "output/default/scan/samples.index.scan.map.json" --out "samples/from_index" --random-sample 20 --seed 42 --verbose
```

Notes:

- ZIP-member entries are supported.
- flat output is the default; pass `--no-flat-output` for per-employee folders.

## Notebook Pipeline Showcase

Use the notebooks in `src/notebooks` to demonstrate the same canonical pipeline layout.

- `src/notebooks/shared_config.json`: shared notebook root id, output root, and per-step filenames
- `src/notebooks/shared_config.py`: resolves notebook context and canonical stage paths
- `src/notebooks/run_pipeline.ipynb`: end-to-end notebook that runs scan, document extraction, event extraction, midnight cleanup, pairing, enrichment, summary, and missing-timbrature audit from the shared config
- `src/notebooks/scan.ipynb`: scan stage
- `src/notebooks/extract_documents.ipynb`: document extraction stage
- `src/notebooks/extract_events_from_documents.ipynb`: event extraction stage

Expected scan artifacts from the scan notebook:

- `<shared_output_root>/scan/included.index.json`
- `<shared_output_root>/scan/filtered.index.json`
- `<shared_output_root>/scan/scan_directory.report.json`

Next command after the scan notebook:

```powershell
python -m "src.extract_documents_from_index" --index "<shared_output_root>/scan/included.index.json" --out "<shared_output_root>/documents" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
```

## Documentation Map

- `PIPELINE_COMMANDS.md`
- `docs/ingestion.md`
- `docs/preparation.md`
- `docs/enrichment.md`
- `docs/aggregation.md`
- `docs/schemas.md`
- `docs/shared_logic_registry.md`
- `llm.md`
- `CODEBASE_BRIEFING.md`

## Current Priorities

- expand automated coverage across the remaining pipeline stages
- keep CLI/docs/notebooks aligned on the same canonical layout
- continue thinning mixed-responsibility modules when new changes touch them
