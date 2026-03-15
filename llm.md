# LLM Navigation Guide
Last updated: 2026-03-15

Purpose: this is the lowest-context entry point for Codex/LLM sessions. Use it to decide what to read next and what to ignore.

## Session Start Order
1. Read `AGENTS.md`.
2. Read `.github/copilot-instructions.md`.
3. Read this file.
4. Read `docs/codebase_map.md`.
5. Only then open the stage doc or package that matches the task.

## Fast Routes By Task

### Pipeline shape, outputs, or commands
- Read `README.md`.
- Read `PIPELINE_COMMANDS.md`.
- If the issue is path wiring or default artifact locations, open `src/pipeline_paths.py`.

### Scan or document extraction
- Read `docs/ingestion.md`.
- Open `src/scan_directory/`.
- Open `src/extract_documents_from_index/`.
- Reuse shared helpers from `src/drive_service/` and `docs/shared_logic_registry.md`.

### Event parsing or page diagnostics
- Read `docs/preparation.md`.
- Open `src/extract_events_from_documents/`.
- Open `src/raw_text_parsing.py`.
- Open `src/extract_events_from_documents/parsers/` only for format-specific behavior.

### Midnight cleanup, pairing, or shift classification
- Read `docs/preparation.md`.
- Open `src/filter_midnight_events/`.
- Open `src/pair_employee_events/`.
- Open `src/shift_services.py` for shared pairing and classification logic.

### Enrichment or yearly summary
- Read `docs/enrichment.md` or `docs/aggregation.md`.
- Open `src/turni_enrichment/` or `src/turni_employee_summary/`.
- Reuse `src/shift_services.py` before adding new shift logic.

### Audit and coverage gaps
- Read `docs/preparation.md`.
- Open `src/parser_recall_audit/`.
- Open `src/timbrature_missing_report/`.
- Open `src/drive_service/text_extraction_csv.py` only if the issue is manifest or doc-json resolution.

### Shared helper extraction or anti-duplication review
- Read `docs/shared_logic_registry.md` first.
- Search existing shared modules before adding helpers.
- Prefer `src/drive_service/`, `src/raw_text_parsing.py`, `src/pdf_text_extraction.py`, and `src/shift_services.py`.

## Default Skip Paths
Do not open these by default unless the task explicitly needs them:
- `output/`
- `samples/`
- `tmp/`
- `src/notebooks/`
- `index.scan.map.json`
- generated reports or CSV artifacts under the pipeline root

## High-Value Files
Open these early when the task is broad or architectural:
- `docs/codebase_map.md`
- `docs/shared_logic_registry.md`
- `src/pipeline_paths.py`
- `src/extract_events_from_documents/page_analysis.py`
- `src/parser_recall_audit/service.py`
- `src/timbrature_missing_report/service.py`

## Test Routing
- Pipeline paths: `tests/test_pipeline_paths.py`, `tests/test_pipeline_path_option_defaults.py`
- Scan: `tests/test_scan_cli.py`, `tests/test_scan_runtime.py`, `tests/test_scan_service.py`
- Document extraction: `tests/test_extract_documents_from_index_step_contract.py`, `tests/test_extract_documents_zip_support.py`
- Event extraction and parsers: `tests/test_extract_events_from_text_*.py`, `tests/test_days_parser_*.py`
- Midnight cleanup: `tests/test_filter_midnight_events_step_contract.py`
- Pairing: `tests/test_pair_employee_events_step_contract.py`
- Enrichment: `tests/test_turni_enrichment_step_contract.py`
- Summary: `tests/test_turni_employee_summary_step_contract.py`
- Audit: `tests/test_parser_recall_audit.py`, `tests/test_timbrature_missing_report.py`

## Doc Roles
- `README.md`: human overview, architecture flow, quickstart, outputs.
- `CODEBASE_BRIEFING.md`: short human summary.
- `docs/codebase_map.md`: code ownership, routing, tests, and skip guidance.
- `docs/shared_logic_registry.md`: shared modules and anti-duplication rules.
- `docs/*.md`: stage behavior and schema details.
