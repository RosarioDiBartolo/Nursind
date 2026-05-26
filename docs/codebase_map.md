# Codebase Map
Last updated: 2026-03-15

Purpose: this file is the code-navigation map for Codex and other LLM sessions. It should help an agent find the owning module, shared helpers, and relevant tests without opening broad stage docs first.

## Read This Before Exploring
- Start with `llm.md` for task routing.
- Read `docs/shared_logic_registry.md` before introducing helpers or moving logic.
- Prefer concrete package modules such as `service.py`, `options.py`, and focused helpers over notebooks when tracing execution.
- Ignore generated pipeline artifacts unless the task is about outputs or debugging a specific run.

## Default Skip Paths
- `output/`
- `samples/`
- `tmp/`
- `src/notebooks/`
- `src/**/__pycache__/`
- large generated index files such as `index.scan.map.json`

## Architecture Backbone

| Concern | Primary owners | Shared dependencies to reuse | Main tests | Notes |
|---|---|---|---|---|
| Pipeline root and artifact wiring | `src/pipeline_paths.py`, `src/pipeline_path_types.py`, `src/*/artifacts.py` | `src/drive_service/fs_utils.py` | `tests/test_pipeline_paths.py`, `tests/test_pipeline_path_option_defaults.py` | Open this first for path-default or cross-step wiring issues. |
| Scan Drive tree into indexes | `src/scan_directory/cli.py`, `src/scan_directory/runtime.py`, `src/scan_directory/scan_service.py` | `src/drive_service/index/`, `src/drive_service/archive_utils.py`, `src/drive_service/index_runtime.py` | `tests/test_scan_cli.py`, `tests/test_scan_runtime.py`, `tests/test_scan_service.py` | Runtime orchestrates; service owns scan behavior. |
| Extract canonical document payloads | `src/extract_documents_from_index/runtime.py`, `src/extract_documents_from_index/service.py`, `src/extract_documents_from_index/workers.py` | `src/pdf_text_extraction.py`, `src/drive_service/index_downloads.py`, `src/drive_service/text_extraction_csv.py`, `src/drive_service/index_runtime.py` | `tests/test_extract_documents_from_index_step_contract.py`, `tests/test_extract_documents_zip_support.py` | Worker/process boundaries matter here. |
| Parse documents into events and page diagnostics | `src/extract_events_from_documents/service.py`, `src/extract_events_from_documents/page_analysis.py` | `src/raw_text_parsing.py`, `src/extract_events_from_documents/source_context.py`, `src/extract_events_from_documents/writers.py`, `src/reporting.py` | `tests/test_extract_events_from_text_step_contract.py`, `tests/test_extract_events_from_text_step_functions.py`, `tests/test_extract_events_from_text_traceability.py` | `page_analysis.py` is the main hotspot. |
| Format-specific parsing | `src/extract_events_from_documents/parsers/` | `src/extract_events_from_documents/parsers/common.py`, `src/raw_text_parsing.py` | `tests/test_days_parser_contracts.py`, `tests/test_days_parser_router.py`, `tests/test_days_parser_onboarding_contract.py` | Only open the specific parser module you need. |
| Parser recall QA | `src/parser_recall_audit/service.py` | `src/drive_service/text_extraction_csv.py`, `src/extract_events_from_documents/page_analysis.py`, `src/reporting.py` | `tests/test_parser_recall_audit.py` | Service currently mixes input loading, heuristics, and scoring. |
| Midnight cleanup | `src/filter_midnight_events/service.py` | `src/reporting.py` plus local dataframe cleanup helpers | `tests/test_filter_midnight_events_step_contract.py` | Small, focused package. |
| Pair employee events into shifts | `src/pair_employee_events/runtime.py`, `src/pair_employee_events/service.py` | `src/pair_employee_events/event_normalization.py`, `src/pair_employee_events/output_formatting.py`, `src/shift_services.py` | `tests/test_pair_employee_events_step_contract.py` | Keep CLI/runtime thin and reuse shared shift helpers. |
| Enrich shifts | `src/turni_enrichment/service.py` | `src/shift_services.py`, `src/reporting.py` | `tests/test_turni_enrichment_step_contract.py` | Uses shared holiday and shift classification helpers. |
| Export long afternoon shifts | `src/turni_afternoon_long_export/service.py` | `src/shift_services.py`, `src/reporting.py` | `tests/test_turni_afternoon_long_export_step_contract.py` | Filters enriched per-employee CSVs to rows where `is_afternoon` and `is_long` are both true. |
| Aggregate yearly summary | `src/turni_employee_summary/service.py` | `src/shift_services.py`, `src/reporting.py` | `tests/test_turni_employee_summary_step_contract.py` | Output-focused package. |
| Missing timbrature audit | `src/timbrature_missing_report/service.py` | `src/timbrature_missing_report/inputs.py`, `src/timbrature_missing_report/accumulator.py`, `src/timbrature_missing_report/issues.py`, `src/drive_service/text_extraction_csv.py`, `src/reporting.py` | `tests/test_timbrature_missing_report.py` | Read `inputs.py` first for layout assumptions. |
| Notebook configuration | `src/notebooks/shared_config.py`, `src/notebooks/pipeline_config.json` | `src/pipeline_paths.py` | `tests/test_notebook_shared_config.py`, `tests/test_run_pipeline_notebook.py` | Ignore notebooks unless the task explicitly mentions them. |

## Shared Logic First

| Shared module | Owns | Typical callers |
|---|---|---|
| `src/drive_service/index_runtime.py` | index metadata refresh, output path resolution, checkpoint flushing | scan and document extraction runtimes |
| `src/drive_service/text_extraction_csv.py` | document manifest rows, doc-json paths, traceability fields | document extraction, event extraction, missing-timbrature audit |
| `src/drive_service/index_downloads.py` | local, Drive, and ZIP-member PDF byte download flow | document extraction, sample download |
| `src/raw_text_parsing.py` | regexes and raw-text parsing primitives | event extraction and parser modules |
| `src/pdf_text_extraction.py` | text-layer checks and PDF text/layout extraction | document extraction |
| `src/shift_services.py` | pairing helpers, holiday classification, turno code logic | pairing, enrichment, summary |
| `src/pipeline_paths.py` | canonical stage layout and default artifact paths | CLI defaults, notebooks, tests |

## Practical Navigation Recipes

### If the task is "a CLI option or default path is wrong"
Read:
- `src/<step>/options.py`
- `src/<step>/artifacts.py`
- `src/pipeline_paths.py`
- the corresponding `tests/test_*step_contract*.py`

### If the task is "events are missing or page diagnostics look wrong"
Read:
- `src/extract_events_from_documents/service.py`
- `src/extract_events_from_documents/page_analysis.py`
- `src/extract_events_from_documents/source_context.py`
- `src/raw_text_parsing.py`
- parser-specific module only if a format-specific bug is likely

### If the task is "coverage or audit output looks wrong"
Read:
- `src/timbrature_missing_report/inputs.py`
- `src/timbrature_missing_report/service.py`
- `src/timbrature_missing_report/accumulator.py`
- `src/parser_recall_audit/service.py` only if page diagnostics are upstream of the issue

### If the task is "we should extract shared logic"
Read:
- `docs/shared_logic_registry.md`
- the affected `service.py`
- the obvious shared candidates under `src/drive_service/`, `src/raw_text_parsing.py`, `src/pdf_text_extraction.py`, and `src/shift_services.py`

## Current Hotspots
These files are structurally important but heavier than they should be. Prefer focused edits and split responsibilities when touching them.

- `src/pipeline_paths.py`
- `src/parser_recall_audit/service.py`
- `src/extract_events_from_documents/page_analysis.py`
- `src/timbrature_missing_report/service.py`
- `src/shift_services.py`

## Documentation Roles
- `README.md`: public overview and canonical pipeline explanation.
- `PIPELINE_COMMANDS.md`: command cookbook.
- `llm.md`: fast routing for LLM sessions.
- `docs/codebase_map.md`: ownership and navigation map.
- `docs/shared_logic_registry.md`: anti-duplication rules and shared-module inventory.
- `docs/ingestion.md`, `docs/preparation.md`, `docs/enrichment.md`, `docs/aggregation.md`, `docs/schemas.md`: stage behavior and schema details.
