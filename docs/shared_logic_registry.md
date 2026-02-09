# Shared Logic Registry

Purpose: keep a single, agent-friendly inventory of reusable modules so new work extends shared code instead of duplicating it.

## Maintenance Rules
- Read this file before adding helpers to any CLI or feature script.
- If logic is reused by 2+ files, extract it into a shared module and add/update an entry here.
- Keep entries short and concrete: module path, what it owns, where it is used.
- When replacing duplicated logic with a shared helper, note the old pattern under `Avoid Duplicates`.

## Shared Modules
| Module | Owns | Used By |
|---|---|---|
| `src/drive_service/index_runtime.py` | Index runtime helpers: `resolve_output_path`, `doc_attr`, `update_index_meta`, periodic flush/progress | `src/extract_text_from_index/runtime.py`, `src/extract_text_from_index/planning.py` |
| `src/drive_service/io_json.py` | JSON read/write helpers for consistent encoding and formatting | Drive/index scripts and reporting code |
| `src/drive_service/fs_utils.py` | Filesystem helpers (`ensure_dir`, `ensure_parent_dir`) | Most pipeline scripts and drive utilities |
| `src/drive_service/names.py` | Safe/normalized naming helpers (`safe_name`, normalize helpers) | Scan, download, extraction, output naming |
| `src/pdf_text_extraction.py` | PDF text extraction primitives (`extract_text`, `extract_text_vertical`) | `src/extract_text_from_index/quality.py` |
| `src/raw_text_parsing.py` | Shared raw-text parsing primitives and regexes for text->days/events flow | `src/extract_days_from_text_raw.py`, `src/extract_events_from_days_raw.py` |
| `src/shift_services.py` | Shared shift/pair services (`PairsCloser`, `PairsPathResolver`, datetime helpers, classifiers, `compute_turno`) | Pairing/enrichment/summary scripts |

## Avoid Duplicates
- Do not re-implement output path resolution in pipelines; use `index_runtime.resolve_output_path`.
- Do not re-implement mixed dict/object field reads for index items; use `index_runtime.doc_attr`.
- Do not re-implement index metadata refresh and periodic checkpoints; use `index_runtime.update_index_meta` and `index_runtime.maybe_flush_indexes`.
- Do not re-implement PDF text extraction helpers; use `pdf_text_extraction`.
- Keep CLI modules focused on argument parsing and orchestration; move reusable business logic into shared modules and register it here.

## Update Checklist
1. Shared helper added or changed.
2. Registry entry added/updated in this file.
3. Agent guidance updated if workflow expectations changed (`AGENTS.md`, `llm.md`).
