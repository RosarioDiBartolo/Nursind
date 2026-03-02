# Shared Logic Registry

Purpose: keep a single, agent-friendly inventory of reusable modules so new work extends shared code instead of duplicating it.

## Maintenance Rules
- Read this file before adding helpers to any CLI or feature script.
- If logic is reused by 2+ files, extract it into a shared module and add/update an entry here.
- Keep entries short and concrete: module path, what it owns, keywords/aliases, inputs/outputs, where it is used.
- When replacing duplicated logic with a shared helper, note the old pattern under `Avoid Duplicates`.

## Shared Modules
| Module | Owns | Keywords/Aliases | Common Inputs/Outputs | Used By | Do Not Reimplement |
|---|---|---|---|---|---|
| `src/drive_service/index_runtime.py` | Index runtime helpers: `resolve_output_path`, `doc_attr`, `update_index_meta`, periodic flush/progress | output path, index metadata, progress checkpoints, mixed dict/object reads | In: index doc/item, output dir/name config. Out: resolved paths, updated metadata, flush decisions | `src/extract_text_from_index/runtime.py`, `src/extract_text_from_index/planning.py`, `src/pair_employee_events_from_days_raw/runtime.py` | Output path resolution, metadata refresh, checkpoint/flush orchestration |
| `src/pipeline_paths.py` | Shared pipeline output directory model (`PipelinePaths`, `build_pipelines_paths`/`build_output_paths`) with canonical stage paths and auto-create | output paths, output manager, stage folders, scan/text/events/shifts/enrichment/aggregation | In: optional root prefix + base output dir. Out: canonical stage directories as `Path`s, ensured folders | Pipeline CLIs + notebook shared config | Repeating hard-coded output folder constants per pipeline step |
| `src/drive_service/index/` | Index schema models (`MapIndex`, `ListIndex`) and shared converters/CLI service for map<->list transforms | index schema, map index, list index, converters, index transforms | In: map/list index JSON. Out: typed index models, converted index payloads | Scan, extraction, pairing pipelines, index conversion utility | Map/list conversion rules and schema model duplication |
| `src/drive_service/io_json.py` | JSON read/write helpers for consistent encoding and formatting | json io, read json, write json, encoding, pretty json | In: file paths + payloads. Out: parsed JSON objects, normalized JSON files | Drive/index scripts and reporting code | Custom JSON encoding/formatting wrappers across scripts |
| `src/drive_service/fs_utils.py` | Filesystem helpers (`ensure_dir`, `ensure_parent_dir`) | mkdir, ensure dir, ensure parent, path setup | In: directory/file paths. Out: guaranteed directory existence | Most pipeline scripts and drive utilities | Inline directory-creation utilities |
| `src/drive_service/names.py` | Safe/normalized naming helpers (`safe_name`, normalize helpers) | sanitize name, normalize filename, safe path segment | In: raw names/labels. Out: normalized safe names | Scan, download, extraction, output naming | Ad-hoc filename sanitization logic |
| `src/drive_service/archive_utils.py` | ZIP utilities: member-path normalization, archive-member IDs, list/extract ZIP PDF members | zip, archive, member path, archive id, zip pdf extraction | In: ZIP paths and member names. Out: normalized member IDs/paths, extracted PDF members | `src/scan_directory/scan_service.py` (ZIP expansion), `src/extract_text_from_index/planning.py`, `src/extract_text_from_index/workers.py` | ZIP member ID/path normalization and archive PDF traversal |
| `src/drive_service/index_downloads.py` | Index-aware PDF byte download helper (`download_pdf_bytes_for_index_item`) for direct and ZIP-member file IDs with optional archive cache | index download, zip member download, archive cache, synthetic file id | In: index item identity (`file_id`, optional source/archive metadata) + Drive client. Out: PDF bytes or normalized failure reason | `src/extract_text_from_index/workers.py`, `src/download_from_index.py` | Duplicate ZIP-member resolution/download logic across index consumers |
| `src/scan_directory/runtime.py` | Shared scan orchestration (`get_root_name`, report merge, `run_scan`) and scan report emission | drive scan runtime, scan report, employee-scope error handling, map merge | In: creds/drive/root/workers/output paths. Out: included/filtered indexes + `scan_directory.report.json` + run summary dict | `src/scan_directory/cli.py`, `src/notebooks/scan.ipynb` | Duplicate scan orchestration logic across CLI and notebooks |
| `src/pdf_text_extraction.py` | PDF text extraction primitives (`extract_text`, `extract_text_vertical`) | pdf extraction, pdfminer, vertical text | In: PDF paths/pages/options. Out: extracted raw text | `src/extract_text_from_index/quality.py` | PDF text extraction entry points and option handling |
| `src/raw_text_parsing.py` | Shared raw-text parsing primitives and regexes for direct text->events flow | raw parsing, regex parsing, day header parsing, event extraction | In: raw extracted text. Out: parsed headers, event matches, year/month metadata | `src/extract_events_from_text_raw/service.py`, `src/extract_events_from_text_raw/parsers/common.py` | Duplicate regex/date/event parsing primitives |
| `src/extract_events_from_text_raw/parsers/` | Extensible document parser platform (`BaseFormatParser`, auto-discovery loader, deterministic router, per-format parser modules) | parser registry, parser auto-discovery, routing score, cartellino/timbrature strategies | In: full document text + candidate row text. Out: selected parser + per-row values/hints used during direct event extraction | `src/extract_events_from_text_raw/service.py` | Hard-coded `if/elif` parser routing and format-specific extraction logic in service |
| `src/shift_services.py` | Shared shift/pair services (`PairsCloser`, `PairsPathResolver`, datetime helpers, classifiers, `compute_turno`, turno code/bucket assignment) | shifts, pairing, turno, classifiers, datetime helpers | In: normalized events/shifts and timestamps. Out: paired shifts, turno code/bucket data | Pairing/enrichment/summary scripts | Pair-closing, turno computation, classification helpers |

## Avoid Duplicates
- Do not re-implement output path resolution in pipelines; use `index_runtime.resolve_output_path`.
- Do not hardcode per-step output folder roots across scripts; use `output_paths.build_output_paths`.
- Do not re-implement mixed dict/object field reads for index items; use `index_runtime.doc_attr`.
- Do not re-implement index metadata refresh and periodic checkpoints; use `index_runtime.update_index_meta` and `index_runtime.maybe_flush_indexes`.
- Do not duplicate map/list index conversion logic in scripts; use `drive_service.index.converters`.
- Do not duplicate ZIP-member-aware index download flow; use `drive_service.index_downloads.download_pdf_bytes_for_index_item`.
- Do not re-implement PDF text extraction helpers; use `pdf_text_extraction`.
- Do not duplicate scan orchestration in CLI and notebooks; use `src.scan_directory.runtime.run_scan`.
- Keep CLI modules focused on argument parsing and orchestration; move reusable business logic into shared modules and register it here.

## Update Checklist
1. Shared helper added or changed.
2. Registry entry added/updated in this file.
3. Agent guidance updated if workflow expectations changed (`AGENTS.md`, `.github/copilot-instructions.md`, and relevant `docs/*` pages).
