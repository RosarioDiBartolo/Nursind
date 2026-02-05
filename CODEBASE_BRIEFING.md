# Codebase Briefing
Last updated: 2026-01-30

## Purpose
Parse Italian payroll PDFs (cartellini + timbrature) into normalized CSV/JSON outputs and support bulk Google Drive scans/fetches.

## Layout
- Parsing lives under `src/parsing/` with the public facade in `parsing` and parser implementations under `src/parsing/parsers/` (cartellino, timbrature_compact, timbrature_elenco). Shared helpers stay in `parsing/parser_shared` and `parsing/timbrature_shared`.
- Drive tooling lives under `src/drive_scripts/` (scan, fetch/parse, download, index helpers).
- Scripts: `src/fetch_index_pipeline.py` (parallel download/parse), `src/process_extractions.py` (overtime summaries), `src/turni_summary.py` (riepilogo turni Excel-like; F include domeniche + festivita italiane; anni auto dalla prima/ultima timbratura se non specificati), `convert_index_to_map.py` (legacy list -> map index).

## Key Entry Points
- Parsing service: `parsing.parse_pdf` / `parsing.parse_text`
- Cartellino parser: `parsing.parsers.cartellino.parse_pdf`
- Timbrature parsers: `parsing.parsers.timbrature_elenco.parse_pdf`, `parsing.parsers.timbrature_compact.parse_pdf`
- Auto-detect router (compat): `parsing.parser_service.router.parse_pdf`
- CLIs: `python -m parsing.cli parse ...`, `python -m parsing.parsers.cartellino.cli parse ...`
- Drive: `python -m drive_scripts.scan_directory`, `python -m drive_scripts.fetch_index`, `python -m drive_scripts.download_index`, `python src/fetch_index_pipeline.py ...`

## Outputs (per PDF)
- `days.csv` (or `*.days.csv` via cartellino CLI): `year, month, day, dow, mo_f, mo_t, mo_lav, raw`
- `pairs.csv`: `year, month, day, dow, pair_index, entry_ts, exit_ts, duration_hhmm, turno, entry_raw, exit_raw`
- `totals.json`
- `report.json` (`{ meta, totals, validation }`)

## Detection + Pairing
- Router scores keyword hints + day-line patterns; vertical reconstruction runs when both family scores are zero.
- Pairing keeps incomplete E/U rows and matches the closest exit after entry (allows next-day exit).

## Timbrature Specifics
- Day parsing uses numeric heuristics; `mo_f`/`mo_t`/`mo_lav` are parsed but not used for validation.
- Validation compares summed complete pair durations against `ore_lavorate` with tolerance 1.0 hour.

## Drive Index Schemas
- `Index` (list) in `drive_scripts.index_service`: `{ root_id, generated_at, employee_count, files[] }`.
- `MapIndex` (map) in `drive_scripts.map_index_service`: `{ root_id, generated_at, employee_count, total_files, files{file_id: IndexFile} }` and can load legacy lists with `allow_legacy=True`.
- `IndexFile` fields: `employee`, `employee_id`, `file_id`, `file_name`, `outputs?`, `reason?`, `type?`.

## Overtime Summary
- `src/process_extractions.py` reads an included index, closes incomplete pairs within `--close-gap-hours` (default 16h), and writes per-employee `result.csv` + `report.json` plus a summary JSON/CSV (default `output/overtime_summary.*`). Optional excluded index adds broken/excluded counts and `file_danneggiati` in the summary. Report/summary keys are now Italian (e.g., `turni_totali`, `turni_straordinari`, `turni_notte`, `turni_pomeriggio`, `turni_festivi`, and `turni_notte_pomeriggio_festivi`, which counts unique shifts across those categories without double-counting).

## Tests
- Parsing samples: `tests/test_samples.py`, `tests/test_timbrature_samples.py`, `tests/test_parse_samples.py`
- Router: `tests/test_router_detection.py`, `tests/test_router_rotated_samples.py`
- Cartellino rotated: `tests/test_rotated_cartellino_samples.py`
- Sample outputs: `tests/test_check_samples.py`
- Drive utilities: `tests/test_scan_service.py`, `tests/test_download_index.py`, `tests/test_index_service.py`

## Docs
- Parser service walkthrough: `src/parsing/parser_service/PARSING_SERVICE.md`
- Detailed LLM briefing: `llm.txt`
