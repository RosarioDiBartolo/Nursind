# Codebase Briefing (Short)
Last updated: 2026-02-09

## Purpose
Process payroll PDFs through a text-first pipeline and produce per-employee shift summaries.

## Active pipeline
1. `src/scan_directory`
2. `src/extract_text_from_index/`
3. `src/extract_days_from_text_raw.py`
4. `src/extract_events_from_days_raw.py`
5. `src/filter_midnight_events_from_days_raw.py`
6. `src/pair_employee_events_from_days_raw.py`
7. `src/turni_enrichment.py`
8. `src/turni_employee_summary.py`

## Key entry points
- Scan: `python -m "src.scan_directory"`
- Text extraction: `python -m "src.extract_text_from_index"`
- Days/events/clean/pair: `python -m "src.extract_days_from_text_raw"`, `python -m "src.extract_events_from_days_raw"`, `python -m "src.filter_midnight_events_from_days_raw"`, `python -m "src.pair_employee_events_from_days_raw"`
- Enrich + summary: `python -m "src.turni_enrichment"`, `python -m "src.turni_employee_summary"`

## Details
For full architecture, schemas, conventions, and workflows, see `llm.md`.
