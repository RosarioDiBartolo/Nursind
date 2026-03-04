# Codebase Briefing (Short)
Last updated: 2026-03-02

## Purpose
Process payroll PDFs through a text-first pipeline and produce per-employee shift summaries.

## Active pipeline
1. `src/scan_directory`
2. `src/extract_documents_from_index/`
3. `src/extract_events_from_documents/`
4. `src/filter_midnight_events/`
5. `src/pair_employee_events/`
6. `src/turni_enrichment.py`
7. `src/turni_employee_summary.py`

## Key entry points
- Scan: `python -m "src.scan_directory"`
- Document extraction: `python -m "src.extract_documents_from_index"`
- Events/clean/pair: `python -m "src.extract_events_from_documents"`, `python -m "src.filter_midnight_events"`, `python -m "src.pair_employee_events"`
- Enrich + summary: `python -m "src.turni_enrichment"`, `python -m "src.turni_employee_summary"`

## Details
For full architecture, commands, and schemas, see:
- `README.md`
- `PIPELINE_COMMANDS.md`
- `docs/ingestion.md`
- `docs/preparation.md`
- `docs/enrichment.md`
- `docs/aggregation.md`
- `docs/schemas.md`
