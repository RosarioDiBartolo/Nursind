# Codebase Briefing (Short)
Last updated: 2026-03-15

## Purpose
Process payroll PDFs through a canonical document-first pipeline and produce per-employee shift summaries plus audit outputs.

## Fast Navigation
- LLM/Codex sessions should start with `llm.md`.
- For code ownership, test routing, and skip guidance, use `docs/codebase_map.md`.
- For shared helper reuse rules, use `docs/shared_logic_registry.md`.

## Active pipeline
1. `src/scan_directory`
2. `src/extract_documents_from_index`
3. `src/extract_events_from_documents`
4. `src/filter_midnight_events`
5. `src/pair_employee_events`
6. `src/turni_enrichment`
7. `src/turni_employee_summary`
8. `src/timbrature_missing_report`

The supported filesystem layout under the shared pipeline root is:
`scan`, `documents`, `events`, `shifts`, `enrichment`, `aggregation`.

## Key entry points
- Scan: `python -m "src.scan_directory"`
- Document extraction: `python -m "src.extract_documents_from_index"`
- Events, clean, pair: `python -m "src.extract_events_from_documents"`, `python -m "src.filter_midnight_events"`, `python -m "src.pair_employee_events"`
- Enrich + summary: `python -m "src.turni_enrichment"`, `python -m "src.turni_employee_summary"`
- Audit: `python -m "src.timbrature_missing_report"`

## Details
For full architecture, commands, and schemas, see:
- `README.md`
- `llm.md`
- `docs/codebase_map.md`
- `PIPELINE_COMMANDS.md`
- `docs/ingestion.md`
- `docs/preparation.md`
- `docs/enrichment.md`
- `docs/aggregation.md`
- `docs/schemas.md`
- `docs/shared_logic_registry.md`
