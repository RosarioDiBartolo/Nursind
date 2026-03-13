# LLM Context Index
Last updated: 2026-03-13

Purpose: this file is a lightweight map to the canonical project docs. Do not duplicate detailed behavior here.

## Start Here
- `README.md`: architecture flow, quickstart, canonical module entry points, canonical pipeline layout.
- `PIPELINE_COMMANDS.md`: canonical command list for the active pipeline.

## Stage Docs
- `docs/ingestion.md`: scan + document extraction.
- `docs/preparation.md`: document-to-events extraction, midnight cleanup, pairing, and audit.
- `docs/enrichment.md`: enrichment step and fields.
- `docs/aggregation.md`: yearly employee summary.
- `docs/schemas.md`: CSV schemas and field reference.

## Shared Logic + Agent Rules
- `docs/shared_logic_registry.md`: required shared-module reuse and anti-duplication inventory.
- `AGENTS.md`: agent workflow and discovery protocol requirements.
- `.github/copilot-instructions.md`: generic coding guidelines for this repo.

## Code Entry Points
- `src/scan_directory`
- `src/extract_documents_from_index`
- `src/extract_events_from_documents`
- `src/filter_midnight_events`
- `src/pair_employee_events`
- `src/turni_enrichment`
- `src/turni_employee_summary`
- `src/timbrature_missing_report`
