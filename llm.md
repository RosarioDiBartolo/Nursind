# LLM Context Index
Last updated: 2026-02-19

Purpose: this file is a lightweight map to the canonical project docs. Do not duplicate detailed behavior here.

## Start Here
- `README.md`: architecture flow, quickstart, canonical module entry points.
- `PIPELINE_COMMANDS.md`: canonical command list for the active pipeline.

## Stage Docs
- `docs/ingestion.md`: scan + text extraction.
- `docs/preparation.md`: days/events extraction, midnight cleanup, pairing.
- `docs/enrichment.md`: enrichment step and fields.
- `docs/aggregation.md`: yearly employee summary.
- `docs/schemas.md`: CSV schemas and field reference.

## Shared Logic + Agent Rules
- `docs/shared_logic_registry.md`: required shared-module reuse and anti-duplication inventory.
- `AGENTS.md`: agent workflow and discovery protocol requirements.
- `.github/copilot-instructions.md`: generic coding guidelines for this repo.

## Code Entry Points
- `src/scan_directory`
- `src/extract_text_from_index`
- `src/extract_days_from_text_raw.py`
- `src/extract_events_from_days_raw.py`
- `src/filter_midnight_events_from_days_raw.py`
- `src/pair_employee_events_from_days_raw.py`
- `src/turni_enrichment.py`
- `src/turni_employee_summary.py`
