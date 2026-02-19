# AI Coding Guidelines (General)
Keep this file **generic**. Project-specific context should live in `README.md` and `docs/*`.

## Mandatory context
- Always read `AGENTS.md` when present.
- Read `README.md` before making changes.
- Read `docs/shared_logic_registry.md` before introducing or moving shared helpers.
- Read the relevant step docs in `docs/*` for impacted pipeline stages.

## Markdown files (purpose + usage)
- `README.md`: Public overview and quickstart; keep high-level and current.
- `AGENTS.md`: Agent routing/instructions; must be followed when present.
- `llm.md`: Lightweight navigation/index doc that points to canonical docs.
- `CODEBASE_BRIEFING.md`: One-page human summary; keep short and point to canonical docs.
- `PIPELINE_COMMANDS.md`: Canonical command list for the active pipeline.
- `docs/ingestion.md`: Detailed docs for scan + text extraction pipeline.
- `docs/preparation.md`: Detailed docs for event cleanup + pairing flow.
- `docs/enrichment.md`: Shift enrichment step docs.
- `docs/aggregation.md`: Employee summary docs.
- `docs/schemas.md`: CSV schema reference.
- `docs/shared_logic_registry.md`: Inventory of shared modules and anti-duplication rules.

## General code patterns
- Keep functions small, composable, and focused on a single responsibility.
- Keep files/modules small, cohesive, and logic-focused.
- Prefer low coupling between modules and explicit interfaces between responsibilities.
- Split code by functional boundaries (business logic vs I/O vs orchestration), not by file size alone.
- Limit each file/module to at most 3 responsibilities; split when a 4th responsibility appears.
- Prefer explicit, readable control flow over cleverness.
- Use relative imports within a package; use absolute imports for third-party deps.
- Use `logging.getLogger(__name__)` and configurable log levels for CLIs.
