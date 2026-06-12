# Codebase Briefing

- The project is a script collection, not an installable package.
- `pipeline.json` is the only pipeline configuration and path authority.
- `src/scripts/` contains thin direct-run entry points.
- `src/core/` contains all reusable logic.
- `src/notebooks/` calls the same core services and configuration.
- Canonical outputs remain under scan, documents, events, shifts, enrichment,
  and aggregation stage folders.
- Tests are offline and behavior-focused; avoid adding wrapper-shape tests.

Start with `README.md`, then `docs/codebase_map.md` and
`docs/shared_logic_registry.md`.
