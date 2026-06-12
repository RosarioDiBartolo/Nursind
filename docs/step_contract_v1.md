# Step Contract

Each primary step has:

1. A direct-run script in `src/scripts`.
2. Importable implementation under `src/core`.
3. Inputs and outputs derived from `pipeline.json`.
4. A JSON stage report where the existing pipeline already defines one.

Scripts accept only `--config` and logging flags. They must not contain
business logic. Core writers create output directories before writing.

Tests assert produced artifacts and meaningful behavior, not generic dictionary
shapes or delegation details.
