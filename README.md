# Healthcare Shift Processing Pipeline

This repository is a set of directly runnable Python scripts backed by shared
implementation modules. It is not a distributable Python package.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

Google credentials are read from `.env` or the environment:

```text
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_TOKEN_PATH=token.json
```

## Configuration

All scripts load the root [`pipeline.json`](pipeline.json). Relative output
paths are resolved from the repository root, regardless of the current working
directory.

```json
{
  "pipeline": {
    "name": "GRUPPO 3",
    "base_output_dir": "gruppi"
  },
  "drive": {
    "root_id": "..."
  },
  "steps": {
    "scan": {"workers": 8},
    "pair_events": {"max_gap_hours": 16},
    "enrich_shifts": {"min_hours": 6, "include_holidays": true},
    "summarize_shifts": {
      "year_start": 2014,
      "year_end": 2025,
      "format": "csv"
    }
  }
}
```

## Pipeline

Run these commands from the repository root:

```powershell
.\.venv\Scripts\python.exe src\scripts\scan.py
.\.venv\Scripts\python.exe src\scripts\extract_documents.py
.\.venv\Scripts\python.exe src\scripts\extract_events.py
.\.venv\Scripts\python.exe src\scripts\filter_midnight.py
.\.venv\Scripts\python.exe src\scripts\pair_events.py
.\.venv\Scripts\python.exe src\scripts\enrich_shifts.py
.\.venv\Scripts\python.exe src\scripts\summarize_shifts.py
```

Each script accepts `--config <path>` and `--verbose`.

The canonical output layout is:

```text
<base_output_dir>/<pipeline.name>/
  scan/
  documents/
  events/
  shifts/
  enrichment/
  aggregation/
```

Existing artifact names and CSV/JSON schemas are documented in
[`docs/schemas.md`](docs/schemas.md).

## Architecture

```text
src/
  scripts/       Direct entry points; configuration and orchestration only
  core/
    drive/       Google Drive, indexes, archives, and filesystem helpers
    documents/   PDF download, extraction, and canonical document artifacts
    events/      Event parsing and midnight cleanup
    shifts/      Pairing, enrichment, and aggregation
    tools/       Optional audits and exports
  notebooks/     Interactive wrappers over the same core services
```

Business logic belongs in `src/core`. Scripts and notebooks must remain thin.

Optional utilities live under `src/scripts/tools/`.

## Notebooks

The notebooks in `src/notebooks/` are interactive interfaces over the same
configuration, paths, and core services used by the scripts:

```text
scan.ipynb
extract_documents.ipynb
extract_events.ipynb
filter_midnight.ipynb
pair_events.ipynb
enrich_shifts.ipynb
summarize_shifts.ipynb
run_pipeline.ipynb
```

Each stage notebook has the same layout:

1. Load `pipeline.json` and show the resolved pipeline context.
2. Expose a small set of temporary run controls.
3. Preview required inputs.
4. Build and display the core options.
5. Run the production core service.
6. Preview reports and generated artifacts.

`run_pipeline.ipynb` keeps every stage in a separate cell and provides
`RUN_*` switches, so an interrupted pipeline can resume from existing
artifacts.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest
```

The suite is intentionally small and behavior-focused. It runs offline and
protects configuration, canonical paths, parser fixtures, Drive/index edge
cases, traceability, and the filesystem shift pipeline.
