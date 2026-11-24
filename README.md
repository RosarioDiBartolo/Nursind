# Healthcare Shift Processing Pipeline

This repository is a set of directly runnable Python scripts backed by shared
implementation modules. It is not a distributable Python package.

## Synthetic portfolio demo

For an offline PDF-to-CSV/Excel demonstration with fixed expectations, an
unmatched-punch case, verification commands and presentation materials, see
[`portfolio/README.md`](portfolio/README.md). This demo uses no operational data
or Drive credentials and is separate from NursindPayroll.

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

The complete configuration contract is [`pipeline.schema.json`](pipeline.schema.json).
Copy [`pipeline.example.json`](pipeline.example.json) to `pipeline.json` for a
complete starting point. See [`docs/pipeline_configuration.md`](docs/pipeline_configuration.md)
for requirements and path behavior.

All scripts load the root [`pipeline.json`](pipeline.json). Relative output
paths are resolved from the repository root, regardless of the current working
directory.

```json
{
  "drive": {
    "root_id": "..."
  },
  "steps": {
    "scan": {"workers": 8},
    "pair_events": {"max_gap_hours": 16},
    "enrich_shifts": {"min_hours": 6, "include_holidays": true},
    "summarize_shifts": {
      "format": "csv"
    }
  }
}
```

`pipeline` is optional. When `pipeline.name` is omitted, the pipeline name is
resolved from the Google Drive folder identified by `drive.root_id`. Set
`pipeline.name` only when you want to override the Drive folder name used for
local outputs.

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

Or run a selected subset in canonical order:

```powershell
.\.venv\Scripts\python.exe src\scripts\run_pipeline.py --from extract_events --through summarize_shifts
.\.venv\Scripts\python.exe src\scripts\run_pipeline.py scan pairing summary
```

Each script accepts `--config <path>` and `--verbose`.

The canonical output layout is `output/<pipeline-name>/`, using the resolved
pipeline name (with filesystem-unsafe characters replaced). Everything under
`output/` is ignored by Git. Set `pipeline.base_output_dir` to replace the
`output` base folder; results then go to `<base_output_dir>/<pipeline-name>/`.
Relative base paths resolve from the repository root; absolute paths are also
supported. Custom base folders inside the repository need their own Git ignore
rule:

```text
output/<pipeline-name>/
  scan/
  documents/
  events/
  shifts/
  enrichment/
  aggregation/
    turni_employee_summary.csv
    turni_employee_summary.xlsx
```

Existing artifact names and CSV/JSON schemas are documented in
[`docs/schemas.md`](docs/schemas.md).

## Architecture

For a read-only, content-based inventory of a Drive folder, run
`src/scripts/map_drive.py`. It reuses document extraction and event parsers,
identifies employees per page, and writes isolated mapping artifacts without
organizing Drive files. See [folder mapping](docs/folder_mapping.md).

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

For monthly PDF reports comparing complete post-filter shifts with explicit
theoretical schedules, see [shift deviation export](docs/deviation_export.md).

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

## Parser guide

See the [offline parser catalogue](docs/parsers/README.md) for annotated test
documents, extracted events and links to the rules used by each parser.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest
```

The suite is intentionally small and behavior-focused. It runs offline and
protects configuration, canonical paths, parser fixtures, Drive/index edge
cases, traceability, and the filesystem shift pipeline.

Scanned PDFs can use an optional CPU-only Tesseract fallback, selected per page.
Setup, configuration, and retry instructions are in
[the local OCR guide](docs/ingestion.md#local-ocr-for-scanned-pages).
