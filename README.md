# Healthcare Shift Processing Pipeline

A resilient filesystem-first pipeline for extracting, pairing, enriching, and aggregating employee shift data from healthcare PDF time logs stored in Google Drive.

The active workflow is intentionally narrow and explicit:

- **Scan** Drive folders into canonical index files
- **Extract** canonical document payloads
- **Parse** events from those documents
- **Audit** parser recall from page diagnostics
- **Clean** midnight artifacts
- **Pair** events into shifts
- **Enrich** shifts
- **Aggregate** yearly summaries
- **Audit** missing timbrature against the same pipeline root

## Install and Package Shape

Install the project with one of these entrypoints:

```powershell
python -m pip install .
python -m pip install -e .[dev]
python -m pip install -e .[google]
```

### Supported Public API

Supported imports use the public `cartellino_parser.*` namespace. External modules should treat only these entrypoints as stable:

- `cartellino_parser.PipelineClient`
- `cartellino_parser.models`
- `cartellino_parser.sources`
- `cartellino_parser.exceptions`
- `cartellino_parser.pipeline_paths`

Everything else, including the on-disk `src/` implementation layout and direct `src.*` imports, is internal or compatibility-oriented and should not be the default integration path for a reusable consumer module.

Example import:

```python
from cartellino_parser.pipeline_paths import build_pipeline_paths
```

Installed console scripts are thin wrappers around the same public package layer.

For external modules, prefer the public client API:

```python
from cartellino_parser import PipelineClient
from cartellino_parser.models import ExtractEventsRequest

client = PipelineClient()
report = client.extract_events(
    ExtractEventsRequest(
        input_dir="output/default/documents",
        output_dir="output/default/events",
    )
)

print(report.stage)
print(report.outputs)
```

Drive-backed steps can be configured explicitly instead of relying on ambient environment:

```python
from cartellino_parser import PipelineClient
from cartellino_parser.models import ScanRequest
from cartellino_parser.sources import DriveAuthConfig

client = PipelineClient(
    drive_auth=DriveAuthConfig(
        client_id="...",
        client_secret="...",
        token_path="token.json",
    )
)
scan_report = client.scan(ScanRequest(root_id="<DRIVE_ROOT_FOLDER_ID>"))
```

### Wheel Smoke Test

Use this when validating a release artifact from a clean environment:

```powershell
python -m pip install build
python -m build
python -m venv .pkg-venv
.\.pkg-venv\Scripts\python -m pip install .\dist\*.whl
.\.pkg-venv\Scripts\python -c "from cartellino_parser import PipelineClient; from cartellino_parser.models import ExtractEventsRequest; from cartellino_parser.pipeline_paths import build_pipeline_paths; print(PipelineClient, ExtractEventsRequest, type(build_pipeline_paths('output/demo')).__name__)"
```

## Canonical Pipeline Layout

Use **one shared pipeline root**, for example `output/<pipeline>`.

**Stage folders** under that root:

- `scan`
- `documents`
- `events`
- `shifts`
- `enrichment`
- `aggregation`

The current pipeline supports only **this layout**. **Legacy folders** such as `text_extracted` and `employee_shifts_from_raw` are no longer part of the supported workflow.

## Architecture Flow

1. **Drive indexing** via `cartellino-drive-scan`
2. **Document extraction** via `cartellino-extract-documents`
3. **Direct document-to-events extraction** via `cartellino-extract-events`
4. **Parser recall audit** via `cartellino-parser-recall-audit`
5. **Midnight cleanup** via `cartellino-filter-midnight`
6. **Employee-level pairing** via `cartellino-pair-employee-events`
7. **Shift enrichment** via `cartellino-turni-enrichment`
8. **Yearly aggregation** via `cartellino-turni-summary`
9. **Missing timbrature audit** via `cartellino-missing-timbrature`

## Core Outputs

- `output/<pipeline>/scan/included.index.json`
- `output/<pipeline>/scan/filtered.index.json`
- `output/<pipeline>/documents/docs/*.json`
- `output/<pipeline>/documents/<employee>.csv`
- `output/<pipeline>/events/events.csv`
- `output/<pipeline>/events/pages.csv`
- `output/<pipeline>/events/events.cleaned.csv`
- `output/<pipeline>/shifts/*.pairs.csv`
- `output/<pipeline>/enrichment/*.enriched.csv`
- `output/<pipeline>/aggregation/turni_employee_summary.csv`
- `output/suspicious_pages.csv`
- `output/<pipeline>/missing_timbrature.*`

## Technical Notes

### Persistent indexing

The **scan** and **extraction** stages persist state to disk through JSON indexes instead of relying on in-memory state. That gives:

- **resumability**
- **crash recovery**
- **deterministic** skip/reprocess decisions
- **auditable** included vs excluded outcomes

### Dual-index pattern

The pipeline uses explicit **included/excluded** outputs:

- **scan** emits `scan/included.index.json` and `scan/filtered.index.json`
- **document extraction** emits `documents/included_documents.index.json` and `documents/excluded_documents.index.json`

This keeps **successful** and **failed** records separate while preserving **file-level traceability**.

### Canonical document artifacts

Document extraction writes **one structured JSON per document** under `documents/docs/*.json` and **one thin manifest CSV per employee** under `documents/<employee>.csv`. Later stages operate on those **canonical artifacts** and no longer fall back to **legacy raw text folders**.

## Quickstart

```powershell
python -m pip install -e .[dev]
cartellino-drive-scan --root "<DRIVE_ROOT_FOLDER_ID>" --out "output/<pipeline>/scan" --included "included.index.json" --filtered "filtered.index.json" --report "scan_directory.report.json" --verbose
cartellino-extract-documents --index "output/<pipeline>/scan/included.index.json" --out "output/<pipeline>/documents" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
cartellino-extract-events --input-dir "output/<pipeline>/documents" --output-dir "output/<pipeline>/events" --verbose
cartellino-parser-recall-audit --root-dir "output" --verbose
cartellino-filter-midnight --input-dir "output/<pipeline>/events" --verbose
cartellino-pair-employee-events --input-dir "output/<pipeline>/events" --output-dir "output/<pipeline>/shifts" --verbose
cartellino-turni-enrichment --input-dir "output/<pipeline>/shifts" --out-dir "output/<pipeline>/enrichment" --verbose
cartellino-turni-summary --enriched-dir "output/<pipeline>/enrichment" --out "output/<pipeline>/aggregation/turni_employee_summary.csv" --format "csv" --verbose
cartellino-missing-timbrature --pipeline-dir "output/<pipeline>" --verbose
python -m pytest -q
```

## Step Details

### 1) Drive indexing

**What it does:**

This step walks the **Google Drive root folder** employee by employee, recursively traversing each employee subtree. It applies **name-based filtering** while scanning: excluded **folder names** are matched as normalized terms, excluded **file names** are matched by substring, and only supported PDF sources are kept.

It also expands **ZIP archives** as virtual folders, so embedded PDFs are indexed as separate source files. The result is a split between **included** files that will move forward and **filtered** files/folders that were skipped with an explicit reason.

**Entry point:** `cartellino-drive-scan`

**Outputs:**

- `output/<pipeline>/scan/included.index.json`
- `output/<pipeline>/scan/filtered.index.json`
- `output/<pipeline>/scan/scan_directory.report.json`

**Notes:**

- **ZIP files** are expanded as virtual folders and PDF members are indexed with synthetic IDs.
- **Direct subfolders** of the scan root are treated as the employee inventory.
- The **scan report** includes `employees_found` and `employees_without_included_files`.

### 2) Document extraction

**What it does:**

This step reads the **included scan index**, downloads each indexed PDF or ZIP member, and writes one **canonical document JSON** per source file with extracted text and layout metadata. It is the stage that turns a Drive file reference into a reusable local artifact for downstream parsing.

It is also where the pipeline enforces **text-layer quality**. Documents without a usable text layer are moved to the **excluded** index with reasons such as `missing_text_layer`, while successful files are tracked in per-employee manifest CSVs and resumable included/excluded indexes.

**Entry point:** `cartellino-extract-documents`

**Input:**

- `output/<pipeline>/scan/included.index.json`

**Produced artifacts:**

- `output/<pipeline>/documents/docs/*.json`
- `output/<pipeline>/documents/<employee>.csv`
- `output/<pipeline>/documents/included_documents.index.json`
- `output/<pipeline>/documents/excluded_documents.index.json`
- `output/<pipeline>/documents/extract_documents_from_index.report.json`

**Behavior:**

- **Resumable** through included/excluded indexes
- **ZIP-member-aware** downloads
- **Text-layer validation** with `missing_text_layer` exclusions
- **One canonical JSON payload per document**

### 3) Document-to-events extraction

**What it does:**

This step loads the per-employee manifest CSVs and the canonical `docs/*.json` payloads, then parses each document directly into a shared **event row** format. Parser routing is driven by the detected document structure, so different layouts such as `cartellino_classic`, `timbrature_web`, and `situazione_mensile` can be handled in one run.

Besides `events.csv`, it also writes **page-level diagnostics** to `pages.csv`. Those page rows capture coverage and parser decisions, which is why later audit steps can reason about suspicious pages and missing month/year resolution.

**Entry point:** `cartellino-extract-events`

**Input:**

- `output/<pipeline>/documents`

**Outputs:**

- `output/<pipeline>/events/events.csv`
- `output/<pipeline>/events/pages.csv`

**Behavior:**

- **Dynamic format routing** (`cartellino_classic`, `timbrature_web`, `situazione_mensile`, fallback)
- **Full-document parsing** from canonical JSON payloads
- **Traceability fields** carried onto event rows
- **No raw `.txt` fallback**

### 4) Parser recall audit

**What it does:**

This step reviews the generated `pages.csv` files across one or more pipeline folders and builds a **manual review queue** for likely parser misses. It looks for patterns such as **tiny relevant pages**, **large zero-event pages**, **low-coverage pages**, and pages where events were dropped because **month/year could not be resolved**.

The audit does not silently "fix" anything. Instead, it ranks suspicious pages, backfills traceability from the manifest files, and writes direct **Drive file links** plus review heuristics so you can inspect the original source document quickly.

**Entry point:** `cartellino-parser-recall-audit`

**Input:**

- output root containing one or more pipeline folders, typically `output`

**Outputs:**

- `output/suspicious_pages.csv`
- `output/parser_recall_audit.report.json`

**Behavior:**

- **Reads** each `output/*/events/pages.csv`
- **Ranks** tiny pages, zero-event pages, low-coverage pages, and missing-year-month pages
- **Carries** direct `source_file_link` references for opening the original Drive PDF
- **Adds heuristics** such as neighboring page coverage and absence-keyword hits for manual triage

### 5) Midnight cleanup

**What it does:**

This step removes **synthetic or non-informative midnight punches** from the raw event stream before pairing. It specifically filters rows that resolve to midnight-style values such as `00:00` or `24:00`, which would otherwise create bad pairings and distort shift reconstruction.

The cleanup is **auditable** because removed rows are written to a separate CSV with a filter reason. That gives you a cleaner event stream for pairing without losing visibility into what was discarded.

**Entry point:** `cartellino-filter-midnight`

**Input:**

- `output/<pipeline>/events/events.csv`

**Outputs:**

- `output/<pipeline>/events/events.cleaned.csv`
- `output/<pipeline>/events/events.midnight_removed.csv`

### 6) Employee-level pairing

**What it does:**

This step groups cleaned events by **employee**, orders them chronologically, and pairs entry/exit punches into shift rows. It operates at employee scope even when the input is a single aggregated `events.cleaned.csv`, so the output becomes one **per-employee pairs file**.

The pairing logic includes a **max-gap guard** and **overnight support**, which helps reject implausible matches while still allowing shifts that cross midnight. The report file is the main place to look for pairing errors or employees whose outputs could not be generated.

**Entry point:** `cartellino-pair-employee-events`

**Inputs:**

- cleaned events under `output/<pipeline>/events`

**Outputs:**

- `output/<pipeline>/shifts/*.pairs.csv`
- `output/<pipeline>/shifts/pair_employee_events.report.json`

**Behavior:**

- **Employee-scope chronological pairing**
- `--max-gap-hours` **guard**
- **Overnight support**
- **No deprecated `--index` mode**

### 7) Shift enrichment

**What it does:**

This step takes each employee's paired shifts and computes the classification fields needed for reporting. It fixes **overnight exits** when an exit timestamp is earlier than its entry timestamp, computes **duration** and **is_long**, and classifies the shift against holidays and time-of-day rules.

The main outputs added here are `turno_code`, `turno_bucket`, and `year`. Those fields are what the final aggregation step uses to count shifts consistently across employees and years.

**Entry point:** `cartellino-turni-enrichment`

**Inputs:**

- `output/<pipeline>/shifts/*.pairs.csv`

**Outputs:**

- `output/<pipeline>/enrichment/*.enriched.csv`
- optional `output/<pipeline>/enrichment/turni_enrichment.stats.json`

### 8) Yearly aggregation

**What it does:**

This step reads the enriched per-employee CSVs and converts them into a compact **summary table**. It counts shifts per `(employee, turno, year)` across the configured year window and writes the result as `turni_employee_summary.csv`.

It is a **reporting** step, not a reconstruction step. By the time data reaches aggregation, pairing and enrichment should already be settled; this stage just totals the classified shifts into a format that is easier to analyze.

**Entry point:** `cartellino-turni-summary`

**Inputs:**

- `output/<pipeline>/enrichment/*.enriched.csv`

**Outputs:**

- `output/<pipeline>/aggregation/turni_employee_summary.csv`

### 9) Missing timbrature audit

**What it does:**

This step cross-checks the whole canonical pipeline for **employee coverage gaps** and processing issues. It combines the **scan report**, document manifests, document exclusions, `pages.csv`, and the pairing report so every scanned employee can be evaluated against the same timeline.

It produces both **operational findings** such as `missing_text_layer`, missing page month/year, or pairing failures, and **coverage gaps** for months in the required `2014-01..2025-12` range that do not appear in relevant pages. This makes it the main exception-reporting step for "who is missing what, and why?"

**Entry point:** `cartellino-missing-timbrature`

**Input:**

- canonical pipeline root `output/<pipeline>`

**Typical outputs:**

- `output/<pipeline>/missing_timbrature.report.json`
- `output/<pipeline>/missing_timbrature.summary.csv`
- `output/<pipeline>/missing_timbrature.findings.csv`
- `output/<pipeline>/missing_timbrature.coverage.csv`

**Behavior:**

- **Reads** the canonical `documents`, `events`, and `shifts` folders
- **Uses** the scan report `employees_found` inventory
- **Flags** non-OCR source files, unresolved page month/year, pairing failures, and missing `2014-01..2025-12` coverage months from `pages.csv` rows marked `relevant_for_coverage=true`
- **Rejects** legacy-only layouts instead of auto-detecting them

## Optional: Download PDFs From an Index

Use this helper when you already have a map index and want local PDF samples.

```powershell
cartellino-download-from-index --index "output/<pipeline>/scan/samples.index.scan.map.json" --out "samples/from_index" --random-sample 20 --seed 42 --verbose
```

**Notes:**

- **ZIP-member entries** are supported.
- **Flat output** is the default; pass `--no-flat-output` for per-employee folders.

## Notebook Pipeline Showcase

Use the notebooks in `src/notebooks` to demonstrate the same canonical pipeline layout.

- `src/notebooks/shared_config.json`: shared notebook root settings (`root_id`, optional `root_prefix`, optional `base_output`) and notebook-specific non-path config
- `src/notebooks/shared_config.py`: resolves notebook context, canonical stage paths, and per-step artifact names from `cartellino_parser.pipeline_paths`
- `src/notebooks/run_pipeline.ipynb`: end-to-end notebook that runs scan, document extraction, event extraction, midnight cleanup, pairing, enrichment, summary, and missing-timbrature audit from the shared config
- `src/notebooks/scan.ipynb`: scan stage
- `src/notebooks/extract_documents.ipynb`: document extraction stage
- `src/notebooks/extract_events_from_documents.ipynb`: event extraction stage

Expected scan artifacts from the scan notebook:

- `<shared_output_root>/scan/included.index.json`
- `<shared_output_root>/scan/filtered.index.json`
- `<shared_output_root>/scan/scan_directory.report.json`

Next command after the scan notebook:

```powershell
cartellino-extract-documents --index "<shared_output_root>/scan/included.index.json" --out "<shared_output_root>/documents" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
```

## Documentation Map

- `llm.md`
- `docs/codebase_map.md`
- `PIPELINE_COMMANDS.md`
- `docs/ingestion.md`
- `docs/preparation.md`
- `docs/enrichment.md`
- `docs/aggregation.md`
- `docs/schemas.md`
- `docs/shared_logic_registry.md`
- `CODEBASE_BRIEFING.md`

## Current Priorities

- expand automated coverage across the remaining pipeline stages
- keep CLI/docs/notebooks aligned on the same canonical layout
- continue thinning mixed-responsibility modules when new changes touch them
