# Shared Logic Registry

Read this file before adding helpers. Logic used by multiple steps belongs in
`src/core`, not in scripts or notebooks.

| Module | Responsibility |
|---|---|
| `core.config` | Validate `pipeline.json` and resolve repository-relative settings |
| `core.config_values` | Shared parsing helpers for optional scalar configuration values |
| `core.pipeline` | Canonical pipeline step ordering, selection, and shared stage orchestration |
| `core.csv_validation` | Shared required-column validation for CSV/DataFrame inputs |
| `core.paths` | Derive canonical stage directories and artifact filenames |
| `core.drive.fs_utils` | Directory and parent-directory creation |
| `core.drive.io_json` | Consistent JSON I/O |
| `core.drive.metadata` | Google Drive root metadata lookup |
| `core.drive.names` | Safe and normalized names |
| `core.drive.archive_utils` | ZIP member normalization and extraction |
| `core.drive.index` | Map/list index models and conversions |
| `core.drive.index_downloads` | Local, Drive, and ZIP-member PDF loading |
| `core.drive.text_extraction_csv` | Document manifests and document JSON paths |
| `core.pdf` | PDF text-layer and layout extraction |
| `core.parsing` | Shared text/date/event parsing primitives and calendar weekday codes |
| `core.reporting` | Stage report construction and persistence |
| `core.shift_logic` | Pair closing, datetime normalization, and shift classification |
| `core.shifts.year_columns` | Resolve explicit or data-discovered year columns for shift reports |
| `core.table_outputs` | Shared CSV plus Excel table writing for report artifacts |
| `core.tools.clean_shifts_export` | Clean per-employee shift Excel export from enriched shifts |
| `core.tools.turni_custom_counts` | Custom yearly category counts for afternoons, nights, Saturday mornings, and holiday mornings |

Do not add path defaults to individual steps. Do not duplicate JSON, ZIP,
filename, report, parser, or shift-classification helpers.

`notebooks.interface` contains display-only helpers for bounded CSV previews,
artifact tables, and report summaries. It must not contain pipeline behavior;
reusable processing logic still belongs in `core`.

Document OCR shares the following implementation (discovery searches completed;
existing native quality scoring and PDF line grouping reused):

| Module | Responsibility |
|---|---|
| `core.documents.ocr_options` | Validated, serializable CPU OCR settings for scripts, notebooks, services, and workers |
| `core.documents.ocr` | Tesseract dependency checks, bounded local execution, TSV-to-layout conversion |
| `core.documents.page_extraction` | Page selection and native/OCR extraction with per-page provenance |
| `core.documents.quality` | Shared native normal/vertical text selection and quality scoring |
| `core.pdf` | Shared per-page native text/layout extraction and word-to-line grouping in PDF points |

`core.documents.progress.completed_with_progress` provides completion iteration
with periodic wait logging, reused for download waits, extraction queue limits,
and final worker draining. Worker initialization reuses
`core.drive.logging_utils.setup_logging` to enable INFO logs on Windows.

| Module | Responsibility |
|---|---|
| `core.drive.names.is_macos_metadata` | Shared AppleDouble / __MACOSX exclusion for Drive scans, ZIP members and local indexes |
| `core.events.extraction.workflow_header` | Recognize/decode WORKFLOW monthly headers and resolve padded month/year titles |
| `core.events.extraction.parsers.workflow_monthly` | Parse the six monthly punch columns with source coordinates and overnight exit offsets |

`ParsedEvent.day_offset` defaults to zero; page event construction applies explicit
parser offsets before chronological pairing. Native monthly headers reuse existing
`core.parsing.MONTH_WORDS`; source document text is preserved.
