# Shared Logic Registry

Read this file before adding helpers. Logic used by multiple steps belongs in
`src/core`, not in scripts or notebooks.

| Module | Responsibility |
|---|---|
| `core.config` | Validate `pipeline.json` and resolve repository-relative settings |
| `core.paths` | Derive canonical stage directories and artifact filenames |
| `core.drive.fs_utils` | Directory and parent-directory creation |
| `core.drive.io_json` | Consistent JSON I/O |
| `core.drive.names` | Safe and normalized names |
| `core.drive.archive_utils` | ZIP member normalization and extraction |
| `core.drive.index` | Map/list index models and conversions |
| `core.drive.index_downloads` | Local, Drive, and ZIP-member PDF loading |
| `core.drive.text_extraction_csv` | Document manifests and document JSON paths |
| `core.pdf` | PDF text-layer and layout extraction |
| `core.parsing` | Shared text/date/event parsing primitives |
| `core.reporting` | Stage report construction and persistence |
| `core.shift_logic` | Pair closing, datetime normalization, and shift classification |

Do not add path defaults to individual steps. Do not duplicate JSON, ZIP,
filename, report, parser, or shift-classification helpers.

`notebooks.interface` contains display-only helpers for bounded CSV previews,
artifact tables, and report summaries. It must not contain pipeline behavior;
reusable processing logic still belongs in `core`.
