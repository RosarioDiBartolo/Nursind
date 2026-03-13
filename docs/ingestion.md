# Ingestion (Scan + Document Extraction)

This stage discovers source files in Drive and extracts structured document payloads from each indexed PDF.

## Entry points

- `python -m "src.scan_directory"`
- `python -m "src.extract_documents_from_index"`
- `python -m "src.download_from_index"` (optional helper for local sample downloads)

## Inputs

- Drive root folder id (`--root`) for scan.
- `scan/included.index.json` as input for document extraction.
- A MapIndex file (`--index`) for `src.download_from_index`.

## Outputs

- `scan/included.index.json`
- `scan/filtered.index.json`
- `scan/scan_directory.report.json`
- Structured text-layout JSON docs under `output/text_extracted/docs/*.json`
- Per-employee extracted document manifest CSVs under `output/text_extracted/<employee>.csv`
- `output/text_extracted/included_documents.index.json`
- `output/text_extracted/excluded_documents.index.json`
- `output/text_extracted/extract_documents_from_index.report.json`
- Downloaded PDFs under `samples/from_index/*.pdf` by default (`--no-flat-output` keeps `samples/from_index/<employee>/*.pdf`)

## Key behavior

- Document extraction is resumable through included/excluded document indexes.
- `--skip-included` is on by default; use `--reprocess-included` to force retries.
- `--reprocess-excluded` allows retrying previously failed files.
- Periodic flush (`--flush-every`) protects progress on long runs.
- Scan treats `.zip` files as virtual folders: each PDF member is indexed as a separate file entry.
- ZIP members are emitted with synthetic `file_id` values and virtual paths like `.../archive.zip/<member>.pdf`.
- ZIP scan failures are added to filtered index with reasons (`invalid_zip_archive`, `zip_scan_error:*`, `zip_no_pdf_members`).
- Scan continues when a single employee folder fails; failures are captured in `scan/scan_directory.report.json`.
- Scan report fields include `employee_total`, `employee_succeeded`, `employee_failed`, `employees_found`, `included_total`, `filtered_total`, `employees_without_included_files_count`, `employees_without_included_files`, and `scan_errors`.
- `employees_found` contains one row per direct employee folder with `employee`, `employee_id`, `included_files`, `excluded_files`, `excluded_folders`, `excluded_total`, `status`, and optional `error`.
- `employees_without_included_files` is computed from the direct subfolders of the scan root that completed successfully but produced `0` included files.
- Extraction recognizes ZIP-member `file_id` values and downloads the parent archive, then extracts the member PDF bytes before text extraction.
- ZIP archive downloads are cached per worker thread to avoid re-downloading the same archive for multiple members.
- ZIP extraction failures are recorded in excluded index with reasons like `zip_archive_download_error:*`, `zip_member_not_found`, `zip_member_invalid_pdf`.
- PDFs without a text layer are excluded in this step with reason `missing_text_layer`.
- Each document JSON stores canonical extracted text plus page/line/word layout for text-layer PDFs.
- Each employee CSV is only a manifest rowset pointing to the per-document JSON files and recording traceability/quality metadata.
- `src.download_from_index` also recognizes ZIP-member `file_id` values and downloads member PDFs from parent archives.
- `src.download_from_index` supports random sampling through `--random-sample` (optional `--seed`).

## Typical commands

```powershell
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "scan" --included "included.index.json" --filtered "filtered.index.json" --report "scan_directory.report.json" --verbose
python -m "src.extract_documents_from_index" --index "scan/included.index.json" --out "output/text_extracted" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
python -m "src.download_from_index" --index "scan/samples.index.scan.map.json" --out "samples/from_index" --random-sample 20 --seed 42 --verbose
```

## Notebook walkthrough

Notebook entrypoints:

- `src/notebooks/scan.ipynb`
- `src/notebooks/extract_documents.ipynb`

Notebook contract:

- Set the shared Drive root id once in `src/notebooks/shared_config.json`.
- Each step notebook loads that shared config, resolves the same per-root output folders, and can be run independently.
- The scan notebook writes the canonical scan artifacts (`included.index.json`, `filtered.index.json`, `scan_directory.report.json`) under the shared output root.
- The extract notebook consumes that shared scan output and writes the canonical text extraction artifacts under the same shared output root.

Reusable structure references:

- `src/notebooks/_step_template.ipynb`
- `src/notebooks/shared_config.py`
