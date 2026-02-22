# Ingestion (Scan + Text Extraction)

This stage discovers source files in Drive and extracts plain text from each indexed PDF.

## Entry points

- `python -m "src.scan_directory"`
- `python -m "src.extract_text_from_index"`
- `python -m "src.download_from_index"` (optional helper for local sample downloads)

## Inputs

- Drive root folder id (`--root`) for scan.
- `scan/included.index.json` as input for text extraction.
- A MapIndex file (`--index`) for `src.download_from_index`.

## Outputs

- `scan/included.index.json`
- `scan/filtered.index.json`
- `scan/scan_directory.report.json`
- Extracted text files under `output/text_extracted/<employee>/*.txt`
- `output/text_extracted/included_text.index.json`
- `output/text_extracted/excluded_text.index.json`
- `output/text_extracted/extract_text_from_index.report.json`
- Downloaded PDFs under `samples/from_index/<employee>/*.pdf` (optional helper output)

## Key behavior

- Extraction is resumable through included/excluded text indexes.
- `--skip-included` is on by default; use `--reprocess-included` to force retries.
- `--reprocess-excluded` allows retrying previously failed files.
- Periodic flush (`--flush-every`) protects progress on long runs.
- Scan treats `.zip` files as virtual folders: each PDF member is indexed as a separate file entry.
- ZIP members are emitted with synthetic `file_id` values and virtual paths like `.../archive.zip/<member>.pdf`.
- ZIP scan failures are added to filtered index with reasons (`invalid_zip_archive`, `zip_scan_error:*`, `zip_no_pdf_members`).
- Scan continues when a single employee folder fails; failures are captured in `scan/scan_directory.report.json`.
- Scan report fields include `employee_total`, `employee_succeeded`, `employee_failed`, `included_total`, `filtered_total`, and `scan_errors`.
- Extraction recognizes ZIP-member `file_id` values and downloads the parent archive, then extracts the member PDF bytes before text extraction.
- ZIP archive downloads are cached per worker thread to avoid re-downloading the same archive for multiple members.
- ZIP extraction failures are recorded in excluded index with reasons like `zip_archive_download_error:*`, `zip_member_not_found`, `zip_member_invalid_pdf`.

## Typical commands

```powershell
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "scan" --included "included.index.json" --filtered "filtered.index.json" --report "scan_directory.report.json" --verbose
python -m "src.extract_text_from_index" --index "scan/included.index.json" --out "output/text_extracted" --included "included_text.index.json" --excluded "excluded_text.index.json" --verbose
python -m "src.download_from_index" --index "scan/samples.index.scan.map.json" --out "samples/from_index" --limit 20 --verbose
```

## Notebook walkthrough

Notebook entrypoint for the scan step:

- `src/notebooks/scan.ipynb`

Notebook run modes:

- `live`: execute Drive scan logic and write `scan/included.index.json` and `scan/filtered.index.json`.
- `live`: execute Drive scan logic, continue on per-employee failures, and emit `scan/scan_directory.report.json`.
- `demo`: load existing scan indexes and re-save to canonical scan output paths for reproducible walkthroughs.

Notebook contract:

- Parameters are centralized in one cell (`RUN_MODE`, `ROOT_ID`, output names, workers, verbosity).
- Validation cell must pass strict `MapIndex` reload checks.
- Diagnostics include filtered reason distribution, type split, and record samples.
- Handoff cell points to `src.extract_text_from_index` using `scan/included.index.json`.

Reusable structure reference:

- `src/notebooks/_step_template.ipynb`
