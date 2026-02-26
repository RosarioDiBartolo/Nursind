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
- Extracted text files under `output/text_extracted/<employee>/*.txt`
- `output/text_extracted/included_text.index.json`
- `output/text_extracted/excluded_text.index.json`
- `output/text_extracted/extract_text_from_index.report.json`
- Downloaded PDFs under `samples/from_index/*.pdf` by default (`--no-flat-output` keeps `samples/from_index/<employee>/*.pdf`)

## Key behavior

- Extraction is resumable through included/excluded text indexes.
- `--skip-included` is on by default; use `--reprocess-included` to force retries.
- `--reprocess-excluded` allows retrying previously failed files.
- Periodic flush (`--flush-every`) protects progress on long runs.
- Scan treats `.zip` files as virtual folders: each PDF member is indexed as a separate file entry.
- ZIP members are emitted with synthetic `file_id` values and virtual paths like `.../archive.zip/<member>.pdf`.
- ZIP scan failures are added to filtered index with reasons (`invalid_zip_archive`, `zip_scan_error:*`, `zip_no_pdf_members`).
- Extraction recognizes ZIP-member `file_id` values and downloads the parent archive, then extracts the member PDF bytes before text extraction.
- ZIP archive downloads are cached per worker thread to avoid re-downloading the same archive for multiple members.
- ZIP extraction failures are recorded in excluded index with reasons like `zip_archive_download_error:*`, `zip_member_not_found`, `zip_member_invalid_pdf`.
- `src.download_from_index` also recognizes ZIP-member `file_id` values and downloads member PDFs from parent archives.
- `src.download_from_index` supports random sampling through `--random-sample` (optional `--seed`).

## Typical commands

```powershell
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "scan" --included "included.index.json" --filtered "filtered.index.json" --verbose
python -m "src.extract_text_from_index" --index "scan/included.index.json" --out "output/text_extracted" --included "included_text.index.json" --excluded "excluded_text.index.json" --verbose
python -m "src.download_from_index" --index "scan/samples.index.scan.map.json" --out "samples/from_index" --random-sample 20 --seed 42 --verbose
```
