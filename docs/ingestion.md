# Ingestion (Scan + Document Extraction)

This stage discovers source files in Drive and extracts structured document payloads from each indexed PDF.

## Entry points

- `python -m "src.scan_directory"`
- `python -m "src.extract_documents_from_index"`
- `python -m "src.download_from_index"` for optional local sample downloads

Canonical pipeline root example:
`output/default`

## Inputs

- Drive root folder id (`--root`) for scan.
- `output/default/scan/included.index.json` as input for document extraction.
- A MapIndex file (`--index`) for `src.download_from_index`.

## Outputs

- `output/default/scan/included.index.json`
- `output/default/scan/filtered.index.json`
- `output/default/scan/scan_directory.report.json`
- Structured text-layout JSON docs under `output/default/documents/docs/*.json`
- Per-employee extracted document manifest CSVs under `output/default/documents/<employee>.csv`
- `output/default/documents/included_documents.index.json`
- `output/default/documents/excluded_documents.index.json`
- `output/default/documents/extract_documents_from_index.report.json`
- Downloaded PDFs under `samples/from_index/*.pdf` by default (`--no-flat-output` keeps `samples/from_index/<employee>/*.pdf`)

## Key behavior

- Document extraction is resumable through included/excluded document indexes.
- `--skip-included` is on by default; use `--reprocess-included` to force retries.
- `--reprocess-excluded` allows retrying previously failed files.
- Periodic flush (`--flush-every`) protects progress on long runs.
- Scan treats `.zip` files as virtual folders: each PDF member is indexed as a separate file entry.
- ZIP members are emitted with synthetic `file_id` values and virtual paths like `.../archive.zip/<member>.pdf`.
- ZIP scan failures are added to filtered index with reasons such as `invalid_zip_archive`, `zip_scan_error:*`, and `zip_no_pdf_members`.
- Extraction recognizes ZIP-member `file_id` values and downloads the parent archive, then extracts the member PDF bytes before text extraction.
- ZIP archive downloads are cached per worker thread to avoid re-downloading the same archive for multiple members.
- PDFs without a text layer are excluded in this step with reason `missing_text_layer`.
- Each document JSON stores canonical extracted text plus page/line/word layout for text-layer PDFs.
- Each employee CSV is only a manifest rowset pointing to the per-document JSON files and recording traceability and quality metadata.

## Typical commands

```powershell
python -m "src.scan_directory" --root "<DRIVE_ROOT_FOLDER_ID>" --out "output/default/scan" --included "included.index.json" --filtered "filtered.index.json" --report "scan_directory.report.json" --verbose
python -m "src.extract_documents_from_index" --index "output/default/scan/included.index.json" --out "output/default/documents" --included "included_documents.index.json" --excluded "excluded_documents.index.json" --verbose
python -m "src.download_from_index" --index "output/default/scan/samples.index.scan.map.json" --out "samples/from_index" --random-sample 20 --seed 42 --verbose
python -m pytest -q
```

## Notebook walkthrough

Notebook entrypoints:

- `src/notebooks/scan.ipynb`
- `src/notebooks/extract_documents.ipynb`

Notebook contract:

- Set the shared notebook root settings once in `src/notebooks/shared_config.json` (`root_id`, optional `root_prefix`, and optional `base_output`).
- Each notebook loads `src/notebooks/shared_config.py`, which resolves the same canonical stage folders and injects step artifact names from `src.pipeline_paths`.
- The scan notebook writes the canonical scan artifacts under the shared output root.
- The extract notebook consumes that shared scan output and writes the canonical document artifacts under the same shared output root.

Reusable structure references:

- `src/notebooks/_step_template.ipynb`
- `src/notebooks/shared_config.py`
