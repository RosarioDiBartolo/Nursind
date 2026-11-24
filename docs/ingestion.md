# Ingestion

```powershell
.\.venv\Scripts\python.exe src\scripts\scan.py
.\.venv\Scripts\python.exe src\scripts\extract_documents.py
```

Both scripts read root `pipeline.json`. Scan writes included/filtered indexes
under `scan/`. Document extraction consumes the included index and writes
employee manifests plus `documents/docs/*.json`.

Implementation: `core.drive.scan`, `core.documents`, and shared `core.drive`
helpers.

## Local OCR for scanned pages

OCR is optional and disabled by default. It uses the existing pdfplumber/PDFium
renderer and the Tesseract executable on the CPU; no cloud OCR or GPU is used.
Native extraction remains unchanged when OCR is disabled. No additional Python
package is required.

Install Tesseract 5 and the `ita` and `eng` language data following the
[official installation guidance](https://tesseract-ocr.github.io/tessdoc/Installation.html).
On Windows, the standard `C:/Program Files/Tesseract-OCR/tesseract.exe` location
is detected automatically; otherwise add it to PATH or configure `executable`.
Check the installation with `tesseract --list-langs` (or its full executable path).

Merge the following into `steps.extract_documents` in your existing `pipeline.json`:

```json
{
  "extract_workers": 1,
  "max_in_flight": 2,
  "ocr": {
    "enabled": true,
    "language": "ita+eng",
    "dpi": 300,
    "psm": 6,
    "timeout_seconds": 120
  }
}
```

One extraction worker and one Tesseract thread are appropriate starting values
for the i7-3770 / 32 GB RAM machine. Pages are processed sequentially within each
document. The default limit is 25 million rendered pixels per page; oversized
pages fail before rendering. Temporary page images are removed after each OCR
attempt, including failures. Tesseract runs locally and does not upload pages.
Downloads from Drive retain their existing behavior.

Run the usual `src/scripts/extract_documents.py` command. The extraction and full
pipeline notebooks also read `ocr` from the same configuration. The single- and
multi-document core services accept an optional `OcrOptions` instance.

To retry previously excluded scans, temporarily set `reprocess_excluded: true`
in the same step configuration. To revisit mixed documents previously included
with only their native pages, use `reprocess_included: true`. These settings retry
all documents in the respective indexes, so remove them after the intended run.

### Page selection and outputs

When enabled, native normal/vertical extraction is selected per page with the
existing quality scoring. OCR is used for visible content without usable text,
substantial low-quality native text (or replacement characters), or large image regions without sufficient native text.
Defaults: image regions covering at least 25% of the page need at least 100 native
characters and 5 native lines inside the region. These thresholds are heuristic,
not proof that a page is complete; large photos can also trigger OCR. Configure
`min_native_chars`, `min_native_lines`, and `min_image_area_ratio` to tune them.
The default PSM 6 treats the page as one text block; try PSM 3 for automatic layout
segmentation if needed. Rotated scans are not automatically deskewed/oriented.

OCR TSV words are converted from image pixels to PDF points and grouped into the
same canonical line structure used by the parsers. Native pages retain their
original layout. Entirely empty PDF pages retain their page positions. Scanned
pages producing no recognized words fail explicitly rather than being silently
omitted (including image-only blank pages).

Artifacts retain schema `text_layout_v1`, with additive extraction metadata:

- `selected_mode`: `normal`, `vertical`, `ocr`, or `mixed`.
- `has_text_layer`: whether **any original page** had native PDF characters; an
  OCR-only document remains `false` even after successful extraction.
- `pages`: page number, selected method (`normal`, `vertical`, `ocr`, `blank`),
  original text-layer presence, OCR reason, quality scores, and OCR confidence.
- `ocr_pages`: count of pages actually processed with OCR.
- OCR words include Tesseract `confidence` (0–100). Page `ocr_confidence` is its
  arithmetic word mean, not a calibrated probability or guarantee of correctness.

Reports count `used_ocr` documents and `ocr_pages`. Missing executables or language
packs fail runtime preflight before indexes are changed. Timeouts, empty OCR,
rendering failures, and oversized pages fail the affected document; no partial
new document is written. Existing exclusion/reprocessing behavior still applies.

Before processing a whole archive, compare 20–30 representative pages against
manual transcription, especially dates, times, row alignment, and missing events.
The extractor does not infer missing punches or fix OCR numbers silently. Parser
accuracy and throughput need a real Tesseract benchmark on the target machine.

### Extraction progress logs

At the default INFO level, extraction logs each download/read start and finish,
file queue/start/save or failure, native/OCR page start and finish, and the total
completed/queued documents. OCR logs rendering, Tesseract invocation and timeout.
Worker logging is initialized explicitly for Windows spawned processes. While
waiting for download or extraction futures, the coordinator reports outstanding
work every 15 seconds; this is a waiting notice, not proof of worker progress.
Document text, credentials and tokens are not logged. File names/IDs are logged.
These logs are independent of `log_every` (the existing index progress setting).
Changes take effect on the next run; already-running workers keep the old code.

Drive scanning and local/ZIP PDF discovery ignore macOS AppleDouble `._*` sidecars
and `__MACOSX` containers. Drive sidecars are reported with reason `macos_metadata`;
ZIP member discovery omits them. Such entries are metadata, not document PDFs.

### Estrazione da elenco di file Drive

Da PowerShell, per elaborare un CSV con intestazione `file_id` o `file_url` e produrre i riepiloghi per dipendente:

```powershell
.\.venv\Scripts\python.exe src\scripts\map_drive.py --config pipeline.json --input-csv input.csv --through summarize_shifts
```

Senza `--input-csv`, lo step `scan` percorre la cartella Drive configurata e scrive `mapping/scan/inventory.csv`. L'estrazione considera i PDF. Gli eventuali termini di esclusione si configurano in `steps.scan.exclude_terms`. Per rianalizzare i JSON locali usare `--from extract_events --through summarize_shifts`.

In `mapping/events/`, `files.csv` riporta lo stato di ogni file, `file_employees.csv` elenca le associazioni e `identity_coverage.csv` conta le pagine di timbrature per parser ed esito. In `mapping/grouped/`, `current.txt` contiene il nome della cartella della generazione corrente. Ogni gruppo contiene gli input sotto `events/` e i risultati degli step richiesti. Gli elementi senza attribuzione restano in `unresolved/`.

Un codice fiscale o una matricola con azienda riconosciuta permette di unire file distinti. Con il solo nome il gruppo ha ambito documento. Per confermare un collegamento tra file passare `--identities abbinamenti.csv` con colonne `file_id,employee,employee_id` allo step `group_by_employee`.
