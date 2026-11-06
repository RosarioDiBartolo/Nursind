# Event Preparation

```powershell
.\.venv\Scripts\python.exe src\scripts\extract_events.py
.\.venv\Scripts\python.exe src\scripts\filter_midnight.py
.\.venv\Scripts\python.exe src\scripts\pair_events.py
```

Event extraction writes `events.csv` and `pages.csv`. Cleanup writes
`events.cleaned.csv` and the removed-row audit. Pairing writes one
`*.pairs.csv` per employee.

Implementation: `core.events.extraction`, `core.events.filtering`, and
`core.shifts.pairing`.

## WORKFLOW monthly attendance PDFs

`workflow_monthly` recognizes the WORKFLOW CARTELLINO MENSILE template, including
font-mapped ASCII headers and G padding in month names. It reads the six fixed
punch columns from PDF-point word positions relative to page width. Single-letter
weekday rows are supported; the output weekday is computed from the resolved date.
Missing entry/exit slots retain their column identity. Hours, schedule codes and
justification values outside the punch columns are excluded.

For this format, the month/year comes from the monthly title; hire dates are never
used as a substitute for an incomplete title. Undated pages are flagged rather
than assigned a guessed month. Layout coordinates are required, as produced by
native and OCR document extraction. This is a template-specific parser, not a
universal inference of entry/exit from arbitrary time sequences.

An exit earlier than the paired entry in the same monthly row receives a one-day
offset before chronological pairing (including month/year boundaries). The source
page, line, original token and bounding box stay traceable. Existing parsers retain
their timestamp semantics.

After a parser correction, reuse canonical JSON documents:

```powershell
.\.venv\Scripts\python.exe src\scripts\run_pipeline.py --from extract_events --through summarize_shifts
```
