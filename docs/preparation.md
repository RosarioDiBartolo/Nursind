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

## Damaged monthly attendance text

For `cartellino_unico`, explicit E/U punches take priority over unlabelled
durations when no punch-column layout is available. Numbered entry and exit
headings can also identify punch columns on OCR pages. Undated candidates appear
in the extraction report under `issues[].undated_events`, with day, time, kind
and source line.

For `cartellino_unico` and `timbrature_web`, a repaired title requires at least
three distinct printed days and agreement with every parsed weekday on the page.
A printed month alone can be resolved between two dated titles exactly two months
apart if the intervening month and the printed weekdays agree.

For `situazione_mensile`, month/year recovery uses the title before the daily
table. Supported OCR substitutions in four-character year tokens and an
unambiguous month match are allowed; footer print dates are never substitutes
for missing titles. Future years and unresolved titles are rejected, not guessed
from neighboring pages. A rejected title appears as `error_missing_year_month`.

OCR time substitutions are restricted to explicitly marked E/U time tokens,
preserve character offsets, and retain the original raw text. Daily rows whose
weekday conflicts with their calendar date are excluded and reported as
`error_day_weekday_mismatch`. Pairing also refuses same-day monthly punches from
different source lines; overnight pairs across consecutive dates remain allowed.

Extraction and pairing reports use `status=warning` and populated `issues` when
pages are rejected or punches remain unmatched. Undated event pages contribute
to the coverage denominator. These warnings require review even when subsequent
enrichment/summary stages complete successfully; their status describes their
own computation, not completeness of the input documents.

Pair CSVs retain `entry_event_ref` and `exit_event_ref`, plus original source file
identity when available, for verification against canonical document JSON.

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
