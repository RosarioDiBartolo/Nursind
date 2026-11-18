# Shift deviation reports

```powershell
.\.venv\Scripts\python.exe src\scripts\run_pipeline.py --from extract_events --through summarize_shifts
.\.venv\Scripts\python.exe src\scripts\tools\deviation_export.py
```

Both commands use `pipeline.json`; `--config` selects another configuration.
The optional tool writes to `<pipeline_root>/deviation_export/`:

- `<employee>.scostamenti.pdf`: coverage summary and monthly tables of selected shifts;
- `<employee>.confronti.csv`: every comparable complete shift;
- `<employee>.scostamenti.csv`: only shifts over the threshold;
- `<employee>.esclusi.csv`: non-comparable pairs with reasons and source anchors;
- `deviation_export.report.json`: counts, exclusions, upstream warnings and input SHA-256 hashes.

The default threshold is strictly greater than 10 minutes in either entry or exit,
using absolute differences. A negative difference means early, a positive one
late. Configure with `--threshold-minutes 15`, for example. This is a documentary
comparison, not a calculation of overtime or legal entitlement.

## Input and theoretical schedule rules

Actual times come exclusively from `shifts/*.pairs.csv` after midnight filtering.
Both source event anchors must match the current `events/events.cleaned.csv`,
including employee, timestamp and entry/exit kind. Pairs from other inputs or
stale event data are excluded with an explicit reason. Regenerate the pipeline
after changing parsers; the export does not fix source documents itself.

The schedule code is read from each event's original document line. Missing,
ambiguous or unconfigured codes are excluded, never guessed from actual times.
Zero-duration pairs are also excluded. Multiple pairs originating from the same
source attendance line are excluded for manual review: individual fragments
must not each be compared with the full theoretical shift. Justification codes
and separate attendance lines on the same day may still require review.

The bundled `src/core/tools/deviation_export/schedules.json` contains the 13
code/time associations transcribed from the user-supplied reference report
`Frisone Carmela.pdf`. That report is a model, not independent proof of the
employer's official schedules. The shared code table is used for all employees;
review those associations before relying on the comparisons. `T02MA`, `T02P`,
`T01B` and any other unconfigured codes remain excluded. Supply a reviewed JSON
with `--schedules <path>` to extend or replace the table without editing code.

Each schedule defines `entry`, `exit`, and optionally `continuation` or
`continuation_only`. Example:

```json
{
  "source": "Reviewed schedule register and effective period",
  "schedules": {
    "T01M": {"entry": "07:00", "exit": "14:00"},
    "T01N": {"entry": "21:00", "exit": "24:00", "continuation": "T01A"},
    "T01A": {"entry": "00:00", "exit": "07:00", "continuation_only": true}
  }
}
```

Night schedules are compared as **one complete shift**, for example 21:00 to
07:00 next day. They require the configured continuation code on the exit source
line and consecutive dates. The report assigns the shift to its entry month,
including month/year boundaries, and labels next-day times `(+1)`. Isolated
night portions are excluded. Midnight punches are not reintroduced.

Warnings in extraction/pairing are carried into the report cover and export
audit. Excluded or missing shifts do not count as shifts with zero deviation.

## Implementation and checks

Business logic is in `core.tools.deviation_export`; the CLI only parses options,
loads the existing pipeline configuration and calls the service. Discovery of
existing helpers was performed before implementation. The tool reuses source
event anchors, `core.reporting`, `core.drive.names.safe_name`, the existing
canonical CSV writer and missing-report CSV reader. PDF layout, source-code
lookup, comparison rules and orchestration live in separate focused modules.

`tests/test_deviation_export.py` covers threshold signs/boundaries, complete
overnight comparisons across a month boundary, invalid schedules, zero-duration
pairs, source traceability, stale inputs, exclusion accounting and PDF contents.
