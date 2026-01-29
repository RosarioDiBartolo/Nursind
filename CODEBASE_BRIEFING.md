# Codebase Briefing
Last updated: 2026-01-28

## Purpose
Parse Italian payroll PDFs (cartellini and timbrature) into normalized CSV/JSON outputs, and support bulk Google Drive scans.

## Key Entry Points
- Cartellino: `cartellino_parser.parser.parse_pdf`
- Timbrature (elenco/compact): `timbrature_elenco_parser.parse_pdf`, `timbrature_elenco_compact_parser.parse_pdf`
- Auto-detect router: `parser_service.router.parse_pdf`
- CLI: `python -m parser_service.cli parse --input <FILE|DIR> --out <DIR>`

## Outputs (per PDF)
- `days.csv`: daily metrics (`mo_f`, `mo_t`, `mo_lav`)
- `pairs.csv`: E/U entry/exit pairs
- `totals.json`: aggregated totals
- `report.json`: `{ meta, totals, validation }`

## Detection
Router scores cartellino vs timbrature using keyword hints and day-line patterns. Vertical reconstruction only triggers when both family scores are zero.

## Pairing
- Pair parsing keeps incomplete E/U rows and matches the closest exit after entry (allows next-day exit only).
- Validation uses summed complete pair durations when available (cartellino +/-0.05 hours, timbrature +/-1.0 hour).

## Timbrature Specifics
- Day parsing uses numeric heuristics; `mo_f`/`mo_t`/`mo_lav` are parsed but not used for validation or totals fitting.
- Pair parsing keeps incomplete E/U pairs (entry-only or exit-only); duration is set only for complete pairs.
- Validation compares summed complete pair durations against `ore_lavorate` with a relaxed tolerance (+/-1.0 hour) when possible.

## Tests
- `tests/test_timbrature_samples.py`: validates sample parsing and pair duration consistency.
- `tests/test_samples.py`: sample harness across parsers.
- `tests/test_check_samples.py`: generates outputs from samples and runs full verification (no skipping).

## Docs
- `PARSING_SERVICE.md` contains a comprehensive parser service walkthrough.
