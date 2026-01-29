# Parsing Service Overview

This document explains how the parsing service works and details the behavior of each parser.

## 1) High-Level Architecture

The parsing service accepts a PDF (or extracted text), determines the document family, and routes to the appropriate parser:

1. Extract text (normal extraction; optional vertical reconstruction in certain cases).
2. Detect document family (cartellino vs timbrature) using keywords and day-line patterns.
3. If timbrature, detect variant (elenco vs compact) using event density and numeric heuristics.
4. Parse metadata, days, pairs, and totals.
5. Validate `ore_lavorate` against summed complete pair durations when available.
6. Emit `days.csv`, `pairs.csv`, `totals.json`, and `report.json`.

Entry points:
- Programmatic: `parser_service.router.parse_pdf`
- CLI: `python -m parser_service.cli parse --input <FILE|DIR> --out <DIR>`

## 2) Document Detection (router)

File: `src/parser_service/router.py`

Detection is heuristic-based:
- **Cartellino hints**: `"cartellino mensile"`, `"ore lavorate"`, `"ore dovute programmate"`, `"ore dovute contrattuali"`, `"db/cr netto"`.
- **Timbrature hints**: `"elenco timbrature"`, `"totali mensili nel mese di"`, `"matricola"`, `"utente "`.
- Day-line patterns:
  - Cartellino: `DD LU|MA|ME|GI|VE|SA|DO`
  - Timbrature: lines with a valid day header + event tokens `E/U HH:MM`.

Scoring:
- Hints count * 5 + day-line hits for each family.
- If both scores are zero → detection fails.
- If scores are too close (difference ≤ 2) → ambiguous and fails in strict mode.

**Vertical fallback behavior**:
- The router only switches to vertical reconstruction if *both* family scores are zero.
- This avoids discarding valid normal extraction when hints are present but day-line patterning differs.

Strict mode:
- Set `PARSER_DETECT_STRICT=1` to raise on ambiguities.

## 3) Parsing Pipeline (Shared Concepts)

All parsers output:
- `days.csv`: day-level records
- `pairs.csv`: time entry/exit pairs
- `totals.json`: aggregated totals
- `report.json`: `{ meta, totals, validation }`

Validation always compares:
- Sum of complete pair durations vs `totals["ore_lavorate"]`.

## 4) Cartellino Parser

Files:
- `src/cartellino_parser/parser.py`
- `src/cartellino_parser/parse_days.py`
- `src/cartellino_parser/parse_pairs.py`
- `src/cartellino_parser/parse_totals.py`

Flow:
1. **Text extraction** via `parser_shared.extract.extract_text`.
2. If no day lines → try `extract_text_vertical`.
3. **Metadata** from `cartellino_parser.utils.parse_month_year` and `parse_employee`.
4. **Days** from `parse_days`, which expects lines like:
   ```
   05 VE 7.12 7.00 7.12 ...
   ```
   It uses the last 3 numeric tokens as `mo_f`, `mo_t`, `mo_lav`.
5. **Pairs** from `parse_pairs` (E/U events).
6. **Totals** from `parse_totals`.
7. **Validation** with strict tolerance (+/-0.05 hours) when complete pair durations are available.

Notes:
- Pair parsing keeps incomplete E/U rows and matches the closest exit after entry (allows next-day exit only).
- `mo_f`/`mo_t`/`mo_lav` are parsed for reference only and are not used for validation or totals fitting.

## 5) Timbrature Parsers (Shared Pipeline)

Files:
- `src/timbrature_shared/parser_base.py`
- `src/timbrature_shared/parse_pairs.py`
- `src/timbrature_shared/parse_totals.py`
- `src/timbrature_shared/utils.py`

Shared flow:
1. Extract text.
2. Parse metadata: `timbrature_shared.utils.parse_month_year`, `parse_employee`.
3. Parse day records with variant-specific `parse_days`.
4. Parse pairs (`E` entry / `U` exit).
5. Parse totals (from `Totali mensili nel mese di` window).
6. Validate with relaxed tolerance (+/-1.0 hour) when possible.

### 5.1) Timbrature Elenco Parser

File: `src/timbrature_elenco_parser/parse_days.py`

Day parsing behavior:
- Extracts numeric tokens from each day line.
- If `E/U` events exist, it expects at least 2 large values:
  - `mo_f` = first value
  - `mo_lav` = second value
- If no events, it uses available values; no totals adjustment is applied.

### 5.2) Timbrature Compact Parser

File: `src/timbrature_elenco_compact_parser/parse_days.py`

Day parsing behavior:
- Extracts numeric tokens and keeps values ≥ 1.0.
- Picks `mo_f` / `mo_lav` using heuristics similar to elenco.
- When any events exist in the document, non-event day `mo_lav` starts at 0 (no totals adjustment is applied).

## 6) Pair Parsing (Timbrature)

File: `src/timbrature_shared/parse_pairs.py`

Rules:
- Recognizes `E HH:MM` and `U HH:MM` events.
- Pairs are emitted even when entry or exit is missing (entry-only or exit-only).
- The closest exit after an entry is matched, allowing next-day exits only.
- A `U` without a prior `E` yields an exit-only pair.
- An `E` without a subsequent `U` yields an entry-only pair.
- Overnight shifts are supported: if exit time < entry time, 1 day is added.

Incomplete pairs are kept; validation uses only complete pairs for totals checks.

## 7) Timbrature `mo_lav` Adjustment (Legacy, disabled)

File: `src/timbrature_shared/adjust.py`

Because timbrature layouts vary and day-line numeric positions are inconsistent, `mo_lav` is adjusted to match `totals["ore_lavorate"]`.

Strategy:
1. For each day line, build candidate values:
   - Event day: primary = second large value, alternative = first large value
   - Non-event day: primary = 0.0, alternative = first large value
2. If current sum is **too high**, switch a subset of event days from primary → alternative.
3. If current sum is **too low**, add non-event day values (alternative).
4. Subset selection uses a best-fit algorithm in minutes to closely match totals.

This adjustment is currently disabled; `mo_lav` values remain as parsed and are not used for validation.

## 8) Validation

File: `src/parser_shared/validate.py`

- Cartellino: strict tolerance (+/-0.05 hours) when complete pair durations are available.
- Timbrature: relaxed tolerance (+/-1.0 hour) when complete pair durations are available.

## 9) CLI Utilities

### Parser Service CLI
Command:
```
python -m parser_service.cli parse --input <FILE|DIR> --out <DIR> [--recursive] [--debug-detect] [--dump-text]
```

Debug outputs:
- `detect.normal.json`, `detect.vertical.json`
- `extracted.normal.txt`, `extracted.vertical.txt`

### Sample Checker
Command:
```
python -m parser_service.check_samples --samples samples --out samples/output --sample 1.0 --extract-if-missing
```

It verifies:
- Outputs exist
- Day counts match extracted text
- Validation status
- Pair integrity (missing entry/exit, missing duration, duration mismatch)

## 10) Known Behaviors and Limits

- Timbrature detection can warn on ambiguous variants; current behavior defaults to compact in non-strict mode.
- Some timbrature PDFs contain no E/U events; pairs will be empty.
- Validation may be unavailable when no complete pairs are present.
- Totals fitting is currently disabled; if future formats change significantly, new rules may be needed.

