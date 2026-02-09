# Schemas (CSV)

## days.csv

Columns:
- `year`, `month`, `day`, `dow`
- `mo_f`, `mo_t`, `mo_lav`
- `raw`

Produced by: `src.extract_days_from_text_raw`

## events_from_days_raw.csv

Columns:
- `year`, `month`, `day`, `dow`
- `event_index`, `event_kind`, `event_time_hhmm`, `event_ts`
- `event_raw`, `event_pattern`, `raw`
- `source_row_index`, `source_days_csv`

Produced by: `src.extract_events_from_days_raw`

## events_from_days_raw.cleaned.csv

Same schema as `events_from_days_raw.csv`, but with fake-midnight events removed.

Produced by: `src.filter_midnight_events_from_days_raw`

## employee pairs (`*.pairs.csv`)

Columns:
- `year`, `month`, `day`, `dow`, `pair_index`
- `entry_ts`, `exit_ts`, `duration_hhmm`
- `turno`, `entry_raw`, `exit_raw`
- `file_id`, `file_name`, `source_csv`
- Optional: `closed_inferred` (only when pairing is run with `--keep-inferred-column`)

Produced by: `src.pair_employee_events_from_days_raw`

## enriched employee pairs (`*.enriched.csv`)

Columns:
- `employee`
- `entry_ts`, `exit_ts`, `duration_hours`, `is_long`
- `is_holiday`, `is_afternoon`, `is_night`, `turno_code`, `year`
- `turno`, `file_id`, `file_name`, `source_csv`

Produced by: `src.turni_enrichment`

## turni_employee_summary.csv

Columns:
- `employee`, `turno`
- One column per year in the selected range (`--year-start`..`--year-end`)

Produced by: `src.turni_employee_summary`
