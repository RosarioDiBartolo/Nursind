# Schemas (CSV)

## text layout document JSON (`docs/<file-id>.json`)

Top-level sections:
- `schema_version`
- `source`
- `extraction`
- `document`
- `layout`

Produced by: `src.extract_documents_from_index`

## extracted text employee CSV (`<employee>.csv`)

Columns:
- `employee`, `employee_id`
- `file_id`, `google_drive_file_id`, `file_link`
- `file_name`, `drive_path`
- `source_kind`, `archive_file_id`, `archive_member_path`
- `source_text_ref`, `doc_json`, `has_text_layer`
- `selected_mode`, `tried_vertical`, `normal_quality`, `vertical_quality`

Produced by: `src.extract_documents_from_index`

## events_from_text_raw.csv

Columns:
- `year`, `month`, `day`, `dow`
- `event_index`, `event_kind`, `event_time_hhmm`, `event_ts`
- `event_raw`, `event_pattern`
- `parser_id`, `doc_format`, `source_origin`
- `source_doc_json`, `source_file_id`, `source_file_name`
- `source_employee`, `source_drive_path`, `source_file_link`
- `source_page_no`, `source_line_id`, `source_line_no`, `source_line_text`
- `source_slot`, `source_word_start`, `source_word_end`
- `source_bbox_x0`, `source_bbox_y0`, `source_bbox_x1`, `source_bbox_y1`
- `source_line_start_char`, `source_line_end_char`
- `source_match_start_char`, `source_match_end_char`
- `source_match_col_start`, `source_match_col_end`
- `normalized_from`, `normalization_kind`
- `source_event_ref`

Produced by: `src.extract_events_from_text_raw`

## events_from_text_raw.cleaned.csv

Same schema as `events_from_text_raw.csv`, but with fake-midnight events removed.

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
- `is_holiday`, `is_afternoon`, `is_night`, `turno_code`, `turno_bucket`, `year`
- `turno`, `file_id`, `file_name`, `source_csv`

Produced by: `src.turni_enrichment`

## turni_employee_summary.csv

Columns:
- `employee`, `turno`
- One column per year in the selected range (`--year-start`..`--year-end`)

Produced by: `src.turni_employee_summary`
