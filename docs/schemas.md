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

## events.csv

Columns:
- `event_id`, `event_ts`
- `event_kind`, `event_time_hhmm`, `event_raw`
- `parser_id`, `source_origin`
- `source_doc_json`, `source_file_id`, `source_file_name`, `source_employee`
- `source_page_no`, `source_line_id`, `source_line_no`, `source_slot`
- `source_event_ref`

Produced by: `src.extract_events_from_documents`

## pages.csv

Columns:
- `page_ref`, `source_doc_json`, `source_file_id`, `source_file_name`, `source_employee`
- `page_no`, `page_kind`, `decision`, `decision_reason`, `parser_id`
- `page_year`, `page_month`, `year_month_source`
- `relevant_for_coverage`
- `rows_considered`, `rows_with_events`, `rows_without_events`
- `events_extracted`, `events_dropped_missing_year_month`
- `coverage_ratio_page`, `header_preview`, `parse_error`

Produced by: `src.extract_events_from_documents`

## events.cleaned.csv

Same schema as `events.csv`, but with fake-midnight events removed.

Produced by: `src.filter_midnight_events`

## employee pairs (`*.pairs.csv`)

Columns:
- `year`, `month`, `day`, `dow`, `pair_index`
- `entry_ts`, `exit_ts`, `duration_hhmm`
- `turno`, `entry_raw`, `exit_raw`
- `file_id`, `file_name`, `source_csv`
- Optional: `closed_inferred` (only when pairing is run with `--keep-inferred-column`)

Produced by: `src.pair_employee_events`

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

## missing_timbrature.issues.csv

Columns:
- `employee`, `employee_id`, `issue_type`, `stage`
- `file_id`, `file_link`, `file_name`, `source_doc_json`
- `page_no`, `year`, `month`, `year_month`
- `detail`, `events_dropped`
- `pair_status`, `pair_error_code`, `pair_output_csv`

Produced by: `src.timbrature_missing_report`

