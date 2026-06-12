# Graph Report - .  (2026-06-10)

## Corpus Check
- 31 files · ~59,215 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1187 nodes · 3208 edges · 59 communities (45 shown, 14 thin omitted)
- Extraction: 76% EXTRACTED · 24% INFERRED · 0% AMBIGUOUS · INFERRED: 782 edges (avg confidence: 0.62)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_PipelineClient|PipelineClient]]
- [[_COMMUNITY_text extraction csv py|text extraction csv py]]
- [[_COMMUNITY_build pipeline paths|build pipeline paths]]
- [[_COMMUNITY_service py|service py]]
- [[_COMMUNITY_setup logging|setup logging]]
- [[_COMMUNITY_service py 2|service py 2]]
- [[_COMMUNITY_service py 3|service py 3]]
- [[_COMMUNITY_assert process one contract|assert process one contract]]
- [[_COMMUNITY_service py 4|service py 4]]
- [[_COMMUNITY_common py|common py]]
- [[_COMMUNITY_runtime py|runtime py]]
- [[_COMMUNITY_request from object|request from object]]
- [[_COMMUNITY_service py 5|service py 5]]
- [[_COMMUNITY_raw text parsing py|raw text parsing py]]
- [[_COMMUNITY_service py 6|service py 6]]
- [[_COMMUNITY_service py 7|service py 7]]
- [[_COMMUNITY_CartellinoOcrParser|CartellinoOcrParser]]
- [[_COMMUNITY_pipeline config json|pipeline config json]]
- [[_COMMUNITY_BaseFormatParser|BaseFormatParser]]
- [[_COMMUNITY_Canonical Document Extraction|Canonical Document Extraction]]
- [[_COMMUNITY_exceptions py|exceptions py]]
- [[_COMMUNITY_page analysis py|page analysis py]]
- [[_COMMUNITY_shared config json|shared config json]]
- [[_COMMUNITY_Document to Events Extraction|Document to Events Extraction]]
- [[_COMMUNITY_Shared Logic Registry|Shared Logic Registry]]
- [[_COMMUNITY_pdf text extraction py|pdf text extraction py]]
- [[_COMMUNITY_test scan runtime py|test scan runtime py]]
- [[_COMMUNITY_FakeFuture|FakeFuture]]
- [[_COMMUNITY_BytesIO|BytesIO]]
- [[_COMMUNITY_test extract documents zip support|test extract documents zip support]]
- [[_COMMUNITY_build output path|build output path]]
- [[_COMMUNITY_run scan|run scan]]
- [[_COMMUNITY_Cartellino Classic Event Rows|Cartellino Classic Event Rows]]
- [[_COMMUNITY_collect files recursive|collect files recursive]]
- [[_COMMUNITY_Architecture Ownership Backbone|Architecture Ownership Backbone]]
- [[_COMMUNITY_test scan service py|test scan service py]]
- [[_COMMUNITY_OCR Type 4 Presence Report|OCR Type 4 Presence Report]]
- [[_COMMUNITY_parse numeric token|parse numeric token]]
- [[_COMMUNITY_test download from index py|test download from index py]]
- [[_COMMUNITY_cartellino parser Public API|cartellino parser Public API]]
- [[_COMMUNITY_Cartellino Unico Employee Monthly Card|Cartellino Unico Employee Monthly Card]]
- [[_COMMUNITY_test parser contracts from fixtures|test parser contracts from fixtures]]
- [[_COMMUNITY_init py|init py]]
- [[_COMMUNITY_init py 2|init py 2]]
- [[_COMMUNITY_init py 3|init py 3]]
- [[_COMMUNITY_init py 4|init py 4]]
- [[_COMMUNITY_init py 5|init py 5]]
- [[_COMMUNITY_init py 6|init py 6]]
- [[_COMMUNITY_init py 7|init py 7]]
- [[_COMMUNITY_init py 8|init py 8]]
- [[_COMMUNITY_init py 9|init py 9]]
- [[_COMMUNITY_Aggregation Pipeline Documentation|Aggregation Pipeline Documentation]]
- [[_COMMUNITY_Enrichment Pipeline Documentation|Enrichment Pipeline Documentation]]
- [[_COMMUNITY_Ingestion Pipeline Documentation|Ingestion Pipeline Documentation]]
- [[_COMMUNITY_Preparation Pipeline Documentation|Preparation Pipeline Documentation]]

## God Nodes (most connected - your core abstractions)
1. `PipelineClient` - 49 edges
2. `build_pipeline_paths()` - 48 edges
3. `TurniAfternoonLongExportRequest` - 37 edges
4. `ExtractDocumentsFromIndexOptions` - 34 edges
5. `Any` - 33 edges
6. `DriveAuthConfig` - 33 edges
7. `PipelinePaths` - 33 edges
8. `ParserRecallAuditRequest` - 28 edges
9. `CredentialsError` - 27 edges
10. `StageReport` - 27 edges

## Surprising Connections (you probably didn't know these)
- `Gap and Conflict Detection` --semantically_similar_to--> `MissingTimbratureAuditRequest`  [INFERRED] [semantically similar]
  docs/piattaforma-raccolta-documentale-avvocato.html → src/cartellino_parser/client.py
- `Structured Document Archive` --semantically_similar_to--> `Canonical Document Artifacts`  [INFERRED] [semantically similar]
  docs/piattaforma-raccolta-documentale-avvocato.html → README.md
- `Pages CSV Schema` --shares_data_with--> `ParserRecallAuditRequest`  [EXTRACTED]
  docs/schemas.md → src/cartellino_parser/client.py
- `Healthcare Shift Processing Pipeline` --references--> `ParserRecallAuditRequest`  [EXTRACTED]
  README.md → src/cartellino_parser/client.py
- `Pipeline CSV Schemas` --references--> `MissingTimbratureAuditRequest`  [EXTRACTED]
  docs/schemas.md → src/cartellino_parser/client.py

## Import Cycles
- 1-file cycle: `src/drive_service/index/converters.py -> src/drive_service/index/converters.py`
- 1-file cycle: `src/extract_events_from_documents/page_analysis.py -> src/extract_events_from_documents/page_analysis.py`
- 3-file cycle: `src/extract_events_from_documents/parsers/__init__.py -> src/extract_events_from_documents/parsers/router.py -> src/extract_events_from_documents/parsers/loader.py -> src/extract_events_from_documents/parsers/__init__.py`

## Hyperedges (group relationships)
- **Canonical Document Processing Flow** — docs_ingestion_document_extraction, readme_canonical_document_artifacts, docs_codebase_map_event_extraction, docs_schemas_events_schema [EXTRACTED 1.00]
- **Audit and Coverage Flow** — docs_schemas_pages_schema, readme_parser_recall_audit, readme_missing_timbrature_audit, docs_schemas_missing_timbrature_schemas [EXTRACTED 1.00]
- **Shared Logic Governance** — docs_codebase_map_shared_logic_first, docs_shared_logic_registry_shared_logic_registry, docs_shared_logic_registry_anti_duplication_policy [EXTRACTED 1.00]

## Communities (59 total, 14 thin omitted)

### Community 0 - "PipelineClient"
Cohesion: 0.14
Nodes (58): PipelineClient, Public entrypoint for external modules importing the package., CredentialsError, Raised when credentials are required but unavailable., _coerce_list_of_mappings(), _coerce_mapping(), ExtractDocumentsRequest, ExtractEventsRequest (+50 more)

### Community 1 - "text extraction csv py"
Cohesion: 0.07
Nodes (60): BaseModel, ensure_dir(), ensure_parent_dir(), doc_attr(), maybe_flush_indexes(), update_index_meta(), normalize_name(), safe_name() (+52 more)

### Community 2 - "build pipeline paths"
Cohesion: 0.07
Nodes (55): _canonical_step_defaults(), _default_config_path(), load_notebook_context(), NotebookContext, PathLike, PipelinePaths, PipelineStage, Any (+47 more)

### Community 3 - "service py"
Cohesion: 0.11
Nodes (51): Counter, EmployeeAccumulator, Path, YearMonth, Any, YearMonth, ArgumentParser, Any (+43 more)

### Community 4 - "setup logging"
Cohesion: 0.09
Nodes (40): AnyIndex, load_json(), write_json(), get_logger(), setup_logging(), DuplicatePolicy, build_local_pdf_index(), build_parser() (+32 more)

### Community 5 - "service py 2"
Cohesion: 0.09
Nodes (43): main(), build_parser(), default_input_dir(), _default_paths(), default_report_json_path(), ExtractEventsFromTextOptions, parse_options(), extract_events_from_documents_dir() (+35 more)

### Community 6 - "service py 3"
Cohesion: 0.10
Nodes (43): ArgumentParser, ParagraphStyle, DataFrame, Path, Series, Any, DataFrame, Path (+35 more)

### Community 7 - "assert process one contract"
Cohesion: 0.07
Nodes (37): Directory-Batch Step, Index-Driven Extraction Step, Pipeline-Audit Step, process_many Return Contract, process_one Return Contract, Public Import Surface, Pipeline Step Test Contract, Architecture Brainstorming Notes (+29 more)

### Community 8 - "service py 4"
Cohesion: 0.12
Nodes (32): ShiftClassifier, assign_turno_bucket(), assign_turno_code(), compute_turno(), ItalianHolidayCalendar, DataFrame, date, Series (+24 more)

### Community 9 - "common py"
Cohesion: 0.14
Nodes (24): BaseFormatParser, DocumentLine, DocumentParseResult, EventRecord, document_text(), _event_word_position(), explicit_events_for_line(), get_layout_pages() (+16 more)

### Community 10 - "runtime py"
Cohesion: 0.12
Nodes (34): main(), dedupe_events(), _event_sort_key(), events_to_partial_pairs(), normalize_employee(), normalize_events_file(), build_parser(), default_input_dir() (+26 more)

### Community 11 - "request from object"
Cohesion: 0.08
Nodes (25): _object_to_dict(), request_from_object(), ExtractDocumentsArtifactsSpec, main(), build_parser(), default_excluded_index_path(), default_index_path(), _default_paths() (+17 more)

### Community 12 - "service py 5"
Cohesion: 0.15
Nodes (31): main(), build_parser(), _default_paths(), default_report_json_path(), default_root_dir(), default_suspicious_csv_path(), parse_options(), audit_parser_recall_root() (+23 more)

### Community 13 - "raw text parsing py"
Cohesion: 0.13
Nodes (32): alpha_token(), _coerce_day_prefix(), coerce_year(), _coerce_year_from_header_token(), _collapse_ocr_doubled_token(), detect_doc_format(), EventMatch, infer_year_month_from_filename() (+24 more)

### Community 14 - "service py 6"
Cohesion: 0.15
Nodes (29): build_stage_report(), compact_stage_report(), Any, Path, write_json_report(), ArgumentParser, Any, DataFrame (+21 more)

### Community 15 - "service py 7"
Cohesion: 0.19
Nodes (24): main(), build_parser(), default_input_dir(), _default_paths(), default_removed_csv_path(), default_report_json_path(), FilterMidnightEventsOptions, parse_options() (+16 more)

### Community 16 - "CartellinoOcrParser"
Cohesion: 0.18
Nodes (6): DocumentLine, CartellinoOcrParser, Any, ParsedEvent, _layout_word(), test_cartellino_ocr_parser_handles_spaced_day_headers_and_split_times()

### Community 17 - "pipeline config json"
Cohesion: 0.08
Nodes (25): base_output_dir, extract_documents, extract_events, filter_midnight, known_prefixes, ACETO, AMATUCCI, GASPERINI (+17 more)

### Community 18 - "BaseFormatParser"
Cohesion: 0.16
Nodes (16): ABC, BaseFormatParser, load_parsers(), resolve_parser(), Any, ParsedRow, BaseFormatParser, Any (+8 more)

### Community 19 - "Canonical Document Extraction"
Cohesion: 0.12
Nodes (24): Legacy Aggregation Default Paths, Canonical Document Extraction, Drive Scan, Ingestion Pipeline, Text Layer Validation, ZIP Virtual Folder Support, Auditable Document History, Automatic Document Validation (+16 more)

### Community 20 - "exceptions py"
Cohesion: 0.13
Nodes (19): ArtifactContractError, CartellinoParserError, ConfigurationError, OptionalDependencyError, PackagingError, ProcessingError, Raised when required configuration is missing or invalid., Raised when pipeline artifacts do not match the expected contract. (+11 more)

### Community 21 - "page analysis py"
Cohesion: 0.23
Nodes (21): datetime, ParsedEvent, ParsedRow, base_process_result(), _build_day(), build_event_row(), classify_page_kind(), dedupe_rows() (+13 more)

### Community 22 - "shared config json"
Cohesion: 0.10
Nodes (21): base_output_dir, extract_documents, extract_events, filter_midnight, known_prefixes, ACETO, AMATUCCI, GASPERINI (+13 more)

### Community 23 - "Document to Events Extraction"
Cohesion: 0.17
Nodes (19): Employee Afternoon Shift PDF Report, Employee Turno Yearly Summary, Document-to-Events Extraction, Shift Enrichment and Classification, Turno Bucket, Employee-Level Event Pairing, Document-to-Events Extraction, Midnight Event Cleanup (+11 more)

### Community 24 - "Shared Logic Registry"
Cohesion: 0.18
Nodes (18): Required Helper Discovery Protocol, Agent Instructions, Three Responsibility Module Limit, Codebase Briefing, Codebase Map, Current Structural Hotspots, Shared Logic First, Shared Logic Registry (+10 more)

### Community 25 - "pdf text extraction py"
Cohesion: 0.22
Nodes (16): BinaryIO, extract_best_text(), score_text_quality(), Page, PDF, _cluster_by_x(), extract_layout(), _extract_layout_from_pdf() (+8 more)

### Community 26 - "test scan runtime py"
Cohesion: 0.21
Nodes (11): _FakeDrive, _FakeFailingDrive, _FakeFailingFilesApi, _FakeFilesApi, test_get_root_name_returns_none_on_error(), test_get_root_name_returns_none_when_root_is_missing(), test_run_scan_happy_path(), test_run_scan_ignores_non_folder_children() (+3 more)

### Community 27 - "FakeFuture"
Cohesion: 0.23
Nodes (8): _FakeDownloadPool, _FakeExtractPool, _FakeFuture, Exception, Path, _resolve_future_payload(), test_finalize_extraction_run_marks_interrupted_status(), test_run_extraction_drains_in_flight_extracts_on_keyboard_interrupt()

### Community 28 - "BytesIO"
Cohesion: 0.27
Nodes (10): BytesIO, extract_zip_member_bytes(), list_pdf_members(), normalize_zip_member_path(), parse_archive_member_id(), download_file_bytes(), download_file_stream(), download_pdf_bytes_for_index_item() (+2 more)

### Community 29 - "test extract documents zip support"
Cohesion: 0.24
Nodes (8): build_archive_member_id(), _make_zip_bytes(), Path, test_collect_docs_marks_zip_members_and_unique_stems(), test_download_pdf_bytes_from_local_index_entry_reads_drive_path(), test_download_pdf_bytes_from_zip_member_uses_cache(), test_download_pdf_bytes_zip_member_not_found(), test_extract_and_write_returns_drive_link_and_source_ref()

### Community 30 - "build output path"
Cohesion: 0.33
Nodes (10): _build_output_path(), build_parser(), main(), ArgumentParser, _select_docs_for_download(), Path, test_audit_parser_recall_root_returns_ranked_suspicious_pages(), test_build_parser_recall_report_writes_csv_and_json() (+2 more)

### Community 31 - "run scan"
Cohesion: 0.29
Nodes (9): _build_employee_found_entry(), get_root_name(), merge_reports_to_maps(), Run scan orchestration with continue-on-error employee handling., Return Drive root display name when available., Merge folder reports into included/filtered file maps (last duplicate wins)., run_scan(), Any (+1 more)

### Community 32 - "Cartellino Classic Event Rows"
Cohesion: 0.20
Nodes (10): Cartellino Classic Event Rows, March 2023, Cartellino Classic Day Signature, April 2024, Situazione Mensile Presence Rows, Situazione Mensile Day Signature, January 2023, Suppressed Holiday Quantity (+2 more)

### Community 33 - "collect files recursive"
Cohesion: 0.33
Nodes (8): get_drive_service(), list_children(), build_folder_report(), collect_files_recursive(), file_excluded(), find_excluding_term(), folder_excluded(), _is_pdf_or_zip()

### Community 34 - "Architecture Ownership Backbone"
Cohesion: 0.29
Nodes (8): Afternoon Long Export, Long Afternoon Shift Filter, Architecture Ownership Backbone, Pipeline Path Resolver, Shared Shift Services, Enrichment Pipeline, Turno Bucket Classification, Enriched Employee Pairs Schema

### Community 35 - "test scan service py"
Cohesion: 0.36
Nodes (6): normalize_term(), _make_zip_bytes(), test_build_folder_report_no_stdout_on_excluded_file(), test_collect_files_recursive_expands_zip_pdf_members(), test_file_excluded_matches_terms(), test_folder_excluded_normalizes_name()

### Community 36 - "OCR Type 4 Presence Report"
Cohesion: 0.33
Nodes (7): AUSL Toscana Sud-Est, October 2023, OCR Type 4 Presence Report, Azienda USL Toscana Sud Est, January 2014, OCR Type 5 Shift Table, Cartellino OCR Day Signature

### Community 37 - "parse numeric token"
Cohesion: 0.33
Nodes (7): extract_all_values(), extract_leading_values(), extract_trailing_values(), _format_token_sign(), parse_decimal(), parse_hhmm(), parse_numeric_token()

### Community 39 - "cartellino parser Public API"
Cohesion: 0.50
Nodes (4): Thin CLI Policy, cartellino_parser Public API, Package Smoke Workflow, Clean Wheel Validation

### Community 40 - "Cartellino Unico Employee Monthly Card"
Cohesion: 0.50
Nodes (4): Aceto Caterina, Cartellino Unico Employee Monthly Card, May 2025, Cartellino Unico Day Signature

### Community 41 - "test parser contracts from fixtures"
Cohesion: 0.67
Nodes (3): _document(), Path, test_parser_contracts_from_fixtures()

## Ambiguous Edges - Review These
- `Employee Turno Yearly Summary` → `Legacy Aggregation Default Paths`  [AMBIGUOUS]
  docs/aggregation.md · relation: documents_paths_with

## Knowledge Gaps
- **97 isolated node(s):** `RequestT`, `ArgumentParser`, `ArgumentParser`, `IndexFile`, `Any` (+92 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **14 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Employee Turno Yearly Summary` and `Legacy Aggregation Default Paths`?**
  _Edge tagged AMBIGUOUS (relation: documents_paths_with) - confidence is low._
- **Why does `build_pipeline_paths()` connect `build pipeline paths` to `PipelineClient`, `service py`, `service py 2`, `service py 4`, `runtime py`, `request from object`, `service py 5`, `service py 6`, `service py 7`, `shared config json`?**
  _High betweenness centrality (0.150) - this node is a cross-community bridge._
- **Why does `extract_events()` connect `PipelineClient` to `build pipeline paths`, `raw text parsing py`?**
  _High betweenness centrality (0.100) - this node is a cross-community bridge._
- **Why does `ensure_parent_dir()` connect `text extraction csv py` to `PipelineClient`, `setup logging`, `service py 2`, `service py 4`, `runtime py`, `service py 6`, `service py 7`?**
  _High betweenness centrality (0.086) - this node is a cross-community bridge._
- **Are the 28 inferred relationships involving `PipelineClient` (e.g. with `CredentialsError` and `ExtractDocumentsRequest`) actually correct?**
  _`PipelineClient` has 28 INFERRED edges - model-reasoned connections that need verification._
- **Are the 27 inferred relationships involving `build_pipeline_paths()` (e.g. with `.build_paths()` and `.scan()`) actually correct?**
  _`build_pipeline_paths()` has 27 INFERRED edges - model-reasoned connections that need verification._
- **Are the 33 inferred relationships involving `TurniAfternoonLongExportRequest` (e.g. with `PipelineClient` and `DriveAuthConfig`) actually correct?**
  _`TurniAfternoonLongExportRequest` has 33 INFERRED edges - model-reasoned connections that need verification._