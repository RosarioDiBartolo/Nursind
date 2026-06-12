# Codebase Map

| Responsibility | Primary location | Main test |
|---|---|---|
| Configuration and canonical paths | `src/core/config.py`, `src/core/paths.py` | `tests/test_config_paths.py` |
| Direct entry points | `src/scripts/` | `tests/test_scripts.py` |
| Drive scan | `src/core/drive/scan/` | `tests/test_scan_service.py`, `tests/test_scan_runtime.py` |
| Indexes, ZIPs, downloads | `src/core/drive/` | `tests/test_extract_documents_zip_support.py` |
| Document extraction | `src/core/documents/` | `tests/test_extract_documents_zip_support.py` |
| Event parsing | `src/core/events/extraction/` | parser and traceability tests |
| Midnight cleanup | `src/core/events/filtering/` | `tests/test_shift_pipeline.py` |
| Pairing | `src/core/shifts/pairing/` | `tests/test_shift_pipeline.py` |
| Enrichment | `src/core/shifts/enrichment/` | `tests/test_shift_pipeline.py` |
| Summary | `src/core/shifts/summary/` | `tests/test_shift_pipeline.py` |
| Optional tools | `src/core/tools/`, `src/scripts/tools/` | `tests/test_optional_tools.py` |

Scripts own argument parsing, configuration loading, logging, and orchestration.
Core modules own transformations, I/O, validation, and reports.
