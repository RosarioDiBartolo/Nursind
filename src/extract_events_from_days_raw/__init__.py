from .cli import main
from .options import (
    DEFAULT_DAYS_NAME,
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_JSON,
    ExtractEventsOptions,
    build_parser,
    parse_options,
)
from .runtime import extract_events_from_days_dir, run_from_options
from .service import process_many_days_files, process_one_days_file

__all__ = [
    "DEFAULT_INPUT_DIR",
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_DAYS_NAME",
    "DEFAULT_OUT_NAME",
    "DEFAULT_REPORT_JSON",
    "DEFAULT_MAX_PATTERN_EXAMPLES",
    "DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE",
    "ExtractEventsOptions",
    "build_parser",
    "parse_options",
    "process_one_days_file",
    "process_many_days_files",
    "extract_events_from_days_dir",
    "run_from_options",
    "main",
]
