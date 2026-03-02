from .cli import main
from .options import (
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_PATTERN_EXAMPLES,
    DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
    DEFAULT_OUT_NAME,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_JSON,
    DEFAULT_TEXT_GLOB,
    ExtractEventsFromTextOptions,
    build_parser,
    parse_options,
)
from .runtime import extract_events_from_text_dir, run_from_options
from .service import process_many_text_files, process_one_text_file

__all__ = [
    "DEFAULT_INPUT_DIR",
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_OUT_NAME",
    "DEFAULT_REPORT_JSON",
    "DEFAULT_TEXT_GLOB",
    "DEFAULT_MAX_PATTERN_EXAMPLES",
    "DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE",
    "ExtractEventsFromTextOptions",
    "build_parser",
    "parse_options",
    "process_one_text_file",
    "process_many_text_files",
    "extract_events_from_text_dir",
    "run_from_options",
    "main",
]
