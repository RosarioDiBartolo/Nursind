from .cli import main
from .options import (
    DEFAULT_INPUT_DIR,
    DEFAULT_MAX_NO_DAYS_FILES,
    DEFAULT_MAX_NO_DAYS_LINES,
    DEFAULT_OUT_DIR,
    DEFAULT_OUT_NAME,
    DEFAULT_REPORT_JSON,
    DEFAULT_TEXT_GLOB,
    ExtractDaysOptions,
    build_parser,
    parse_options,
)
from .runtime import build_days_from_text_dir, run_from_options
from .service import process_many_text_files, process_one_text_file

__all__ = [
    "DEFAULT_INPUT_DIR",
    "DEFAULT_OUT_DIR",
    "DEFAULT_OUT_NAME",
    "DEFAULT_REPORT_JSON",
    "DEFAULT_TEXT_GLOB",
    "DEFAULT_MAX_NO_DAYS_FILES",
    "DEFAULT_MAX_NO_DAYS_LINES",
    "ExtractDaysOptions",
    "build_parser",
    "parse_options",
    "process_one_text_file",
    "process_many_text_files",
    "build_days_from_text_dir",
    "run_from_options",
    "main",
]
