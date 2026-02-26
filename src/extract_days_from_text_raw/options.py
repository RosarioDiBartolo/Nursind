from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from src.pipeline_paths import build_pipelines_paths

DEFAULT_OUTPUTS = build_pipelines_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.text_extraction_output)
DEFAULT_OUT_DIR = str(DEFAULT_OUTPUTS.parsing_output)
DEFAULT_OUT_NAME = "days.csv"
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.parsing_output / "extract_days_from_text_raw.report.json"
)
DEFAULT_TEXT_GLOB = "*.txt"
DEFAULT_MAX_NO_DAYS_FILES = 80
DEFAULT_MAX_NO_DAYS_LINES = 8


@dataclass(slots=True)
class ExtractDaysOptions:
    input_dir: str = DEFAULT_INPUT_DIR
    out_dir: str = DEFAULT_OUT_DIR
    out_name: str = DEFAULT_OUT_NAME
    report_json: str = DEFAULT_REPORT_JSON
    text_glob: str = DEFAULT_TEXT_GLOB
    max_no_days_files: int = DEFAULT_MAX_NO_DAYS_FILES
    max_no_days_lines: int = DEFAULT_MAX_NO_DAYS_LINES
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse extracted .txt documents and generate per-file days.csv outputs."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Root folder containing extracted text files (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--out-dir",
        default=DEFAULT_OUT_DIR,
        help=f"Root folder where parsed per-file days.csv will be written (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--text-glob",
        default=DEFAULT_TEXT_GLOB,
        help="Glob pattern used when searching text files recursively (default: *.txt)",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help=(
            "Output suffix written per source file "
            "(e.g. source.txt -> source.days.csv, default: days.csv)"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=(
            "Path of final JSON report "
            f"(default: {DEFAULT_REPORT_JSON})"
        ),
    )
    parser.add_argument(
        "--max-no-days-files",
        type=int,
        default=DEFAULT_MAX_NO_DAYS_FILES,
        help="Maximum number of no-days files kept in report examples (default: 80)",
    )
    parser.add_argument(
        "--max-no-days-lines",
        type=int,
        default=DEFAULT_MAX_NO_DAYS_LINES,
        help="Maximum sample lines per no-days file in report (default: 8)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> ExtractDaysOptions:
    args = build_parser().parse_args(argv)
    return ExtractDaysOptions(
        input_dir=args.input_dir,
        out_dir=args.out_dir,
        out_name=args.out_name,
        report_json=args.report_json,
        text_glob=args.text_glob,
        max_no_days_files=max(0, int(args.max_no_days_files)),
        max_no_days_lines=max(0, int(args.max_no_days_lines)),
        verbose=args.verbose,
    )
