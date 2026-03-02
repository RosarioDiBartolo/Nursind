from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from src.drive_service.output_paths import build_output_paths

DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.text_extraction_output)
DEFAULT_OUTPUT_DIR = str(DEFAULT_OUTPUTS.events_output)
DEFAULT_OUT_NAME = "events_from_text_raw.csv"
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.events_output / "extract_events_from_text_raw.report.json"
)
DEFAULT_TEXT_GLOB = "*.txt"
DEFAULT_MAX_PATTERN_EXAMPLES = 12
DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE = 5


@dataclass(slots=True)
class ExtractEventsFromTextOptions:
    input_dir: str = DEFAULT_INPUT_DIR
    output_dir: str = DEFAULT_OUTPUT_DIR
    out_name: str = DEFAULT_OUT_NAME
    report_json: str = DEFAULT_REPORT_JSON
    text_glob: str = DEFAULT_TEXT_GLOB
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse extracted .txt documents and generate per-file raw events CSV outputs."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Root folder containing extracted text files (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Root folder where per-file events CSVs will be written (default: {DEFAULT_OUTPUT_DIR})",
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
            "(e.g. source.txt -> source.events_from_text_raw.csv)"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=f"Path of final JSON report (default: {DEFAULT_REPORT_JSON})",
    )
    parser.add_argument(
        "--max-pattern-examples",
        type=int,
        default=DEFAULT_MAX_PATTERN_EXAMPLES,
        help="Maximum number of example source lines kept per pattern (default: 12)",
    )
    parser.add_argument(
        "--max-unmatched-examples-per-file",
        type=int,
        default=DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
        help="Maximum unmatched source lines retained per file (default: 5)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> ExtractEventsFromTextOptions:
    args = build_parser().parse_args(argv)
    return ExtractEventsFromTextOptions(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        out_name=args.out_name,
        report_json=args.report_json,
        text_glob=args.text_glob,
        max_pattern_examples=max(0, int(args.max_pattern_examples)),
        max_unmatched_examples_per_file=max(0, int(args.max_unmatched_examples_per_file)),
        verbose=args.verbose,
    )
