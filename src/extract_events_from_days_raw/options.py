from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from src.drive_service.output_paths import build_output_paths

DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.parsing_output)
DEFAULT_OUTPUT_DIR = str(DEFAULT_OUTPUTS.events_output)
DEFAULT_DAYS_NAME = "*.days.csv"
DEFAULT_OUT_NAME = "events_from_days_raw.csv"
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.events_output / "extract_events_from_days_raw.report.json"
)
DEFAULT_MAX_PATTERN_EXAMPLES = 12
DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE = 5


@dataclass(slots=True)
class ExtractEventsOptions:
    input_dir: str = DEFAULT_INPUT_DIR
    output_dir: str = DEFAULT_OUTPUT_DIR
    days_name: str = DEFAULT_DAYS_NAME
    out_name: str = DEFAULT_OUT_NAME
    report_json: str = DEFAULT_REPORT_JSON
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Estrae eventi E/U dal campo raw di days.csv usando parser condiviso."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Directory radice da cui cercare days.csv (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory radice in cui scrivere events CSV (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--days-name",
        default=DEFAULT_DAYS_NAME,
        help="Pattern file days da cercare ricorsivamente (default: *.days.csv)",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help=(
            "Suffisso file output eventi accanto a ogni days file "
            "(e.g. source.days.csv -> source.events_from_days_raw.csv)"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=(
            "Path report JSON finale "
            f"(default: {DEFAULT_REPORT_JSON})"
        ),
    )
    parser.add_argument(
        "--max-pattern-examples",
        type=int,
        default=DEFAULT_MAX_PATTERN_EXAMPLES,
        help="Massimo numero di esempi raw per pattern nel report (default: 12)",
    )
    parser.add_argument(
        "--max-unmatched-examples-per-file",
        type=int,
        default=DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE,
        help="Massimo numero di esempi raw non matchati per file nel report (default: 5)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> ExtractEventsOptions:
    args = build_parser().parse_args(argv)
    return ExtractEventsOptions(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        days_name=args.days_name,
        out_name=args.out_name,
        report_json=args.report_json,
        max_pattern_examples=max(0, int(args.max_pattern_examples)),
        max_unmatched_examples_per_file=max(0, int(args.max_unmatched_examples_per_file)),
        verbose=args.verbose,
    )
