from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from src.drive_service.output_paths import build_output_paths

DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.events_output)
DEFAULT_EVENTS_NAME = "*.events_from_text_raw.csv"
DEFAULT_OUT_NAME = "events_from_text_raw.cleaned.csv"
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.events_output / "events_from_text_raw.clean_midnight.report.json"
)
DEFAULT_REMOVED_CSV = str(
    DEFAULT_OUTPUTS.events_output / "events_from_text_raw.midnight_removed.csv"
)
DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE = 10


@dataclass(slots=True)
class FilterMidnightEventsOptions:
    input_dir: str = DEFAULT_INPUT_DIR
    events_name: str = DEFAULT_EVENTS_NAME
    out_name: str = DEFAULT_OUT_NAME
    report_json: str = DEFAULT_REPORT_JSON
    removed_csv: str = DEFAULT_REMOVED_CSV
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE
    in_place: bool = False
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter midnight events from per-file raw events CSVs."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Root directory containing per-file raw events CSVs (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--events-name",
        default=DEFAULT_EVENTS_NAME,
        help="Recursive glob for raw events files (default: *.events_from_text_raw.csv)",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help=(
            "Cleaned output filename written next to each raw events file "
            "(default: events_from_text_raw.cleaned.csv)"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=(
            "Path of the final JSON report "
            f"(default: {DEFAULT_REPORT_JSON})"
        ),
    )
    parser.add_argument(
        "--removed-csv",
        default=DEFAULT_REMOVED_CSV,
        help=(
            "Path of the aggregate CSV containing all removed rows "
            f"(default: {DEFAULT_REMOVED_CSV})"
        ),
    )
    parser.add_argument(
        "--max-removed-examples-per-file",
        type=int,
        default=DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
        help="Maximum removed-row examples retained per file in the JSON report (default: 10)",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the raw event files instead of writing a separate *.cleaned.csv file",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> FilterMidnightEventsOptions:
    args = build_parser().parse_args(argv)
    return FilterMidnightEventsOptions(
        input_dir=args.input_dir,
        events_name=args.events_name,
        out_name=args.out_name,
        report_json=args.report_json,
        removed_csv=args.removed_csv,
        max_removed_examples_per_file=max(0, int(args.max_removed_examples_per_file)),
        in_place=args.in_place,
        verbose=args.verbose,
    )
