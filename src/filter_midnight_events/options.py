from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Sequence

from src.pipeline_paths import build_pipeline_paths, with_filter_midnight_overrides

from .artifacts import FILTER_MIDNIGHT_ARTIFACTS


def _default_paths():
    return build_pipeline_paths().filter_midnight


def default_input_dir() -> str:
    return str(_default_paths().input_dir)


def default_report_json_path() -> str:
    return str(_default_paths().report_json)


def default_removed_csv_path() -> str:
    return str(_default_paths().removed_csv)


DEFAULT_EVENTS_NAME = FILTER_MIDNIGHT_ARTIFACTS.events_csv.artifact
DEFAULT_OUT_NAME = FILTER_MIDNIGHT_ARTIFACTS.cleaned_events_csv
DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE = 10


@dataclass(slots=True)
class FilterMidnightEventsOptions:
    input_dir: str = field(default_factory=default_input_dir)
    events_name: str = DEFAULT_EVENTS_NAME
    out_name: str = DEFAULT_OUT_NAME
    report_json: str = field(default_factory=default_report_json_path)
    removed_csv: str = field(default_factory=default_removed_csv_path)
    max_removed_examples_per_file: int = DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE
    in_place: bool = False
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(
        description="Filter midnight events from run-level raw events CSV files."
    )
    parser.add_argument(
        "--input-dir",
        default=str(defaults.input_dir),
        help=f"Root directory containing raw events CSV files (default: {defaults.input_dir})",
    )
    parser.add_argument(
        "--events-name",
        default=DEFAULT_EVENTS_NAME,
        help=f"Recursive glob for raw events files (default: {DEFAULT_EVENTS_NAME})",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help=(
            "Cleaned output filename written next to each raw events file "
            f"(default: {DEFAULT_OUT_NAME})"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=FILTER_MIDNIGHT_ARTIFACTS.report_json,
        help=(
            "JSON report output path relative to --input-dir "
            f"(default: {FILTER_MIDNIGHT_ARTIFACTS.report_json})"
        ),
    )
    parser.add_argument(
        "--removed-csv",
        default=FILTER_MIDNIGHT_ARTIFACTS.removed_csv,
        help=(
            "Removed-rows CSV output path relative to --input-dir "
            f"(default: {FILTER_MIDNIGHT_ARTIFACTS.removed_csv})"
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
    paths = with_filter_midnight_overrides(
        build_pipeline_paths(),
        dir=args.input_dir,
        input_dir=args.input_dir,
        report_json=args.report_json,
        removed_csv=args.removed_csv,
    )
    resolved = paths.filter_midnight
    return FilterMidnightEventsOptions(
        input_dir=str(resolved.input_dir),
        events_name=args.events_name,
        out_name=args.out_name,
        report_json=str(resolved.report_json),
        removed_csv=str(resolved.removed_csv),
        max_removed_examples_per_file=max(0, int(args.max_removed_examples_per_file)),
        in_place=args.in_place,
        verbose=args.verbose,
    )
