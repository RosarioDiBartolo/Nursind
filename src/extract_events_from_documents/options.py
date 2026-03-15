from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Sequence

from src.drive_service.text_extraction_csv import TEXT_EXTRACTION_CSV_GLOB
from src.pipeline_paths import build_pipeline_paths, with_extract_events_overrides

from .artifacts import EXTRACT_EVENTS_ARTIFACTS


def _default_paths():
    return build_pipeline_paths().extract_events


def default_input_dir() -> str:
    return str(_default_paths().input_dir)


def default_output_dir() -> str:
    return str(_default_paths().dir)


def default_report_json_path() -> str:
    return str(_default_paths().report_json)


DEFAULT_OUT_NAME = EXTRACT_EVENTS_ARTIFACTS.events_csv
DEFAULT_PAGES_NAME = EXTRACT_EVENTS_ARTIFACTS.pages_csv
DEFAULT_MANIFEST_GLOB = TEXT_EXTRACTION_CSV_GLOB
DEFAULT_MAX_PATTERN_EXAMPLES = 12
DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE = 5


@dataclass(slots=True)
class ExtractEventsFromTextOptions:
    input_dir: str = field(default_factory=default_input_dir)
    output_dir: str = field(default_factory=default_output_dir)
    out_name: str = DEFAULT_OUT_NAME
    pages_name: str = DEFAULT_PAGES_NAME
    report_json: str = field(default_factory=default_report_json_path)
    manifest_glob: str = DEFAULT_MANIFEST_GLOB
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(
        description=(
            "Parse canonical extracted document payloads and generate "
            "run-level events/pages CSV outputs."
        )
    )
    parser.add_argument(
        "--input-dir",
        default=str(defaults.input_dir),
        help=(
            "Root folder containing extracted document manifests/docs "
            f"(default: {defaults.input_dir})"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(defaults.dir),
        help=f"Root folder where output artifacts will be written (default: {defaults.dir})",
    )
    parser.add_argument(
        "--manifest-glob",
        default=DEFAULT_MANIFEST_GLOB,
        help=(
            "Glob pattern recorded in reports for employee manifest discovery "
            f"(default: {DEFAULT_MANIFEST_GLOB})"
        ),
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help=f"Events CSV filename written in --output-dir (default: {DEFAULT_OUT_NAME})",
    )
    parser.add_argument(
        "--pages-name",
        default=DEFAULT_PAGES_NAME,
        help=f"Pages diagnostics CSV filename written in --output-dir (default: {DEFAULT_PAGES_NAME})",
    )
    parser.add_argument(
        "--report-json",
        default=EXTRACT_EVENTS_ARTIFACTS.report_json,
        help=(
            "JSON report output path relative to --output-dir "
            f"(default: {EXTRACT_EVENTS_ARTIFACTS.report_json})"
        ),
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
    paths = with_extract_events_overrides(
        build_pipeline_paths(),
        dir=args.output_dir,
        input_dir=args.input_dir,
        report_json=args.report_json,
    )
    resolved = paths.extract_events
    return ExtractEventsFromTextOptions(
        input_dir=str(resolved.input_dir),
        output_dir=str(resolved.dir),
        out_name=args.out_name,
        pages_name=args.pages_name,
        report_json=str(resolved.report_json),
        manifest_glob=args.manifest_glob,
        max_pattern_examples=max(0, int(args.max_pattern_examples)),
        max_unmatched_examples_per_file=max(0, int(args.max_unmatched_examples_per_file)),
        verbose=args.verbose,
    )
