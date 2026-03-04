from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from src.drive_service.text_extraction_csv import TEXT_EXTRACTION_CSV_GLOB

try:
    from src.drive_service.output_paths import build_pipelines_paths
except Exception:  # pragma: no cover - defensive fallback for unrelated path module failures
    build_pipelines_paths = None

if build_pipelines_paths is not None:
    DEFAULT_OUTPUTS = build_pipelines_paths()
    DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.documents_output)
    DEFAULT_OUTPUT_DIR = str(DEFAULT_OUTPUTS.events_output)
    DEFAULT_REPORT_JSON = str(DEFAULT_OUTPUTS.events_output / "extract_events.report.json")
else:
    DEFAULT_OUTPUTS = None
    DEFAULT_INPUT_DIR = str(Path("output") / "documents")
    DEFAULT_OUTPUT_DIR = str(Path("output") / "events")
    DEFAULT_REPORT_JSON = str(Path(DEFAULT_OUTPUT_DIR) / "extract_events.report.json")

DEFAULT_OUT_NAME = "events.csv"
DEFAULT_PAGES_NAME = "pages.csv"
DEFAULT_MANIFEST_GLOB = TEXT_EXTRACTION_CSV_GLOB
DEFAULT_MAX_PATTERN_EXAMPLES = 12
DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE = 5


@dataclass(slots=True)
class ExtractEventsFromTextOptions:
    input_dir: str = DEFAULT_INPUT_DIR
    output_dir: str = DEFAULT_OUTPUT_DIR
    out_name: str = DEFAULT_OUT_NAME
    pages_name: str = DEFAULT_PAGES_NAME
    report_json: str = DEFAULT_REPORT_JSON
    manifest_glob: str = DEFAULT_MANIFEST_GLOB
    max_pattern_examples: int = DEFAULT_MAX_PATTERN_EXAMPLES
    max_unmatched_examples_per_file: int = DEFAULT_MAX_UNMATCHED_EXAMPLES_PER_FILE
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Parse canonical extracted document payloads and generate "
            "run-level events/pages CSV outputs."
        )
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=(
            "Root folder containing extracted document manifests/docs "
            f"(default: {DEFAULT_INPUT_DIR})"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Root folder where output artifacts will be written (default: {DEFAULT_OUTPUT_DIR})",
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
        pages_name=args.pages_name,
        report_json=args.report_json,
        manifest_glob=args.manifest_glob,
        max_pattern_examples=max(0, int(args.max_pattern_examples)),
        max_unmatched_examples_per_file=max(0, int(args.max_unmatched_examples_per_file)),
        verbose=args.verbose,
    )
