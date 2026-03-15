from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Sequence

from src.pipeline_paths import build_pipeline_paths, with_pair_employee_overrides

from .artifacts import PAIR_EMPLOYEE_ARTIFACTS


def _default_paths():
    return build_pipeline_paths().pair_employee


def default_input_dir() -> str:
    return str(_default_paths().input_dir)


def default_output_dir() -> str:
    return str(_default_paths().dir)


def default_report_json_path() -> str:
    return str(_default_paths().report_json)


DEFAULT_EVENTS_NAME = PAIR_EMPLOYEE_ARTIFACTS.events_csv.artifact
DEFAULT_MAX_GAP_HOURS = 16.0


@dataclass(slots=True)
class PairEmployeeEventsOptions:
    input_dir: str | None = field(default_factory=default_input_dir)
    output_dir: str = field(default_factory=default_output_dir)
    events_name: str = DEFAULT_EVENTS_NAME
    report_json: str = field(default_factory=default_report_json_path)
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS
    employee_filter: str | None = None
    keep_inferred_column: bool = False
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(
        description=(
            "Accoppia eventi E/U puliti a livello dipendente su tutti i file, "
            "consentendo accoppiamenti cross-file."
        )
    )
    parser.add_argument(
        "--input-dir",
        default=str(defaults.input_dir),
        help=(
            "Root directory with cleaned events files. Supports run-level "
            "`events.cleaned.csv` or canonical per-employee layouts. "
            f"(default: {defaults.input_dir})"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(defaults.dir),
        help=f"Output directory for per-employee pairs (default: {defaults.dir}).",
    )
    parser.add_argument(
        "--events-name",
        default=DEFAULT_EVENTS_NAME,
        help=(
            "Filename pattern for cleaned event files to pair "
            f"(default: {DEFAULT_EVENTS_NAME})"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=PAIR_EMPLOYEE_ARTIFACTS.report_json,
        help=(
            "JSON report output path relative to --output-dir "
            f"(default: {PAIR_EMPLOYEE_ARTIFACTS.report_json})"
        ),
    )
    parser.add_argument(
        "--max-gap-hours",
        type=float,
        default=DEFAULT_MAX_GAP_HOURS,
        help="Maximum allowed entry/exit gap in hours (default: 16).",
    )
    parser.add_argument(
        "--employee",
        help="Filter by employee (case-insensitive).",
    )
    parser.add_argument(
        "--keep-inferred-column",
        action="store_true",
        help="Keep the closed_inferred column in the per-employee output.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> PairEmployeeEventsOptions:
    args = build_parser().parse_args(argv)
    input_dir = str(args.input_dir).strip() if args.input_dir is not None else None
    if input_dir == "":
        input_dir = None

    paths = with_pair_employee_overrides(
        build_pipeline_paths(),
        dir=args.output_dir,
        input_dir=input_dir,
        report_json=args.report_json,
    )
    resolved = paths.pair_employee
    return PairEmployeeEventsOptions(
        input_dir=str(resolved.input_dir) if input_dir is not None else None,
        output_dir=str(resolved.dir),
        events_name=args.events_name,
        report_json=str(resolved.report_json),
        max_gap_hours=float(args.max_gap_hours),
        employee_filter=args.employee,
        keep_inferred_column=bool(args.keep_inferred_column),
        verbose=bool(args.verbose),
    )
