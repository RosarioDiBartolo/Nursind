from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from src.drive_service.output_paths import build_output_paths

DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.events_output)
DEFAULT_EVENTS_NAME = "*.events_from_days_raw.csv"
DEFAULT_OUT_NAME = "events_from_days_raw.cleaned.csv"
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.events_output / "events_from_days_raw.clean_midnight.report.json"
)
DEFAULT_REMOVED_CSV = str(
    DEFAULT_OUTPUTS.events_output / "events_from_days_raw.midnight_removed.csv"
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
        description="Filtra eventi con timestamp a mezzanotte dai CSV eventi per-file."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Directory radice in cui cercare i CSV eventi (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--events-name",
        default=DEFAULT_EVENTS_NAME,
        help="Pattern file eventi da cercare ricorsivamente (default: *.events_from_days_raw.csv)",
    )
    parser.add_argument(
        "--out-name",
        default=DEFAULT_OUT_NAME,
        help=(
            "Nome file output pulito accanto a ogni file eventi "
            "(default: events_from_days_raw.cleaned.csv)"
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
        "--removed-csv",
        default=DEFAULT_REMOVED_CSV,
        help=(
            "Path CSV aggregato con tutte le righe rimosse "
            f"(default: {DEFAULT_REMOVED_CSV})"
        ),
    )
    parser.add_argument(
        "--max-removed-examples-per-file",
        type=int,
        default=DEFAULT_MAX_REMOVED_EXAMPLES_PER_FILE,
        help="Massimo numero di esempi rimossi per file nel report JSON (default: 10)",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Sovrascrive i file eventi originali invece di scrivere *.cleaned.csv",
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
