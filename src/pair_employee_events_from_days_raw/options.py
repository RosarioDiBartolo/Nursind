from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from src.drive_service.output_paths import build_output_paths

DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.events_output)
DEFAULT_INDEX = None
DEFAULT_OUTPUT_DIR = str(DEFAULT_OUTPUTS.shifts_output)
DEFAULT_EVENTS_NAME = "*.events_from_days_raw.cleaned.csv"
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.shifts_output / "pair_employee_events_from_days_raw.report.json"
)
DEFAULT_MAX_GAP_HOURS = 16.0


@dataclass(slots=True)
class PairEmployeeEventsOptions:
    input_dir: str | None = DEFAULT_INPUT_DIR
    index_path: str | None = DEFAULT_INDEX
    output_dir: str = DEFAULT_OUTPUT_DIR
    events_name: str = DEFAULT_EVENTS_NAME
    report_json: str = DEFAULT_REPORT_JSON
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS
    employee_filter: str | None = None
    keep_inferred_column: bool = False
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Accoppia eventi E/U puliti a livello dipendente su tutti i file, "
            "consentendo accoppiamenti cross-file."
        )
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=(
            "Directory radice con eventi per-file in struttura "
            "<employee>/<document>/events_from_days_raw.cleaned.csv "
            f"(default: {DEFAULT_INPUT_DIR})"
        ),
    )
    parser.add_argument(
        "--index",
        default=DEFAULT_INDEX,
        help=(
            "Deprecated: path to included.index.json for legacy index mode. "
            "Used only when --input-dir is empty (default: disabled)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory output separata per i pairs per dipendente.",
    )
    parser.add_argument(
        "--events-name",
        default=DEFAULT_EVENTS_NAME,
        help=(
            "Nome file eventi puliti da leggere accanto a days.csv "
            f"(default: {DEFAULT_EVENTS_NAME})"
        ),
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help="Path report JSON finale.",
    )
    parser.add_argument(
        "--max-gap-hours",
        type=float,
        default=DEFAULT_MAX_GAP_HOURS,
        help="Massimo gap ore per chiudere entry/exit (default: 16).",
    )
    parser.add_argument(
        "--employee",
        help="Filtra per dipendente (case-insensitive).",
    )
    parser.add_argument(
        "--keep-inferred-column",
        action="store_true",
        help="Mantieni la colonna closed_inferred nell'output per dipendente.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> PairEmployeeEventsOptions:
    args = build_parser().parse_args(argv)
    input_dir = str(args.input_dir).strip() if args.input_dir is not None else None
    if input_dir == "":
        input_dir = None

    index_path = str(args.index).strip() if args.index is not None else None
    if index_path == "":
        index_path = None

    return PairEmployeeEventsOptions(
        input_dir=input_dir,
        index_path=index_path,
        output_dir=args.output_dir,
        events_name=args.events_name,
        report_json=args.report_json,
        max_gap_hours=float(args.max_gap_hours),
        employee_filter=args.employee,
        keep_inferred_column=bool(args.keep_inferred_column),
        verbose=bool(args.verbose),
    )
