from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from src.drive_service.output_paths import build_output_paths

DEFAULT_MIN_HOURS = 6.0
DEFAULT_OUTPUTS = build_output_paths()
DEFAULT_INPUT_DIR = str(DEFAULT_OUTPUTS.shifts_output)
DEFAULT_OUTPUT_DIR = str(DEFAULT_OUTPUTS.enrichment_output)
DEFAULT_REPORT_JSON = str(DEFAULT_OUTPUTS.enrichment_output / "turni_enrichment.stats.json")


@dataclass(slots=True)
class TurniEnrichmentOptions:
    input_dir: str = DEFAULT_INPUT_DIR
    output_dir: str = DEFAULT_OUTPUT_DIR
    min_hours: float = DEFAULT_MIN_HOURS
    include_holidays: bool = True
    report_json: str = DEFAULT_REPORT_JSON
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Arricchisce i pairs.csv per dipendente con classificazioni turni."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Directory dei pairs.csv per dipendente (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--out-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory di output (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--min-hours",
        type=float,
        default=DEFAULT_MIN_HOURS,
        help="Soglia ore per classificazione lunga (durata > soglia, default: 6.0)",
    )
    parser.add_argument(
        "--no-holidays",
        action="store_true",
        help="Non considerare le festivita italiane (solo domeniche).",
    )
    parser.add_argument(
        "--stats-json",
        default=DEFAULT_REPORT_JSON,
        help=(
            "Path report JSON finale (compat alias storico). "
            f"(default: {DEFAULT_REPORT_JSON})"
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> TurniEnrichmentOptions:
    args = build_parser().parse_args(argv)
    return TurniEnrichmentOptions(
        input_dir=args.input_dir,
        output_dir=args.out_dir,
        min_hours=float(args.min_hours),
        include_holidays=not bool(args.no_holidays),
        report_json=args.stats_json,
        verbose=bool(args.verbose),
    )
