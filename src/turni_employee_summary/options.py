from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Literal, Sequence

from src.pipeline_paths import build_pipelines_paths

DEFAULT_YEAR_START = 2014
DEFAULT_YEAR_END = 2025
DEFAULT_OUTPUTS = build_pipelines_paths()
DEFAULT_ENRICHED_DIR = str(DEFAULT_OUTPUTS.enrichment_output)
DEFAULT_SUMMARY_CSV = str(DEFAULT_OUTPUTS.aggregation_output / "turni_employee_summary.csv")
DEFAULT_REPORT_JSON = str(
    DEFAULT_OUTPUTS.aggregation_output / "turni_employee_summary.report.json"
)
DEFAULT_OUTPUT_FORMAT: Literal["csv", "json"] = "csv"


@dataclass(slots=True)
class TurniEmployeeSummaryOptions:
    enriched_dir: str = DEFAULT_ENRICHED_DIR
    out: str = DEFAULT_SUMMARY_CSV
    report_json: str = DEFAULT_REPORT_JSON
    year_start: int | None = DEFAULT_YEAR_START
    year_end: int | None = DEFAULT_YEAR_END
    output_format: Literal["csv", "json"] = DEFAULT_OUTPUT_FORMAT
    min_hours: float | None = None
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggrega i turni (N/P/F/M/S) per dipendente dai CSV arricchiti."
    )
    parser.add_argument(
        "--enriched-dir",
        default=DEFAULT_ENRICHED_DIR,
        help=f"Directory dei CSV arricchiti (default: {DEFAULT_ENRICHED_DIR})",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_SUMMARY_CSV,
        help=f"Path di output (default: {DEFAULT_SUMMARY_CSV})",
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help=f"Path report JSON finale (default: {DEFAULT_REPORT_JSON})",
    )
    parser.add_argument(
        "--year-start",
        type=int,
        default=DEFAULT_YEAR_START,
        help="Anno iniziale (default: 2014)",
    )
    parser.add_argument(
        "--year-end",
        type=int,
        default=DEFAULT_YEAR_END,
        help="Anno finale (default: 2025)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default=DEFAULT_OUTPUT_FORMAT,
        help="Formato output (default: csv)",
    )
    parser.add_argument(
        "--min-hours",
        type=float,
        help="Soglia ore per fallback classificazione turno_bucket (durata > soglia).",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> TurniEmployeeSummaryOptions:
    args = build_parser().parse_args(argv)
    return TurniEmployeeSummaryOptions(
        enriched_dir=args.enriched_dir,
        out=args.out,
        report_json=args.report_json,
        year_start=args.year_start,
        year_end=args.year_end,
        output_format=args.format,
        min_hours=float(args.min_hours) if args.min_hours is not None else None,
        verbose=bool(args.verbose),
    )
