from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Literal, Sequence

from src.pipeline_paths import build_pipeline_paths, with_turni_employee_summary_overrides

DEFAULT_YEAR_START = 2014
DEFAULT_YEAR_END = 2025
DEFAULT_OUTPUT_FORMAT: Literal["csv", "json"] = "csv"


def _default_paths():
    return build_pipeline_paths().turni_employee_summary


def default_enriched_dir() -> str:
    return str(_default_paths().input_dir)


def default_summary_csv_path() -> str:
    return str(_default_paths().summary_csv)


def default_report_json_path() -> str:
    return str(_default_paths().report_json)


@dataclass(slots=True)
class TurniEmployeeSummaryOptions:
    enriched_dir: str = field(default_factory=default_enriched_dir)
    out: str = field(default_factory=default_summary_csv_path)
    report_json: str = field(default_factory=default_report_json_path)
    year_start: int | None = DEFAULT_YEAR_START
    year_end: int | None = DEFAULT_YEAR_END
    output_format: Literal["csv", "json"] = DEFAULT_OUTPUT_FORMAT
    min_hours: float | None = None
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(
        description="Aggrega i turni (N/P/F/M/S) per dipendente dai CSV arricchiti."
    )
    parser.add_argument(
        "--enriched-dir",
        default=str(defaults.input_dir),
        help=f"Directory dei CSV arricchiti (default: {defaults.input_dir})",
    )
    parser.add_argument(
        "--out",
        default=str(defaults.summary_csv),
        help=f"Path di output (default: {defaults.summary_csv})",
    )
    parser.add_argument(
        "--report-json",
        default=str(defaults.report_json),
        help=f"Path report JSON finale (default: {defaults.report_json})",
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
    paths = with_turni_employee_summary_overrides(
        build_pipeline_paths(),
        input_dir=args.enriched_dir,
        summary_csv=args.out,
        report_json=args.report_json,
    )
    resolved = paths.turni_employee_summary
    return TurniEmployeeSummaryOptions(
        enriched_dir=str(resolved.input_dir),
        out=str(resolved.summary_csv),
        report_json=str(resolved.report_json),
        year_start=args.year_start,
        year_end=args.year_end,
        output_format=args.format,
        min_hours=float(args.min_hours) if args.min_hours is not None else None,
        verbose=bool(args.verbose),
    )
