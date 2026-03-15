from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Sequence

from src.pipeline_paths import build_pipeline_paths, with_turni_enrichment_overrides

from .artifacts import TURNI_ENRICHMENT_ARTIFACTS

DEFAULT_MIN_HOURS = 6.0


def _default_paths():
    return build_pipeline_paths().turni_enrichment


def default_input_dir() -> str:
    return str(_default_paths().input_dir)


def default_output_dir() -> str:
    return str(_default_paths().dir)


def default_report_json_path() -> str:
    return str(_default_paths().report_json)


@dataclass(slots=True)
class TurniEnrichmentOptions:
    input_dir: str = field(default_factory=default_input_dir)
    output_dir: str = field(default_factory=default_output_dir)
    min_hours: float = DEFAULT_MIN_HOURS
    include_holidays: bool = True
    report_json: str = field(default_factory=default_report_json_path)
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(
        description="Arricchisce i pairs.csv per dipendente con classificazioni turni."
    )
    parser.add_argument(
        "--input-dir",
        default=str(defaults.input_dir),
        help=f"Directory dei pairs.csv per dipendente (default: {defaults.input_dir})",
    )
    parser.add_argument(
        "--out-dir",
        default=str(defaults.dir),
        help=f"Directory di output (default: {defaults.dir})",
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
        default=TURNI_ENRICHMENT_ARTIFACTS.report_json,
        help=(
            "Path report JSON finale (compat alias storico) relativo a --out-dir. "
            f"(default: {TURNI_ENRICHMENT_ARTIFACTS.report_json})"
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> TurniEnrichmentOptions:
    args = build_parser().parse_args(argv)
    paths = with_turni_enrichment_overrides(
        build_pipeline_paths(),
        dir=args.out_dir,
        input_dir=args.input_dir,
        report_json=args.stats_json,
    )
    resolved = paths.turni_enrichment
    return TurniEnrichmentOptions(
        input_dir=str(resolved.input_dir),
        output_dir=str(resolved.dir),
        min_hours=float(args.min_hours),
        include_holidays=not bool(args.no_holidays),
        report_json=str(resolved.report_json),
        verbose=bool(args.verbose),
    )
