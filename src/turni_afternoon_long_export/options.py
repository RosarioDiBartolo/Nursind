from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from cartellino_parser.pipeline_paths import build_pipeline_paths

from .artifacts import TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS


def default_enriched_dir() -> str:
    return str(build_pipeline_paths().turni_enrichment.dir)


def default_output_dir() -> str:
    return str(
        build_pipeline_paths().layout.output_root
        / TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS.output_dir_name
    )


def default_report_json_path() -> str:
    return str(Path(default_output_dir()) / TURNI_AFTERNOON_LONG_EXPORT_ARTIFACTS.report_json)


@dataclass(slots=True)
class TurniAfternoonLongExportOptions:
    enriched_dir: str = field(default_factory=default_enriched_dir)
    output_dir: str = field(default_factory=default_output_dir)
    report_json: str = field(default_factory=default_report_json_path)
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Esporta un CSV per dipendente con sole righe dei CSV arricchiti "
            "dove is_afternoon=true e is_long=true."
        )
    )
    parser.add_argument(
        "--enriched-dir",
        default=default_enriched_dir(),
        help=f"Directory dei CSV arricchiti (default: {default_enriched_dir()})",
    )
    parser.add_argument(
        "--out-dir",
        default=default_output_dir(),
        help=f"Directory di output per i CSV filtrati (default: {default_output_dir()})",
    )
    parser.add_argument(
        "--report-json",
        default=default_report_json_path(),
        help=f"Path report JSON finale (default: {default_report_json_path()})",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> TurniAfternoonLongExportOptions:
    args = build_parser().parse_args(argv)
    return TurniAfternoonLongExportOptions(
        enriched_dir=str(Path(args.enriched_dir)),
        output_dir=str(Path(args.out_dir)),
        report_json=str(Path(args.report_json)),
        verbose=bool(args.verbose),
    )
