from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

try:
    from src.drive_service.output_paths import build_pipelines_paths
except Exception:  # pragma: no cover - defensive fallback for unrelated path module failures
    build_pipelines_paths = None

if build_pipelines_paths is not None:
    DEFAULT_OUTPUTS = build_pipelines_paths()
    DEFAULT_OUT = str(DEFAULT_OUTPUTS.documents_output)
    DEFAULT_INDEX = str(DEFAULT_OUTPUTS.scan_output / "included_index.json")
else:
    DEFAULT_OUTPUTS = None
    DEFAULT_OUT = str(Path("output") / "documents")
    DEFAULT_INDEX = str(Path("output") / "scan" / "included_index.json")


@dataclass(slots=True)
class ExtractDocumentsFromIndexOptions:
    out: str = DEFAULT_OUT
    index: str = DEFAULT_INDEX
    excluded: str = "excluded_documents.index.json"
    included: str = "included_documents.index.json"
    skip_included: bool = True
    reprocess_included: bool = False
    reprocess_excluded: bool = False
    workers: int = 8
    download_workers: int | None = None
    extract_workers: int = field(default_factory=lambda: max(1, os.cpu_count() or 1))
    max_in_flight: int = 128
    flush_every: int = 100
    limit: int = 0
    log_every: int = 50
    min_normal_score: float = 0.72
    min_score_delta: float = 0.08
    report: str = "extract_documents_from_index.report.json"
    verbose: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download PDFs from a MapIndex and save per-employee document extraction outputs."
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output directory")
    parser.add_argument(
        "--index",
        default=DEFAULT_INDEX,
        help="Input MapIndex file (must use current files-object schema)",
    )
    parser.add_argument(
        "--excluded",
        default="excluded_documents.index.json",
        help="Excluded index output filename",
    )
    parser.add_argument(
        "--included",
        default="included_documents.index.json",
        help="Included index output filename",
    )
    parser.add_argument(
        "--skip-included",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip files already present in included index (default: true)",
    )
    parser.add_argument(
        "--reprocess-included",
        action="store_true",
        help="Reprocess files already in included index (overrides --skip-included)",
    )
    parser.add_argument(
        "--reprocess-excluded",
        action="store_true",
        help="Reprocess files already present in excluded index",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Legacy alias for download workers (use --download-workers)",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=None,
        help="Download thread workers (defaults to --workers)",
    )
    parser.add_argument(
        "--extract-workers",
        type=int,
        default=max(1, os.cpu_count() or 1),
        help="Extraction process workers",
    )
    parser.add_argument(
        "--max-in-flight",
        type=int,
        default=128,
        help="Max extraction tasks queued in process pool",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=100,
        help="Write indexes every N processed files",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only first N files (0 = no limit)",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=50,
        help="Log progress every N processed files",
    )
    parser.add_argument(
        "--min-normal-score",
        type=float,
        default=0.72,
        help="If normal extraction score is below this threshold, try vertical extraction",
    )
    parser.add_argument(
        "--min-score-delta",
        type=float,
        default=0.08,
        help="Minimum score improvement needed to replace normal with vertical text",
    )
    parser.add_argument(
        "--report",
        default="extract_documents_from_index.report.json",
        help="Run report filename (relative to --out unless absolute)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser


def parse_options(argv: Sequence[str] | None = None) -> ExtractDocumentsFromIndexOptions:
    args = build_parser().parse_args(argv)
    return ExtractDocumentsFromIndexOptions(
        out=args.out,
        index=args.index,
        excluded=args.excluded,
        included=args.included,
        skip_included=args.skip_included,
        reprocess_included=args.reprocess_included,
        reprocess_excluded=args.reprocess_excluded,
        workers=args.workers,
        download_workers=args.download_workers,
        extract_workers=args.extract_workers,
        max_in_flight=args.max_in_flight,
        flush_every=args.flush_every,
        limit=args.limit,
        log_every=args.log_every,
        min_normal_score=args.min_normal_score,
        min_score_delta=args.min_score_delta,
        report=args.report,
        verbose=args.verbose,
    )
