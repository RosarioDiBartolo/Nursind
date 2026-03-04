from __future__ import annotations

import logging
from typing import Sequence

from src.drive_service.logging_utils import setup_logging

from .options import parse_options
from .runtime import run_from_options

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = run_from_options(options)
    stats = report["stats"]
    logger.info(
        "Completato: employees=%s processed=%s with_pairs=%s files(total=%s loaded=%s missing=%s error=%s) events_deduped=%s pairs=%s pairs_deduped=%s inferred=%s unmatched=%s",
        stats["employees_total"],
        stats["employees_processed"],
        stats["employees_with_pairs"],
        stats["event_files_total"],
        stats["event_files_loaded"],
        stats["event_files_missing"],
        stats["event_files_error"],
        stats["events_deduped"],
        stats["pairs_out"],
        stats["pairs_deduped"],
        stats["inferred_pairs"],
        stats["rows_unmatched_after_close"],
    )
    return 0
