from __future__ import annotations

import logging
from typing import Sequence

from src.drive_service.logging_utils import setup_logging

from .options import parse_options
from .service import run_from_options

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = run_from_options(options)
    stats = report["stats"]
    logger.info(
        "Completato: files=%s processati=%s errori=%s files_with_removed=%s rows_removed_midnight=%s",
        stats["files_total"],
        stats["files_processed"],
        stats["files_error"],
        stats["files_with_removed"],
        stats["rows_removed_midnight"],
    )
    return 0
