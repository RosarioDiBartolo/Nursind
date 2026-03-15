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
        "Completed: employees=%s files=%s processed=%s errors=%s rows=%s classified=%s",
        stats["employees_total"],
        stats["files_total"],
        stats["files_processed"],
        stats["files_error"],
        stats["rows_total"],
        stats["rows_classified"],
    )
    return 0
