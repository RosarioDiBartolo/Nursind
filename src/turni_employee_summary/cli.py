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
        "Completato: dipendenti=%s file_totali=%s mancanti=%s errori=%s righe=%s classificate=%s",
        stats["dipendenti"],
        stats["file_totali"],
        stats["file_mancanti"],
        stats["file_errori"],
        stats["righe_totali"],
        stats["righe_classificate"],
    )
    return 0
