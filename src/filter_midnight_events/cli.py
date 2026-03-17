from __future__ import annotations

import logging
from typing import Sequence

from cartellino_parser import PipelineClient
from cartellino_parser._cli_requests import request_from_object
from cartellino_parser.drive_service.logging_utils import setup_logging
from cartellino_parser.models import FilterMidnightRequest

from .options import parse_options

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = PipelineClient().filter_midnight(request_from_object(FilterMidnightRequest, options))
    stats = report.stats
    logger.info(
        "Completato: files=%s processati=%s errori=%s files_with_removed=%s rows_removed_midnight=%s",
        stats["files_total"],
        stats["files_processed"],
        stats["files_error"],
        stats["files_with_removed"],
        stats["rows_removed_midnight"],
    )
    return 0
