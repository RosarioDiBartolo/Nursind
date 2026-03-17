from __future__ import annotations

import logging
from typing import Sequence

from cartellino_parser import PipelineClient
from cartellino_parser._cli_requests import request_from_object
from cartellino_parser.drive_service.logging_utils import setup_logging
from cartellino_parser.models import TurniEmployeeSummaryRequest

from .options import parse_options

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = PipelineClient().build_summary(
        request_from_object(TurniEmployeeSummaryRequest, options)
    )
    stats = report.stats
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
