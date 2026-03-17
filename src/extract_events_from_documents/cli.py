from __future__ import annotations

import logging
from typing import Sequence

from cartellino_parser import PipelineClient
from cartellino_parser._cli_requests import request_from_object
from cartellino_parser.drive_service.logging_utils import setup_logging
from cartellino_parser.models import ExtractEventsRequest

from .options import parse_options

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = PipelineClient().extract_events(request_from_object(ExtractEventsRequest, options))
    stats = report.stats
    logger.info(
        (
            "Completed: files=%s processed=%s errors=%s "
            "files_with_events=%s events=%s rows_with_events=%s"
        ),
        stats["files_total"],
        stats["files_processed"],
        stats["files_error"],
        stats["files_with_events"],
        stats["events_extracted"],
        stats["rows_with_events"],
    )
    return 0
