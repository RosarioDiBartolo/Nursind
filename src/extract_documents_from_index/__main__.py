from __future__ import annotations

from typing import Sequence

from cartellino_parser import PipelineClient
from cartellino_parser._cli_requests import request_from_object
from cartellino_parser.drive_service.logging_utils import get_logger, setup_logging
from cartellino_parser.models import ExtractDocumentsRequest

from .options import ExtractDocumentsFromIndexOptions, build_parser, parse_options

logger = get_logger()


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = PipelineClient().extract_documents(
        request_from_object(ExtractDocumentsRequest, options)
    )
    stats = report.stats
    logger.info(
        "Completed: files=%s processed=%s errors=%s",
        stats.get("files_total", 0),
        stats.get("files_processed", 0),
        stats.get("files_error", 0),
    )
    return 0


__all__ = [
    "ExtractDocumentsFromIndexOptions",
    "build_parser",
    "parse_options",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
