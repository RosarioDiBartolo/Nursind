from __future__ import annotations

import logging
from typing import Sequence

from cartellino_parser import PipelineClient
from cartellino_parser._cli_requests import request_from_object
from cartellino_parser.drive_service.logging_utils import setup_logging
from cartellino_parser.models import MissingTimbratureAuditRequest

from .options import parse_options

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    setup_logging(options.verbose)
    report = PipelineClient().audit_missing_timbrature(
        request_from_object(MissingTimbratureAuditRequest, options)
    )
    stats = report.stats
    outputs = report.outputs
    logger.info(
        (
            "Completed: employees=%s with_any_gaps=%s missing_text_layer=%s "
            "pages_missing_year_month=%s coverage_gaps=%s findings=%s"
        ),
        stats["employees_total"],
        stats["employees_with_any_gaps"],
        stats["missing_text_layer_files"],
        stats["pages_missing_year_month"],
        stats["coverage_gaps_total"],
        stats["findings_total"],
    )
    if outputs:
        logger.info(
            "Outputs: report=%s summary=%s findings=%s coverage=%s",
            outputs.get("report_json"),
            outputs.get("summary_csv"),
            outputs.get("findings_csv"),
            outputs.get("coverage_csv"),
        )
    return 0


__all__ = ["main"]
