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
    outputs = report.get("outputs") or {}
    logger.info(
        (
            "Completed: pipelines=%s pages=%s suspicious=%s tiny=%s zero=%s low=%s "
            "missing_year_month=%s likely_legitimate=%s"
        ),
        stats["pipelines_total"],
        stats["pages_total"],
        stats["suspicious_pages_total"],
        stats["tiny_page_total"],
        stats["zero_event_page_total"],
        stats["low_coverage_page_total"],
        stats["missing_year_month_total"],
        stats["likely_legitimate_no_events_total"],
    )
    if outputs:
        logger.info(
            "Outputs: report=%s suspicious_csv=%s",
            outputs.get("report_json"),
            outputs.get("suspicious_csv"),
        )
    return 0


__all__ = ["main"]
